"""
Sprint 7 Phase 1 adversarial tests.

Covers:
  - DeploymentGate contract enforcement (Workstream A)
  - Per-thesis/symbol/family kill switch granularity (Workstream H)
  - Kill switch persistence across restart (Workstream H)
  - Audit log durability and lineage (Workstream E)
  - Per-thesis cap enforcement and daily loss limits (Workstream D)
  - ThesisStore gate on live_enabled theses (Workstream A integration)
"""

from __future__ import annotations

import json

import pytest

from project.live.audit_log import (
    AuditLog,
    FillEvent,
    OperatorActionEvent,
    OrderIntentEvent,
)
from project.live.contracts.deployment_approval import (
    ChecklistItem,
    DeploymentApprovalRecord,
    PaperRunMetrics,
    create_approval_record,
)
from project.live.contracts.promoted_thesis import (
    LIVE_TRADEABLE_STATES,
    LiveApproval,
    PromotedThesis,
    ThesisCapProfile,
    ThesisEvidence,
    ThesisLineage,
)
from project.live.deployment import DeploymentGate, check_thesis
from project.live.kill_switch import KillSwitchManager
from project.live.risk import DailyLossLedger, PerThesisCap, RiskEnforcer, RuntimeRiskCaps
from project.live.state import LiveStateStore

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_thesis(
    thesis_id: str = "t1",
    deployment_state: str = "paper_only",
    deployment_mode_allowed: str = "paper_only",
    live_approval: LiveApproval | None = None,
    cap_profile: ThesisCapProfile | None = None,
) -> PromotedThesis:
    return PromotedThesis(
        thesis_id=thesis_id,
        timeframe="5m",
        primary_event_id="VOL_SHOCK",
        evidence=ThesisEvidence(sample_size=100),
        lineage=ThesisLineage(run_id="run1", candidate_id="c1"),
        deployment_state=deployment_state,
        deployment_mode_allowed=deployment_mode_allowed,
        live_approval=live_approval or LiveApproval(),
        cap_profile=cap_profile or ThesisCapProfile(),
    )


def _approved_thesis(thesis_id: str = "t1") -> PromotedThesis:
    return _make_thesis(
        thesis_id=thesis_id,
        deployment_state="live_enabled",
        deployment_mode_allowed="live_enabled",
        live_approval=LiveApproval(
            live_approval_status="approved",
            approved_by="irene",
            approved_at="2026-04-04T10:00:00+00:00",
            risk_profile_id="rp_001",
            paper_run_min_days_required=0,
        ),
        cap_profile=ThesisCapProfile(max_notional=5000.0, max_daily_loss=200.0),
    )


# ---------------------------------------------------------------------------
# Workstream A — DeploymentGate
# ---------------------------------------------------------------------------


class TestDeploymentGate:
    def test_paper_only_thesis_passes_gate_without_approval(self):
        thesis = _make_thesis(deployment_state="paper_only")
        violations = check_thesis(thesis)
        assert violations == []

    def test_monitor_only_thesis_passes_gate(self):
        thesis = _make_thesis(deployment_state="monitor_only")
        assert check_thesis(thesis) == []

    def test_live_enabled_thesis_without_approval_blocked(self):
        thesis = _make_thesis(deployment_state="live_enabled")
        violations = check_thesis(thesis)
        assert any("live_approval_status" in v for v in violations)
        assert any("approved_by" in v for v in violations)
        assert any("approved_at" in v for v in violations)

    def test_live_enabled_thesis_missing_cap_profile_blocked(self):
        thesis = _make_thesis(
            deployment_state="live_enabled",
            deployment_mode_allowed="live_enabled",
            live_approval=LiveApproval(
                live_approval_status="approved",
                approved_by="irene",
                approved_at="2026-04-04T10:00:00+00:00",
                risk_profile_id="rp_001",
            ),
            cap_profile=ThesisCapProfile(),  # all zeros — not configured
        )
        violations = check_thesis(thesis)
        assert any("cap_profile" in v for v in violations)

    def test_live_enabled_thesis_with_mode_not_allowed_blocked(self):
        thesis = _make_thesis(
            deployment_state="live_enabled",
            deployment_mode_allowed="paper_only",  # ceiling too low
            live_approval=LiveApproval(
                live_approval_status="approved",
                approved_by="irene",
                approved_at="2026-04-04T10:00:00+00:00",
                risk_profile_id="rp_001",
            ),
            cap_profile=ThesisCapProfile(max_notional=5000.0),
        )
        violations = check_thesis(thesis)
        assert any("deployment_mode_allowed" in v for v in violations)

    def test_fully_approved_live_thesis_passes_gate(self):
        thesis = _approved_thesis()
        violations = check_thesis(thesis)
        assert violations == [], violations

    def test_paper_insufficient_quality_blocked(self):
        thesis = _make_thesis(
            deployment_state="live_enabled",
            deployment_mode_allowed="live_enabled",
            live_approval=LiveApproval(
                live_approval_status="approved",
                approved_by="irene",
                approved_at="2026-04-04T10:00:00+00:00",
                risk_profile_id="rp_001",
                paper_run_quality_status="insufficient",
            ),
            cap_profile=ThesisCapProfile(max_notional=5000.0),
        )
        violations = check_thesis(thesis)
        assert any("insufficient" in v for v in violations)

    def test_paper_duration_not_satisfied_blocked(self):
        thesis = _make_thesis(
            deployment_state="live_enabled",
            deployment_mode_allowed="live_enabled",
            live_approval=LiveApproval(
                live_approval_status="approved",
                approved_by="irene",
                approved_at="2026-04-04T10:00:00+00:00",
                risk_profile_id="rp_001",
                paper_run_min_days_required=14,
                paper_run_observed_days=7,  # only 7 of 14 required
            ),
            cap_profile=ThesisCapProfile(max_notional=5000.0),
        )
        violations = check_thesis(thesis)
        assert any("duration" in v for v in violations)

    def test_gate_batch_raises_in_strict_mode(self):
        bad = _make_thesis(deployment_state="live_enabled")
        gate = DeploymentGate(strict=True)
        with pytest.raises(RuntimeError, match="DeploymentGate blocked"):
            gate.validate_batch([bad])

    def test_gate_batch_non_strict_returns_rejections(self):
        bad = _make_thesis(deployment_state="live_enabled")
        gate = DeploymentGate(strict=False)
        rejections = gate.check_batch([bad])
        assert len(rejections) == 1
        assert "live_approval_status" in rejections[0].reasons[0]

    def test_filter_tradeable_excludes_unapproved(self):
        approved = _approved_thesis("t_approved")
        unapproved = _make_thesis("t_unapproved", deployment_state="live_enabled")
        paper = _make_thesis("t_paper", deployment_state="paper_only")
        gate = DeploymentGate()
        tradeable = gate.filter_tradeable([approved, unapproved, paper])
        assert len(tradeable) == 1
        assert tradeable[0].thesis_id == "t_approved"


# ---------------------------------------------------------------------------
# Workstream A — DeploymentApprovalRecord
# ---------------------------------------------------------------------------


class TestDeploymentApprovalRecord:
    def test_approved_record_validates(self):
        cap = ThesisCapProfile(max_notional=5000.0, max_daily_loss=200.0)
        record = create_approval_record(
            thesis_id="t1",
            approved_by="irene",
            risk_profile_id="rp_001",
            cap_profile=cap,
            paper_metrics=PaperRunMetrics(days_observed=21, fill_count=45),
            checklist=[ChecklistItem(name="monitoring_visible", passed=True)],
        )
        assert record.is_valid_for_live
        assert record.status == "approved"
        assert record.failed_checklist_items == []

    def test_approved_record_requires_approved_by(self):
        cap = ThesisCapProfile(max_notional=5000.0)
        with pytest.raises(Exception):
            DeploymentApprovalRecord(
                thesis_id="t1",
                status="approved",
                approved_by="",  # missing
                approved_at="2026-04-04T10:00:00+00:00",
                risk_profile_id="rp_001",
                cap_profile=cap,
            )

    def test_record_roundtrip_via_file(self, tmp_path):
        cap = ThesisCapProfile(max_notional=5000.0)
        record = create_approval_record(
            thesis_id="t1",
            approved_by="irene",
            risk_profile_id="rp_001",
            cap_profile=cap,
            paper_metrics=PaperRunMetrics(days_observed=21),
            checklist=[],
        )
        path = tmp_path / "approval.json"
        record.save(path)
        loaded = DeploymentApprovalRecord.from_file(path)
        assert loaded.thesis_id == "t1"
        assert loaded.is_valid_for_live
        assert loaded.record_id == record.record_id


# ---------------------------------------------------------------------------
# Workstream H — Per-entity kill switch
# ---------------------------------------------------------------------------


class TestKillSwitchGranularity:
    def _make_km(self, snap_path: str | None = None) -> tuple[LiveStateStore, KillSwitchManager]:
        store = LiveStateStore(snapshot_path=snap_path)
        km = KillSwitchManager(store)
        return store, km

    def test_per_thesis_disable_blocks_thesis(self):
        _, km = self._make_km()
        km.disable_thesis("t1", reason="test")
        blocked, reason = km.is_thesis_blocked("t1", "BTCUSDT")
        assert blocked
        assert "thesis_disabled" in reason

    def test_per_symbol_disable_blocks_any_thesis_on_that_symbol(self):
        _, km = self._make_km()
        km.disable_symbol("BTCUSDT", reason="test")
        # Different thesis, same symbol
        blocked, reason = km.is_thesis_blocked("t_other", "BTCUSDT")
        assert blocked
        assert "symbol_disabled" in reason

    def test_per_family_disable_blocks_family(self):
        _, km = self._make_km()
        km.disable_family("VOL_SHOCK", reason="test")
        blocked, reason = km.is_thesis_blocked("t1", "ETHUSDT", "VOL_SHOCK")
        assert blocked
        assert "family_disabled" in reason

    def test_unrelated_thesis_not_blocked(self):
        _, km = self._make_km()
        km.disable_thesis("t1")
        blocked, _ = km.is_thesis_blocked("t2", "BTCUSDT", "VOL_SHOCK")
        assert not blocked

    def test_global_kill_outranks_per_thesis(self):
        from project.live.kill_switch import KillSwitchReason

        _, km = self._make_km()
        km.trigger(KillSwitchReason.MANUAL, "test global")
        # Even a thesis that is NOT individually disabled is blocked
        blocked, reason = km.is_thesis_blocked("t_clean", "ETHUSDT")
        assert blocked
        assert "global_kill" in reason.lower()

    def test_resume_thesis_re_enables(self):
        _, km = self._make_km()
        km.disable_thesis("t1")
        blocked, _ = km.is_thesis_blocked("t1", "BTCUSDT")
        assert blocked
        km.resume_thesis("t1")
        blocked2, _ = km.is_thesis_blocked("t1", "BTCUSDT")
        assert not blocked2

    def test_kill_state_persists_across_restart(self, tmp_path):
        snap = str(tmp_path / "snap.json")
        store, km = self._make_km(snap_path=snap)
        km.disable_thesis("t1", reason="persisted_disable")
        km.disable_symbol("BTCUSDT", reason="symbol_disable")

        # Reload
        store2 = LiveStateStore.load_snapshot(snap)
        km2 = KillSwitchManager(store2)
        blocked_t, _ = km2.is_thesis_blocked("t1", "ETHUSDT")
        blocked_s, _ = km2.is_thesis_blocked("t2", "BTCUSDT")
        assert blocked_t, "per-thesis disable must survive restart"
        assert blocked_s, "per-symbol disable must survive restart"

    def test_per_thesis_disable_audit_event_emitted(self, tmp_path):
        log = AuditLog(str(tmp_path / "audit.jsonl"))
        store = LiveStateStore()
        km = KillSwitchManager(store, audit_log=log)
        km.disable_thesis("t1", reason="cap_breach", operator="system")
        events = log.load_all()
        assert any(e["action"] == "thesis_disabled" and "t1" in e["scope"] for e in events)


# ---------------------------------------------------------------------------
# Workstream D — Per-thesis caps and daily loss
# ---------------------------------------------------------------------------


class TestRiskEnforcerExtended:
    def _make_enforcer(self, **kwargs) -> RiskEnforcer:
        caps = RuntimeRiskCaps(**kwargs)
        return RiskEnforcer(caps)

    def test_per_thesis_notional_cap_rejects(self):
        enforcer = self._make_enforcer(
            per_thesis={"t1": PerThesisCap(thesis_id="t1", max_notional=3000.0)}
        )
        notional, breach = enforcer.check_and_apply_caps(
            thesis_id="t1",
            symbol="BTCUSDT",
            family="VOL_SHOCK",
            attempted_notional=4000.0,
            portfolio_state={},
            active_thesis_ids=[],
            timestamp="2026-04-04T10:00:00",
        )
        assert notional == 0.0
        assert breach is not None
        assert breach.cap_type == "per_thesis_notional"

    def test_global_order_notional_ceiling_rejects(self):
        enforcer = self._make_enforcer(max_order_notional=5000.0)
        notional, breach = enforcer.check_and_apply_caps(
            thesis_id="t1",
            symbol="BTCUSDT",
            family="VOL_SHOCK",
            attempted_notional=8000.0,
            portfolio_state={},
            active_thesis_ids=[],
            timestamp="2026-04-04T10:00:00",
        )
        assert notional == 0.0
        assert breach.cap_type == "order_notional"

    def test_per_thesis_daily_loss_rejects_after_breach(self):
        enforcer = self._make_enforcer(
            per_thesis={"t1": PerThesisCap(thesis_id="t1", max_daily_loss=100.0)}
        )
        enforcer.daily_loss.record_fill_pnl("t1", -150.0)  # 150 USD loss > 100 limit
        notional, breach = enforcer.check_and_apply_caps(
            thesis_id="t1",
            symbol="BTCUSDT",
            family="VOL_SHOCK",
            attempted_notional=1000.0,
            portfolio_state={},
            active_thesis_ids=["t1"],
            timestamp="2026-04-04T10:00:00",
        )
        assert notional == 0.0
        assert breach.cap_type == "per_thesis_daily_loss"

    def test_global_daily_loss_rejects(self):
        enforcer = self._make_enforcer(max_daily_loss=500.0)
        enforcer.daily_loss.record_fill_pnl("t1", -600.0)  # > global limit
        notional, breach = enforcer.check_and_apply_caps(
            thesis_id="t1",
            symbol="BTCUSDT",
            family="VOL_SHOCK",
            attempted_notional=1000.0,
            portfolio_state={},
            active_thesis_ids=["t1"],
            timestamp="2026-04-04T10:00:00",
        )
        assert notional == 0.0
        assert breach.cap_type == "global_daily_loss"

    def test_per_thesis_active_orders_blocked(self):
        enforcer = self._make_enforcer(
            per_thesis={"t1": PerThesisCap(thesis_id="t1", max_active_orders=2)}
        )
        notional, breach = enforcer.check_and_apply_caps(
            thesis_id="t1",
            symbol="BTCUSDT",
            family="VOL_SHOCK",
            attempted_notional=1000.0,
            portfolio_state={},
            active_thesis_ids=["t1"],
            timestamp="2026-04-04T10:00:00",
            active_order_count_by_thesis={"t1": 2},  # at limit
        )
        assert notional == 0.0
        assert breach.cap_type == "per_thesis_orders"

    def test_no_cap_breach_passes_through(self):
        enforcer = self._make_enforcer()
        notional, breach = enforcer.check_and_apply_caps(
            thesis_id="t1",
            symbol="BTCUSDT",
            family="VOL_SHOCK",
            attempted_notional=1000.0,
            portfolio_state={},
            active_thesis_ids=[],
            timestamp="2026-04-04T10:00:00",
        )
        assert notional == 1000.0
        assert breach is None

    def test_daily_loss_ledger_rolls_at_midnight(self):
        from datetime import date

        ledger = DailyLossLedger()
        ledger.record_fill_pnl("t1", -50.0)
        # Simulate day roll
        ledger._date = date(2026, 1, 1)  # stale date
        ledger.record_fill_pnl("t1", -10.0)  # triggers roll
        assert ledger.thesis_loss_today("t1") == 10.0  # only today's loss


# ---------------------------------------------------------------------------
# Workstream E — Audit log
# ---------------------------------------------------------------------------


class TestAuditLog:
    def test_append_and_load_all(self, tmp_path):
        log = AuditLog(tmp_path / "audit.jsonl")
        log.append(OrderIntentEvent(thesis_id="t1", client_order_id="coid1", symbol="BTCUSDT"))
        log.append(FillEvent(client_order_id="coid1", thesis_id="t1", symbol="BTCUSDT"))
        events = log.load_all()
        assert len(events) == 2
        assert events[0]["event_type"] == "order_intent"
        assert events[1]["event_type"] == "fill_event"

    def test_survives_restart(self, tmp_path):
        path = tmp_path / "audit.jsonl"
        log = AuditLog(path)
        log.append(OrderIntentEvent(thesis_id="t1", client_order_id="coid1"))
        log.append(FillEvent(client_order_id="coid1", thesis_id="t1"))
        # Reload
        log2 = AuditLog(path)
        events = log2.load_all()
        assert len(events) == 2

    def test_fill_lineage_reconstruction(self, tmp_path):
        log = AuditLog(tmp_path / "audit.jsonl")
        intent = OrderIntentEvent(
            thesis_id="t1",
            thesis_version="v2",
            promotion_run_id="run42",
            validation_run_id="val7",
            client_order_id="coid1",
            symbol="BTCUSDT",
        )
        log.append(intent)
        fill = FillEvent(
            client_order_id="coid1",
            intent_event_id=intent.event_id,
            thesis_id="t1",
            thesis_version="v2",
            promotion_run_id="run42",
            validation_run_id="val7",
            symbol="BTCUSDT",
            fill_price=80010.0,
        )
        log.append(fill)
        lineage = log.reconstruct_fill_lineage(fill.event_id)
        assert lineage["thesis_id"] == "t1"
        assert lineage["promotion_run_id"] == "run42"
        assert lineage["order_intent"]["client_order_id"] == "coid1"

    def test_corrupt_line_does_not_crash_load(self, tmp_path):
        path = tmp_path / "audit.jsonl"
        log = AuditLog(path)
        log.append(OrderIntentEvent(thesis_id="t1"))
        # Inject a corrupt line
        with open(path, "a") as f:
            f.write("NOT_JSON\n")
        log.append(FillEvent(thesis_id="t1"))
        events = log.load_all()
        assert len(events) == 2  # corrupt line is skipped

    def test_load_by_type(self, tmp_path):
        log = AuditLog(tmp_path / "audit.jsonl")
        log.append(OrderIntentEvent(thesis_id="t1"))
        log.append(FillEvent(thesis_id="t1"))
        log.append(OperatorActionEvent(action="disable_thesis", target="t1"))
        fills = log.load_by_type("fill_event")
        assert len(fills) == 1
        assert fills[0]["event_type"] == "fill_event"

    def test_load_for_thesis(self, tmp_path):
        log = AuditLog(tmp_path / "audit.jsonl")
        log.append(OrderIntentEvent(thesis_id="t1"))
        log.append(OrderIntentEvent(thesis_id="t2"))
        log.append(FillEvent(thesis_id="t1"))
        t1_events = log.load_for_thesis("t1")
        assert len(t1_events) == 2
        assert all(e["thesis_id"] == "t1" for e in t1_events)


# ---------------------------------------------------------------------------
# Stage-boundary tests: raw candidates cannot deploy, only live_enabled can trade
# ---------------------------------------------------------------------------


class TestDeploymentStateBoundaries:
    @pytest.mark.parametrize(
        "state",
        [
            "promoted",
            "paper_enabled",
            "paper_approved",
            "live_eligible",
            "live_paused",
            "live_disabled",
            "retired",
        ],
    )
    def test_non_live_enabled_not_in_tradeable_states(self, state: str):
        assert state not in LIVE_TRADEABLE_STATES

    def test_only_live_enabled_in_tradeable_states(self):
        assert "live_enabled" in LIVE_TRADEABLE_STATES
        assert len(LIVE_TRADEABLE_STATES) == 1

    def test_thesis_store_raises_on_live_enabled_without_approval(self, tmp_path):
        # Build a minimal thesis JSON with live_enabled but no approval
        bad_thesis = _make_thesis("t_bad", deployment_state="live_enabled")
        artifact = {
            "schema_version": "promoted_theses_v1",
            "run_id": "run1",
            "generated_at_utc": "2026-04-04T10:00:00Z",
            "thesis_count": 1,
            "active_thesis_count": 0,
            "pending_thesis_count": 1,
            "theses": [bad_thesis.model_dump(mode="json")],
        }
        path = tmp_path / "theses.json"
        path.write_text(json.dumps(artifact))

        from project.live.thesis_store import ThesisStore

        with pytest.raises(RuntimeError, match="DeploymentGate blocked"):
            ThesisStore.from_path(path, strict_live_gate=True)

    def test_thesis_store_accepts_live_enabled_with_full_approval(self, tmp_path):
        approved = _approved_thesis("t_ok")
        artifact = {
            "schema_version": "promoted_theses_v1",
            "run_id": "run1",
            "generated_at_utc": "2026-04-04T10:00:00Z",
            "thesis_count": 1,
            "active_thesis_count": 0,
            "pending_thesis_count": 1,
            "theses": [approved.model_dump(mode="json")],
        }
        path = tmp_path / "theses.json"
        path.write_text(json.dumps(artifact))

        from project.live.thesis_store import ThesisStore

        store = ThesisStore.from_path(path, strict_live_gate=True)
        assert store.all()[0].thesis_id == "t_ok"

    def test_thesis_store_non_strict_loads_despite_violations(self, tmp_path):
        bad_thesis = _make_thesis("t_bad", deployment_state="live_enabled")
        artifact = {
            "schema_version": "promoted_theses_v1",
            "run_id": "run1",
            "generated_at_utc": "2026-04-04T10:00:00Z",
            "thesis_count": 1,
            "active_thesis_count": 0,
            "pending_thesis_count": 1,
            "theses": [bad_thesis.model_dump(mode="json")],
        }
        path = tmp_path / "theses.json"
        path.write_text(json.dumps(artifact))

        from project.live.thesis_store import ThesisStore

        # strict=False: gate logs warnings but does not raise
        store = ThesisStore.from_path(path, strict_live_gate=False)
        assert len(store.all()) == 1
