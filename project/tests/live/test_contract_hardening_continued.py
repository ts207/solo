from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from project.live.contracts.promoted_thesis import (
    LiveApproval,
    PromotedThesis,
    RuntimeThesisManifest,
    ThesisCapProfile,
    ThesisEvidence,
    ThesisLineage,
)
from project.live.risk import RiskEnforcer, RuntimeRiskCaps
from project.live.runtime_admission import validate_runtime_mode_against_theses
from project.research.promotion.promotion_eligibility import cost_survival_ratio
from project.events.event_output_schema import normalize_event_output_frame


def _base_thesis(**kwargs):
    payload = dict(
        thesis_id="t1",
        timeframe="5m",
        primary_event_id="VOL_SHOCK",
        evidence=ThesisEvidence(sample_size=10),
        lineage=ThesisLineage(run_id="r", candidate_id="c"),
    )
    payload.update(kwargs)
    return PromotedThesis(**payload)


def test_live_state_parses_but_runtime_rejects_without_manifest_hashes() -> None:
    thesis = _base_thesis(deployment_state="live_enabled")
    with pytest.raises(ValueError, match="runtime_manifest missing required hashes"):
        validate_runtime_mode_against_theses("trading", [thesis])


def test_live_state_accepts_complete_manifest_approval_and_caps() -> None:
    manifest = RuntimeThesisManifest(
        thesis_id="t1",
        promotion_state="live_enabled",
        expires_at_utc=(datetime.now(UTC) + timedelta(days=1)).isoformat(),
        allowed_runtime_modes=["trading"],
        event_contract_hash="e",
        template_contract_hash="t",
        domain_graph_hash="d",
        evidence_bundle_hash="b",
        risk_contract_hash="r",
    )
    thesis = _base_thesis(
        deployment_state="live_enabled",
        runtime_manifest=manifest,
        live_approval=LiveApproval(live_approval_status="approved"),
        cap_profile=ThesisCapProfile(max_notional=1.0),
    )
    validate_runtime_mode_against_theses("trading", [thesis])


def test_zero_global_risk_caps_do_not_block_specific_family_cap() -> None:
    enforcer = RiskEnforcer(RuntimeRiskCaps(per_family_caps={"VOL": 100.0}))
    notional, breach = enforcer.check_and_apply_caps(
        thesis_id="t1",
        symbol="BTCUSDT",
        attempted_notional=50.0,
        family="VOL",
        timestamp="2026-01-01T00:00:00Z",
        active_thesis_ids=[],
        portfolio_state={"family_exposures": {"VOL": 25.0}, "gross_exposure": 0.0, "symbol_exposures": {}},
    )
    assert notional == 50.0
    assert breach is None


def test_cost_survival_prefers_explicit_stress_metrics() -> None:
    row = {
        "net_mean_bps_cost_1x": 2.0,
        "net_mean_bps_cost_1_5x": 1.0,
        "net_mean_bps_cost_2x": -0.1,
        "net_mean_bps_cost_3x": -1.0,
        "gate_after_cost_positive": "pass",
    }
    assert cost_survival_ratio(row) == 0.5


def test_event_output_normalizes_numeric_event_direction_to_side() -> None:
    df = pd.DataFrame(
        [{
            "event_name": "VOL_SHOCK",
            "event_version": "v2",
            "symbol": "BTCUSDT",
            "timeframe": "5m",
            "ts_start": "2026-01-01T00:00:00Z",
            "ts_end": "2026-01-01T00:00:00Z",
            "phase": "onset",
            "family": "VOLATILITY_TRANSITION",
            "subtype": "vol_shock",
            "evidence_mode": "direct",
            "severity": 0.5,
            "confidence": 0.8,
            "event_direction": -1,
            "trigger_value": 1.2,
            "threshold_snapshot": {},
            "required_context_present": True,
            "data_quality_flag": "ok",
            "merge_key": "BTCUSDT:VOL_SHOCK",
            "cooldown_until": None,
            "source_features": {},
            "detector_metadata": {},
        }]
    )
    out = normalize_event_output_frame(df)
    assert out.loc[0, "event_side"] == "bearish"
    assert int(out.loc[0, "event_direction"]) == -1
