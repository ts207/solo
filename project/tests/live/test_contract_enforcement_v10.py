from __future__ import annotations

from datetime import UTC, datetime, timedelta

from project.live.contracts.promoted_thesis import RuntimeThesisManifest
from project.live.runtime_admission import validate_runtime_manifest
from project.portfolio.admission_policy import ConflictResolution, build_admission_trace
from project.research.promotion.promotion_decisions import (
    _data_quality_promotion_block,
    _semantic_promotion_gate,
)
from project.research.search.compatibility import event_template_compatibility_verdict
from project.domain.hypotheses import HypothesisSpec, TriggerSpec, TriggerType


def test_semantic_gate_blocks_stale_data_quality() -> None:
    verdict = _semantic_promotion_gate(
        {
            "template_id": "basis_convergence",
            "direction": "long",
            "compatibility_status": "allowed",
            "compatibility_promotion_allowed": True,
            "mechanism_label": "basis_convergence",
            "mechanism_valid": True,
            "mechanism_success_rate": 0.60,
            "data_quality_state": "stale",
            "anchor_role": "alpha_anchor",
            "context_timing": "trigger",
        }
    )
    assert verdict["semantic_pass"] is False
    assert "data_quality_stale" in verdict["semantic_reasons"]


def test_runtime_manifest_required_rejects_default_empty_manifest() -> None:
    class Thesis:
        thesis_id = "t1"
        deployment_state = "paper_enabled"
        runtime_manifest = RuntimeThesisManifest()

    try:
        validate_runtime_manifest(Thesis(), "simulation", require_manifest=True)
    except ValueError as exc:
        assert "runtime_manifest required" in str(exc)
    else:
        raise AssertionError("expected manifest rejection")


def test_runtime_manifest_expiry_rejects() -> None:
    class Thesis:
        thesis_id = "t1"
        deployment_state = "paper_enabled"
        runtime_manifest = RuntimeThesisManifest(
            thesis_id="t1",
            promotion_state="paper_enabled",
            allowed_runtime_modes=["simulation"],
            expires_at_utc=(datetime.now(UTC) - timedelta(days=1)).isoformat(),
        )

    try:
        validate_runtime_manifest(Thesis(), "simulation", require_manifest=True)
    except ValueError as exc:
        assert "expired" in str(exc)
    else:
        raise AssertionError("expected expiry rejection")


def test_compatibility_blocks_guard_data_quality() -> None:
    spec = HypothesisSpec(
        trigger=TriggerSpec(trigger_type=TriggerType.EVENT, event_id="BAND_BREAK"),
        direction="long",
        horizon="12",
        template_id="overshoot_repair",
        context={"data_quality_state": "missing_required_feature"},
    )
    verdict = event_template_compatibility_verdict(spec)
    assert verdict.promotion_allowed is False
    assert verdict.paper_allowed is False
    assert verdict.reason_codes[0] == "data_quality_missing_required_feature"


def test_portfolio_admission_trace_contains_audit_fields() -> None:
    resolution = ConflictResolution(
        decision="reject",
        reason="guard_veto_present",
        accepted_ids=[],
        rejected_ids=["t1"],
        size_scalars={},
    )
    rows = build_admission_trace(
        [{"thesis_id": "t1", "symbol": "BTCUSDT", "event_id": "SPREAD_BLOWOUT", "anchor_role": "execution_guard"}],
        resolution,
    )
    assert rows[0]["decision"] == "reject"
    assert rows[0]["decision_reason"] == "guard_veto_present"
    assert rows[0]["event_id"] == "SPREAD_BLOWOUT"
