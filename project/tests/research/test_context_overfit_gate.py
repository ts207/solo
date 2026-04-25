from __future__ import annotations

from project.domain.hypotheses import HypothesisSpec, TriggerSpec
from project.research.search.validation import validate_hypothesis_spec


def test_context_overfit_gate_rejects_three_dimensional_discovery_context() -> None:
    spec = HypothesisSpec(
        trigger=TriggerSpec.event("LIQUIDATION_CASCADE"),
        direction="long",
        horizon="15m",
        template_id="forced_flow_rebound",
        context={
            "forced_flow_phase": "cooldown",
            "liquidity_phase": "refill",
            "execution_friction": "normal",
        },
        entry_lag=1,
    )

    errors = validate_hypothesis_spec(spec)

    assert any("context-overfit gate rejects 3 context dimensions" in error for error in errors)


def test_context_overfit_gate_accepts_pre_registered_two_dimensional_context() -> None:
    spec = HypothesisSpec(
        trigger=TriggerSpec.event("LIQUIDATION_CASCADE"),
        direction="long",
        horizon="15m",
        template_id="forced_flow_rebound",
        context={"forced_flow_phase": "cooldown", "liquidity_phase": "refill"},
        entry_lag=1,
    )

    errors = validate_hypothesis_spec(spec)

    assert not errors
