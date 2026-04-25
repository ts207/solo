from __future__ import annotations

from project.domain.hypotheses import HypothesisSpec, TriggerSpec
from project.research.search.validation import validate_hypothesis_spec


def test_event_template_matrix_blocks_generic_funding_continuation() -> None:
    spec = HypothesisSpec(
        trigger=TriggerSpec.event("FUNDING_PERSISTENCE_TRIGGER"),
        direction="long",
        horizon="15m",
        template_id="continuation",
        entry_lag=1,
    )

    errors = validate_hypothesis_spec(spec)

    assert any("compatibility forbids" in error for error in errors)


def test_event_template_matrix_requires_context_for_forced_flow_rebound() -> None:
    spec = HypothesisSpec(
        trigger=TriggerSpec.event("LIQUIDATION_CASCADE"),
        direction="long",
        horizon="15m",
        template_id="forced_flow_rebound",
        entry_lag=1,
    )

    errors = validate_hypothesis_spec(spec)

    assert any("requires context" in error for error in errors)


def test_event_template_matrix_accepts_required_contexts() -> None:
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


def test_oi_spike_positive_requires_price_oi_quadrant_for_continuation() -> None:
    spec = HypothesisSpec(
        trigger=TriggerSpec.event("OI_SPIKE_POSITIVE"),
        direction="long",
        horizon="60m",
        template_id="carry_continuation_confirmed",
        context={"oi_phase": "expansion", "liquidity_phase": "normal"},
        entry_lag=1,
    )

    errors = validate_hypothesis_spec(spec)

    assert any("price_oi_quadrant" in error for error in errors)


def test_oi_spike_positive_accepts_quadrant_context_for_confirmed_continuation() -> None:
    spec = HypothesisSpec(
        trigger=TriggerSpec.event("OI_SPIKE_POSITIVE"),
        direction="long",
        horizon="60m",
        template_id="carry_continuation_confirmed",
        context={"oi_phase": "expansion", "price_oi_quadrant": "price_up_oi_up"},
        entry_lag=1,
    )

    errors = validate_hypothesis_spec(spec)

    assert not errors
