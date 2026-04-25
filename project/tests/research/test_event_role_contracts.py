from __future__ import annotations

from project.domain.hypotheses import HypothesisSpec, TriggerSpec
from project.research.search.validation import validate_hypothesis_spec


def test_context_event_cannot_be_standalone_alpha_hypothesis() -> None:
    spec = HypothesisSpec(
        trigger=TriggerSpec.event("FUNDING_TIMESTAMP_EVENT"),
        direction="long",
        horizon="15m",
        template_id="mean_reversion",
        entry_lag=1,
    )

    errors = validate_hypothesis_spec(spec)

    assert any("role contract blocks standalone alpha" in error for error in errors)


def test_trade_trigger_event_can_be_standalone_when_other_contracts_pass() -> None:
    spec = HypothesisSpec(
        trigger=TriggerSpec.event("BASIS_DISLOC"),
        direction="long",
        horizon="15m",
        template_id="basis_repair",
        entry_lag=1,
    )

    errors = validate_hypothesis_spec(spec)

    assert not errors
