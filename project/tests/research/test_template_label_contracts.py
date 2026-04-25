from __future__ import annotations

from project.domain.hypotheses import HypothesisSpec, TriggerSpec
from project.research.search.label_contracts import validate_template_label_contract


def test_basis_repair_uses_basis_specific_primary_label() -> None:
    spec = HypothesisSpec(
        trigger=TriggerSpec.event("BASIS_DISLOC"),
        direction="long",
        horizon="15m",
        template_id="basis_repair",
        entry_lag=1,
    )

    assert validate_template_label_contract(spec) == []


def test_liquidity_repair_uses_liquidity_primary_label() -> None:
    spec = HypothesisSpec(
        trigger=TriggerSpec.event("LIQUIDITY_VACUUM"),
        direction="long",
        horizon="15m",
        template_id="liquidity_refill_repair",
        context={"liquidity_phase": "refill"},
        entry_lag=1,
    )

    assert validate_template_label_contract(spec) == []
