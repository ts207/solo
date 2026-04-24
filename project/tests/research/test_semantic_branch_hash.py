from __future__ import annotations

from project.domain.hypotheses import HypothesisSpec, TriggerSpec


def test_semantic_branch_hash_collapses_template_aliases_with_same_trade_semantics() -> None:
    trigger = TriggerSpec.event("VOL_SHOCK")
    base = HypothesisSpec(
        trigger=trigger,
        direction="long",
        horizon="15m",
        template_id="continuation",
        entry_lag=1,
    )
    alias = HypothesisSpec(
        trigger=trigger,
        direction="long",
        horizon="15m",
        template_id="trend_continuation",
        entry_lag=1,
    )

    assert base.hypothesis_id() != alias.hypothesis_id()
    assert base.semantic_branch_hash() == alias.semantic_branch_hash()
