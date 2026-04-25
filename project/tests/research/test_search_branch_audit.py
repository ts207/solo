from __future__ import annotations

from project.domain.hypotheses import HypothesisSpec, TriggerSpec
from project.research.search.stage_models import CandidateHypothesis


def test_candidate_record_exposes_semantic_branch_hash_and_key() -> None:
    spec = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="15m",
        template_id="continuation",
        entry_lag=1,
    )

    row = CandidateHypothesis(spec=spec, search_spec_name="unit").to_record()

    assert row["branch_hash"] == spec.semantic_branch_hash()
    assert row["branch_key"] == spec.semantic_branch_key()


def test_semantic_branch_hash_is_template_name_independent_for_aliases() -> None:
    base = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="15m",
        template_id="continuation",
        entry_lag=1,
    )
    alias = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="15m",
        template_id="trend_continuation",
        entry_lag=1,
    )

    assert base.semantic_branch_key() == alias.semantic_branch_key()
