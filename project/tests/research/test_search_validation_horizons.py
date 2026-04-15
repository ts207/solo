from project.domain.hypotheses import HypothesisSpec, TriggerSpec
from project.research.search.validation import validate_hypothesis_spec


def _base_spec(horizon: str) -> HypothesisSpec:
    return HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon=horizon,
        template_id="continuation",
        entry_lag=1,
    )


def test_validate_hypothesis_spec_accepts_arbitrary_bar_count_horizons():
    spec = _base_spec("72b")
    errors = validate_hypothesis_spec(spec)
    assert not any("Invalid horizon" in err for err in errors)


def test_validate_hypothesis_spec_rejects_unparseable_horizons():
    spec = _base_spec("later")
    errors = validate_hypothesis_spec(spec)
    assert any("Invalid horizon" in err for err in errors)
