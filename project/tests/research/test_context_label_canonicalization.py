from __future__ import annotations

import pandas as pd

from project.domain.hypotheses import HypothesisSpec, TriggerSpec
from project.research.context_labels import expand_dimension_values
from project.research.search.evaluator_utils import context_mask
from project.research.search.feasibility import check_hypothesis_feasibility
from project.research.search.validation import validate_hypothesis_spec


def test_context_mask_accepts_legacy_carry_state_aliases() -> None:
    features = pd.DataFrame(
        {
            "funding_positive": [1.0, 0.0],
            "funding_negative": [0.0, 1.0],
        }
    )

    positive_mask = context_mask({"carry_state": "positive"}, features, use_context_quality=False)
    negative_mask = context_mask({"carry_state": "negative"}, features, use_context_quality=False)

    assert positive_mask is not None
    assert negative_mask is not None
    assert positive_mask.tolist() == [True, False]
    assert negative_mask.tolist() == [False, True]


def test_expand_dimension_values_maps_market_state_labels_to_stored_codes() -> None:
    assert expand_dimension_values("ms_trend_state", ["bullish"]) == [
        1.0,
        1,
        "1",
        "1.0",
        "bullish",
    ]
    assert expand_dimension_values("ms_spread_state", ["wide"]) == [
        1.0,
        1,
        "1",
        "1.0",
        "wide",
    ]


def test_context_mask_matches_dimension_encoded_market_states() -> None:
    features = pd.DataFrame(
        {
            "ms_trend_state": [0.0, 1.0, 2.0],
            "ms_spread_state": [0.0, 1.0, 0.0],
        }
    )

    bullish_mask = context_mask({"ms_trend_state": "bullish"}, features, use_context_quality=False)
    bearish_mask = context_mask({"ms_trend_state": "bearish"}, features, use_context_quality=False)
    wide_mask = context_mask({"ms_spread_state": "wide"}, features, use_context_quality=False)

    assert bullish_mask is not None
    assert bearish_mask is not None
    assert wide_mask is not None
    assert bullish_mask.tolist() == [False, True, False]
    assert bearish_mask.tolist() == [False, False, True]
    assert wide_mask.tolist() == [False, True, False]


def test_context_mask_falls_back_to_state_columns_when_dimension_column_is_absent() -> None:
    features = pd.DataFrame({"crowding_state": [0.0, 1.0]})

    crowded_mask = context_mask({"funding_regime": "crowded"}, features, use_context_quality=False)

    assert crowded_mask is not None
    assert crowded_mask.tolist() == [False, True]


def test_validate_hypothesis_spec_uses_authoritative_context_registry() -> None:
    valid = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="24b",
        template_id="continuation",
        entry_lag=1,
        context={"ms_trend_state": "bullish"},
    )
    invalid = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="24b",
        template_id="continuation",
        entry_lag=1,
        context={"ms_trend_state": "trend"},
    )

    assert not any("Context label" in err for err in validate_hypothesis_spec(valid))
    assert any("Context label 'trend'" in err for err in validate_hypothesis_spec(invalid))


def test_check_hypothesis_feasibility_accepts_context_dimension_columns() -> None:
    features = pd.DataFrame({"ms_trend_state": [0.0, 1.0, 2.0], "ms_spread_state": [0.0, 1.0, 0.0]})
    trend_spec = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="24b",
        template_id="continuation",
        entry_lag=1,
        context={"ms_trend_state": "bullish"},
    )
    spread_spec = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="24b",
        template_id="continuation",
        entry_lag=1,
        context={"ms_spread_state": "wide"},
    )

    assert "missing_context_state_column" not in check_hypothesis_feasibility(trend_spec, features=features).reasons
    assert "missing_context_state_column" not in check_hypothesis_feasibility(spread_spec, features=features).reasons


def test_check_hypothesis_feasibility_without_features_accepts_valid_context_labels() -> None:
    spec = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="24b",
        template_id="continuation",
        entry_lag=1,
        context={"ms_trend_state": "bullish", "ms_spread_state": "wide"},
    )

    reasons = check_hypothesis_feasibility(spec).reasons

    assert "unknown_context_mapping" not in reasons
