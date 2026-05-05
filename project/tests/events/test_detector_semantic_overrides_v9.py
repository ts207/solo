from __future__ import annotations

import pandas as pd

from project.events.detectors.dislocation_base import BasisDislocationDetectorV2
from project.events.detectors.registry import get_detector_class
from project.events.detectors.trend import RangeBreakoutDetector, PullbackPivotDetector
from project.events.detectors.positioning_base import (
    FundingPosExtremeOnsetDetectorV2,
    FundingNegExtremeOnsetDetectorV2,
    PriceDownOIDownDetectorV2,
    PriceUpOIDownDetectorV2,
)
from project.events.detectors.liquidity_base import LiquidityVacuumDetectorV2, DepthCollapseDetectorV2


def _ts(n: int) -> pd.Series:
    return pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC").to_series(index=range(n))


def test_variant_detector_classes_are_first_class() -> None:
    expected = {
        "FUNDING_POS_EXTREME_ONSET": "FundingPosExtremeOnsetDetectorV2",
        "FUNDING_NEG_EXTREME_ONSET": "FundingNegExtremeOnsetDetectorV2",
        "FUNDING_POS_PERSISTENCE": "FundingPosPersistenceDetectorV2",
        "FUNDING_NEG_PERSISTENCE": "FundingNegPersistenceDetectorV2",
        "FUNDING_POS_NORMALIZATION": "FundingPosNormalizationDetectorV2",
        "FUNDING_NEG_NORMALIZATION": "FundingNegNormalizationDetectorV2",
        "FUNDING_FLIP_TO_POSITIVE": "FundingFlipToPositiveDetectorV2",
        "FUNDING_FLIP_TO_NEGATIVE": "FundingFlipToNegativeDetectorV2",
        "PRICE_UP_OI_UP": "PriceUpOIUpDetectorV2",
        "PRICE_DOWN_OI_UP": "PriceDownOIUpDetectorV2",
        "PRICE_UP_OI_DOWN": "PriceUpOIDownDetectorV2",
        "PRICE_DOWN_OI_DOWN": "PriceDownOIDownDetectorV2",
    }
    for event_id, class_name in expected.items():
        cls = get_detector_class(event_id)
        assert cls is not None, event_id
        assert cls.__name__ == class_name


def test_basis_side_is_spread_semantics_not_price_fallback() -> None:
    det = BasisDislocationDetectorV2()
    idx = 4
    features = {
        "basis_zscore": pd.Series([-3.0, -2.0, 0.0, 2.0, 4.0]),
        "basis_bps": pd.Series([-30.0, -20.0, 0.0, 20.0, 40.0]),
        "dynamic_threshold": pd.Series([2.0] * 5),
    }
    assert det.compute_polarity_semantics(idx, features) == "basis_spread_direction"
    assert det.compute_event_side(idx, 1.0, features) == "bullish"
    assert det.compute_polarity_source(idx, 1.0, features) == "basis_zscore"
    assert det.compute_magnitude_source(idx, 1.0, features) == "basis_zscore"


def test_trend_breakout_and_pullback_have_explicit_price_direction() -> None:
    rb = RangeBreakoutDetector()
    rb_features = {
        "close": pd.Series([100.0, 101.0, 110.0]),
        "rolling_max": pd.Series([101.0, 102.0, 105.0]),
        "rolling_min": pd.Series([99.0, 99.0, 99.0]),
    }
    assert rb.compute_direction(2, rb_features) == "up"

    pp = PullbackPivotDetector()
    pp_features = {
        "trend": pd.Series([0.02, -0.02]),
        "retrace": pd.Series([-0.01, 0.01]),
    }
    assert pp.compute_direction(0, pp_features) == "up"
    assert pp.compute_direction(1, pp_features) == "down"


def test_funding_variants_filter_by_signed_funding() -> None:
    df = pd.DataFrame(
        {
            "timestamp": _ts(3),
            "funding_abs_pct": [96.0, 96.0, 96.0],
            "funding_abs": [0.001, 0.001, 0.001],
            "funding_rate_scaled": [0.001, -0.001, 0.001],
        }
    )
    features = {
        "funding_abs_pct": pd.Series([96.0, 96.0, 96.0]),
        "funding_abs": pd.Series([0.001, 0.001, 0.001]),
        "funding_signed": pd.Series([0.001, -0.001, 0.001]),
        "mask": pd.Series([True, True, True]),
    }
    assert FundingPosExtremeOnsetDetectorV2().compute_raw_mask(df, features=features).tolist() == [True, False, True]
    assert FundingNegExtremeOnsetDetectorV2().compute_raw_mask(df, features=features).tolist() == [False, True, False]
    assert FundingPosExtremeOnsetDetectorV2().compute_event_side(0, 1.0, features) == "bullish"
    assert FundingNegExtremeOnsetDetectorV2().compute_event_side(1, 1.0, features) == "bearish"


def test_price_oi_quadrant_is_explicit() -> None:
    features = {
        "oi_z": pd.Series([2.0, 2.0]),
        "oi_pct_change": pd.Series([-0.02, -0.02]),
        "close_ret": pd.Series([0.01, -0.01]),
    }
    up_down = PriceUpOIDownDetectorV2()
    down_down = PriceDownOIDownDetectorV2()
    assert up_down.compute_polarity_semantics(0, features) == "price_oi_quadrant"
    assert up_down._price_oi_quadrant(0, features) == "price_up_oi_down"
    assert down_down._price_oi_quadrant(1, features) == "price_down_oi_down"
    assert up_down.compute_event_side(0, 1.0, features) == "bullish"
    assert down_down.compute_event_side(1, 1.0, features) == "bearish"


def test_liquidity_guards_are_neutral_not_directional_alpha() -> None:
    features = {
        "depth": pd.Series([10.0]),
        "depth_median": pd.Series([100.0]),
        "spread": pd.Series([50.0]),
        "spread_median": pd.Series([10.0]),
        "imbalance": pd.Series([0.0]),
        "evidence_tier": pd.Series(["direct"]),
        "canonical_spread_wide": pd.Series([True]),
    }
    vac = LiquidityVacuumDetectorV2()
    depth = DepthCollapseDetectorV2()
    assert vac.compute_polarity_semantics(0, features) == "neutral_guard"
    assert vac.compute_event_side(0, 1.0, features) == "neutral"
    assert depth.compute_polarity_semantics(0, features) == "execution_guard"
    assert depth.compute_event_side(0, 1.0, features) == "neutral"
