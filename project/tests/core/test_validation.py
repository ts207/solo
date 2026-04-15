from __future__ import annotations

import pandas as pd
import numpy as np
import pytest

from project.core.validation import assert_ohlcv_geometry, filter_ohlcv_geometry_violations


def _make_ohlcv(**overrides) -> pd.DataFrame:
    row = {"open": 100.0, "high": 105.0, "low": 95.0, "close": 102.0}
    row.update(overrides)
    return pd.DataFrame([row])


# ---------------------------------------------------------------------------
# assert_ohlcv_geometry - valid cases
# ---------------------------------------------------------------------------


def test_assert_ohlcv_geometry_valid_passes():
    df = _make_ohlcv()
    assert_ohlcv_geometry(df)  # must not raise


def test_assert_ohlcv_geometry_high_equals_low_passes():
    df = _make_ohlcv(open=100.0, high=100.0, low=100.0, close=100.0)
    assert_ohlcv_geometry(df)


def test_assert_ohlcv_geometry_null_prices_skip_check():
    df = _make_ohlcv(open=np.nan, high=np.nan, low=np.nan, close=np.nan)
    assert_ohlcv_geometry(df)  # no non-null rows to violate


def test_assert_ohlcv_geometry_partial_nulls_skip_check():
    # high is null; can't compare it — should not raise
    df = _make_ohlcv(high=np.nan)
    assert_ohlcv_geometry(df)


# ---------------------------------------------------------------------------
# assert_ohlcv_geometry - violation cases
# ---------------------------------------------------------------------------


def test_assert_ohlcv_geometry_negative_open_raises():
    df = _make_ohlcv(open=-1.0)
    with pytest.raises(ValueError, match="non-positive"):
        assert_ohlcv_geometry(df)


def test_assert_ohlcv_geometry_zero_close_raises():
    df = _make_ohlcv(close=0.0)
    with pytest.raises(ValueError, match="non-positive"):
        assert_ohlcv_geometry(df)


def test_assert_ohlcv_geometry_high_less_than_open_raises():
    df = _make_ohlcv(open=110.0, high=105.0, low=95.0, close=102.0)
    with pytest.raises(ValueError, match="high < open"):
        assert_ohlcv_geometry(df)


def test_assert_ohlcv_geometry_high_less_than_close_raises():
    df = _make_ohlcv(open=100.0, high=101.0, low=95.0, close=103.0)
    with pytest.raises(ValueError, match="high < close"):
        assert_ohlcv_geometry(df)


def test_assert_ohlcv_geometry_high_less_than_low_raises():
    # high=90 >= open=85 and >= close=88, but < low=95
    df = _make_ohlcv(open=85.0, high=90.0, low=95.0, close=88.0)
    with pytest.raises(ValueError, match="high < low"):
        assert_ohlcv_geometry(df)


def test_assert_ohlcv_geometry_low_greater_than_open_raises():
    df = _make_ohlcv(open=90.0, high=105.0, low=95.0, close=102.0)
    with pytest.raises(ValueError, match="low > open"):
        assert_ohlcv_geometry(df)


def test_assert_ohlcv_geometry_low_greater_than_close_raises():
    df = _make_ohlcv(open=100.0, high=105.0, low=98.0, close=96.0)
    with pytest.raises(ValueError, match="low > close"):
        assert_ohlcv_geometry(df)


def test_assert_ohlcv_geometry_reports_row_count_in_message():
    # Two bad rows
    df = pd.DataFrame([
        {"open": 110.0, "high": 105.0, "low": 95.0, "close": 102.0},
        {"open": 115.0, "high": 105.0, "low": 95.0, "close": 102.0},
    ])
    with pytest.raises(ValueError, match="2 row"):
        assert_ohlcv_geometry(df)


# ---------------------------------------------------------------------------
# filter_ohlcv_geometry_violations
# ---------------------------------------------------------------------------


def test_filter_keeps_clean_rows():
    df = pd.DataFrame([
        {"open": 100.0, "high": 105.0, "low": 95.0, "close": 102.0},
        {"open": 200.0, "high": 210.0, "low": 190.0, "close": 205.0},
    ])
    clean, dropped = filter_ohlcv_geometry_violations(df)
    assert dropped == 0
    assert len(clean) == 2


def test_filter_drops_negative_price_rows():
    df = pd.DataFrame([
        {"open": 100.0, "high": 105.0, "low": 95.0, "close": 102.0},
        {"open": -1.0, "high": 105.0, "low": 95.0, "close": 102.0},
    ])
    clean, dropped = filter_ohlcv_geometry_violations(df)
    assert dropped == 1
    assert len(clean) == 1
    assert float(clean["open"].iloc[0]) == 100.0


def test_filter_drops_geometry_violating_rows():
    df = pd.DataFrame([
        {"open": 100.0, "high": 105.0, "low": 95.0, "close": 102.0},
        {"open": 110.0, "high": 105.0, "low": 95.0, "close": 102.0},  # high < open
    ])
    clean, dropped = filter_ohlcv_geometry_violations(df)
    assert dropped == 1
    assert len(clean) == 1


def test_filter_preserves_null_rows():
    # Rows with NaN prices are not dropped — they represent gaps
    df = pd.DataFrame([
        {"open": np.nan, "high": np.nan, "low": np.nan, "close": np.nan},
        {"open": 100.0, "high": 105.0, "low": 95.0, "close": 102.0},
    ])
    clean, dropped = filter_ohlcv_geometry_violations(df)
    assert dropped == 0
    assert len(clean) == 2


def test_filter_empty_dataframe():
    df = pd.DataFrame(columns=["open", "high", "low", "close"])
    clean, dropped = filter_ohlcv_geometry_violations(df)
    assert dropped == 0
    assert len(clean) == 0


def test_filter_missing_price_columns_returns_unchanged():
    df = pd.DataFrame([{"volume": 1000.0}])
    clean, dropped = filter_ohlcv_geometry_violations(df)
    assert dropped == 0
    assert len(clean) == 1
