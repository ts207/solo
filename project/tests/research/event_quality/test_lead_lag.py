# tests/research/event_quality/test_lead_lag.py
import numpy as np
import pandas as pd
import pytest

from project.research.event_quality.lead_lag import (
    compute_event_event_lead_lag,
    compute_event_return_lead_lag,
)


def _make_lead_lag_features(n_bars: int = 600) -> pd.DataFrame:
    """
    event_early fires, then event_late fires 5 bars later (causal chain).
    event_early is also predictive of positive next-horizon return.
    """
    np.random.seed(99)
    close = 100.0 + np.cumsum(np.random.normal(0, 0.05, n_bars + 50))
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-01", periods=n_bars, freq="5min"),
            "close": close[:n_bars],
        }
    )
    # early fires every 30 bars
    early_mask = np.array([i % 30 == 0 for i in range(n_bars)])
    # late fires 5 bars after early (clipped to valid range)
    late_indices = np.where(early_mask)[0] + 5
    late_indices = late_indices[late_indices < n_bars]
    late_mask = np.zeros(n_bars, dtype=bool)
    late_mask[late_indices] = True
    df["event_early"] = early_mask
    df["event_late"] = late_mask
    return df


def test_event_return_lead_lag_shape():
    df = _make_lead_lag_features()
    result = compute_event_return_lead_lag(df, horizons=[3, 6, 12])
    assert isinstance(result, pd.DataFrame)
    assert "event_id" in result.columns
    assert "horizon_bars" in result.columns
    assert "mean_return_bps" in result.columns
    assert "t_stat" in result.columns
    assert "n" in result.columns
    # 2 events × 3 horizons = 6 rows
    assert len(result) == 6


def test_event_event_lead_lag_detects_causal_order():
    df = _make_lead_lag_features()
    result = compute_event_event_lead_lag(df, max_lag=10)
    # early → late should have high frequency at lag 5
    early_to_late = result[(result["source_event"] == "early") & (result["target_event"] == "late")]
    assert len(early_to_late) > 0
    lag5 = early_to_late[early_to_late["lag_bars"] == 5]
    assert len(lag5) == 1
    assert lag5["frequency"].iloc[0] > 0.8


def test_event_event_lead_lag_reverse_is_low():
    df = _make_lead_lag_features()
    result = compute_event_event_lead_lag(df, max_lag=10)
    # late does NOT precede early (early fires first)
    late_to_early = result[
        (result["source_event"] == "late")
        & (result["target_event"] == "early")
        & (result["lag_bars"] <= 5)
    ]
    max_freq = late_to_early["frequency"].max() if len(late_to_early) > 0 else 0.0
    assert max_freq < 0.2


def test_event_return_lead_lag_no_close_raises():
    df = pd.DataFrame(
        {"timestamp": pd.date_range("2023-01-01", periods=10, freq="5min"), "event_x": True}
    )
    with pytest.raises(ValueError, match="close"):
        compute_event_return_lead_lag(df, horizons=[3])


def test_event_event_lead_lag_uses_valid_source_denominator():
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-01", periods=6, freq="5min"),
            "close": [100.0] * 6,
            "event_early": [True, False, False, False, False, True],
            "event_late": [False, True, False, False, False, False],
        }
    )

    result = compute_event_event_lead_lag(df, max_lag=1)
    row = result[
        (result["source_event"] == "early")
        & (result["target_event"] == "late")
        & (result["lag_bars"] == 1)
    ].iloc[0]

    assert row["frequency"] == pytest.approx(1.0)
    assert row["n_source"] == 1
    assert row["n_source_total"] == 2
