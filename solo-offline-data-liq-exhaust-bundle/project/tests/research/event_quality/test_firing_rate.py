# tests/research/event_quality/test_firing_rate.py
import pandas as pd
import numpy as np
import pytest
from project.research.event_quality.firing_rate import compute_firing_rates


def _make_features(n_bars: int = 500) -> pd.DataFrame:
    """Synthetic feature DataFrame with two event columns."""
    dates = pd.date_range("2023-01-01", periods=n_bars, freq="5min")
    df = pd.DataFrame(
        {
            "timestamp": dates,
            "close": 100.0 + np.cumsum(np.random.normal(0, 0.05, n_bars)),
        }
    )
    # event_alpha fires every 10 bars → 50 fires
    df["event_alpha"] = [i % 10 == 0 for i in range(n_bars)]
    # event_beta fires every 100 bars → 5 fires
    df["event_beta"] = [i % 100 == 0 for i in range(n_bars)]
    return df


def test_firing_rates_basic():
    df = _make_features()
    result = compute_firing_rates(df, bars_per_day=288)
    assert isinstance(result, pd.DataFrame)
    assert "event_id" in result.columns
    assert "n_fires" in result.columns
    assert "fire_rate_per_1000_bars" in result.columns
    assert "events_per_day" in result.columns
    assert "below_min_n" in result.columns


def test_firing_rates_counts():
    df = _make_features(n_bars=500)
    result = compute_firing_rates(df, bars_per_day=288, min_n=20)
    # Filter for alpha/beta specifically
    alpha = result[result["event_id"] == "alpha"]
    beta = result[result["event_id"] == "beta"]
    assert len(alpha) == 1
    assert int(alpha["n_fires"].iloc[0]) == 50
    assert int(beta["n_fires"].iloc[0]) == 5


def test_firing_rates_min_n_flag():
    df = _make_features(n_bars=500)
    result = compute_firing_rates(df, min_n=20)
    alpha = result[result["event_id"] == "alpha"]
    beta = result[result["event_id"] == "beta"]
    assert alpha["below_min_n"].iloc[0] == False
    assert beta["below_min_n"].iloc[0] == True


def test_firing_rates_no_event_columns():
    df = pd.DataFrame(
        {"timestamp": pd.date_range("2023-01-01", periods=10, freq="5min"), "close": 100.0}
    )
    result = compute_firing_rates(df)
    assert result.empty
