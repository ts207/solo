# tests/research/event_quality/test_information_gain.py
import pandas as pd
import numpy as np
import pytest
from project.research.event_quality.information_gain import compute_information_gain


def _make_predictive_event(n_bars: int = 1000, horizon_bars: int = 12) -> pd.DataFrame:
    """
    event_good fires at bars where next-horizon return is strongly positive.
    event_random fires randomly (no predictive content).
    """
    np.random.seed(42)
    close = 100.0 + np.cumsum(np.random.normal(0, 0.1, n_bars + horizon_bars))
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-01", periods=n_bars, freq="5min"),
            "close": close[:n_bars],
        }
    )
    # forward return
    fwd = np.log(close[horizon_bars : n_bars + horizon_bars]) - np.log(close[:n_bars])
    # event_good: fires on top quartile of forward returns (circular: use fwd > median)
    df["event_good"] = fwd > np.percentile(fwd, 75)
    # event_random: fires at uniform random bars (~25% rate)
    rng = np.random.default_rng(0)
    df["event_random"] = rng.random(n_bars) < 0.25
    return df


def test_information_gain_returns_dataframe():
    df = _make_predictive_event()
    result = compute_information_gain(df, horizon_bars=12)
    assert isinstance(result, pd.DataFrame)
    assert "event_id" in result.columns
    assert "ig_bits" in result.columns
    assert "n_fires" in result.columns
    assert "baseline_entropy_bits" in result.columns


def test_information_gain_predictive_beats_random():
    df = _make_predictive_event()
    result = compute_information_gain(df, horizon_bars=12)
    good_ig = result[result["event_id"] == "good"]["ig_bits"].iloc[0]
    random_ig = result[result["event_id"] == "random"]["ig_bits"].iloc[0]
    # A perfectly aligned event should have much higher IG than a random one
    assert good_ig > random_ig


def test_information_gain_random_event_near_zero():
    df = _make_predictive_event()
    result = compute_information_gain(df, horizon_bars=12)
    random_ig = result[result["event_id"] == "random"]["ig_bits"].iloc[0]
    # Random event has near-zero IG
    assert random_ig < 0.05


def test_information_gain_nonnegative():
    df = _make_predictive_event()
    result = compute_information_gain(df, horizon_bars=12)
    assert (result["ig_bits"] >= 0).all()
