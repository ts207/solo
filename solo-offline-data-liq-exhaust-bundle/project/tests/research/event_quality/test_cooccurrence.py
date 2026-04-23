# tests/research/event_quality/test_cooccurrence.py
import pandas as pd
import numpy as np
import pytest
from project.research.event_quality.cooccurrence import compute_cooccurrence


def _make_synced_events(n_bars: int = 300) -> pd.DataFrame:
    """event_a and event_b fire at exactly the same bars (every 20 bars)."""
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-01", periods=n_bars, freq="5min"),
            "close": 100.0,
        }
    )
    df["event_a"] = [i % 20 == 0 for i in range(n_bars)]
    df["event_b"] = [i % 20 == 0 for i in range(n_bars)]
    return df


def _make_independent_events(n_bars: int = 300) -> pd.DataFrame:
    """event_a fires at even 20-bar multiples, event_c fires at odd 20-bar multiples + 10."""
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-01", periods=n_bars, freq="5min"),
            "close": 100.0,
        }
    )
    df["event_a"] = [i % 20 == 0 for i in range(n_bars)]
    # offset by 10 so they never overlap within ±5 bars
    df["event_c"] = [i % 20 == 10 for i in range(n_bars)]
    return df


def test_cooccurrence_perfectly_synced():
    df = _make_synced_events()
    result = compute_cooccurrence(df, window_bars=3)
    pair = result[(result["event_a"] == "a") & (result["event_b"] == "b")]
    assert len(pair) == 1
    assert pair["p_b_given_a"].iloc[0] == pytest.approx(1.0, abs=0.01)


def test_cooccurrence_independent():
    df = _make_independent_events()
    result = compute_cooccurrence(df, window_bars=3)
    pair = result[(result["event_a"] == "a") & (result["event_b"] == "c")]
    assert len(pair) == 1
    # Should be near 0 — they never fire within ±3 bars of each other
    assert pair["p_b_given_a"].iloc[0] < 0.1


def test_cooccurrence_redundant_flag():
    df = _make_synced_events()
    result = compute_cooccurrence(df, window_bars=3, redundancy_threshold=0.5)
    pair = result[(result["event_a"] == "a") & (result["event_b"] == "b")]
    assert pair["redundancy_candidate"].iloc[0] == True


def test_cooccurrence_returns_expected_columns():
    df = _make_synced_events()
    result = compute_cooccurrence(df, window_bars=3)
    assert "event_a" in result.columns
    assert "event_b" in result.columns
    assert "p_b_given_a" in result.columns
    assert "n_a_fires" in result.columns
    assert "n_co_fires" in result.columns
