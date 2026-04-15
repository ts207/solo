from __future__ import annotations

import pandas as pd
import pytest

from project.research.gating import calculate_expectancy


def test_calculate_expectancy_off_grid_event_uses_backward_feature_bar():
    sym_events = pd.DataFrame(
        {
            "symbol": ["BTCUSDT"],
            "enter_ts": pd.to_datetime(["2026-01-01T00:02:00Z"], utc=True),
        }
    )
    features_df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:05:00Z",
                    "2026-01-01T00:10:00Z",
                    "2026-01-01T00:15:00Z",
                ],
                utc=True,
            ),
            "close": [100.0, 50.0, 200.0, 200.0],
        }
    )

    mean_ret, _, n_events, _ = calculate_expectancy(
        sym_events=sym_events,
        features_df=features_df,
        rule="continuation",
        horizon="5m",
        entry_lag_bars=1,
        min_samples=1,
    )

    # Backward match for 00:02 is the 00:00 feature bar.
    # entry_lag=1 → entry at 00:05 bar (close=50); horizon=1 bar → 00:10 bar (close=200).
    # return = 200/50 - 1 = 3.0
    assert n_events == 1.0
    assert mean_ret == pytest.approx(3.0, abs=1e-9)


def test_calculate_expectancy_enforces_entry_lag():
    sym_events = pd.DataFrame(
        {
            "symbol": ["BTCUSDT"],
            "enter_ts": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
        }
    )
    features_df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:05:00Z",
                    "2026-01-01T00:10:00Z",
                    "2026-01-01T00:15:00Z",
                ],
                utc=True,
            ),
            "close": [100.0, 50.0, 100.0, 100.0],
        }
    )

    lag1_mean, _, n1, _ = calculate_expectancy(
        sym_events=sym_events,
        features_df=features_df,
        rule="continuation",
        horizon="5m",
        entry_lag_bars=1,
        min_samples=1,
    )
    lag2_mean, _, n2, _ = calculate_expectancy(
        sym_events=sym_events,
        features_df=features_df,
        rule="continuation",
        horizon="5m",
        entry_lag_bars=2,
        min_samples=1,
    )

    assert n1 == 1.0
    assert n2 == 1.0
    assert lag1_mean > lag2_mean


def test_calculate_expectancy_rejects_same_bar_entry():
    sym_events = pd.DataFrame(
        {
            "symbol": ["BTCUSDT"],
            "enter_ts": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
        }
    )
    features_df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z"],
                utc=True,
            ),
            "close": [100.0, 101.0],
        }
    )

    with pytest.raises(ValueError, match="entry_lag_bars must be >= 1"):
        calculate_expectancy(
            sym_events=sym_events,
            features_df=features_df,
            rule="continuation",
            horizon="5m",
            entry_lag_bars=0,
            min_samples=1,
        )
