from __future__ import annotations

import numpy as np
import pandas as pd

from project.research.gating import (
    _realized_signed_return_from_path,
    build_event_return_frame,
    calculate_expectancy,
    calculate_expectancy_stats,
    distribution_stats,
    one_sided_p_from_t,
)


def test_one_sided_p_handles_degenerate_degrees_of_freedom() -> None:
    assert one_sided_p_from_t(2.5, df=0) == 1.0
    assert one_sided_p_from_t(-2.5, df=0) == 1.0


def test_distribution_stats_preserves_direction_with_small_samples() -> None:
    stats = distribution_stats(np.array([0.02, -0.01]))
    assert stats["mean"] == 0.005
    assert stats["t_stat"] > 0
    assert 0.0 <= stats["p_value"] <= 1.0


def test_distribution_stats_handles_zero_variance_and_nan_only_input() -> None:
    zero_var = distribution_stats(np.array([0.01, 0.01, 0.01]))
    assert zero_var["mean"] == 0.01
    assert zero_var["std"] == 0.0
    assert zero_var["p_value"] == 1.0

    nan_only = distribution_stats(np.array([np.nan, np.inf, -np.inf, 0.01]))
    assert nan_only["mean"] == 0.0
    assert nan_only["std"] == 0.0
    assert nan_only["t_stat"] == 0.0
    assert nan_only["p_value"] == 1.0


def test_distribution_stats_retains_loser_sign() -> None:
    stats = distribution_stats(np.array([-0.03, -0.02, -0.01, -0.04]))
    assert stats["mean"] < 0
    assert stats["t_stat"] < 0
    assert stats["p_value"] > 0.95



def test_build_event_return_frame_and_expectancy_stats() -> None:
    timestamps = pd.date_range("2024-01-01", periods=6, freq="5min", tz="UTC")
    sym_events = pd.DataFrame(
        {
            "enter_ts": [timestamps[0], timestamps[1]],
            "evt_split_label": ["train", "validation"],
            "evt_vol_regime": ["low", "high"],
            "evt_liquidity_state": ["stable", "stress"],
        }
    )
    features_df = pd.DataFrame(
        {
            "timestamp": timestamps,
            "close": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0],
        }
    )

    frame = build_event_return_frame(
        sym_events,
        features_df,
        rule="enter_long_market",
        horizon="5m",
        side_policy="both",
        canonical_family="basis",
        shift_labels_k=0,
        entry_lag_bars=1,
        horizon_bars_override=1,
        cost_bps=10.0,
        direction_override=1.0,
    )
    assert len(frame) == 2
    assert frame["forward_return"].gt(0).all()
    assert frame.loc[0, "split_label"] == "train"

    stats = calculate_expectancy_stats(
        sym_events,
        features_df,
        rule="enter_long_market",
        horizon="5m",
        side_policy="both",
        canonical_family="basis",
        shift_labels_k=0,
        entry_lag_bars=1,
        min_samples=2,
        horizon_bars_override=1,
    )
    assert stats["n_events"] == 2.0
    assert stats["mean_return"] > 0
    assert stats["gate_max_drawdown"] is True
    assert stats["stability_pass"] is True
    assert stats["stability_pass"] == stats["gate_max_drawdown"]

    mean_return, p_value, n_events, gate = calculate_expectancy(
        sym_events,
        features_df,
        rule="enter_long_market",
        horizon="5m",
        shift_labels_k=0,
        entry_lag_bars=1,
        min_samples=2,
    )
    assert n_events == 2.0
    assert mean_return > 0
    assert 0.0 <= p_value <= 1.0
    assert gate is True


def test_calculate_expectancy_stats_maps_stability_pass_from_drawdown_gate(monkeypatch) -> None:
    timestamps = pd.date_range("2024-01-01", periods=6, freq="5min", tz="UTC")
    sym_events = pd.DataFrame({"enter_ts": [timestamps[0], timestamps[1]]})
    features_df = pd.DataFrame(
        {
            "timestamp": timestamps,
            "close": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0],
        }
    )

    monkeypatch.setattr(
        "project.research.gating.max_drawdown_gate",
        lambda returns: {"max_drawdown": 1.0, "dd_to_expectancy_ratio": 9.0, "gate_max_drawdown": False},
    )

    stats = calculate_expectancy_stats(
        sym_events,
        features_df,
        rule="enter_long_market",
        horizon="5m",
        side_policy="both",
        canonical_family="basis",
        shift_labels_k=0,
        entry_lag_bars=1,
        min_samples=2,
        horizon_bars_override=1,
    )

    assert stats["n_events"] == 2.0
    assert stats["gate_max_drawdown"] is False
    assert stats["stability_pass"] is False


def test_return_path_thresholds_apply_stop_loss_and_take_profit() -> None:
    stop_loss = _realized_signed_return_from_path(
        price_path=np.array([100.0, 99.0, 98.0]),
        direction_sign=1.0,
        stop_loss_bps=50.0,
    )
    assert stop_loss < 0

    take_profit = _realized_signed_return_from_path(
        price_path=np.array([100.0, 100.3, 101.0]),
        direction_sign=1.0,
        take_profit_bps=20.0,
    )
    assert take_profit > 0


def test_build_event_return_frame_ignores_nan_direction_override() -> None:
    timestamps = pd.date_range("2024-01-01", periods=4, freq="5min", tz="UTC")
    sym_events = pd.DataFrame(
        {
            "enter_ts": [timestamps[0]],
            "direction": ["down"],
            "split_label": ["test"],
        }
    )
    features_df = pd.DataFrame(
        {
            "timestamp": timestamps,
            "close": [100.0, 95.0, 90.0, 90.0],
        }
    )

    frame = build_event_return_frame(
        sym_events,
        features_df,
        rule="continuation",
        horizon="5m",
        side_policy="directional",
        canonical_family="VOLATILITY_TRANSITION",
        entry_lag_bars=1,
        horizon_bars_override=1,
        direction_override=float("nan"),
    )

    assert frame.loc[0, "direction_sign"] == -1.0
    assert frame.loc[0, "forward_return_raw"] > 0.0


def test_calculate_expectancy_stats_fails_closed_on_invalid_split_labels() -> None:
    timestamps = pd.date_range("2024-01-01", periods=6, freq="5min", tz="UTC")
    sym_events = pd.DataFrame(
        {
            "enter_ts": [timestamps[0], timestamps[1]],
            "evt_split_label": ["train", ""],
        }
    )
    features_df = pd.DataFrame(
        {
            "timestamp": timestamps,
            "close": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0],
        }
    )

    stats = calculate_expectancy_stats(
        sym_events,
        features_df,
        rule="enter_long_market",
        horizon="5m",
        side_policy="both",
        canonical_family="basis",
        shift_labels_k=0,
        entry_lag_bars=1,
        min_samples=2,
        horizon_bars_override=1,
    )

    assert stats["p_value"] == 1.0
    assert stats["t_stat"] == 0.0
    assert stats["n_events"] == 2.0
