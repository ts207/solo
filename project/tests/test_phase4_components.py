import numpy as np
import pandas as pd
import pytest

from project.live.drift import calculate_feature_drift, monitor_execution_drift
from project.live.health_checks import check_kill_switch_triggers
from project.research.stability import evaluate_regime_stability
from project.research.walkforward import (
    WindowResult,
    evaluate_walkforward_stability,
    generate_walkforward_windows,
)


def test_walkforward_windows():
    idx = pd.date_range("2024-01-01", periods=100, freq="1h")
    windows = generate_walkforward_windows(
        idx, train_size_bars=40, test_size_bars=10, step_size_bars=10
    )
    assert len(windows) == 6  # (100 - 50) / 10 + 1 = 6
    assert len(windows[0][0]) == 40
    assert len(windows[0][1]) == 10


def test_walkforward_degradation_is_stable_near_zero_train_expectancy():
    results = [
        WindowResult(
            train_range=(pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")),
            test_range=(pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-04")),
            train_metrics={"expectancy_bps": 10.0},
            test_metrics={"expectancy_bps": 5.0},
        ),
        WindowResult(
            train_range=(pd.Timestamp("2024-01-05"), pd.Timestamp("2024-01-06")),
            test_range=(pd.Timestamp("2024-01-07"), pd.Timestamp("2024-01-08")),
            train_metrics={"expectancy_bps": 1e-9},
            test_metrics={"expectancy_bps": 3.0},
        ),
    ]
    out = evaluate_walkforward_stability(results)
    assert abs(out["avg_train_test_degradation"] - (8.0 / 10.0)) < 1e-9


def test_regime_stability():
    returns = pd.Series([0.001, -0.0005, 0.0012, 0.0008, -0.0002])
    regimes = pd.Series(["VOL_LOW", "VOL_LOW", "VOL_HIGH", "VOL_HIGH", "VOL_LOW"])
    stability = evaluate_regime_stability(returns, regimes)
    assert "VOL_LOW" in stability["expectancy_by_regime_bps"]
    assert stability["sr_stability_ratio"] is not None


def test_feature_drift():
    research = pd.Series(np.random.normal(0, 1, 100))
    live = pd.Series(np.random.normal(0.5, 1, 100))  # Small shift
    drift = calculate_feature_drift(research, live)
    assert drift["drift_score"] > 0


def test_kill_switch():
    result = check_kill_switch_triggers(
        live_performance_expectancy=2.0,
        research_mean_expectancy=10.0,  # 20% of research
        max_drawdown_limit=100.0,
        current_drawdown=50.0,
        recent_invalidation_streak=0,
    )
    assert result["should_kill"]
    assert "low_expectancy" in result["reasons"]


def test_execution_drift_detects_sub_1bps_slippage_deterioration() -> None:
    drift = monitor_execution_drift(
        research_slippage_bps=0.1,
        live_slippage_bps=0.8,
        research_fill_rate=0.95,
        live_fill_rate=0.95,
    )

    assert drift["slippage_drift_ratio"] == pytest.approx(8.0)
    assert drift["alert"] is True


if __name__ == "__main__":
    test_walkforward_windows()
    test_regime_stability()
    test_feature_drift()
    test_kill_switch()
    print("All Phase 4 component tests passed.")
