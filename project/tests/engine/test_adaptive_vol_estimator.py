"""
E4-T2: Adaptive vol estimator option.

EWMA mode must respond faster to vol changes than the fixed rolling window.
The vol window and annualization constant must also be configurable.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from project.engine.risk_allocator import RiskLimits, allocate_position_scales


def _ts(n: int) -> pd.DatetimeIndex:
    return pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")


def _make_pnl_with_vol_spike(n: int, spike_start: int, spike_magnitude: float = 5.0) -> pd.Series:
    """PnL series that is calm for the first `spike_start` bars then spikes in volatility."""
    ts = _ts(n)
    rng = np.random.default_rng(42)
    pnl = np.concatenate(
        [
            rng.normal(0.0, 0.001, spike_start),
            rng.normal(0.0, 0.001 * spike_magnitude, n - spike_start),
        ]
    )
    return pd.Series(pnl, index=ts)


class TestAdaptiveVolEstimator:
    def test_ewma_mode_accepted_by_risk_limits(self):
        """RiskLimits must accept vol_estimator_mode='ewma' without error."""
        limits = RiskLimits(
            target_annual_vol=0.2,
            vol_estimator_mode="ewma",
            vol_ewma_halflife_bars=576,
        )
        assert limits.vol_estimator_mode == "ewma"

    def test_rolling_mode_is_default(self):
        """Default vol_estimator_mode must be 'rolling' (backward compat)."""
        limits = RiskLimits()
        assert limits.vol_estimator_mode == "rolling"

    def test_ewma_reacts_faster_to_vol_spike(self):
        """
        After a sudden vol spike, EWMA mode must scale down positions faster
        than the fixed 5760-bar rolling window.
        """
        n = 7000
        spike_at = 5000  # vol spike after bar 5000
        ts = _ts(n)
        pnl = _make_pnl_with_vol_spike(n, spike_start=spike_at, spike_magnitude=10.0)
        pos = {"s1": pd.Series(1.0, index=ts)}
        req = {"s1": pd.Series(1.0, index=ts)}

        limits_rolling = RiskLimits(
            target_annual_vol=0.2,
            vol_estimator_mode="rolling",
            vol_window_bars=5760,
        )
        limits_ewma = RiskLimits(
            target_annual_vol=0.2,
            vol_estimator_mode="ewma",
            vol_ewma_halflife_bars=288,  # 1-day halflife
        )

        scales_rolling, _ = allocate_position_scales(
            pos, req, limits_rolling, portfolio_pnl_series=pnl
        )
        scales_ewma, _ = allocate_position_scales(pos, req, limits_ewma, portfolio_pnl_series=pnl)

        # After the spike (last 10% of bars), EWMA should scale down more aggressively
        post_spike = slice(spike_at + 500, n)
        rolling_avg = float(scales_rolling["s1"].iloc[post_spike].abs().mean())
        ewma_avg = float(scales_ewma["s1"].iloc[post_spike].abs().mean())

        assert ewma_avg < rolling_avg, (
            f"EWMA mode should respond faster to vol spike: "
            f"ewma_avg={ewma_avg:.4f}, rolling_avg={rolling_avg:.4f}"
        )

    def test_configurable_window_changes_scaling(self):
        """vol_window_bars must control the rolling window size."""
        n = 3000
        ts = _ts(n)
        pnl = _make_pnl_with_vol_spike(n, spike_start=1000, spike_magnitude=8.0)
        pos = {"s1": pd.Series(1.0, index=ts)}
        req = {"s1": pd.Series(1.0, index=ts)}

        limits_long = RiskLimits(target_annual_vol=0.2, vol_window_bars=2880)  # 10-day window
        limits_short = RiskLimits(target_annual_vol=0.2, vol_window_bars=288)  # 1-day window

        scales_long, _ = allocate_position_scales(pos, req, limits_long, portfolio_pnl_series=pnl)
        scales_short, _ = allocate_position_scales(pos, req, limits_short, portfolio_pnl_series=pnl)

        # Shorter window should react faster — different results (not identical)
        assert not scales_long["s1"].equals(scales_short["s1"]), (
            "Different vol_window_bars must produce different scaling results"
        )

    def test_invalid_mode_raises(self):
        """vol_estimator_mode must only accept 'rolling' or 'ewma'."""
        with pytest.raises((ValueError, TypeError)):
            RiskLimits(target_annual_vol=0.2, vol_estimator_mode="garch")
