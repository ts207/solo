import importlib

import numpy as np
import pandas as pd
import pytest

from project.reliability.temporal_invariance import (
    InvarianceCheckSpec,
    PerturbationSpec,
    assert_future_invariance,
    future_missing_data,
    future_noise,
    future_price_spike,
)

MODULES_TO_TEST = [
    "project.features.liquidity_vacuum",
    "project.features.vol_shock_relaxation",
    "project.features.context_states",
    "project.features.funding_persistence",
    "project.features.microstructure",
    "project.features.vol_regime",
]


@pytest.mark.parametrize("module_path", MODULES_TO_TEST)
def test_module_temporal_invariance(module_path):
    """
    Generic test to ensure all refactored modules are PIT-safe.
    """
    module = importlib.import_module(module_path)

    # Generate mock data
    dates = pd.date_range("2024-01-01", periods=500, freq="5min", tz="UTC")
    df = pd.DataFrame(
        {
            "timestamp": dates,
            "close": 100 + np.cumsum(np.random.randn(500)),
            "high": 101 + np.cumsum(np.random.randn(500)),
            "low": 99 + np.cumsum(np.random.randn(500)),
            "volume": np.abs(np.random.randn(500)) * 1000,
            "buy_volume": np.abs(np.random.randn(500)) * 500,
            "funding_rate_scaled": np.random.randn(500),
            "oi_delta_1h": np.random.randn(500),
            "rv_pct": np.random.rand(500) * 100,
            "quote_volume": np.random.rand(500) * 1000000,
            "trend_return": np.random.randn(500),
            "spread_z": np.random.randn(500),
            "bid_price": 99.9,
            "ask_price": 100.1,
        }
    )

    # Find a detector or calculator function
    func = None
    if hasattr(module, "detect_liquidity_vacuum_events"):
        func = lambda d: module.detect_liquidity_vacuum_events(d, "BTC")
    elif hasattr(module, "detect_vol_shock_relaxation_events"):
        func = lambda d: module.detect_vol_shock_relaxation_events(d, "BTC")[0]
    elif hasattr(module, "calculate_ms_vol_state"):
        # For context_states, we test a wrapper
        def wrapper(d):
            return module.calculate_ms_vol_state(d["rv_pct"])

        func = wrapper
    elif hasattr(module, "build_funding_persistence_state"):
        func = lambda d: module.build_funding_persistence_state(d, "BTC")
    elif hasattr(module, "calculate_roll"):
        func = lambda d: module.calculate_roll(d["close"])
    elif hasattr(module, "calculate_rv_percentile_24h"):
        func = lambda d: module.calculate_rv_percentile_24h(d["close"])

    if func:
        spec = InvarianceCheckSpec(
            name=f"invariance_{module_path}",
            build_output=func,
            extract_comparable_prefix=lambda out, cutoff, warmup: (
                out.iloc[warmup : cutoff + 1]
                if hasattr(out, "iloc")
                else out.iloc[warmup : cutoff + 1]
                if isinstance(out, (pd.Series, pd.DataFrame))
                else out
            ),
            cutoffs=[250, 400],
            warmup=100,
            perturbations=[
                PerturbationSpec("price_spike", lambda d, c: future_price_spike(d.copy(), c)),
                PerturbationSpec("missing_data", lambda d, c: future_missing_data(d.copy(), c)),
                PerturbationSpec("noise", lambda d, c: future_noise(d.copy(), c)),
            ],
        )
        assert_future_invariance(df, spec)
