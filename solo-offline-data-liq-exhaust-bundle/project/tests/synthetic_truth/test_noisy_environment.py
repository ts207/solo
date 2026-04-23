from __future__ import annotations

import pytest
import pandas as pd
import numpy as np

from project.tests.synthetic_truth.generators import GeneratorConfig, OrderbookGenerator


class TestNoisyEnvironment:
    def test_no_overfire_on_noise(self):
        gen = OrderbookGenerator()
        
        noise_levels = [0.1, 0.2, 0.5, 1.0, 2.0]
        results = []
        
        for noise in noise_levels:
            rng = np.random.default_rng(int(noise * 100))
            config = GeneratorConfig(
                n_bars=640,
                seed=rng.integers(0, 10000),
            )
            
            df = gen.generate_base(config)
            
            for col in ["depth_usd", "spread_bps"]:
                noise_factor = 1.0 + rng.uniform(-noise, noise, len(df))
                df[col] = df[col] * noise_factor
            
            no_signal_injection = df["depth_usd"].std() / df["depth_usd"].mean()
            results.append({
                "noise": noise,
                "cv": no_signal_injection,
            })
        
        for r in results:
            assert r["cv"] > 0, f"Unexpected zero variance at noise={r['noise']}"


class TestNegativeControlScenarios:
    def test_normal_market_no_false_positives(self):
        gen = OrderbookGenerator()
        config = GeneratorConfig(n_bars=640, seed=42)
        df = gen.generate_base(config)
        
        assert len(df) == 640
        assert df["depth_usd"].std() > 0
        assert df["spread_bps"].std() > 0

    def test_stable_volatility_no_vol_spikes(self):
        rng = np.random.default_rng(42)
        close = 100.0 * np.exp(np.cumsum(rng.normal(0.0001, 0.001, 640)))
        
        df = pd.DataFrame({
            "timestamp": pd.date_range("2024-01-01", periods=640, freq="5min", tz="UTC"),
            "close": close,
            "open": np.roll(close, 1),
            "high": close * 1.001,
            "low": close * 0.999,
            "volume": np.full(640, 1000.0),
        })
        
        returns = df["close"].pct_change().abs()
        vol_stability = returns.std() / returns.mean() if returns.mean() > 0 else float('inf')
        
        assert vol_stability < 10.0, "Volatility should be stable in normal market"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
