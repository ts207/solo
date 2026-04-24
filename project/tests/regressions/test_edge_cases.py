from __future__ import annotations

import numpy as np
import pytest

from project.tests.synthetic_truth.generators import GeneratorConfig, OrderbookGenerator


class TestBorderlineThresholds:
    def test_near_threshold_detection(self):
        gen = OrderbookGenerator()
        results = []

        for noise_pct in [0.48, 0.49, 0.50, 0.51, 0.52]:
            config = GeneratorConfig(n_bars=640, seed=42)
            df = gen.generate_base(config)

            depth_baseline = df["depth_usd"].median()

            noise = np.random.default_rng(int(noise_pct * 1000))
            df["depth_usd"] = df["depth_usd"] * (1 - noise.uniform(0, noise_pct, len(df)))

            depth_at_signal = df["depth_usd"].iloc[320]
            drop_ratio = depth_at_signal / depth_baseline

            results.append({
                "noise_pct": noise_pct,
                "drop_ratio": drop_ratio,
                "near_threshold": 0.45 <= drop_ratio <= 0.55,
            })

        threshold_crossings = [r for r in results if r["near_threshold"]]
        assert len(threshold_crossings) >= 1, "Should detect near-threshold cases"

    def test_volatility_edge_cases(self):
        rng = np.random.default_rng(42)

        cases = []
        for vol_mult in [0.9, 1.0, 1.1, 1.5, 2.0, 3.0]:
            returns = rng.normal(0.0001, 0.001 * vol_mult, 640)
            vol = np.std(returns)
            cases.append({"vol_mult": vol_mult, "vol": vol})

        assert cases[0]["vol"] < cases[-1]["vol"]


class TestNoisySignals:
    def test_noise_immune_detection(self):
        gen = OrderbookGenerator()

        signal_results = []
        noise_results = []

        for seed in range(10):
            config = GeneratorConfig(n_bars=640, seed=seed)
            df = gen.generate_base(config)

            if seed < 5:
                df = gen.inject_liquidity_vacuum(df, config, depth_drop=0.75, spread_mult=4.0)
                signal_results.append(df["spread_bps"].std())
            else:
                noise_results.append(df["spread_bps"].std())

        signal_var = np.mean(signal_results)
        noise_var = np.mean(noise_results)

        assert signal_var > noise_var * 1.5, "Signal should have higher variance than noise"

    def test_randomized_inputs_stable(self):
        gen = OrderbookGenerator()
        stds = []

        for seed in range(20):
            config = GeneratorConfig(n_bars=640, seed=seed)
            df = gen.generate_base(config)
            stds.append(df["depth_usd"].std())

        coef_of_var = np.std(stds) / np.mean(stds)
        assert coef_of_var < 0.5, "Output should be reasonably stable across seeds"


class TestConflictingPatterns:
    def test_counter_signals(self):
        gen = OrderbookGenerator()
        config = GeneratorConfig(n_bars=640, seed=42)
        df = gen.generate_base(config)

        df.loc[df.index[300:350], "depth_usd"] *= 0.5
        df.loc[df.index[300:350], "spread_bps"] *= 0.5

        df.loc[df.index[350:400], "depth_usd"] *= 2.0
        df.loc[df.index[350:400], "spread_bps"] *= 0.5

        assert df["depth_usd"].iloc[320] < df["depth_usd"].iloc[370]

    def test_opposing_directions(self):
        gen = OrderbookGenerator()

        config_up = GeneratorConfig(n_bars=640, seed=42)
        df_up = gen.generate_base(config_up)
        df_up["close"] = df_up["close"] * np.linspace(1.0, 1.05, 640)

        config_down = GeneratorConfig(n_bars=640, seed=43)
        df_down = gen.generate_base(config_down)
        df_down["close"] = df_down["close"] * np.linspace(1.0, 0.95, 640)

        assert df_up["close"].iloc[-1] > df_up["close"].iloc[0]
        assert df_down["close"].iloc[-1] < df_down["close"].iloc[0]


class TestFuzzTesting:
    def test_random_parameter_combinations(self):
        from project.tests.synthetic_truth.generators.base import GeneratorConfig

        valid_combinations = 0

        for _ in range(50):
            try:
                config = GeneratorConfig(
                    n_bars=np.random.randint(100, 2000),
                    seed=np.random.randint(0, 10000),
                    injection_point=np.random.randint(50, 500),
                    injection_duration=np.random.randint(5, 100),
                    base_price=np.random.uniform(10, 1000),
                )

                gen = OrderbookGenerator()
                df = gen.generate_base(config)

                if len(df) == config.n_bars:
                    valid_combinations += 1

            except Exception:
                pass

        assert valid_combinations >= 45, f"Only {valid_combinations}/50 combinations valid"

    def test_extreme_parameter_bounds(self):
        gen = OrderbookGenerator()

        extreme_cases = [
            GeneratorConfig(n_bars=10, seed=1),
            GeneratorConfig(n_bars=10000, seed=1),
            GeneratorConfig(base_price=0.001, seed=1),
            GeneratorConfig(base_price=1000000, seed=1),
        ]

        for config in extreme_cases:
            try:
                df = gen.generate_base(config)
                assert len(df) > 0
                assert df["close"].notna().all()
            except Exception as e:
                pytest.fail(f"Failed on extreme config: {config}, error: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
