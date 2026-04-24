from __future__ import annotations

import pytest

from project.tests.synthetic_truth.generators.copula import (
    COPULA_SCENARIOS,
    CopulaConfig,
    CopulaScenarioSpec,
    GaussianCopulaGenerator,
)


class TestGaussianCopulaGenerator:
    def test_generate_correlated_pair(self):
        gen = GaussianCopulaGenerator()
        config = CopulaConfig(n_samples=100, seed=42, rho=0.8)

        df = gen._generate_correlated_pair(config)

        assert len(df) == 100
        assert "close_a" in df.columns
        assert "close_b" in df.columns
        assert "spread" in df.columns
        assert "spread_zscore" in df.columns

    def test_correlated_prices_move_together(self):
        gen = GaussianCopulaGenerator()
        config = CopulaConfig(n_samples=640, seed=42, rho=0.9)

        df = gen._generate_correlated_pair(config)

        corr = df["close_a"].corr(df["close_b"])
        assert corr > 0.7

    def test_inject_decoupled_regime(self):
        gen = GaussianCopulaGenerator()
        config = CopulaConfig(n_samples=100, seed=42, rho=0.9)

        df = gen._generate_correlated_pair(config)
        df = gen.inject_decoupled_regime(df, config)

        pre_corr = df["close_a"].iloc[:50].corr(df["close_b"].iloc[:50])
        post_corr = df["close_a"].iloc[50:].corr(df["close_b"].iloc[50:])

        assert pre_corr > post_corr

    def test_inject_correlation_breakdown(self):
        gen = GaussianCopulaGenerator()
        config = CopulaConfig(n_samples=100, seed=42, rho=0.9, correlation_break_point=50)

        df = gen._generate_correlated_pair(config)
        df = gen.inject_correlation_breakdown(df, config)

        pre_corr = df["close_a"].iloc[:50].corr(df["close_b"].iloc[:50])
        post_corr = df["close_a"].iloc[50:].corr(df["close_b"].iloc[50:])

        assert abs(post_corr) < abs(pre_corr)


class TestCopulaScenarioSpec:
    def test_create_generator(self):
        spec = CopulaScenarioSpec(
            name="test",
            regime="correlated",
            expected_pair_events={},
        )
        gen = spec.create_generator()
        assert isinstance(gen, GaussianCopulaGenerator)

    def test_generate_all_scenarios(self):
        for name, spec in COPULA_SCENARIOS.items():
            df = spec.generate(seed=42)
            assert len(df) > 0
            assert "close_a" in df.columns
            assert "close_b" in df.columns


class TestCrossAssetValidation:
    def test_pair_detector_divergence_only(self):

        for name, spec in COPULA_SCENARIOS.items():
            df = spec.generate(seed=42)

            if spec.expected_pair_events.get("COPULA_PAIRS_TRADING"):
                spread_std = df["spread"].std()
                assert spread_std > 0.01 or abs(df["spread_zscore"].max()) > 2.0

    def test_correlated_regime_no_divergence(self):
        spec = COPULA_SCENARIOS["correlated_normal"]
        df = spec.generate(seed=42)

        spread_std = df["spread"].std()
        max_zscore = abs(df["spread_zscore"]).max()

        assert max_zscore < 3.0, "Correlated regime should not have extreme divergence"

    def test_breakdown_regime_has_divergence(self):
        spec = COPULA_SCENARIOS["correlation_breakdown"]
        df = spec.generate(seed=42)

        post_break_spread = df["spread"].iloc[320:]
        max_zscore = abs(df["spread_zscore"]).iloc[320:].max()

        assert max_zscore > 2.0, "Breakdown regime should have divergence signal"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
