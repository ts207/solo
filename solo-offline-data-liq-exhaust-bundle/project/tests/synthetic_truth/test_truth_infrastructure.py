from __future__ import annotations

import pytest
import pandas as pd
import numpy as np

from project.tests.synthetic_truth.generators import (
    GeneratorConfig,
    OrderbookGenerator,
    TradeFlowGenerator,
    PriceSeriesGenerator,
    ContextGenerator,
)
from project.tests.synthetic_truth.scenarios.factory import ScenarioFactory, ScenarioSpec, GENERATOR_MAP
from project.tests.synthetic_truth.scenarios.registry import SCENARIO_REGISTRY, list_scenarios, get_scenario
from project.tests.synthetic_truth.assertions import (
    EventTruthValidator,
    ValidationResult,
    TriggerMatcher,
    NoTriggerMatcher,
)
from project.tests.synthetic_truth.assertions.matchers import MatchResult


class TestGeneratorConfig:
    def test_default_config(self):
        config = GeneratorConfig()
        assert config.n_bars == 640
        assert config.seed == 42
        assert config.injection_point == 320
        assert config.injection_duration == 20
        assert config.base_price == 100.0

    def test_custom_config(self):
        config = GeneratorConfig(n_bars=1000, seed=123, injection_point=500)
        assert config.n_bars == 1000
        assert config.seed == 123
        assert config.injection_point == 500


class TestOrderbookGenerator:
    def test_generate_base(self):
        gen = OrderbookGenerator()
        config = GeneratorConfig(n_bars=100, seed=42)
        df = gen.generate_base(config)

        assert len(df) == 100
        assert "timestamp" in df.columns
        assert "close" in df.columns
        assert "depth_usd" in df.columns
        assert "spread_bps" in df.columns
        assert "ms_imbalance_24" in df.columns

    def test_inject_liquidity_vacuum(self):
        gen = OrderbookGenerator()
        config = GeneratorConfig(n_bars=100, seed=42, injection_point=50, injection_duration=20)
        df = gen.generate_base(config)

        base_depth = df["depth_usd"].iloc[40]
        df = gen.inject_liquidity_vacuum(df, config, depth_drop=0.75, spread_mult=4.0)

        assert df["depth_usd"].iloc[55] < base_depth
        assert df["spread_bps"].iloc[55] > df["spread_bps"].iloc[40]

    def test_required_columns(self):
        gen = OrderbookGenerator()
        cols = gen.required_columns()
        assert "timestamp" in cols
        assert "depth_usd" in cols
        assert "spread_bps" in cols


class TestTradeFlowGenerator:
    def test_generate_base(self):
        gen = TradeFlowGenerator()
        config = GeneratorConfig(n_bars=100, seed=42)
        df = gen.generate_base(config)

        assert len(df) == 100
        assert "quote_volume" in df.columns
        assert "rv_96" in df.columns

    def test_inject_exhaustion_decay(self):
        gen = TradeFlowGenerator()
        config = GeneratorConfig(n_bars=100, seed=42, injection_point=50, injection_duration=20)
        df = gen.generate_base(config)

        base_vol = df["quote_volume"].iloc[40]
        df = gen.inject_exhaustion_decay(df, config, volume_ramp_mult=4.0, volume_collapse_pct=0.8)

        assert df["quote_volume"].iloc[55] > base_vol


class TestPriceSeriesGenerator:
    def test_generate_base(self):
        gen = PriceSeriesGenerator()
        config = GeneratorConfig(n_bars=100, seed=42)
        df = gen.generate_base(config)

        assert len(df) == 100
        assert "open" in df.columns
        assert "high" in df.columns
        assert "low" in df.columns
        assert "volume" in df.columns

    def test_inject_trending(self):
        gen = PriceSeriesGenerator()
        config = GeneratorConfig(n_bars=100, seed=42, injection_point=50, injection_duration=20)
        df = gen.generate_base(config)

        base_price = df["close"].iloc[40]
        df = gen.inject_trending(df, config, direction="up", magnitude_pct=5.0)

        assert df["close"].iloc[65] > base_price


class TestContextGenerator:
    def test_generate_base(self):
        gen = ContextGenerator()
        config = GeneratorConfig(n_bars=100, seed=42)
        df = gen.generate_base(config)

        assert len(df) == 100
        assert "ms_spread_state" in df.columns
        assert "vol_regime" in df.columns
        assert "carry_state" in df.columns


class TestScenarioFactory:
    def test_create_from_spec(self):
        spec = ScenarioSpec(
            name="test_scenario",
            event_type="LIQUIDITY_VACUUM",
            polarity="positive",
            generator_type="orderbook",
            injection_method="inject_liquidity_vacuum",
            injection_params={"depth_drop": 0.75, "spread_mult": 4.0},
        )

        factory = ScenarioFactory(spec)
        df, ground_truth = factory.create(seed=42)

        assert len(df) == 640
        assert ground_truth["scenario_name"] == "test_scenario"
        assert ground_truth["event_type"] == "LIQUIDITY_VACUUM"
        assert ground_truth["polarity"] == "positive"

    def test_for_event(self):
        factory = ScenarioFactory.for_event("LIQUIDITY_VACUUM", "positive")
        assert factory.spec.event_type == "LIQUIDITY_VACUUM"
        assert factory.spec.polarity == "positive"


class TestScenarioRegistry:
    def test_registry_populated(self):
        scenarios = list_scenarios()
        assert len(scenarios) > 0

    def test_get_scenario(self):
        scenario = get_scenario("LIQUIDITY_VACUUM_positive")
        assert scenario is not None
        assert scenario.event_type == "LIQUIDITY_VACUUM"
        assert scenario.polarity == "positive"


class TestMatchers:
    def test_trigger_matcher_success(self):
        events = pd.DataFrame({
            "event_type": ["LIQUIDITY_VACUUM", "LIQUIDITY_VACUUM"],
            "severity_bucket": ["moderate", "major"],
            "direction": ["non_directional", "non_directional"],
        })

        matcher = TriggerMatcher(["LIQUIDITY_VACUUM"])
        result = matcher.match(events, {})

        assert result.matched is True

    def test_trigger_matcher_missed(self):
        events = pd.DataFrame({
            "event_type": ["LIQUIDITY_STRESS_PROXY"],
        })

        matcher = TriggerMatcher(["LIQUIDITY_VACUUM"])
        result = matcher.match(events, {})

        assert result.matched is False
        assert "MISSED_TRIGGER" in result.message or "Missed" in result.message

    def test_no_trigger_matcher_success(self):
        events = pd.DataFrame({
            "event_type": ["LIQUIDITY_VACUUM"],
        })

        matcher = NoTriggerMatcher(["LIQUIDITY_STRESS_PROXY"])
        result = matcher.match(events, {})

        assert result.matched is True

    def test_no_trigger_matcher_false_positive(self):
        events = pd.DataFrame({
            "event_type": ["LIQUIDITY_STRESS_PROXY", "LIQUIDITY_VACUUM"],
        })

        matcher = NoTriggerMatcher(["LIQUIDITY_STRESS_PROXY"])
        result = matcher.match(events, {})

        assert result.matched is False


class TestEventTruthValidator:
    def test_validator_with_positive_scenario(self):
        spec = ScenarioSpec(
            name="LIQUIDITY_VACUUM_positive",
            event_type="LIQUIDITY_VACUUM",
            polarity="positive",
            generator_type="orderbook",
            injection_method="inject_liquidity_vacuum",
            injection_params={"depth_drop": 0.75, "spread_mult": 4.0},
            expected_events={"LIQUIDITY_VACUUM": True},
        )

        validator = EventTruthValidator(spec)
        result = validator.validate(seed=42)

        assert isinstance(result, ValidationResult)
        assert result.scenario_name == "LIQUIDITY_VACUUM_positive"
        assert result.event_type == "LIQUIDITY_VACUUM"

    def test_validator_with_negative_scenario(self):
        spec = ScenarioSpec(
            name="LIQUIDITY_VACUUM_negative",
            event_type="LIQUIDITY_VACUUM",
            polarity="negative",
            generator_type="orderbook",
            injection_method="inject_normal_market",
            injection_params={},
            expected_events={"LIQUIDITY_VACUUM": False},
        )

        validator = EventTruthValidator(spec)
        result = validator.validate(seed=42)

        assert isinstance(result, ValidationResult)
        assert result.scenario_name == "LIQUIDITY_VACUUM_negative"


class TestGeneratorMap:
    def test_all_generators_registered(self):
        for name, cls in GENERATOR_MAP.items():
            gen = cls()
            assert hasattr(gen, "generate_base")
            assert hasattr(gen, "inject_signal")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
