from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

import pandas as pd

from project.tests.synthetic_truth.generators import (
    ContextGenerator,
    GeneratorConfig,
    OrderbookGenerator,
    PriceSeriesGenerator,
    TradeFlowGenerator,
)


@dataclass
class ScenarioSpec:
    name: str
    event_type: str
    polarity: Literal["positive", "negative"]
    generator_type: str
    injection_method: str
    injection_params: dict[str, Any] = field(default_factory=dict)
    expected_events: dict[str, bool] = field(default_factory=dict)
    excluded_events: dict[str, bool] = field(default_factory=dict)
    difficulty: str = "easy"
    tags: list[str] = field(default_factory=list)
    n_bars: int = 640
    injection_point: int = 320
    injection_duration: int = 20

    def __post_init__(self):
        if not self.name:
            self.name = f"{self.event_type}_{self.polarity}"
        if not self.expected_events and self.event_type:
            self.expected_events = {self.event_type: self.polarity == "positive"}


GENERATOR_MAP = {
    "orderbook": OrderbookGenerator,
    "trade_flow": TradeFlowGenerator,
    "price_series": PriceSeriesGenerator,
    "context": ContextGenerator,
}


class ScenarioFactory:
    def __init__(self, spec: ScenarioSpec):
        self.spec = spec
        self._generator = None

    @property
    def generator(self):
        if self._generator is None:
            gen_cls = GENERATOR_MAP.get(self.spec.generator_type, OrderbookGenerator)
            self._generator = gen_cls()
        return self._generator

    def create(self, seed: int | None = None) -> tuple[pd.DataFrame, dict]:
        if seed is None:
            seed = self.spec.injection_params.get("seed", 42)

        config = GeneratorConfig(
            n_bars=self.spec.n_bars,
            seed=seed,
            injection_point=self.spec.injection_point,
            injection_duration=self.spec.injection_duration,
        )

        df = self.generator.generate_base(config)

        if self.spec.polarity == "positive":
            injection_method = getattr(self.generator, self.spec.injection_method, None)
            if injection_method:
                df = injection_method(df, config, **self.spec.injection_params)
            else:
                df = self.generator.inject_signal(df, config)

        ground_truth = {
            "scenario_name": self.spec.name,
            "event_type": self.spec.event_type,
            "polarity": self.spec.polarity,
            "expected_events": self.spec.expected_events,
            "excluded_events": self.spec.excluded_events,
            "injection_point": self.spec.injection_point,
            "injection_duration": self.spec.injection_duration,
        }

        return df, ground_truth

    @classmethod
    def for_event(cls, event_type: str, polarity: str) -> ScenarioFactory:
        from project.tests.synthetic_truth.scenarios.registry import SCENARIO_REGISTRY

        key = f"{event_type}_{polarity}"
        if key in SCENARIO_REGISTRY:
            return cls(SCENARIO_REGISTRY[key])

        return cls(
            ScenarioSpec(
                name=key,
                event_type=event_type,
                polarity=polarity,
                generator_type="orderbook",
                injection_method="inject_normal_market",
            )
        )

    def __repr__(self) -> str:
        return f"ScenarioFactory({self.spec.name})"
