from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional
import math

import numpy as np
import pandas as pd

from project.tests.synthetic_truth.generators.base import BaseGenerator, GeneratorConfig


@dataclass
class CopulaConfig:
    rho: float = 0.8
    n_samples: int = 640
    seed: int = 42
    regime: Literal["correlated", "decoupled", "lagged", "breakdown"] = "correlated"
    correlation_break_point: int = 320
    decay_strength: float = 0.1


class GaussianCopulaGenerator(BaseGenerator):
    """Generates synthetic correlated pairs using Gaussian copula."""

    def required_columns(self) -> tuple[str, ...]:
        return ("timestamp", "close_a", "close_b", "spread", "spread_zscore")

    def generate_base(self, config: GeneratorConfig) -> pd.DataFrame:
        copula_config = CopulaConfig(
            n_samples=config.n_bars,
            seed=config.seed,
            rho=0.8,
        )
        return self._generate_correlated_pair(copula_config)

    def inject_signal(self, df: pd.DataFrame, config: GeneratorConfig) -> pd.DataFrame:
        return df

    def _generate_correlated_pair(self, config: CopulaConfig) -> pd.DataFrame:
        rng = np.random.default_rng(config.seed)
        
        mean = [0, 0]
        cov = [[1, config.rho], [config.rho, 1]]
        samples = rng.multivariate_normal(mean, cov, size=config.n_samples)
        
        x = samples[:, 0]
        y = samples[:, 1]
        
        u = pd.Series(self._norm_cdf(x))
        v = pd.Series(self._norm_cdf(y))
        
        price_a = 100 * np.exp(np.cumsum(x * 0.01))
        price_b = 100 * np.exp(np.cumsum(y * 0.01))
        
        spread = (price_b / price_a) - 1.0
        
        spread_mean = spread.mean()
        spread_std = spread.std()
        spread_zscore = (spread - spread_mean) / (spread_std if spread_std > 0 else 1)
        
        df = pd.DataFrame({
            "close_a": price_a,
            "close_b": price_b,
            "spread": spread,
            "spread_zscore": spread_zscore,
        })
        
        df = self._ensure_timestamp(df, GeneratorConfig(n_bars=len(df)))
        
        return df

    def _norm_cdf(self, x: np.ndarray) -> np.ndarray:
        return np.array([0.5 * (1 + math.erf(val / np.sqrt(2))) for val in x])

    def inject_correlated_regime(self, df: pd.DataFrame, config: CopulaConfig) -> pd.DataFrame:
        return df

    def inject_decoupled_regime(self, df: pd.DataFrame, config: CopulaConfig) -> pd.DataFrame:
        df = df.copy()
        
        rng = np.random.default_rng(config.seed + 100)
        noise = rng.normal(0, 0.02, len(df))
        
        df["close_b"] = df["close_b"] * (1 + noise)
        
        spread = (df["close_b"] / df["close_a"]) - 1.0
        df["spread"] = spread
        df["spread_zscore"] = (spread - spread.mean()) / (spread.std() if spread.std() > 0 else 1)
        
        return df

    def inject_lagged_reaction(self, df: pd.DataFrame, config: CopulaConfig) -> pd.DataFrame:
        df = df.copy()
        
        lag = 5
        df["close_b"] = df["close_b"].shift(lag).fillna(df["close_b"].iloc[0])
        
        spread = (df["close_b"] / df["close_a"]) - 1.0
        df["spread"] = spread
        df["spread_zscore"] = (spread - spread.mean()) / (spread.std() if spread.std() > 0 else 1)
        
        return df

    def inject_correlation_breakdown(self, df: pd.DataFrame, config: CopulaConfig) -> pd.DataFrame:
        df = df.copy()
        
        break_point = config.correlation_break_point
        
        rng = np.random.default_rng(config.seed + 200)
        post_break_noise = rng.normal(0, 0.05, len(df) - break_point)
        
        df.loc[break_point:, "close_b"] = (
            df["close_b"].iloc[break_point:].values * (1 + post_break_noise)
        )
        
        spread = (df["close_b"] / df["close_a"]) - 1.0
        df["spread"] = spread
        df["spread_zscore"] = (spread - spread.mean()) / (spread.std() if spread.std() > 0 else 1)
        
        return df


@dataclass
class CopulaScenarioSpec:
    name: str
    regime: Literal["correlated", "decoupled", "lagged", "breakdown"]
    expected_pair_events: dict[str, bool]
    injection_point: int = 320
    duration: int = 20

    def create_generator(self) -> GaussianCopulaGenerator:
        return GaussianCopulaGenerator()

    def generate(self, seed: int = 42) -> pd.DataFrame:
        gen = self.create_generator()
        copula_config = CopulaConfig(
            n_samples=640,
            seed=seed,
            regime=self.regime,
            correlation_break_point=self.injection_point,
        )
        
        df = gen._generate_correlated_pair(copula_config)
        
        if self.regime == "decoupled":
            df = gen.inject_decoupled_regime(df, copula_config)
        elif self.regime == "lagged":
            df = gen.inject_lagged_reaction(df, copula_config)
        elif self.regime == "breakdown":
            df = gen.inject_correlation_breakdown(df, copula_config)
        
        return df


COPULA_SCENARIOS = {
    "correlated_normal": CopulaScenarioSpec(
        name="correlated_normal",
        regime="correlated",
        expected_pair_events={"COPULA_PAIRS_TRADING": False},
    ),
    "decoupled_pair": CopulaScenarioSpec(
        name="decoupled_pair",
        regime="decoupled",
        expected_pair_events={"COPULA_PAIRS_TRADING": True},
    ),
    "lagged_reaction": CopulaScenarioSpec(
        name="lagged_reaction",
        regime="lagged",
        expected_pair_events={"COPULA_PAIRS_TRADING": True},
    ),
    "correlation_breakdown": CopulaScenarioSpec(
        name="correlation_breakdown",
        regime="breakdown",
        expected_pair_events={"COPULA_PAIRS_TRADING": True},
    ),
}
