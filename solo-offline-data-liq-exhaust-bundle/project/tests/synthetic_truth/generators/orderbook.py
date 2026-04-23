from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from .base import BaseGenerator, GeneratorConfig


class OrderbookGenerator(BaseGenerator):
    """Generates synthetic orderbook metrics for liquidity scenarios."""

    def required_columns(self) -> tuple[str, ...]:
        return ("timestamp", "close", "high", "low", "depth_usd", "spread_bps", "ms_imbalance_24")

    def generate_base(self, config: GeneratorConfig) -> pd.DataFrame:
        rng = np.random.default_rng(config.seed)
        n = config.n_bars

        close = config.base_price * np.exp(np.cumsum(rng.normal(0.0001, 0.001, n)))
        high = close * (1 + rng.uniform(0.0005, 0.002, n))
        low = close * (1 - rng.uniform(0.0005, 0.002, n))

        depth_usd = rng.uniform(80000, 150000, n).astype(float)
        spread_bps = rng.uniform(5, 15, n).astype(float)
        ms_imbalance_24 = rng.uniform(-0.2, 0.2, n).astype(float)

        df = pd.DataFrame(
            {
                "close": close,
                "high": high,
                "low": low,
                "depth_usd": depth_usd,
                "spread_bps": spread_bps,
                "ms_imbalance_24": ms_imbalance_24,
            }
        )
        df = self._ensure_timestamp(df, config)
        return df

    def inject_signal(self, df: pd.DataFrame, config: GeneratorConfig) -> pd.DataFrame:
        return df

    def inject_spoofing_walls(
        self, df: pd.DataFrame, config: GeneratorConfig, depth_drop: float = 0.80
    ) -> pd.DataFrame:
        df = df.copy()
        ip = config.injection_point
        dur = config.injection_duration

        df["depth_usd"] = self._smooth_transition(
            df["depth_usd"].to_numpy(), ip, dur, df["depth_usd"].iloc[ip] * (1 - depth_drop)
        )
        df["spread_bps"] = self._smooth_transition(
            df["spread_bps"].to_numpy(), ip, dur, df["spread_bps"].iloc[ip] * 3.0
        )
        return df

    def inject_liquidity_vacuum(
        self, df: pd.DataFrame, config: GeneratorConfig, depth_drop: float = 0.75, spread_mult: float = 4.0
    ) -> pd.DataFrame:
        df = df.copy()
        ip = config.injection_point
        dur = config.injection_duration

        df["depth_usd"] = self._smooth_transition(
            df["depth_usd"].to_numpy(), ip, dur, df["depth_usd"].iloc[ip] * (1 - depth_drop)
        )
        df["spread_bps"] = self._smooth_transition(
            df["spread_bps"].to_numpy(), ip, dur, df["spread_bps"].iloc[ip] * spread_mult
        )
        return df

    def inject_depth_imbalance(
        self, df: pd.DataFrame, config: GeneratorConfig, imbalance_target: float = 0.85
    ) -> pd.DataFrame:
        df = df.copy()
        ip = config.injection_point
        dur = config.injection_duration

        df["ms_imbalance_24"] = self._smooth_transition(
            df["ms_imbalance_24"].to_numpy(), ip, dur, imbalance_target
        )
        return df

    def inject_normal_market(self, df: pd.DataFrame, config: GeneratorConfig) -> pd.DataFrame:
        return df

    def inject_liquidity_shock(
        self,
        df: pd.DataFrame,
        config: GeneratorConfig,
        depth_drop: float = 0.85,
        spread_mult: float = 4.0,
    ) -> pd.DataFrame:
        df = df.copy()
        ip = config.injection_point
        dur = config.injection_duration

        df["depth_usd"] = self._smooth_transition(
            df["depth_usd"].to_numpy(), ip, dur, df["depth_usd"].iloc[ip] * (1 - depth_drop)
        )
        df["spread_bps"] = self._smooth_transition(
            df["spread_bps"].to_numpy(), ip, dur, df["spread_bps"].iloc[ip] * spread_mult
        )
        return df
