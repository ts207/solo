from __future__ import annotations

import numpy as np
import pandas as pd

from .base import BaseGenerator, GeneratorConfig


class TradeFlowGenerator(BaseGenerator):
    """Generates synthetic trade flow for orderflow/exhaustion scenarios."""

    def required_columns(self) -> tuple[str, ...]:
        return ("timestamp", "close", "high", "low", "quote_volume", "rv_96")

    def generate_base(self, config: GeneratorConfig) -> pd.DataFrame:
        rng = np.random.default_rng(config.seed)
        n = config.n_bars

        close = config.base_price * np.exp(np.cumsum(rng.normal(0.0001, 0.001, n)))
        high = close * (1 + rng.uniform(0.0005, 0.002, n))
        low = close * (1 - rng.uniform(0.0005, 0.002, n))

        quote_volume = rng.uniform(800, 1200, n).astype(float)
        rv_96 = rng.uniform(0.001, 0.003, n).astype(float)

        df = pd.DataFrame(
            {
                "close": close,
                "high": high,
                "low": low,
                "quote_volume": quote_volume,
                "rv_96": rv_96,
            }
        )
        df = self._ensure_timestamp(df, config)
        return df

    def inject_signal(self, df: pd.DataFrame, config: GeneratorConfig) -> pd.DataFrame:
        return df

    def inject_exhaustion_decay(
        self,
        df: pd.DataFrame,
        config: GeneratorConfig,
        volume_ramp_mult: float = 4.0,
        volume_collapse_pct: float = 0.80,
        rv_increase: float = 3.0,
    ) -> pd.DataFrame:
        df = df.copy()
        ip = config.injection_point
        dur = config.injection_duration

        base_vol = df["quote_volume"].iloc[ip]
        ramp_up_end = ip + dur // 3
        ramp_down_start = ip + 2 * dur // 3

        vol = df["quote_volume"].to_numpy().copy()
        for i in range(ip, ramp_up_end):
            alpha = (i - ip) / max(1, ramp_up_end - ip)
            vol[i] = base_vol * (1 + alpha * (volume_ramp_mult - 1))
        for i in range(ramp_up_end, ramp_down_start):
            vol[i] = base_vol * volume_ramp_mult
        for i in range(ramp_down_start, min(len(vol), ip + dur)):
            alpha = (i - ramp_down_start) / max(1, ip + dur - ramp_down_start)
            vol[i] = base_vol * volume_ramp_mult * (1 - alpha * volume_collapse_pct)
        df["quote_volume"] = vol

        base_rv = df["rv_96"].iloc[ip]
        df["rv_96"] = self._smooth_transition(
            df["rv_96"].to_numpy(), ip, dur, base_rv * rv_increase
        )

        return df

    def inject_aggressive_burst(
        self, df: pd.DataFrame, config: GeneratorConfig, volume_mult: float = 5.0, price_move_pct: float = 2.0
    ) -> pd.DataFrame:
        df = df.copy()
        ip = config.injection_point
        dur = config.injection_duration

        df["quote_volume"] = self._smooth_transition(
            df["quote_volume"].to_numpy(), ip, dur, df["quote_volume"].iloc[ip] * volume_mult
        )

        close_arr = df["close"].to_numpy().copy()
        target_close = close_arr[ip] * (1 + price_move_pct / 100)
        close_arr = self._smooth_transition(close_arr, ip, dur, target_close)
        df["close"] = close_arr

        high_arr = df["high"].to_numpy().copy()
        high_arr[ip:ip + dur] = df["close"].iloc[ip:ip + dur].to_numpy() * 1.003
        df["high"] = high_arr

        return df

    def inject_steady_flow(self, df: pd.DataFrame, config: GeneratorConfig) -> pd.DataFrame:
        return df
