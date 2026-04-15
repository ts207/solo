from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd

from .base import BaseGenerator, GeneratorConfig


class PriceSeriesGenerator(BaseGenerator):
    """Generates synthetic OHLCV price series for trend/volatility scenarios."""

    def required_columns(self) -> tuple[str, ...]:
        return ("timestamp", "close", "high", "low", "open", "volume", "open_interest", "basis_zscore", "spread_zscore", "slippage")

    def generate_base(self, config: GeneratorConfig) -> pd.DataFrame:
        rng = np.random.default_rng(config.seed)
        n = config.n_bars

        close = config.base_price * np.exp(np.cumsum(rng.normal(0.0001, 0.001, n)))
        open_arr = np.roll(close, 1)
        open_arr[0] = config.base_price

        high = np.maximum(close, open_arr) * (1 + rng.uniform(0.0003, 0.001, n))
        low = np.minimum(close, open_arr) * (1 - rng.uniform(0.0003, 0.001, n))
        volume = rng.uniform(800, 1200, n).astype(float)
        open_interest = rng.uniform(50000, 100000, n).astype(float)
        basis_zscore = rng.normal(0, 1, n).astype(float)
        spread_zscore = rng.normal(0, 1, n).astype(float)
        slippage = rng.uniform(0.0001, 0.0005, n).astype(float)

        df = pd.DataFrame(
            {
                "close": close,
                "open": open_arr,
                "high": high,
                "low": low,
                "volume": volume,
                "open_interest": open_interest,
                "basis_zscore": basis_zscore,
                "spread_zscore": spread_zscore,
                "slippage": slippage,
            }
        )
        df = self._ensure_timestamp(df, config)
        return df

    def inject_signal(self, df: pd.DataFrame, config: GeneratorConfig) -> pd.DataFrame:
        return df

    def inject_trending(
        self,
        df: pd.DataFrame,
        config: GeneratorConfig,
        direction: Literal["up", "down"] = "up",
        magnitude_pct: float = 5.0,
    ) -> pd.DataFrame:
        df = df.copy()
        ip = config.injection_point
        dur = config.injection_duration

        sign = 1 if direction == "up" else -1
        target_ret = sign * magnitude_pct / 100

        close_arr = df["close"].to_numpy().copy()
        base_close = close_arr[ip]
        for i in range(ip, min(len(close_arr), ip + dur)):
            progress = (i - ip) / max(1, dur)
            close_arr[i] = base_close * (1 + progress * target_ret)
        df["close"] = close_arr

        mult = 1 + abs(target_ret) * 2
        df["volume"] = self._smooth_transition(
            df["volume"].to_numpy(), ip, dur, df["volume"].iloc[ip] * mult
        )

        high_arr = df["high"].to_numpy().copy()
        high_arr[ip:ip + dur] = df["close"].iloc[ip:ip + dur].to_numpy() * 1.003
        df["high"] = high_arr

        low_arr = df["low"].to_numpy().copy()
        low_arr[ip:ip + dur] = df["close"].iloc[ip:ip + dur].to_numpy() * 0.997
        df["low"] = low_arr

        return df

    def inject_mean_reverting(
        self,
        df: pd.DataFrame,
        config: GeneratorConfig,
        oscillation_pct: float = 2.0,
        frequency: int = 10,
    ) -> pd.DataFrame:
        df = df.copy()
        n = len(df)

        t = np.arange(n) - config.injection_point
        wave = oscillation_pct / 100 * np.sin(2 * np.pi * t / frequency)

        close_arr = df["close"].to_numpy().copy()
        for i in range(max(0, config.injection_point - frequency), min(n, config.injection_point + config.injection_duration + frequency)):
            idx = i - config.injection_point
            close_arr[i] *= (1 + oscillation_pct / 100 * np.sin(2 * np.pi * idx / frequency))
        df["close"] = close_arr

        return df

    def inject_volatility_spike(
        self,
        df: pd.DataFrame,
        config: GeneratorConfig,
        vol_mult: float = 4.0,
        wick_mult: float = 3.0,
    ) -> pd.DataFrame:
        df = df.copy()
        ip = config.injection_point
        dur = config.injection_duration

        base_vol = df["volume"].iloc[ip]
        df["volume"] = self._smooth_transition(
            df["volume"].to_numpy(), ip, dur, base_vol * vol_mult
        )

        close_arr = df["close"].to_numpy()
        high_arr = df["high"].to_numpy().copy()
        low_arr = df["low"].to_numpy().copy()

        for i in range(ip, min(len(df), ip + dur)):
            center = (close_arr[i - 1] if i > 0 else close_arr[0])
            move = np.abs(close_arr[i] - center)
            high_arr[i] = close_arr[i] + move * wick_mult
            low_arr[i] = close_arr[i] - move * wick_mult

        df["high"] = high_arr
        df["low"] = low_arr

        return df

    def inject_zscore_stretch(
        self,
        df: pd.DataFrame,
        config: GeneratorConfig,
        zscore_mult: float = 4.0,
    ) -> pd.DataFrame:
        df = df.copy()
        ip = config.injection_point
        dur = config.injection_duration

        arr = df["basis_zscore"].to_numpy().copy()
        arr = self._smooth_transition(arr, ip, dur, zscore_mult * 3.0)
        df["basis_zscore"] = arr

        return df

    def inject_band_break(
        self,
        df: pd.DataFrame,
        config: GeneratorConfig,
        break_magnitude: float = 3.5,
    ) -> pd.DataFrame:
        df = df.copy()
        ip = config.injection_point
        dur = config.injection_duration

        arr = df["basis_zscore"].to_numpy().copy()
        arr = self._smooth_transition(arr, ip, dur, break_magnitude * 2.0)
        df["basis_zscore"] = arr

        return df

    def inject_slippage_spike(
        self,
        df: pd.DataFrame,
        config: GeneratorConfig,
        slippage_mult: float = 5.0,
        spread_mult: float = 3.0,
    ) -> pd.DataFrame:
        df = df.copy()
        ip = config.injection_point
        dur = config.injection_duration

        df["slippage"] = self._smooth_transition(
            df["slippage"].to_numpy(), ip, dur, df["slippage"].iloc[ip] * slippage_mult
        )
        df["spread_zscore"] = self._smooth_transition(
            df["spread_zscore"].to_numpy(), ip, dur, spread_mult * 3.0
        )

        return df

    def inject_oi_spike(
        self,
        df: pd.DataFrame,
        config: GeneratorConfig,
        direction: Literal["positive", "negative"] = "positive",
        spike_mult: float = 4.0,
    ) -> pd.DataFrame:
        df = df.copy()
        ip = config.injection_point
        dur = config.injection_duration

        base_oi = df["open_interest"].iloc[ip]
        if direction == "positive":
            target = base_oi * spike_mult
        else:
            target = base_oi / spike_mult
        df["open_interest"] = self._smooth_transition(
            df["open_interest"].to_numpy(), ip, dur, target
        )

        return df

    def inject_oi_flush(
        self,
        df: pd.DataFrame,
        config: GeneratorConfig,
        flush_pct: float = 0.10,
    ) -> pd.DataFrame:
        df = df.copy()
        ip = config.injection_point
        dur = config.injection_duration

        base_oi = df["open_interest"].iloc[ip]
        df["open_interest"] = self._smooth_transition(
            df["open_interest"].to_numpy(), ip, dur, base_oi * (1 - flush_pct)
        )

        return df

    def inject_liquidation_cascade(
        self,
        df: pd.DataFrame,
        config: GeneratorConfig,
        cascade_depth_pct: float = 15.0,
        volume_mult: float = 5.0,
    ) -> pd.DataFrame:
        df = df.copy()
        ip = config.injection_point
        dur = config.injection_duration

        base_price = df["close"].iloc[ip]
        target_price = base_price * (1 - cascade_depth_pct / 100)
        df["close"] = self._smooth_transition(df["close"].to_numpy(), ip, dur, target_price)

        df["volume"] = self._smooth_transition(
            df["volume"].to_numpy(), ip, dur, df["volume"].iloc[ip] * volume_mult
        )

        return df

    def inject_gap_overshoot(
        self,
        df: pd.DataFrame,
        config: GeneratorConfig,
        gap_pct: float = 3.0,
        overshoot_pct: float = 2.0,
    ) -> pd.DataFrame:
        df = df.copy()
        ip = config.injection_point
        dur = config.injection_duration

        base_close = df["close"].iloc[ip]
        gap_price = base_close * (1 + gap_pct / 100)
        overshoot_price = base_close * (1 + (gap_pct + overshoot_pct) / 100)
        df["close"] = self._smooth_transition(df["close"].to_numpy(), ip, dur, gap_price)
        df.loc[ip + dur:ip + dur + 5, "close"] = overshoot_price

        return df

    def inject_overshoot_after_shock(
        self,
        df: pd.DataFrame,
        config: GeneratorConfig,
        shock_pct: float = 5.0,
        overshoot_pct: float = 2.0,
    ) -> pd.DataFrame:
        df = df.copy()
        ip = config.injection_point
        dur = config.injection_duration

        base_close = df["close"].iloc[ip]
        shock_price = base_close * (1 + shock_pct / 100)
        df["close"] = self._smooth_transition(df["close"].to_numpy(), ip, dur, shock_price)
        overshoot_price = base_close * (1 + (shock_pct + overshoot_pct) / 100)
        df.loc[ip + dur:ip + dur + 5, "close"] = overshoot_price

        return df

    def inject_rebound(
        self,
        df: pd.DataFrame,
        config: GeneratorConfig,
        rebound_pct: float = 8.0,
        prior_decline_pct: float = 20.0,
    ) -> pd.DataFrame:
        df = df.copy()
        ip = config.injection_point
        dur = config.injection_duration

        base_price = df["close"].iloc[ip]
        decline_price = base_price * (1 - prior_decline_pct / 100)
        df.loc[:ip, "close"] = self._smooth_transition(
            df["close"].iloc[:ip].to_numpy(), 0, ip, decline_price
        )
        rebound_price = base_price * (1 + rebound_pct / 100)
        df["close"] = self._smooth_transition(df["close"].to_numpy(), ip, dur, rebound_price)

        return df

    def inject_sr_break(
        self,
        df: pd.DataFrame,
        config: GeneratorConfig,
        break_pct: float = 2.0,
    ) -> pd.DataFrame:
        df = df.copy()
        ip = config.injection_point
        dur = config.injection_duration

        base_close = df["close"].iloc[ip]
        target_price = base_close * (1 + break_pct / 100)
        df["close"] = self._smooth_transition(df["close"].to_numpy(), ip, dur, target_price)

        return df

    def inject_lead_lag_break(
        self,
        df: pd.DataFrame,
        config: GeneratorConfig,
        lag_bps: int = 50,
    ) -> pd.DataFrame:
        df = df.copy()
        ip = config.injection_point
        dur = config.injection_duration

        lag_factor = 1 + lag_bps / 10000
        df["close"] = self._smooth_transition(
            df["close"].to_numpy(), ip, dur, df["close"].iloc[ip] * lag_factor
        )

        return df

    def inject_index_divergence(
        self,
        df: pd.DataFrame,
        config: GeneratorConfig,
        divergence_pct: float = 5.0,
    ) -> pd.DataFrame:
        df = df.copy()
        ip = config.injection_point
        dur = config.injection_duration

        basis_target = divergence_pct / 100
        df["basis_zscore"] = self._smooth_transition(
            df["basis_zscore"].to_numpy(), ip, dur, basis_target * 3.0
        )

        return df

    def inject_news_volatility(
        self,
        df: pd.DataFrame,
        config: GeneratorConfig,
        vol_mult: float = 3.0,
    ) -> pd.DataFrame:
        df = df.copy()
        ip = config.injection_point
        dur = config.injection_duration

        base_vol = df["volume"].iloc[ip]
        df["volume"] = self._smooth_transition(
            df["volume"].to_numpy(), ip, dur, base_vol * vol_mult
        )

        return df

    def inject_pairs_divergence(
        self,
        df: pd.DataFrame,
        config: GeneratorConfig,
        spread_zscore: float = 3.5,
    ) -> pd.DataFrame:
        df = df.copy()
        ip = config.injection_point
        dur = config.injection_duration

        df["basis_zscore"] = self._smooth_transition(
            df["basis_zscore"].to_numpy(), ip, dur, spread_zscore
        )

        return df

    def inject_stable_market(
        self,
        df: pd.DataFrame,
        config: GeneratorConfig,
    ) -> pd.DataFrame:
        return df
