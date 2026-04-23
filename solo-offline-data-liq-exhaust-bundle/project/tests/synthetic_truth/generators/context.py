from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd

from .base import BaseGenerator, GeneratorConfig


class ContextGenerator(BaseGenerator):
    """Generates synthetic microstructure context states."""

    def required_columns(self) -> tuple[str, ...]:
        return ("timestamp", "ms_spread_state", "vol_regime", "carry_state", "fee_state", "fee_state_confidence", "fee_state_entropy")

    def generate_base(self, config: GeneratorConfig) -> pd.DataFrame:
        rng = np.random.default_rng(config.seed)
        n = config.n_bars

        df = pd.DataFrame(
            {
                "ms_spread_state": np.full(n, 1.0),
                "ms_spread_state_confidence": np.full(n, 0.75),
                "ms_spread_state_entropy": np.full(n, 0.3),
                "vol_regime": np.full(n, 1.0),
                "vol_regime_confidence": np.full(n, 0.75),
                "vol_regime_entropy": np.full(n, 0.3),
                "carry_state": np.full(n, 1.0),
                "carry_state_confidence": np.full(n, 0.75),
                "carry_state_entropy": np.full(n, 0.3),
                "fee_state": np.full(n, 1.0),
                "fee_state_confidence": np.full(n, 0.75),
                "fee_state_entropy": np.full(n, 0.3),
            }
        )
        df = self._ensure_timestamp(df, config)
        return df

    def inject_signal(self, df: pd.DataFrame, config: GeneratorConfig) -> pd.DataFrame:
        return df

    def inject_spread_regime_shift(
        self,
        df: pd.DataFrame,
        config: GeneratorConfig,
        target_state: Literal["low", "mid", "high"] = "high",
    ) -> pd.DataFrame:
        df = df.copy()
        ip = config.injection_point
        dur = config.injection_duration

        state_map = {"low": 0.0, "mid": 1.0, "high": 2.0}
        target = state_map.get(target_state, 1.0)

        arr = df["ms_spread_state"].to_numpy().copy()
        arr = self._smooth_transition(arr, ip, dur, target)
        df["ms_spread_state"] = arr
        df["ms_spread_state_confidence"] = np.where(
            (np.arange(len(df)) >= ip) & (np.arange(len(df)) < ip + dur),
            0.85,
            0.75,
        )

        return df

    def inject_vol_regime_shift(
        self,
        df: pd.DataFrame,
        config: GeneratorConfig,
        target_regime: Literal["low", "mid", "high"] = "high",
    ) -> pd.DataFrame:
        df = df.copy()
        ip = config.injection_point
        dur = config.injection_duration

        state_map = {"low": 0.0, "mid": 1.0, "high": 2.0}
        target = state_map.get(target_regime, 1.0)

        arr = df["vol_regime"].to_numpy().copy()
        arr = self._smooth_transition(arr, ip, dur, target)
        df["vol_regime"] = arr

        return df

    def inject_carry_shift(
        self,
        df: pd.DataFrame,
        config: GeneratorConfig,
        target_carry: Literal["bear", "neutral", "bull"] = "bear",
    ) -> pd.DataFrame:
        df = df.copy()
        ip = config.injection_point
        dur = config.injection_duration

        state_map = {"bear": 0.0, "neutral": 1.0, "bull": 2.0}
        target = state_map.get(target_carry, 1.0)

        arr = df["carry_state"].to_numpy().copy()
        arr = self._smooth_transition(arr, ip, dur, target)
        df["carry_state"] = arr

        return df

    def inject_fee_regime_shift(
        self,
        df: pd.DataFrame,
        config: GeneratorConfig,
        target_regime: Literal["low", "mid", "high"] = "high",
    ) -> pd.DataFrame:
        df = df.copy()
        ip = config.injection_point
        dur = config.injection_duration

        state_map = {"low": 0.0, "mid": 1.0, "high": 2.0}
        target = state_map.get(target_regime, 1.0)

        arr = df["fee_state"].to_numpy().copy()
        arr = self._smooth_transition(arr, ip, dur, target)
        df["fee_state"] = arr
        df["fee_state_confidence"] = np.where(
            (np.arange(len(df)) >= ip) & (np.arange(len(df)) < ip + dur),
            0.85,
            0.75,
        )

        return df

    def inject_stable_fee(
        self,
        df: pd.DataFrame,
        config: GeneratorConfig,
    ) -> pd.DataFrame:
        return df
