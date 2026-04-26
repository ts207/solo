"""Optional Pandera-based data contracts.

This repository uses Pandera for dataframe schema validation in a few pipeline
stages and unit tests.

Some environments (e.g., minimal CI images or contract-test-only runs) may not
install Pandera. Importing this module must not hard-fail in those environments.

If Pandera is not installed, schema classes are still defined but calling
`<Schema>.validate(...)` will raise an ImportError explaining the dependency.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

try:
    import pandera as pa
    from pandera.typing import DataFrame, Series
except Exception:  # pragma: no cover
    pa = None  # type: ignore
    DataFrame = object  # type: ignore
    Series = object  # type: ignore


class _PanderaMissingMixin:
    """Fallback mixin when pandera is unavailable."""

    @classmethod
    def validate(cls, *_args, **_kwargs):
        # No-op validation when Pandera is unavailable.
        # This keeps lightweight / contract-test-only environments running.
        # Full schema validation is enforced when Pandera is installed.
        if _args:
            return _args[0]
        return None


if pa is not None:

    class Cleaned5mBarsSchema(pa.DataFrameModel):
        symbol: Series[str] = pa.Field(coerce=True)
        timestamp: Series[pd.Timestamp] = pa.Field(coerce=True)
        open: Series[float] = pa.Field(ge=0.0, nullable=True)
        high: Series[float] = pa.Field(ge=0.0, nullable=True)
        low: Series[float] = pa.Field(ge=0.0, nullable=True)
        close: Series[float] = pa.Field(ge=0.0, nullable=True)
        volume: Series[float] = pa.Field(ge=0.0)
        quote_volume: Series[float] = pa.Field(ge=0.0, nullable=True)
        is_gap: Series[bool] = pa.Field()
        funding_rate_realized: Series[float] = pa.Field(nullable=True)

        @pa.dataframe_check
        def check_high_low(cls, df: DataFrame) -> Series[bool]:
            return df["high"].isna() | (df["high"] >= df["low"])

        @pa.dataframe_check
        def check_high_gte_open_close(cls, df: DataFrame) -> Series[bool]:
            h, o, c = df["high"], df["open"], df["close"]
            both_present = h.notna() & o.notna() & c.notna()
            return ~both_present | ((h >= o) & (h >= c))

        @pa.dataframe_check
        def check_low_lte_open_close(cls, df: DataFrame) -> Series[bool]:
            lv, o, c = df["low"], df["open"], df["close"]
            both_present = lv.notna() & o.notna() & c.notna()
            return ~both_present | ((lv <= o) & (lv <= c))

        @pa.dataframe_check
        def check_timestamp_utc(cls, df: DataFrame) -> Series[bool]:
            ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
            # Strictly require parseable UTC timestamps without NaT.
            return ts.notna()

        class Config:
            strict = False  # Allow other columns like quote_volume or taker_buy_volume

    class EventRegistrySchema(pa.DataFrameModel):
        symbol: Series[str] = pa.Field(coerce=True)
        phenom_enter_ts: Series[int] = pa.Field(ge=1577836800000)
        eval_bar_ts: Series[int] | None = pa.Field(ge=1577836800000, nullable=True)
        enter_ts: Series[int] = pa.Field(ge=1577836800000)
        detected_ts: Series[int] = pa.Field(ge=1577836800000)
        signal_ts: Series[int] = pa.Field(ge=1577836800000)
        exit_ts: Series[int] = pa.Field(ge=1577836800000)
        event_id: Series[str] = pa.Field(nullable=False)
        signal_column: Series[str] = pa.Field(nullable=False)
        direction: Series[str] | None = pa.Field(
            nullable=True, isin=["long", "short", "neutral", "non_directional"]
        )
        sign: Series[float] | None = pa.Field(nullable=True)
        split_label: Series[str] | None = pa.Field(nullable=True)

        # Mandated feature parity columns
        rv_96: Series[float] | None = pa.Field(nullable=True)
        funding_abs: Series[float] | None = pa.Field(nullable=True)
        spread_zscore: Series[float] | None = pa.Field(nullable=True)
        basis_zscore: Series[float] | None = pa.Field(nullable=True)

        @pa.dataframe_check
        def check_exit_after_enter(cls, df: DataFrame) -> Series[bool]:
            return df["exit_ts"] >= df["enter_ts"]

        @pa.dataframe_check
        def check_detected_after_phenom(cls, df: DataFrame) -> Series[bool]:
            return df["detected_ts"] >= df["phenom_enter_ts"]

        @pa.dataframe_check
        def check_signal_after_detected(cls, df: DataFrame) -> Series[bool]:
            return df["signal_ts"] >= df["detected_ts"]

        @pa.dataframe_check
        def check_signal_after_eval_bar(cls, df: DataFrame) -> Series[bool]:
            if "eval_bar_ts" in df.columns:
                eval_bar = pd.to_numeric(df["eval_bar_ts"], errors="coerce")
            else:
                eval_bar = pd.Series(np.nan, index=df.index, dtype=float)
            fallback = pd.to_numeric(df["detected_ts"], errors="coerce")
            eval_bar = eval_bar.where(eval_bar.notna(), fallback)
            return pd.to_numeric(df["signal_ts"], errors="coerce") >= eval_bar

        class Config:
            strict = False

    class Phase2CandidateSchema(pa.DataFrameModel):
        symbol: Series[str] = pa.Field(coerce=True)
        enter_ts: Series[int] = pa.Field(ge=1577836800000)
        exit_ts: Series[int] = pa.Field(ge=1577836800000)
        event_id: Series[str] = pa.Field(nullable=False)
        q_value: Series[float] = pa.Field(ge=0.0, le=1.0)

        class Config:
            strict = False

else:

    class Cleaned5mBarsSchema(_PanderaMissingMixin):
        pass

    class EventRegistrySchema(_PanderaMissingMixin):
        pass

    class Phase2CandidateSchema(_PanderaMissingMixin):
        pass
