from __future__ import annotations

from typing import Iterable, Sequence

ENGINE_ARTIFACT_SCHEMA_VERSION = "engine_artifact_v2"
STRATEGY_FRAME_SCHEMA_VERSION = "strategy_frame_v2"
TRACE_SCHEMA_VERSION = "strategy_trace_v2"
PORTFOLIO_FRAME_SCHEMA_VERSION = "portfolio_frame_v2"

import pandas as pd


STRATEGY_FRAME_REQUIRED_COLUMNS: tuple[str, ...] = (
    "timestamp",
    "symbol",
    "strategy",
    "signal_position",
    "requested_position_scale",
    "target_position",
    "executed_position",
    "prior_executed_position",
    "fill_mode",
    "gross_pnl",
    "transaction_cost",
    "slippage_cost",
    "funding_pnl",
    "borrow_cost",
    "net_pnl",
    "close",
    "bar_return_close_to_close",
    "turnover",
)

TRACE_REQUIRED_COLUMNS: tuple[str, ...] = (
    "timestamp",
    "symbol",
    "strategy",
    "signal_position",
    "target_position",
    "executed_position",
    "prior_executed_position",
    "fill_mode",
    "gross_pnl",
    "net_pnl",
    "turnover",
)

PORTFOLIO_FRAME_REQUIRED_COLUMNS: tuple[str, ...] = (
    "timestamp",
    "portfolio_net_pnl",
    "portfolio_equity",
    "portfolio_equity_return",
)


class SchemaValidationError(ValueError):
    """Raised when an engine artifact does not satisfy the canonical schema."""


def _missing_columns(df: pd.DataFrame, required_columns: Sequence[str]) -> list[str]:
    return [col for col in required_columns if col not in df.columns]


def _ensure_timestamp_column(df: pd.DataFrame, frame_name: str) -> None:
    if "timestamp" not in df.columns:
        raise SchemaValidationError(f"{frame_name} is missing required column: timestamp")
    ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    if ts.isna().any():
        bad = int(ts.isna().sum())
        raise SchemaValidationError(
            f"{frame_name} has {bad} invalid timestamp value(s) in column 'timestamp'."
        )


def validate_required_columns(
    df: pd.DataFrame,
    required_columns: Iterable[str],
    *,
    frame_name: str,
) -> None:
    missing = _missing_columns(df, tuple(required_columns))
    if missing:
        raise SchemaValidationError(
            f"{frame_name} is missing required columns: {', '.join(sorted(missing))}"
        )
    _ensure_timestamp_column(df, frame_name)


def validate_strategy_frame_schema(df: pd.DataFrame, *, frame_name: str = "strategy frame") -> None:
    validate_required_columns(df, STRATEGY_FRAME_REQUIRED_COLUMNS, frame_name=frame_name)


def validate_trace_schema(df: pd.DataFrame, *, frame_name: str = "strategy trace") -> None:
    validate_required_columns(df, TRACE_REQUIRED_COLUMNS, frame_name=frame_name)


def validate_portfolio_frame_schema(
    df: pd.DataFrame, *, frame_name: str = "portfolio frame"
) -> None:
    validate_required_columns(df, PORTFOLIO_FRAME_REQUIRED_COLUMNS, frame_name=frame_name)
