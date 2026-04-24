from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass
class AnalyzerResult:
    name: str
    summary: dict[str, Any]
    tables: dict[str, pd.DataFrame] = field(default_factory=dict)


class BaseEventAnalyzer(ABC):
    """Base class for analyzer-v2 components.

    Each analyzer consumes an event frame and optional market context and returns
    a structured result with a summary payload plus zero or more tabular outputs.
    """

    name: str = "base"

    def validate_events(self, events: pd.DataFrame) -> pd.DataFrame:
        if events is None:
            return pd.DataFrame()
        if not isinstance(events, pd.DataFrame):
            raise TypeError("events must be a pandas DataFrame")
        return events.copy()

    def validate_market(self, market: pd.DataFrame | None) -> pd.DataFrame | None:
        if market is None:
            return None
        if not isinstance(market, pd.DataFrame):
            raise TypeError("market must be a pandas DataFrame")
        return market.copy()

    @abstractmethod
    def analyze(
        self,
        events: pd.DataFrame,
        *,
        market: pd.DataFrame | None = None,
        **kwargs: Any,
    ) -> AnalyzerResult:
        raise NotImplementedError


def ensure_timestamp(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


def resolve_event_time_column(events: pd.DataFrame) -> str:
    for candidate in ("signal_ts", "detected_ts", "eval_bar_ts", "timestamp"):
        if candidate in events.columns:
            return candidate
    raise KeyError("events is missing a usable timestamp column")


def resolve_market_time_column(market: pd.DataFrame) -> str:
    for candidate in ("timestamp", "ts", "datetime"):
        if candidate in market.columns:
            return candidate
    raise KeyError("market is missing a usable timestamp column")


def resolve_price_column(market: pd.DataFrame) -> str:
    for candidate in ("close", "mid", "price", "px_close"):
        if candidate in market.columns:
            return candidate
    raise KeyError("market is missing a usable price column")


def summarize_counts_by_group(df: pd.DataFrame, group_col: str) -> dict[str, int]:
    if group_col not in df.columns or df.empty:
        return {}
    counts = df.groupby(group_col, dropna=False).size()
    return {str(k): int(v) for k, v in counts.to_dict().items()}
