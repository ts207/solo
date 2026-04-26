from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from project.core.exceptions import ContractViolationError

DEFAULT_TIMEFRAME: str = "5m"
SUPPORTED_TIMEFRAMES: tuple[str, ...] = ("1m", "5m", "15m", "1h", "4h", "1d")

_TIMEFRAME_TO_MINUTES: dict[str, int] = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "1h": 60,
    "4h": 240,
    "1d": 1440,
}

_TIMEFRAME_TO_PANDAS_FREQ: dict[str, str] = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "1h": "1h",
    "4h": "4h",
    "1d": "1D",
}

_BARS_PER_YEAR: dict[str, int] = {
    "1m": 525600,
    "5m": 105120,
    "15m": 35040,
    "1h": 8760,
    "4h": 2190,
    "1d": 365,
}


@dataclass(frozen=True)
class TimeframeSpec:
    timeframe: str
    minutes: int
    pandas_freq: str
    bars_per_year: int


def is_valid_timeframe(tf: str) -> bool:
    return str(tf or "").strip().lower() in _TIMEFRAME_TO_MINUTES


def normalize_timeframe(tf: str) -> str:
    raw = str(tf or "").strip().lower()
    if raw not in _TIMEFRAME_TO_MINUTES:
        raise ContractViolationError(
            f"Unsupported timeframe: '{tf}'. Supported timeframes: {list(SUPPORTED_TIMEFRAMES)}"
        )
    return raw


def parse_timeframes(timeframes_str: str | Iterable[str]) -> list[str]:
    if isinstance(timeframes_str, str):
        raw_values = timeframes_str.split(",")
    else:
        raw_values = list(timeframes_str)
    out: list[str] = []
    seen: set[str] = set()
    for raw in raw_values:
        token = str(raw or "").strip()
        if not token:
            continue
        if not is_valid_timeframe(token):
            continue
        normalized = normalize_timeframe(token)
        if normalized not in seen:
            out.append(normalized)
            seen.add(normalized)
    return out or [DEFAULT_TIMEFRAME]


def timeframe_spec(tf: str) -> TimeframeSpec:
    normalized = normalize_timeframe(tf)
    return TimeframeSpec(
        timeframe=normalized,
        minutes=_TIMEFRAME_TO_MINUTES[normalized],
        pandas_freq=_TIMEFRAME_TO_PANDAS_FREQ[normalized],
        bars_per_year=_BARS_PER_YEAR[normalized],
    )


def timeframe_to_minutes(tf: str) -> int:
    return timeframe_spec(tf).minutes


def timeframe_to_pandas_freq(tf: str) -> str:
    return timeframe_spec(tf).pandas_freq


def bars_dataset_name(tf: str) -> str:
    return f"bars_{normalize_timeframe(tf)}"


def ohlcv_dataset_name(tf: str) -> str:
    return f"ohlcv_{normalize_timeframe(tf)}"


def funding_dataset_name(tf: str) -> str:
    return f"funding_{normalize_timeframe(tf)}"


def bars_per_year(tf: str) -> int:
    return timeframe_spec(tf).bars_per_year


def make_ohlcv_artifact_token(timeframe: str) -> str:
    return f"raw.perp.{ohlcv_dataset_name(timeframe)}"


def make_funding_artifact_token(timeframe: str) -> str:
    return f"raw.perp.{funding_dataset_name(timeframe)}"


def make_spot_ohlcv_artifact_token(timeframe: str) -> str:
    return f"raw.spot.{ohlcv_dataset_name(timeframe)}"


def make_clean_artifact_token(timeframe: str, market: str = "perp") -> str:
    normalize_timeframe(timeframe)
    return f"clean.{market}.*"


def make_feature_artifact_token(timeframe: str, market: str = "perp", version: str = "v2") -> str:
    normalize_timeframe(timeframe)
    return f"features.{market}.{version}"
