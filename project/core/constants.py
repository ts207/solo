from __future__ import annotations

import re

# Canonical annualization factors by timeframe.
BARS_PER_YEAR_BY_TIMEFRAME: dict[str, int] = {
    "1m": 365 * 24 * 60,
    "5m": 365 * 24 * 12,
    "15m": 365 * 24 * 4,
    "30m": 365 * 24 * 2,
    "60m": 365 * 24,
    "1h": 365 * 24,
    "4h": 365 * 6,
    "24h": 365,
    "1d": 365,
}

# Canonical horizon label to 5-minute bars mapping used by research tooling.
HORIZON_BARS_BY_TIMEFRAME: dict[str, int] = {
    "1m": 1,
    "5m": 1,
    "4b": 4,
    "8b": 8,
    "12b": 12,
    "15m": 3,
    "16b": 16,
    "24b": 24,
    "30m": 6,
    "60m": 12,
    "1h": 12,
    "4h": 48,
    "24h": 288,
    "1d": 288,
}

# Default horizon grid for event quality / conditional analysis at 5m base bars.
DEFAULT_EVENT_HORIZON_BARS: list[int] = [1, 3, 12]

_ARBITRARY_BAR_HORIZON_RE = re.compile(r"^(?P<bars>\d+)(?:b)?$")


def bars_per_year_for_timeframe(timeframe: str, default: int | None = None) -> int:
    key = str(timeframe or "").strip().lower()
    if default is None:
        default = BARS_PER_YEAR_BY_TIMEFRAME["5m"]
    return int(BARS_PER_YEAR_BY_TIMEFRAME.get(key, int(default)))


def parse_horizon_bars(horizon: str | int | None, default: int | None = None) -> int:
    key = str(horizon or "").strip().lower()
    if key in HORIZON_BARS_BY_TIMEFRAME:
        return int(HORIZON_BARS_BY_TIMEFRAME[key])

    match = _ARBITRARY_BAR_HORIZON_RE.fullmatch(key)
    if match:
        return int(match.group("bars"))

    if default is not None:
        return int(default)

    raise ValueError(
        "Unknown horizon label "
        f"{horizon!r}. Supported canonical labels: {list(HORIZON_BARS_BY_TIMEFRAME.keys())}; "
        "arbitrary '<N>b' bar counts are also allowed."
    )


def horizon_bars_for_label(horizon: str | int | None, default: int = 12) -> int:
    return parse_horizon_bars(horizon, default=default)
