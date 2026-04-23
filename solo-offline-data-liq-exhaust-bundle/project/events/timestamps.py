from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class CanonicalEventTimestamps:
    """Canonical milestone-2 event timestamps.

    eval_bar_ts
        Bar close timestamp at which all detector inputs are fully known.
    detected_ts
        Equal to ``eval_bar_ts`` under the milestone-2 policy.
    signal_ts
        First tradable timestamp after detection.
    """

    eval_bar_ts: pd.Timestamp
    detected_ts: pd.Timestamp
    signal_ts: pd.Timestamp


def _coerce_utc_timestamp(value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if pd.isna(ts):
        raise ValueError("timestamp value cannot be NaT")
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts


def next_bar_timestamp(
    eval_bar_ts: Any,
    *,
    timeframe_minutes: int = 5,
    delay_bars: int = 1,
    next_bar_ts: Any | None = None,
) -> pd.Timestamp:
    """Return the first tradable timestamp after an evaluation bar.

    ``delay_bars`` is floored at 1 because a detector cannot emit a signal on the
    same timestamp at which the bar closes.
    """

    eval_ts = _coerce_utc_timestamp(eval_bar_ts)
    if next_bar_ts is not None:
        signal_ts = _coerce_utc_timestamp(next_bar_ts)
        if signal_ts <= eval_ts:
            raise ValueError("next_bar_ts must be strictly after eval_bar_ts")
        return signal_ts

    bars = max(int(delay_bars), 1)
    return eval_ts + pd.Timedelta(minutes=int(timeframe_minutes) * bars)


def compute_canonical_timestamps(
    eval_bar_ts: Any,
    *,
    timeframe_minutes: int = 5,
    signal_delay_bars: int = 1,
    next_bar_ts: Any | None = None,
) -> CanonicalEventTimestamps:
    eval_ts = _coerce_utc_timestamp(eval_bar_ts)
    signal_ts = next_bar_timestamp(
        eval_ts,
        timeframe_minutes=timeframe_minutes,
        delay_bars=signal_delay_bars,
        next_bar_ts=next_bar_ts,
    )
    return CanonicalEventTimestamps(
        eval_bar_ts=eval_ts,
        detected_ts=eval_ts,
        signal_ts=signal_ts,
    )


def normalize_timestamp_fields(
    event: dict[str, Any],
    *,
    timeframe_minutes: int = 5,
    signal_delay_bars: int = 1,
    next_bar_ts: Any | None = None,
) -> dict[str, Any]:
    """Fill canonical timing fields on a mutable event mapping.

    Existing timestamp values are normalized to UTC. Missing ``detected_ts`` and
    ``signal_ts`` values are populated from the canonical milestone-2 policy.
    """

    if "eval_bar_ts" not in event:
        raise ValueError("event must contain eval_bar_ts")

    eval_ts = _coerce_utc_timestamp(event["eval_bar_ts"])
    detected_raw = event.get("detected_ts", eval_ts)
    signal_raw = event.get("signal_ts")

    detected_ts = _coerce_utc_timestamp(detected_raw)
    if signal_raw is None:
        signal_ts = next_bar_timestamp(
            eval_ts,
            timeframe_minutes=timeframe_minutes,
            delay_bars=signal_delay_bars,
            next_bar_ts=next_bar_ts,
        )
    else:
        signal_ts = _coerce_utc_timestamp(signal_raw)

    event["eval_bar_ts"] = eval_ts
    event["detected_ts"] = detected_ts
    event["signal_ts"] = signal_ts
    return event
