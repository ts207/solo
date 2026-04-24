from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

import pandas as pd


@dataclass(frozen=True)
class EventTimingContract:
    """
    Canonical fields for event timing semantics.

    event_ts_raw: The exact sub-bar timestamp of the physical event detection.
    event_ts_snapped: The open time of the bar during which the event was detected.
    signal_bar_open_time: The open time of the bar that completes the signal.
    first_tradable_bar_open_time: The earliest bar open time where a trade can be executed.
    active_start_time: Start of the phenomenological active interval.
    active_end_time: End of the phenomenological active interval.
    effective_entry_bar_open_time: The open time of the bar where entry actually occurs (respects lag).
    """

    event_ts_raw: pd.Timestamp
    event_ts_snapped: pd.Timestamp
    signal_bar_open_time: pd.Timestamp
    first_tradable_bar_open_time: pd.Timestamp
    active_start_time: pd.Timestamp
    active_end_time: pd.Timestamp
    effective_entry_bar_open_time: pd.Timestamp


def snap_to_bar(ts: pd.Timestamp, bar_minutes: int = 5) -> pd.Timestamp:
    """Centralized snapping of timestamps to bar boundaries."""
    if pd.isna(ts):
        return pd.NaT
    # Frequency floor to snap to start of the bar
    return ts.floor(f"{bar_minutes}min")


def validate_timing_invariants(timing: EventTimingContract, entry_lag_bars: int = 1) -> None:
    """Property tests for timing semantics."""
    if pd.isna(timing.event_ts_raw):
        return

    if timing.active_start_time > timing.active_end_time:
        raise ValueError(
            f"Active interval never begins after it ends: {timing.active_start_time} > {timing.active_end_time}"
        )

    if timing.first_tradable_bar_open_time < timing.signal_bar_open_time:
        raise ValueError(
            f"Tradable signal cannot precede signal bar: {timing.first_tradable_bar_open_time} < {timing.signal_bar_open_time}"
        )

    # R1: earliest signalable/tradable bar is t+1 (detection + 1)
    if timing.first_tradable_bar_open_time <= timing.event_ts_snapped:
        raise ValueError(
            f"Tradable bar must be after detection bar open: {timing.first_tradable_bar_open_time} <= {timing.event_ts_snapped}"
        )


def compute_event_emission_timing(
    detection_ts: pd.Timestamp,
    horizon_bars: int,
    entry_lag_bars: int = 1,
    bar_minutes: int = 5,
) -> EventTimingContract:
    """
    Centralized event timing logic.
    - detection_ts: raw sub-bar detection time
    - entry_lag_bars: bars to wait before execution (minimum 1)
    """
    detection_snapped = snap_to_bar(detection_ts, bar_minutes)

    # Signal completes at the end of the detection bar (i.e. next bar open)
    signal_bar_open = detection_snapped + pd.Timedelta(minutes=bar_minutes)

    # Earliest tradable bar open
    # If entry_lag_bars = 1, we trade on the first bar AFTER the detection bar.
    # This is signal_bar_open.
    first_tradable_bar = detection_snapped + pd.Timedelta(minutes=int(entry_lag_bars) * bar_minutes)
    if first_tradable_bar < signal_bar_open:
        first_tradable_bar = signal_bar_open  # Safety floor

    active_start = first_tradable_bar
    active_end = active_start + pd.Timedelta(minutes=int(horizon_bars) * bar_minutes)

    # Effective entry respects lag
    effective_entry = first_tradable_bar

    timing = EventTimingContract(
        event_ts_raw=detection_ts,
        event_ts_snapped=detection_snapped,
        signal_bar_open_time=signal_bar_open,
        first_tradable_bar_open_time=first_tradable_bar,
        active_start_time=active_start,
        active_end_time=active_end,
        effective_entry_bar_open_time=effective_entry,
    )

    validate_timing_invariants(timing, entry_lag_bars)
    return timing


def is_observational(row: Dict[str, Any]) -> bool:
    return bool(row.get("is_observational", False))


def is_signal_eligible(row: Dict[str, Any]) -> bool:
    return bool(row.get("is_signal_eligible", True))


def is_tradable_now(row: Dict[str, Any]) -> bool:
    return bool(row.get("is_tradable_now", False))


def is_tradable_next_bar(row: Dict[str, Any]) -> bool:
    return bool(row.get("is_tradable_next_bar", True))
