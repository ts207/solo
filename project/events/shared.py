from __future__ import annotations

from typing import Any

import pandas as pd

from project.events.emission import emit_canonical_event, to_event_row

EVENT_COLUMNS = [
    "event_type",
    "event_id",
    "symbol",
    "anchor_ts",
    "eval_bar_ts",
    "enter_ts",
    "detected_ts",
    "signal_ts",
    "exit_ts",
    "event_idx",
    "year",
    "event_score",
    "evt_signal_intensity",
    "severity_bucket",
    "direction",
    "sign",
    "basis_z",
    "spread_z",
    "funding_rate_bps",
    "carry_state",
    "ms_trend_state",
    "ms_spread_state",
    "features_payload",
]


def normalize_event_direction(direction: Any) -> str:
    token = str(direction or "").strip().lower()
    if token in {"up", "long", "buy", "pos", "positive", "1", "+1"}:
        return "up"
    if token in {"down", "short", "sell", "neg", "negative", "-1"}:
        return "down"
    return "non_directional"


def direction_to_sign(direction: Any) -> int:
    normalized = normalize_event_direction(direction)
    if normalized == "up":
        return 1
    if normalized == "down":
        return -1
    return 0


def format_event_id(event_type: str, symbol: str, idx: int, sub_idx: int = 0) -> str:
    return f"{event_type.lower()}_{symbol}_{idx:08d}_{sub_idx:03d}"


def emit_event(
    *,
    event_type: str,
    symbol: str,
    event_id: str,
    eval_bar_ts: pd.Timestamp,
    direction: str = "non_directional",
    sign: int = 0,
    intensity: float = 1.0,
    severity: str = "moderate",
    metadata: dict[str, Any] | None = None,
    causal: bool | None = None,
    shift_bars: int = 0,
    timeframe_minutes: int = 5,
) -> dict[str, Any]:
    """
    Standardize event emission under the milestone-2 PIT policy.

    ``shift_bars`` now means *additional* bars of delay beyond the mandatory
    next-bar signal. ``shift_bars=0`` therefore emits ``signal_ts`` on the next
    tradable bar after ``eval_bar_ts``.
    """
    merged_meta = dict(metadata or {})
    if causal is not None:
        merged_meta.setdefault("causal", bool(causal))

    normalized_direction = normalize_event_direction(direction)
    normalized_sign = direction_to_sign(normalized_direction)
    if normalized_sign == 0:
        try:
            normalized_sign = int(sign)
        except (TypeError, ValueError):
            normalized_sign = 0

    record = emit_canonical_event(
        event_type=event_type,
        asset=symbol,
        eval_bar_ts=eval_bar_ts,
        event_id=event_id,
        intensity=float(intensity),
        severity=severity,
        meta=merged_meta,
        timeframe_minutes=timeframe_minutes,
        signal_delay_bars=max(int(shift_bars), 0) + 1,
    )
    return to_event_row(
        record,
        symbol=symbol,
        direction=normalized_direction,
        sign=normalized_sign,
        severity_label=severity,
    )
