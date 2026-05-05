from __future__ import annotations

from typing import Any

import pandas as pd

from project.events.emission import emit_canonical_event, to_event_row
from project.events.polarity import (
    normalize_event_side,
    normalize_polarity_semantics,
    side_to_direction,
    side_to_legacy_direction,
)

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
    "event_side",
    "event_direction",
    "magnitude",
    "polarity_semantics",
    "polarity_source",
    "magnitude_source",
    "anchor_role",
    "basis_z",
    "spread_z",
    "funding_rate_bps",
    "carry_state",
    "ms_trend_state",
    "ms_spread_state",
    "features_payload",
]


def normalize_event_direction(direction: Any) -> str:
    return side_to_legacy_direction(direction)


def direction_to_sign(direction: Any) -> int:
    return side_to_direction(direction)


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
    severity_bucket: str | None = None,
    event_side: str | None = None,
    event_direction: int | None = None,
    magnitude: float | None = None,
    polarity_semantics: str = "unknown",
    polarity_source: str = "unknown",
    magnitude_source: str = "unknown",
    anchor_role: str = "alpha_anchor",
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
    side = normalize_event_side(event_side if event_side is not None else direction)
    if event_direction is not None:
        normalized_sign = side_to_direction(side, event_direction)
    if normalized_sign == 0:
        normalized_sign = side_to_direction(side, sign)
    normalized_direction = side_to_legacy_direction(side if side != "unknown" else normalized_direction)
    row = to_event_row(
        record,
        symbol=symbol,
        direction=normalized_direction,
        sign=normalized_sign,
        severity_label=severity_bucket or severity,
    )
    row["event_side"] = side
    row["event_direction"] = int(normalized_sign)
    row["magnitude"] = magnitude if magnitude is not None else abs(float(intensity))
    row["polarity_semantics"] = normalize_polarity_semantics(polarity_semantics)
    row["polarity_source"] = str(polarity_source or "unknown")
    row["magnitude_source"] = str(magnitude_source or ("intensity" if magnitude is None else "explicit"))
    row["anchor_role"] = str(anchor_role or "alpha_anchor").strip().lower() or "alpha_anchor"
    return row
