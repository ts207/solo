from __future__ import annotations

from typing import Any, Mapping

from project.events.event_aliases import resolve_event_alias
from project.events.registry import get_event_definition
from project.events.schemas import EventRecord
from project.events.timestamps import compute_canonical_timestamps

SEVERITY_TO_INT = {
    "minor": 1,
    "low": 1,
    "moderate": 2,
    "medium": 2,
    "major": 3,
    "high": 3,
    "extreme": 4,
}

INT_TO_SEVERITY = {
    value: key
    for key, value in SEVERITY_TO_INT.items()
    if key in {"minor", "moderate", "major", "extreme"}
}


def coerce_severity(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    normalized = str(value).strip().lower()
    return SEVERITY_TO_INT.get(normalized, 2)


def emit_canonical_event(
    *,
    event_type: str,
    asset: str,
    eval_bar_ts: Any,
    event_id: str,
    intensity: float = 1.0,
    severity: Any = 2,
    bar_type: str | None = None,
    observable_type: str | None = None,
    interpretation: str | None = None,
    event_family: str | None = None,
    meta: Mapping[str, Any] | None = None,
    episode_id: str | None = None,
    detector_version: str = "v1",
    event_version: str = "v1",
    timeframe_minutes: int = 5,
    signal_delay_bars: int = 1,
    next_bar_ts: Any | None = None,
) -> EventRecord:
    normalized_event_type = resolve_event_alias(str(event_type).strip().upper())
    registry_row = get_event_definition(normalized_event_type) or {}
    timestamps = compute_canonical_timestamps(
        eval_bar_ts,
        timeframe_minutes=timeframe_minutes,
        signal_delay_bars=signal_delay_bars,
        next_bar_ts=next_bar_ts,
    )
    return EventRecord(
        event_id=event_id,
        event_family=event_family or str(registry_row.get("family", "unknown")),
        event_type=normalized_event_type,
        observable_type=observable_type or str(registry_row.get("observable_type", "unknown")),
        interpretation=interpretation or str(registry_row.get("interpretation", "")),
        asset=str(asset),
        bar_type=bar_type or str(registry_row.get("bar_type", "bar_close")),
        eval_bar_ts=timestamps.eval_bar_ts,
        detected_ts=timestamps.detected_ts,
        signal_ts=timestamps.signal_ts,
        intensity=float(intensity),
        severity=coerce_severity(severity),
        episode_id=episode_id,
        detector_version=detector_version,
        event_version=event_version,
        meta=dict(meta or {}),
    )


def to_event_row(
    record: EventRecord,
    *,
    symbol: str,
    direction: str = "non_directional",
    sign: int = 0,
    severity_label: str | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    label = severity_label or INT_TO_SEVERITY.get(int(record.severity), "moderate")
    payload = {
        "event_type": record.event_type,
        "event_id": record.event_id,
        "symbol": symbol,
        "anchor_ts": record.eval_bar_ts,
        "eval_bar_ts": record.eval_bar_ts,
        "enter_ts": record.signal_ts,
        "detected_ts": record.detected_ts,
        "signal_ts": record.signal_ts,
        "exit_ts": record.signal_ts,
        "event_score": float(record.intensity),
        "evt_signal_intensity": float(record.intensity),
        "severity_bucket": label,
        "direction": direction,
        "sign": int(sign),
        "year": int(record.eval_bar_ts.year),
        "timestamp": record.signal_ts,
    }
    merged_meta = dict(record.meta)
    merged_meta.update(dict(metadata or {}))
    payload.update(merged_meta)
    return payload
