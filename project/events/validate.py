from __future__ import annotations

from dataclasses import fields
from numbers import Number
from typing import Any, Iterable, Mapping, Sequence

from project.events.registry import get_event_definition
from project.events.schemas import EventRecord

REQUIRED_EVENT_FIELDS = tuple(field.name for field in fields(EventRecord))
REQUIRED_TIMESTAMP_FIELDS = ("eval_bar_ts", "detected_ts", "signal_ts")


def validate_event_payload(
    event: Mapping[str, Any], *, require_registry_entry: bool = True
) -> None:
    missing = [name for name in REQUIRED_EVENT_FIELDS if name not in event]
    if missing:
        raise ValueError(f"Event payload missing required fields: {missing}")

    event_type = str(event.get("event_type", "")).strip().upper()
    if not event_type:
        raise ValueError("event_type must be non-empty")
    if require_registry_entry and get_event_definition(event_type) is None:
        raise ValueError(f"event_type '{event_type}' not found in event registry")

    for field_name in REQUIRED_TIMESTAMP_FIELDS:
        if event.get(field_name) is None:
            raise ValueError(f"{field_name} must be present and non-null")

    intensity = event.get("intensity")
    if not isinstance(intensity, Number):
        raise ValueError("intensity must be numeric")

    severity = event.get("severity")
    if not isinstance(severity, Number):
        raise ValueError("severity must be numeric")

    meta = event.get("meta")
    if not isinstance(meta, Mapping):
        raise ValueError("meta must be a mapping")


def validate_event_frame_columns(columns: Iterable[str]) -> None:
    observed = set(str(column) for column in columns)
    missing = [name for name in REQUIRED_EVENT_FIELDS if name not in observed]
    if missing:
        raise ValueError(f"Event frame missing required columns: {missing}")


def validate_event_rows(
    rows: Sequence[Mapping[str, Any]], *, require_registry_entry: bool = True
) -> None:
    for row in rows:
        validate_event_payload(row, require_registry_entry=require_registry_entry)
