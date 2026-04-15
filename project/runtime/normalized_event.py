from __future__ import annotations

from project.core.coercion import safe_float, safe_int, as_bool

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

_DEFAULT_LANE_ID = "alpha_5s"
_DEFAULT_VENUE_ID = "bybit"
_DEFAULT_ROLE = "alpha"
_DEFAULT_PROVENANCE = "market"
_MAX_ISSUE_EXAMPLES = 20


@dataclass(frozen=True)
class NormalizedEvent:
    event_id: str
    event_type: str
    lane_id: str
    source_id: str
    source_seq: int
    event_time_us: int
    recv_time_us: int
    instrument_id: str
    venue_id: str
    role: str
    provenance: str
    order_id: str = ""


def to_us(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, str):
        token = value.strip()
        if not token:
            return None
        try:
            if token.isdigit() or (token.startswith("-") and token[1:].isdigit()):
                value = int(token)
            else:
                dt = datetime.fromisoformat(token.replace("Z", "+00:00"))
                return int(dt.timestamp() * 1_000_000)
        except Exception:
            return None

    if hasattr(value, "to_pydatetime"):
        try:
            dt = value.to_pydatetime()
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1_000_000)
        except Exception:
            return None

    if isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1_000_000)

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        value = int(value)

    if isinstance(value, int):
        abs_value = abs(int(value))
        if abs_value >= 10**17:
            return int(value) // 1000  # ns -> us
        if abs_value >= 10**14:
            return int(value)  # us
        if abs_value >= 10**11:
            return int(value) * 1000  # ms -> us
        if abs_value >= 10**8:
            return int(value) * 1_000_000  # s -> us
        return int(value)
    return None


def _first_timestamp_us(row: Mapping[str, Any], fields: Iterable[str]) -> Optional[int]:
    for field in fields:
        if field not in row:
            continue
        value = to_us(row.get(field))
        if value is not None:
            return int(value)
    return None


def normalize_event_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    max_events: Optional[int] = None,
) -> Tuple[List[NormalizedEvent], List[str]]:
    events: List[NormalizedEvent] = []
    issues: List[str] = []
    max_n = int(max_events) if max_events is not None else 0

    for idx, raw in enumerate(rows):
        if max_n > 0 and idx >= max_n:
            break
        row = dict(raw)
        event_time_us = _first_timestamp_us(
            row,
            ["enter_ts", "eval_bar_ts", "timestamp", "phenom_enter_ts", "signal_ts", "detected_ts"],
        )
        recv_time_us = _first_timestamp_us(
            row,
            ["detected_ts", "signal_ts", "timestamp", "enter_ts", "eval_bar_ts"],
        )
        if event_time_us is None or recv_time_us is None:
            if len(issues) < _MAX_ISSUE_EXAMPLES:
                issues.append(
                    f"row[{idx}] missing event/recv timestamps: "
                    f"event_time={event_time_us} recv_time={recv_time_us}"
                )
            continue

        event_type = str(row.get("event_type", "")).strip() or "unknown_event_type"
        symbol = str(row.get("symbol", "")).strip() or "UNKNOWN"
        lane_id = str(row.get("lane_id", "")).strip() or _DEFAULT_LANE_ID
        source_id = str(row.get("source_id", "")).strip() or f"{event_type}:{symbol}"
        source_seq = safe_int(row.get("source_seq"), idx)
        event_id = str(row.get("event_id", "")).strip() or f"{event_type}_{symbol}_{idx:08d}"
        venue_id = str(row.get("venue_id", "")).strip() or _DEFAULT_VENUE_ID
        role = str(row.get("role", "")).strip().lower() or _DEFAULT_ROLE
        provenance = (
            str(
                row.get("provenance", "")
                or row.get("feature_provenance", "")
                or row.get("source_provenance", "")
            )
            .strip()
            .lower()
            or _DEFAULT_PROVENANCE
        )

        events.append(
            NormalizedEvent(
                event_id=event_id,
                event_type=event_type,
                lane_id=lane_id,
                source_id=source_id,
                source_seq=source_seq,
                event_time_us=int(event_time_us),
                recv_time_us=int(recv_time_us),
                instrument_id=symbol,
                venue_id=venue_id,
                role=role,
                provenance=provenance,
                order_id=str(row.get("order_id", "")).strip(),
            )
        )

    events.sort(
        key=lambda e: (
            int(e.recv_time_us),
            int(e.event_time_us),
            str(e.source_id),
            int(e.source_seq),
            str(e.event_id),
        )
    )
    return events, issues


def event_to_record(event: NormalizedEvent) -> Dict[str, Any]:
    return dict(asdict(event))


def events_to_records(events: Iterable[NormalizedEvent]) -> List[Dict[str, Any]]:
    return [event_to_record(event) for event in events]


def normalized_events_from_frame(
    frame: pd.DataFrame,
    *,
    max_events: Optional[int] = None,
) -> List[NormalizedEvent]:
    if frame.empty:
        return []

    rows = frame
    max_n = int(max_events) if max_events is not None else 0
    if max_n > 0:
        rows = frame.iloc[:max_n]

    events: List[NormalizedEvent] = []
    for row in rows.itertuples(index=False):
        events.append(
            NormalizedEvent(
                event_id=str(getattr(row, "event_id", "")),
                event_type=str(getattr(row, "event_type", "") or "unknown_event_type"),
                lane_id=str(getattr(row, "lane_id", "") or "alpha_5s"),
                source_id=str(getattr(row, "source_id", "")),
                source_seq=int(getattr(row, "source_seq", 0) or 0),
                event_time_us=int(getattr(row, "event_time_us", 0) or 0),
                recv_time_us=int(getattr(row, "recv_time_us", 0) or 0),
                instrument_id=str(getattr(row, "instrument_id", "") or "UNKNOWN"),
                venue_id=str(getattr(row, "venue_id", "") or _DEFAULT_VENUE_ID),
                role=str(getattr(row, "role", "") or "alpha"),
                provenance=str(getattr(row, "provenance", "") or "market"),
                order_id=str(getattr(row, "order_id", "") or ""),
            )
        )
    return events
