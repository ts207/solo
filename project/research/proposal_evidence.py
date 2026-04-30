from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


PASSING_EVENT_LIFT_DECISION = "advance_to_mechanism_proposal"
PASSING_SCORECARD_DECISION = "allow_event_lift"


@dataclass(frozen=True)
class EventLiftEvidence:
    row: dict[str, Any]
    path: Path


def event_lift_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return bool(value)


def _fmt_bool(value: Any) -> str:
    return "true" if event_lift_bool(value) else "false"


def event_lift_not_promotable_message(row: dict[str, Any]) -> str:
    return (
        "event_lift report is not promotable: "
        f"decision={row.get('decision')} "
        f"promotion_eligible={_fmt_bool(row.get('promotion_eligible'))} "
        f"audit_only={_fmt_bool(row.get('audit_only'))}"
    )


def load_event_lift_rows(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and isinstance(payload.get("rows"), list):
        return [row for row in payload["rows"] if isinstance(row, dict)]
    if isinstance(payload, dict):
        return [payload]
    return []


def event_lift_matches(
    row: dict[str, Any],
    *,
    mechanism_id: str,
    event_id: str,
    regime_id: str,
    symbol: str,
    direction: str,
    horizon_bars: int,
) -> bool:
    return (
        str(row.get("mechanism_id") or "") == str(mechanism_id)
        and str(row.get("event_id") or "") == str(event_id)
        and str(row.get("regime_id") or "") == str(regime_id)
        and str(row.get("symbol") or "") == str(symbol)
        and str(row.get("direction") or "").lower() == str(direction).lower()
        and int(row.get("horizon_bars") or 0) == int(horizon_bars)
    )


def event_lift_is_passing(row: dict[str, Any]) -> bool:
    return (
        str(row.get("decision") or "") == PASSING_EVENT_LIFT_DECISION
        and event_lift_bool(row.get("promotion_eligible")) is True
        and event_lift_bool(row.get("audit_only")) is False
        and str(row.get("classification") or "") != "audit_only"
        and str(row.get("scorecard_decision") or "") == PASSING_SCORECARD_DECISION
    )


def find_event_lift_evidence(
    *,
    data_root: Path,
    mechanism_id: str,
    event_id: str,
    regime_id: str,
    symbol: str,
    direction: str,
    horizon_bars: int,
    event_lift_run_id: str | None = None,
) -> EventLiftEvidence | None:
    base = data_root / "reports" / "event_lift"
    if event_lift_run_id:
        path = base / event_lift_run_id / "event_lift.json"
        if not path.exists():
            return None
        for row in load_event_lift_rows(path):
            if event_lift_matches(
                row,
                mechanism_id=mechanism_id,
                event_id=event_id,
                regime_id=regime_id,
                symbol=symbol,
                direction=direction,
                horizon_bars=horizon_bars,
            ):
                return EventLiftEvidence(row=row, path=path)
        return None

    candidates = sorted(
        base.glob("*/event_lift.json"),
        key=lambda item: (item.stat().st_mtime, str(item)),
        reverse=True,
    )
    for path in candidates:
        for row in load_event_lift_rows(path):
            if event_lift_matches(
                row,
                mechanism_id=mechanism_id,
                event_id=event_id,
                regime_id=regime_id,
                symbol=symbol,
                direction=direction,
                horizon_bars=horizon_bars,
            ) and event_lift_is_passing(row):
                return EventLiftEvidence(row=row, path=path)
    return None
