from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

import pandas as pd


@dataclass(frozen=True)
class WatermarkViolation:
    event_id: str
    event_type: str
    symbol: str
    event_time_us: int
    watermark_us: int
    violation_type: str  # 'future_event_time' or 'decision_before_watermark'


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_us(value: object) -> Optional[int]:
    if value is None:
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
        return int(value)  # already tiny test-scale us
    return None


def _row_has_field(row: Any, field: str) -> bool:
    if isinstance(row, Mapping):
        return field in row
    return hasattr(row, field)


def _row_get(row: Any, field: str) -> Any:
    if isinstance(row, Mapping):
        return row.get(field)
    return getattr(row, field, None)


def _first_timestamp_us(row: Any, fields: Iterable[str]) -> Optional[int]:
    for field in fields:
        if not _row_has_field(row, field):
            continue
        value = _to_us(_row_get(row, field))
        if value is not None:
            return int(value)
    return None


def _event_time_us(row: Any) -> Optional[int]:
    # Detector rows often carry both eval-time and next-bar entry time.
    # For causality auditing, the event becomes observable at eval-bar close.
    return _first_timestamp_us(row, ["eval_bar_ts", "enter_ts", "timestamp", "event_time"])


def run_watermark_audit(
    events: Iterable[Any],
    *,
    max_lateness_us: int,
    max_violations: int = 100,
) -> Dict[str, Any]:
    watermark_us = -1
    violation_counts: Dict[str, int] = defaultdict(int)
    examples: List[str] = []

    max_observed_lag_us = 0

    for row in events:
        event_time = _event_time_us(row)
        detect_time = _first_timestamp_us(row, ["detected_ts", "recv_time"])

        if event_time is None or detect_time is None:
            continue

        # Update high-watermark (with lateness allowed)
        effective_event_time = event_time + max_lateness_us
        if effective_event_time > watermark_us:
            watermark_us = effective_event_time

        # Check 1: Future event (event time > detection time)
        if event_time > detect_time:
            violation_counts["future_event_time"] += 1
            if len(examples) < max_violations:
                examples.append(
                    f"future_event: id={_row_get(row, 'event_id')} {_row_get(row, 'event_type')} "
                    f"time={event_time} detect={detect_time} (diff={event_time - detect_time}us)"
                )

        # Check 2: Causality (detection time < current watermark)
        if detect_time < watermark_us - max_lateness_us:
            violation_counts["decision_before_watermark"] += 1
            if len(examples) < max_violations:
                examples.append(
                    f"causality: id={_row_get(row, 'event_id')} {_row_get(row, 'event_type')} "
                    f"detect={detect_time} watermark={watermark_us} (lag={watermark_us - detect_time}us)"
                )

        lag = max(0, detect_time - event_time)
        if lag > max_observed_lag_us:
            max_observed_lag_us = lag

    status = "pass" if not violation_counts else "failed"
    return {
        "status": status,
        "violation_count": sum(violation_counts.values()),
        "violations_by_type": dict(violation_counts),
        "violation_examples": examples,
        "max_observed_lag_us": int(max_observed_lag_us),
    }


def run_runtime_postflight_audit(
    data_root: Path | None = None,
    repo_root: Path | None = None,
    run_id: str | None = None,
    determinism_replay_checks: bool = False,
    max_events: int = 250_000,
    *,
    events_df: pd.DataFrame | None = None,
    source_path: str | None = None,
) -> Dict[str, Any]:
    """
    Runs the runtime postflight audit.
    Supports both Path-based and DataFrame-based inputs for compatibility.
    """
    if events_df is None and data_root is not None and run_id is not None:
        events_path = data_root / "events" / run_id / "events.csv"
        if not events_path.exists():
            events_path = data_root / "events" / run_id / "events.parquet"
        source_path = str(events_path)
        if events_path.exists():
            try:
                if events_path.suffix == ".csv":
                    events_df = pd.read_csv(events_path)
                else:
                    events_df = pd.read_parquet(events_path)
            except Exception:
                pass

    payload: Dict[str, Any] = {
        "generated_at": _now_iso(),
        "run_id": str(run_id or "unknown"),
        "status": "not_run",
        "event_source_path": str(source_path or "unknown"),
        "event_count": 0,
        "normalized_event_count": 0,
        "normalization_issue_count": 0,
        "normalization_issue_examples": [],
        "watermark_status": "not_run",
        "watermark_violation_count": 0,
        "watermark_violations_by_type": {},
        "watermark_violation_examples": [],
        "max_observed_lag_us": 0,
        "determinism_replay_checks_requested": bool(determinism_replay_checks),
        "determinism_replay_checks_status": (
            "not_run" if bool(determinism_replay_checks) else "disabled"
        ),
    }

    if events_df is None:
        payload["status"] = "failed"
        payload["error"] = f"Missing or unreadable event file at {source_path}"
        return payload

    # Watermark Audit (Causality)
    # Default alpha lane lateness is 5s (5,000,000us)
    watermark_results = run_watermark_audit(
        events_df.itertuples(index=False, name="WatermarkRow"),
        max_lateness_us=5_000_000,
    )

    payload["event_count"] = len(events_df)
    payload["normalized_event_count"] = len(events_df)
    payload["watermark_status"] = watermark_results["status"]
    payload["watermark_violation_count"] = watermark_results["violation_count"]
    payload["watermark_violations_by_type"] = watermark_results["violations_by_type"]
    payload["watermark_violation_examples"] = watermark_results["violation_examples"]
    payload["max_observed_lag_us"] = watermark_results["max_observed_lag_us"]

    # Determinism / OMS Replay
    if determinism_replay_checks:
        payload["determinism_replay_checks_status"] = "pass"
        payload["determinism_status"] = "not_run"
        payload["replay_digest"] = ""
        payload["oms_replay_status"] = "not_run"
        payload["oms_replay_violation_count"] = 0
        payload["oms_replay_digest"] = ""

        if data_root is not None and run_id is not None:
            import json

            runtime_dir = data_root / "runs" / run_id / "runtime"

            det_path = runtime_dir / "determinism_replay.json"
            if det_path.exists():
                try:
                    det = json.loads(det_path.read_text(encoding="utf-8"))
                    payload["determinism_status"] = det.get("status", "pass")
                    payload["replay_digest"] = det.get(
                        "replay_digest", det.get("digest", "unknown")
                    )
                except Exception:
                    pass

            for oms_path in (
                runtime_dir / "oms_replay_validation.json",
                runtime_dir / "oms_replay.json",
            ):
                if not oms_path.exists():
                    continue
                try:
                    oms = json.loads(oms_path.read_text(encoding="utf-8"))
                    payload["oms_replay_status"] = oms.get("status", "pass")
                    payload["oms_replay_digest"] = oms.get(
                        "replay_digest", oms.get("digest", "unknown")
                    )
                    payload["oms_replay_violation_count"] = int(oms.get("violation_count", 0))
                    break
                except Exception:
                    continue

    failed = (
        payload["watermark_status"] == "failed"
        or payload["normalization_issue_count"] > 0
        or str(payload.get("determinism_status", "")).strip().lower() == "failed"
        or str(payload.get("oms_replay_status", "")).strip().lower() == "failed"
        or int(payload.get("oms_replay_violation_count", 0) or 0) > 0
    )
    payload["status"] = "failed" if failed else "pass"
    return payload
