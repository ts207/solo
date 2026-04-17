from __future__ import annotations

import contextlib
import json
import logging
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import pandas as pd

from project import PROJECT_ROOT
from project.core.config import get_data_root
from project.io.utils import read_table_auto

_LOG = logging.getLogger(__name__)


def _path_or_none(value: str | None) -> Path | None:
    if value is None:
        return None
    text = str(value).strip()
    return Path(text) if text else None


@contextmanager
def _scratch_dir(preferred: str | None) -> Iterator[Path]:
    preferred_path = _path_or_none(preferred)
    if preferred_path is not None:
        preferred_path.mkdir(parents=True, exist_ok=True)
        yield preferred_path
        return
    with tempfile.TemporaryDirectory(prefix="edge_chatgpt_") as tmp_dir:
        yield Path(tmp_dir)


def _resolve_data_root(value: str | None) -> Path:
    return _path_or_none(value) or get_data_root()


def _read_json_dict(path: Path, *, include_errors: bool = False) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        _LOG.warning("Failed to read JSON artifact %s: %s", path, exc)
        if include_errors:
            return {
                "__invalid_json__": True,
                "__error__": str(exc),
                "__path__": str(path),
            }
        return {}
    return payload if isinstance(payload, dict) else {}


def _invalid_run_summary(path: Path, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    raw = dict(payload or {})
    run_id = path.parent.name
    error = str(raw.get("__error__", "")).strip()
    return {
        "run_id": run_id,
        "program_id": None,
        "status": "invalid_manifest",
        "mechanical_outcome": None,
        "checklist_decision": None,
        "failed_stage": None,
        "objective_name": None,
        "objective_id": None,
        "promotion_profile": None,
        "experiment_type": None,
        "start": None,
        "end": None,
        "finished_at": None,
        "started_at": None,
        "planned_stage_count": None,
        "completed_stage_count": None,
        "artifact_count": None,
        "candidate_count": None,
        "promoted_count": None,
        "normalized_symbols": [],
        "normalized_timeframes": [],
        "symbols_label": "",
        "manifest_error": error,
        "manifest_path": str(path),
    }


def _read_table(path: Path) -> pd.DataFrame:
    return read_table_auto(path)


def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, dict):
        return {str(key): _clean_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_clean_value(item) for item in value]
    if hasattr(value, "item"):
        try:
            return _clean_value(value.item())
        except Exception:
            pass
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _sort_records(records: list[dict[str, Any]], *keys: str) -> list[dict[str, Any]]:
    return sorted(
        records,
        key=lambda row: tuple(str(row.get(key) or "") for key in keys),
        reverse=True,
    )


def _safe_int(value: Any) -> int:
    try:
        if pd.isna(value):
            return 0
    except Exception:
        pass
    try:
        return int(value or 0)
    except Exception:
        return 0


def _safe_float(value: Any) -> float | None:
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return None


def _first_present(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value in (None, ""):
            continue
        try:
            if pd.isna(value):
                continue
        except Exception:
            pass
        return value
    return None


def _per_trade_to_bps(value: Any) -> float | None:
    numeric = _safe_float(value)
    if numeric is None:
        return None
    return round(numeric * 10_000.0, 4)


def _repo_root() -> Path:
    return PROJECT_ROOT.parent


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _normalize_timeout_sec(value: Any) -> int:
    if value is None:
        return 300
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return 300
    return max(15, min(3600, normalized))


def _normalize_limit(value: Any) -> int:
    if value is None:
        return 8
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return 8
    return max(1, min(24, normalized))


def _parse_json_like(value: Any) -> Any:
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text[:1] in {"{", "["}:
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return value
    return value


def _normalize_summary(value: Any) -> dict[str, Any]:
    candidate = _parse_json_like(value)
    if candidate is None:
        return {}
    if isinstance(candidate, dict):
        return _clean_value(candidate)
    if isinstance(candidate, list):
        with contextlib.suppress(TypeError, ValueError):
            return _clean_value(dict(candidate))
        return {"items": _clean_value(candidate)}
    return {"text": _clean_value(candidate)}


def _normalize_sections(value: Any) -> list[dict[str, Any]]:
    candidate = _parse_json_like(value)
    if candidate is None:
        return []
    if isinstance(candidate, dict):
        if "heading" in candidate or "body" in candidate:
            return [
                {
                    "heading": str(candidate.get("heading") or "Details"),
                    "body": str(_clean_value(candidate.get("body")) or ""),
                }
            ]
        return [
            {
                "heading": str(key),
                "body": str(_clean_value(item) or ""),
            }
            for key, item in candidate.items()
        ]
    if isinstance(candidate, list):
        normalized: list[dict[str, Any]] = []
        for index, item in enumerate(candidate, start=1):
            if isinstance(item, dict):
                heading = item.get("heading") or item.get("title") or f"Section {index}"
                body = item.get("body")
                if body in (None, ""):
                    remainder = {
                        str(key): _clean_value(val)
                        for key, val in item.items()
                        if key not in {"heading", "title", "body"}
                    }
                    body = json.dumps(remainder, indent=2, sort_keys=True) if remainder else ""
                normalized.append({"heading": str(heading), "body": str(_clean_value(body) or "")})
            else:
                normalized.append(
                    {
                        "heading": f"Section {index}",
                        "body": str(_clean_value(item) or ""),
                    }
                )
        return normalized
    return [{"heading": "Details", "body": str(_clean_value(candidate) or "")}]
