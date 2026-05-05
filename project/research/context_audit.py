from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from project import PROJECT_ROOT

CONTEXT_COLUMNS = (
    "ms_vol_state",
    "ms_vol_confidence",
    "ms_vol_entropy",
    "ms_oi_state",
    "ms_oi_confidence",
    "ms_oi_entropy",
    "ms_funding_state",
    "ms_funding_confidence",
    "ms_funding_entropy",
    "spread_bps",
    "depth_usd",
    "expected_cost_bps",
    "microstructure_regime",
    "data_quality_flag",
)


def resolve_data_root(data_root: str | Path | None = None) -> Path:
    if data_root:
        return Path(data_root)
    return PROJECT_ROOT.parent / "data"


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {"value": value}
    except Exception as exc:  # pragma: no cover - defensive artifact reader
        return {"_read_error": str(exc), "path": str(path)}


def _candidate_paths(root: Path, run_id: str) -> list[Path]:
    return [
        root / "reports" / "edge_candidates" / run_id / "edge_candidates_normalized.parquet",
        root / "reports" / "phase2" / run_id / "phase2_candidates.parquet",
        root / "reports" / "phase2" / run_id / "top_candidates.parquet",
    ]


def _summarize_frame(frame: Any) -> dict[str, Any]:
    columns = [str(c) for c in getattr(frame, "columns", [])]
    seen = [c for c in CONTEXT_COLUMNS if c in columns]
    missing = [c for c in CONTEXT_COLUMNS if c not in columns]
    out: dict[str, Any] = {
        "row_count": int(len(frame)),
        "context_columns_seen": seen,
        "context_columns_missing": missing,
        "event_count_by_ms_vol_state": {},
        "event_count_by_microstructure_regime": {},
        "data_quality_counts": {},
    }
    try:
        if "ms_vol_state" in columns:
            counts = frame["ms_vol_state"].value_counts(dropna=False).to_dict()
            out["event_count_by_ms_vol_state"] = {str(k): int(v) for k, v in counts.items()}
        if "microstructure_regime" in columns:
            counts = frame["microstructure_regime"].value_counts(dropna=False).to_dict()
            out["event_count_by_microstructure_regime"] = {str(k): int(v) for k, v in counts.items()}
        if "data_quality_flag" in columns:
            counts = frame["data_quality_flag"].value_counts(dropna=False).to_dict()
            out["data_quality_counts"] = {str(k): int(v) for k, v in counts.items()}
    except Exception as exc:  # pragma: no cover
        out["summary_error"] = str(exc)
    return out


def build_context_audit_report(
    *,
    run_id: str,
    data_root: str | Path | None = None,
    write: bool = False,
) -> dict[str, Any]:
    root = resolve_data_root(data_root)
    candidate_path = next((path for path in _candidate_paths(root, run_id) if path.exists()), None)
    frame_summary: dict[str, Any]
    if candidate_path is None:
        frame_summary = {
            "row_count": 0,
            "context_columns_seen": [],
            "context_columns_missing": list(CONTEXT_COLUMNS),
            "event_count_by_ms_vol_state": {},
            "event_count_by_microstructure_regime": {},
            "data_quality_counts": {},
            "warning": "No candidate parquet artifact found for context audit.",
        }
    else:
        try:
            import pandas as pd

            frame_summary = _summarize_frame(pd.read_parquet(candidate_path))
        except Exception as exc:
            frame_summary = {
                "row_count": 0,
                "context_columns_seen": [],
                "context_columns_missing": list(CONTEXT_COLUMNS),
                "event_count_by_ms_vol_state": {},
                "event_count_by_microstructure_regime": {},
                "data_quality_counts": {},
                "warning": f"Could not read candidate artifact: {exc}",
            }
    phase2_diag = _read_json(root / "reports" / "phase2" / run_id / "phase2_diagnostics.json")
    payload = {
        "kind": "context_audit",
        "run_id": str(run_id),
        "data_root": str(root),
        "candidate_artifact": str(candidate_path) if candidate_path else None,
        "phase2_diagnostics": phase2_diag,
        **frame_summary,
    }
    if write:
        out = root / "reports" / "context_audit" / run_id / "context_audit.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
        payload["written_path"] = str(out)
    return payload
