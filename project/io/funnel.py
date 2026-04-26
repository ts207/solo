from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from project.io.utils import atomic_write_json, ensure_dir, read_parquet, write_parquet


def _reports_root(data_root: Path) -> Path:
    return Path(data_root) / "reports" / "phase2"


def funnel_path(run_id: str, *, data_root: Path) -> Path:
    return _reports_root(data_root) / str(run_id) / "funnel.json"


def funnel_index_path(*, data_root: Path) -> Path:
    return _reports_root(data_root) / "funnel_index.parquet"


def _stage_count(payload: Mapping[str, Any], stage: str) -> int:
    stages = payload.get("stages", {})
    if isinstance(stages, Mapping):
        value = stages.get(stage, {})
        if isinstance(value, Mapping):
            try:
                return int(value.get("count", 0) or 0)
            except (TypeError, ValueError):
                return 0
    try:
        return int(payload.get(stage, 0) or 0)
    except (TypeError, ValueError):
        return 0


def flatten_funnel(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Flatten a funnel payload to the one-row index schema."""
    return {
        "schema_version": int(payload.get("schema_version", 1) or 1),
        "run_id": str(payload.get("run_id", "")),
        "program_id": str(payload.get("program_id", "")),
        "proposal_id": str(payload.get("proposal_id", "")),
        "wrote_at": str(payload.get("wrote_at", "")),
        "generated": _stage_count(payload, "generated"),
        "feasible": _stage_count(payload, "feasible"),
        "t_gross_passed": _stage_count(payload, "t_gross_passed"),
        "t_net_passed": _stage_count(payload, "t_net_passed"),
        "mean_net_passed": _stage_count(payload, "mean_net_passed"),
        "q_passed": _stage_count(payload, "q_passed"),
        "robust_passed": _stage_count(payload, "robust_passed"),
        "cost_survival_passed": _stage_count(payload, "cost_survival_passed"),
        "promoted_research": _stage_count(payload, "promoted_research"),
        "promoted_deploy": _stage_count(payload, "promoted_deploy"),
    }


def write_funnel(run_id: str, payload: Mapping[str, Any], *, data_root: Path) -> Path:
    """Atomically write ``data/reports/phase2/<run_id>/funnel.json``."""
    path = funnel_path(run_id, data_root=data_root)
    atomic_write_json(path, dict(payload), sort_keys=True)
    return path


def append_funnel_index(payload: Mapping[str, Any], *, data_root: Path) -> Path:
    """Upsert one run into the flattened funnel index."""
    path = funnel_index_path(data_root=data_root)
    ensure_dir(path.parent)
    row = flatten_funnel(payload)
    new_row = pd.DataFrame([row])
    if path.exists():
        try:
            existing = read_parquet(path)
            if "run_id" in existing.columns:
                existing = existing[existing["run_id"].astype(str) != row["run_id"]]
            frame = pd.concat([existing, new_row], ignore_index=True)
        except Exception:
            frame = new_row
    else:
        frame = new_row
    write_parquet(frame, path)
    return path


def load_funnel(run_id: str, *, data_root: Path) -> dict[str, Any] | None:
    path = funnel_path(run_id, data_root=data_root)
    if not path.exists():
        return None
    import json

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None
