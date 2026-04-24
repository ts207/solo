from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from project.core.config import get_data_root
from project.research.validation.manifest import load_manifest

_LOG = logging.getLogger(__name__)

def list_runs(stage: Optional[str] = None, data_root: Optional[Path] = None) -> List[Dict[str, Any]]:
    root = data_root or get_data_root()
    runs = []

    # Stages are stored in different places, but we can look for artifact_manifest.json
    # reports/phase2, reports/validation, reports/promotions
    search_paths = [
        root / "reports" / "phase2",
        root / "reports" / "validation",
        root / "reports" / "promotions"
    ]

    for base in search_paths:
        if not base.exists():
            continue
        for run_dir in base.iterdir():
            if not run_dir.is_dir():
                continue
            manifest_path = run_dir / "artifact_manifest.json"
            if manifest_path.exists():
                try:
                    m = load_manifest(manifest_path)
                    if stage is None or m.stage == stage:
                        runs.append(m.to_dict())
                except Exception as exc:
                    _LOG.warning("Failed loading artifact manifest %s: %s", manifest_path, exc)
                    invalid_row = {
                        "run_id": run_dir.name,
                        "stage": "invalid_manifest",
                        "created_at": "",
                        "manifest_path": str(manifest_path),
                        "manifest_error": str(exc),
                    }
                    if stage is None or invalid_row["stage"] == stage:
                        runs.append(invalid_row)
    return sorted(runs, key=lambda x: x["created_at"], reverse=True)

def compare_manifests(run_id_a: str, run_id_b: str, stage: str, data_root: Optional[Path] = None) -> Dict[str, Any]:
    root = data_root or get_data_root()
    # Find directories
    path_map = {
        "discover": "phase2",
        "validate": "validation",
        "promote": "promotions"
    }
    stage_dir = path_map.get(stage)
    if not stage_dir:
        raise ValueError(f"Unsupported stage for comparison: {stage}")

    path_a = root / "reports" / stage_dir / run_id_a / "artifact_manifest.json"
    path_b = root / "reports" / stage_dir / run_id_b / "artifact_manifest.json"

    if not path_a.exists() or not path_b.exists():
        raise FileNotFoundError("One or both run manifests missing")

    ma = load_manifest(path_a)
    mb = load_manifest(path_b)

    return {
        "run_id_a": run_id_a,
        "run_id_b": run_id_b,
        "stage": stage,
        "created_at_a": ma.created_at,
        "created_at_b": mb.created_at,
        "artifact_diff": {
            "only_in_a": [k for k in ma.artifacts if k not in mb.artifacts],
            "only_in_b": [k for k in mb.artifacts if k not in ma.artifacts],
            "both": [k for k in ma.artifacts if k in mb.artifacts]
        },
        "upstream_diff": {
            "only_in_a": [r for r in ma.upstream_run_ids if r not in mb.upstream_run_ids],
            "only_in_b": [r for r in mb.upstream_run_ids if r not in ma.upstream_run_ids],
        }
    }
