from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from project.core.config import get_data_root
from project.core.exceptions import DataIntegrityError
from project.research.services.pathing import (
    phase2_candidates_path as canonical_phase2_candidates_path,
)
from project.research.services.pathing import (
    phase2_diagnostics_path as canonical_phase2_diagnostics_path,
)


def data_root(root: Path | None = None) -> Path:
    return Path(root) if root is not None else get_data_root()


def run_dir(run_id: str, root: Path | None = None) -> Path:
    return data_root(root) / "runs" / str(run_id)


def reports_dir(root: Path | None = None) -> Path:
    return data_root(root) / "reports"


def run_manifest_path(run_id: str, root: Path | None = None) -> Path:
    return run_dir(run_id, root) / "run_manifest.json"


def research_checklist_dir(run_id: str, root: Path | None = None) -> Path:
    return run_dir(run_id, root) / "research_checklist"


def checklist_path(run_id: str, root: Path | None = None) -> Path:
    return research_checklist_dir(run_id, root) / "checklist.json"


def release_signoff_path(run_id: str, root: Path | None = None) -> Path:
    return research_checklist_dir(run_id, root) / "release_signoff.json"


def kpi_scorecard_path(run_id: str, root: Path | None = None) -> Path:
    return run_dir(run_id, root) / "kpi_scorecard.json"


def promotion_dir(run_id: str, root: Path | None = None) -> Path:
    return reports_dir(root) / "promotions" / str(run_id)


def blueprint_dir(run_id: str, root: Path | None = None) -> Path:
    return reports_dir(root) / "strategy_blueprints" / str(run_id)


def live_thesis_root(root: Path | None = None) -> Path:
    return data_root(root) / "live" / "theses"


def live_thesis_dir(run_id: str, root: Path | None = None) -> Path:
    return live_thesis_root(root) / str(run_id)


def promoted_theses_path(run_id: str, root: Path | None = None) -> Path:
    return live_thesis_dir(run_id, root) / "promoted_theses.json"


def live_thesis_index_path(root: Path | None = None) -> Path:
    return live_thesis_root(root) / "index.json"


def promotion_summary_path(run_id: str, root: Path | None = None) -> Path:
    return promotion_dir(run_id, root) / "promotion_summary.json"


def promotion_report_path(run_id: str, root: Path | None = None) -> Path:
    return promotion_dir(run_id, root) / "promotion_report.json"


def promoted_blueprints_path(run_id: str, root: Path | None = None) -> Path:
    return promotion_dir(run_id, root) / "promoted_blueprints.jsonl"


def blueprint_summary_path(run_id: str, root: Path | None = None) -> Path:
    return blueprint_dir(run_id, root) / "blueprint_summary.json"


def phase2_candidates_path(
    run_id: str,
    root: Path | None = None,
) -> Path:
    resolved_root = data_root(root)
    parquet = canonical_phase2_candidates_path(data_root=resolved_root, run_id=run_id)
    if parquet.exists():
        return parquet
    csv = parquet.with_suffix(".csv")
    if csv.exists():
        return csv
    return parquet


def phase2_diagnostics_path(
    run_id: str,
    root: Path | None = None,
) -> Path:
    resolved_root = data_root(root)
    canonical = canonical_phase2_diagnostics_path(data_root=resolved_root, run_id=run_id)
    return canonical


def load_json_dict(path: Path) -> dict[str, Any]:
    if not Path(path).exists():
        return {}
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise DataIntegrityError(f"Failed to read JSON artifact {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise DataIntegrityError(f"JSON artifact {path} did not contain an object payload")
    return payload
