from __future__ import annotations

import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

from project.artifacts import (
    checklist_path,
    load_json_dict,
    promoted_blueprints_path,
    promotion_report_path,
)


def checklist_decision(run_id: str, data_root: Path) -> str:
    payload = load_json_dict(checklist_path(run_id, data_root))
    if not payload:
        return "missing"
    return str(payload.get("decision", "missing")).strip().upper() or "missing"


def load_candidate_detail(source_path: Path, candidate_id: str) -> Dict[str, object]:
    if not source_path.exists():
        return {}
    normalized_candidate_id = str(candidate_id).strip()
    if not normalized_candidate_id:
        return {}
    if source_path.suffix.lower() == ".json":
        try:
            payload = json.loads(source_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return {}
        if isinstance(payload, dict):
            candidates = payload.get("candidates", [])
            if isinstance(candidates, list):
                for item in candidates:
                    if (
                        isinstance(item, dict)
                        and str(item.get("candidate_id", "")).strip() == normalized_candidate_id
                    ):
                        return dict(item)
            if str(payload.get("candidate_id", "")).strip() == normalized_candidate_id:
                return dict(payload)
            return {}
        if isinstance(payload, list):
            for item in payload:
                if (
                    isinstance(item, dict)
                    and str(item.get("candidate_id", "")).strip() == normalized_candidate_id
                ):
                    return dict(item)
    if source_path.suffix.lower() == ".csv":
        try:
            df = pd.read_csv(source_path)
        except (OSError, UnicodeDecodeError, pd.errors.ParserError):
            return {}
        if df.empty:
            return {}
        if "candidate_id" in df.columns:
            matched = df[df["candidate_id"].astype(str).str.strip() == normalized_candidate_id]
            if not matched.empty:
                return matched.iloc[0].to_dict()
        return {}
    if source_path.suffix.lower() == ".parquet":
        try:
            df = pd.read_parquet(source_path)
        except (ImportError, OSError, ValueError):
            return {}
        if df.empty:
            return {}
        if "candidate_id" in df.columns:
            matched = df[df["candidate_id"].astype(str).str.strip() == normalized_candidate_id]
            if not matched.empty:
                return matched.iloc[0].to_dict()
        return {}
    return {}


def load_promoted_blueprints(
    run_id: str, data_root: Path
) -> Tuple[List[Dict[str, object]], Dict[str, Path]]:
    promoted_path = promoted_blueprints_path(run_id, data_root)
    report_path = promotion_report_path(run_id, data_root)
    blueprints: List[Dict[str, object]] = []
    if promoted_path.exists():
        for line in promoted_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            if isinstance(payload, dict):
                blueprints.append(payload)
    report_by_id: Dict[str, Dict[str, object]] = {}
    if report_path.exists():
        report_payload = load_json_dict(report_path)
        tested = report_payload.get("tested", []) if isinstance(report_payload, dict) else []
        if isinstance(tested, list):
            for row in tested:
                if not isinstance(row, dict):
                    continue
                blueprint_id = str(row.get("blueprint_id", "")).strip()
                if blueprint_id:
                    report_by_id[blueprint_id] = row

    rows: List[Dict[str, object]] = []
    for blueprint in blueprints:
        blueprint_id = str(blueprint.get("id", "")).strip()
        promotion = (
            blueprint.get("promotion", {}) if isinstance(blueprint.get("promotion"), dict) else {}
        )
        if not promotion and blueprint_id:
            promotion = report_by_id.get(blueprint_id, {})
        rows.append(
            {
                "blueprint": blueprint,
                "promotion": promotion if isinstance(promotion, dict) else {},
            }
        )
    return rows, {"promoted_path": promoted_path, "report_path": report_path}
