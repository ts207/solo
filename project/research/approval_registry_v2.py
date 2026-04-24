from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping

import yaml

from project import PROJECT_ROOT
from project.events.registry import load_milestone_event_registry
from project.research.approval_workflow_v2 import ApprovalDecision

DEFAULT_REGISTRY_PATH = PROJECT_ROOT.parent / "spec" / "events" / "registry.yaml"


def _normalize_registry_key(key: str, row: Mapping[str, Any]) -> str:
    return str(row.get("event_type") or key).strip().upper()


def build_registry_status_snapshot(
    decisions: Mapping[str, ApprovalDecision],
    *,
    registry: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    current = dict(registry or load_milestone_event_registry())
    snapshot: dict[str, dict[str, Any]] = {}
    for key, row in current.items():
        norm_key = _normalize_registry_key(key, row)
        item = dict(row)
        item["event_type"] = norm_key
        decision = decisions.get(norm_key)
        if decision is not None:
            item["prior_status"] = str(decision.current_status)
            item["status"] = str(decision.recommended_status)
            item["approval"] = {
                "approved": bool(decision.approved),
                "reasons": list(decision.reasons),
                "metrics": dict(decision.metrics),
            }
        snapshot[norm_key] = item
    for key, decision in decisions.items():
        norm_key = str(key).strip().upper()
        if norm_key in snapshot:
            continue
        snapshot[norm_key] = {
            "event_type": norm_key,
            "family": "unknown",
            "status": str(decision.recommended_status),
            "prior_status": str(decision.current_status),
            "approval": {
                "approved": bool(decision.approved),
                "reasons": list(decision.reasons),
                "metrics": dict(decision.metrics),
            },
        }
    return snapshot


def write_registry_status_artifacts(
    decisions: Mapping[str, ApprovalDecision],
    *,
    output_dir: str | Path,
    registry_path: str | Path = DEFAULT_REGISTRY_PATH,
    registry: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Path]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    snapshot = build_registry_status_snapshot(decisions, registry=registry)
    yaml_path = out_dir / "registry_status_snapshot.yaml"
    json_path = out_dir / "approval_decisions.json"
    promoted_path = out_dir / "promoted_registry.yaml"

    ordered_yaml = {k.lower(): v for k, v in sorted(snapshot.items())}
    yaml_path.write_text(yaml.safe_dump(ordered_yaml, sort_keys=False), encoding="utf-8")

    decisions_payload = {key: asdict(value) for key, value in sorted(decisions.items())}
    json_path.write_text(json.dumps(decisions_payload, indent=2, sort_keys=True), encoding="utf-8")

    promoted_registry: dict[str, dict[str, Any]] = {}
    for key, row in dict(registry or load_milestone_event_registry()).items():
        norm_key = _normalize_registry_key(key, row)
        decision = decisions.get(norm_key)
        updated = dict(row)
        updated["event_type"] = norm_key
        if decision is not None:
            updated["status"] = str(decision.recommended_status)
        promoted_registry[str(key).lower()] = updated
    promoted_path.write_text(yaml.safe_dump(promoted_registry, sort_keys=False), encoding="utf-8")

    return {
        "registry_status_snapshot": yaml_path,
        "approval_decisions": json_path,
        "promoted_registry": promoted_path,
        "source_registry": Path(registry_path),
    }
