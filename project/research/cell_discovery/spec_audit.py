from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

import yaml

from project.research.cell_discovery.registry import load_registry

GUARD_EVENT_TYPES = {
    "FUNDING_TIMESTAMP_EVENT",
    "SESSION_OPEN_EVENT",
    "SLIPPAGE_SPIKE_EVENT",
    "SPREAD_BLOWOUT",
    "SPREAD_REGIME_WIDENING_EVENT",
}


def _read_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"YAML must be a mapping: {path}")
    return payload


def _verify_report_summary(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = list(payload.get("cell_feasibility", []) or [])
    by_event: dict[str, Counter[str]] = {}
    by_context: dict[str, Counter[str]] = {}
    missing_by_context: dict[str, Counter[str]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        event_type = str(row.get("event_type", "")).strip().upper() or "UNKNOWN"
        context = str(row.get("context_cell", "")).strip() or "unknown"
        status = str(row.get("status", "")).strip() or "unknown"
        by_event.setdefault(event_type, Counter())[status] += 1
        by_context.setdefault(context, Counter())[status] += 1
        condition = row.get("condition_status", {})
        missing = condition.get("missing_condition_keys", []) if isinstance(condition, dict) else []
        for key in list(missing or []):
            missing_by_context.setdefault(context, Counter())[str(key)] += 1
    return {
        "verify_report_path": str(path),
        "verify_status": payload.get("status"),
        "verify_blocked_reasons": list(payload.get("blocked_reasons", []) or []),
        "verify_by_event": {key: dict(counter) for key, counter in sorted(by_event.items())},
        "verify_by_context": {key: dict(counter) for key, counter in sorted(by_context.items())},
        "missing_condition_keys_by_context": {
            key: dict(counter) for key, counter in sorted(missing_by_context.items())
        },
    }


def build_spec_audit(
    *,
    spec_dir: str | Path,
    template_registry: str | Path = "spec/templates/event_template_registry.yaml",
    verify_report: str | Path | None = None,
) -> dict[str, Any]:
    spec_path = Path(spec_dir)
    events_doc = _read_yaml(spec_path / "event_atoms.yaml")
    metadata = events_doc.get("metadata", {})
    metadata = metadata if isinstance(metadata, dict) else {}
    surface_role = str(metadata.get("surface_role", "")).strip() or "unspecified"
    registry = load_registry(spec_path)
    template_doc = _read_yaml(Path(template_registry))
    template_events = template_doc.get("events", {})
    if not isinstance(template_events, dict):
        raise ValueError(f"template registry must contain mapping field 'events': {template_registry}")

    event_rows: list[dict[str, Any]] = []
    event_issue_count = 0
    role_warnings: list[str] = []
    for atom in registry.event_atoms:
        template_meta = template_events.get(atom.event_type, {})
        allowed = list(template_meta.get("templates", []) or []) if isinstance(template_meta, dict) else []
        current = list(atom.templates)
        missing_allowed = [template for template in allowed if template not in current]
        unsupported = [template for template in current if template not in allowed]
        if unsupported:
            event_issue_count += 1
        event_role = "guard_only" if atom.event_type in GUARD_EVENT_TYPES else "alpha_or_repair"
        if event_role == "guard_only" and surface_role not in {"guard_filter", "exploratory"}:
            role_warnings.append(
                f"{atom.event_type} is guard_only but surface_role is {surface_role}"
            )
        event_rows.append(
            {
                "event_atom_id": atom.atom_id,
                "event_type": atom.event_type,
                "event_role": event_role,
                "current_templates": current,
                "allowed_templates": allowed,
                "missing_allowed_templates": missing_allowed,
                "unsupported_templates": unsupported,
                "template_status": "fail" if unsupported else "pass",
            }
        )

    context_rows = [
        {
            "context_cell": cell.cell_id,
            "dimension": cell.dimension,
            "values": list(cell.values),
            "required_feature_key": cell.required_feature_key,
            "executability_class": cell.executability_class,
            "promotion_friendly": cell.executability_class == "runtime",
        }
        for cell in registry.context_cells
    ]
    context_counts = Counter(row["executability_class"] for row in context_rows)
    verify_path = Path(verify_report) if verify_report else None
    verify_summary = _verify_report_summary(verify_path) if verify_path else {}
    return {
        "exit_code": 1 if event_issue_count else 0,
        "status": "fail" if event_issue_count else "ok",
        "spec_dir": str(spec_dir),
        "surface_role": surface_role,
        "metadata": metadata,
        "template_registry": str(template_registry),
        "event_count": len(event_rows),
        "context_count": len(context_rows),
        "template_issue_count": event_issue_count,
        "role_warning_count": len(role_warnings),
        "role_warnings": role_warnings,
        "context_class_counts": dict(sorted(context_counts.items())),
        "events": event_rows,
        "contexts": context_rows,
        **verify_summary,
    }
