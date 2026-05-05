from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from project import PROJECT_ROOT


def resolve_data_root(data_root: str | Path | None = None) -> Path:
    if data_root:
        return Path(data_root)
    return PROJECT_ROOT.parent / "data"


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {"value": payload}
    except Exception as exc:
        return {"_read_error": str(exc), "path": str(path)}


def _exists(path: Path) -> dict[str, Any]:
    return {"path": str(path), "exists": path.exists()}


def _artifact_map(run_id: str, root: Path) -> dict[str, Path]:
    return {
        "phase2_dir": root / "reports" / "phase2" / run_id,
        "edge_candidates_dir": root / "reports" / "edge_candidates" / run_id,
        "validation_dir": root / "reports" / "validation" / run_id,
        "promotion_dir": root / "reports" / "promotions" / run_id,
        "thesis_path": root / "live" / "theses" / run_id / "promoted_theses.json",
        "context_audit": root / "reports" / "context_audit" / run_id / "context_audit.json",
    }


def _summarize_doctor(run_id: str, root: Path, top_k: int) -> dict[str, Any] | None:
    try:
        from project.scripts.discover_doctor import build_discover_doctor_report

        payload = build_discover_doctor_report(run_id=run_id, data_root=root, top_k=top_k)
        return {
            "status": payload.get("status"),
            "classification": payload.get("classification"),
            "evidence_class": payload.get("evidence_class"),
            "next_safe_command": payload.get("next_safe_command"),
            "forbidden_rescue_actions": payload.get("forbidden_rescue_actions", []),
        }
    except Exception as exc:
        return {"status": "unavailable", "message": str(exc)}


def build_run_status_report(
    *,
    run_id: str,
    data_root: str | Path | None = None,
    top_k: int = 10,
) -> dict[str, Any]:
    root = resolve_data_root(data_root)
    paths = _artifact_map(run_id, root)
    artifacts = {name: _exists(path) for name, path in paths.items()}
    phase2_diag = _read_json(paths["phase2_dir"] / "phase2_diagnostics.json")
    validation_bundle = _read_json(paths["validation_dir"] / "validation_bundle.json")
    validation_report = _read_json(paths["validation_dir"] / "validation_report.json")
    promotion_diag = _read_json(paths["promotion_dir"] / "promotion_diagnostics.json")
    context_audit = _read_json(paths["context_audit"])

    stage = "not_started"
    if paths["thesis_path"].exists():
        stage = "thesis_exported"
    elif paths["promotion_dir"].exists():
        stage = "promotion"
    elif paths["validation_dir"].exists():
        stage = "validation"
    elif paths["phase2_dir"].exists() or paths["edge_candidates_dir"].exists():
        stage = "discovery"

    doctor = _summarize_doctor(run_id, root, top_k) if paths["phase2_dir"].exists() else None
    next_safe = "Run data preflight before discovery."
    if doctor and doctor.get("next_safe_command"):
        next_safe = str(doctor.get("next_safe_command"))
    elif paths["thesis_path"].exists():
        next_safe = "Inspect thesis or bind paper config."
    elif paths["validation_dir"].exists():
        next_safe = "Inspect validation outputs before promotion."

    return {
        "kind": "run_status",
        "run_id": run_id,
        "data_root": str(root),
        "stage": stage,
        "artifacts": artifacts,
        "doctor": doctor,
        "phase2_diagnostics": phase2_diag,
        "validation": {
            "bundle_present": validation_bundle is not None,
            "report": validation_report,
        },
        "promotion_diagnostics": promotion_diag,
        "context_audit": context_audit,
        "next_safe_command": next_safe,
    }


def build_rejection_explanation(
    *,
    run_id: str,
    data_root: str | Path | None = None,
    top_k: int = 10,
) -> dict[str, Any]:
    status = build_run_status_report(run_id=run_id, data_root=data_root, top_k=top_k)
    doctor = status.get("doctor") if isinstance(status.get("doctor"), dict) else {}
    promotion = status.get("promotion_diagnostics") if isinstance(status.get("promotion_diagnostics"), dict) else None
    validation = status.get("validation", {}) if isinstance(status.get("validation"), dict) else {}

    primary = "unknown"
    failure_class = "unknown"
    if doctor and doctor.get("classification"):
        primary = str(doctor.get("classification"))
        if doctor.get("status") == "blocked":
            failure_class = "mechanical_or_pre_metric"
        elif doctor.get("status") == "rejected":
            failure_class = "research_low_value"
        else:
            failure_class = str(doctor.get("status"))
    elif promotion:
        primary = "promotion_rejected_or_incomplete"
        failure_class = "promotion"
    elif validation and validation.get("bundle_present"):
        primary = "validation_incomplete_or_rejected"
        failure_class = "validation"
    elif status.get("stage") == "not_started":
        primary = "no_artifacts_found"
        failure_class = "mechanical"

    return {
        "kind": "rejection_explanation",
        "run_id": run_id,
        "primary_rejection": primary,
        "failure_class": failure_class,
        "doctor": doctor,
        "validation": validation,
        "promotion_diagnostics": promotion,
        "safe_next_action": status.get("next_safe_command"),
        "forbidden_rescue_actions": (doctor or {}).get(
            "forbidden_rescue_actions",
            [
                "change_horizon",
                "drop_bad_years",
                "loosen_gates",
                "switch_context_without_mechanism",
                "add_symbols_without_mechanism",
            ],
        ),
    }
