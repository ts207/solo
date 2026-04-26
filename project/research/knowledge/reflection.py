from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from project.core.config import get_data_root
from project.io.utils import read_parquet
from project.research.knowledge.schemas import REFLECTION_COLUMNS, canonical_json
from project.specs.manifest import load_run_manifest

REFLECTION_VERSION = "v2"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_optional_table(path: Path) -> pd.DataFrame:
    if path.exists():
        return read_parquet(path)
    csv_path = path.with_suffix(".csv")
    if csv_path.exists():
        return pd.read_csv(csv_path)
    return pd.DataFrame()


def _load_run_manifest_local(run_id: str, *, data_root: Path) -> dict[str, Any]:
    local_path = data_root / "runs" / run_id / "run_manifest.json"
    local_payload = _read_json(local_path)
    if local_payload:
        return local_payload
    payload = load_run_manifest(run_id)
    return payload if isinstance(payload, dict) else {}


def _load_stage_manifests(run_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not run_dir.exists():
        return rows
    for path in sorted(run_dir.glob("*.json")):
        if path.name == "run_manifest.json":
            continue
        payload = _read_json(path)
        if not payload:
            continue
        payload.setdefault("stage", path.stem)
        payload.setdefault("_path", str(path))
        rows.append(payload)
    return rows


def _int_like(value: Any) -> int:
    try:
        if pd.isna(value):
            return 0
    except TypeError:
        pass
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _float_like(value: Any) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return float("nan")
    return numeric


def _dominant_value(series: pd.Series) -> str:
    cleaned = series.dropna().astype(str).str.strip()
    cleaned = cleaned[cleaned != ""]
    if cleaned.empty:
        return ""
    return str(cleaned.value_counts().idxmax())


def _expected_artifacts(manifest: dict[str, Any], *, data_root: Path) -> list[tuple[str, Path]]:
    run_id = str(manifest.get("run_id", "")).strip()
    planned = {
        str(stage).strip()
        for stage in manifest.get("planned_stages", []) or []
        if str(stage).strip()
    }
    expected: list[tuple[str, Path]] = []
    if "phase2_search_engine" in planned or "summarize_discovery_quality" in planned:
        expected.append(
            (
                "phase2.discovery_quality_summary",
                data_root / "reports" / "phase2" / run_id / "discovery_quality_summary.json",
            )
        )
    if "export_edge_candidates" in planned:
        expected.append(
            (
                "edge_candidates.normalized",
                data_root
                / "reports"
                / "edge_candidates"
                / run_id
                / "edge_candidates_normalized.parquet",
            )
        )
    if "promote_candidates" in planned:
        expected.append(
            (
                "promotion.audit",
                data_root
                / "reports"
                / "promotions"
                / run_id
                / "promotion_statistical_audit.parquet",
            )
        )
    return expected


def _classify_mechanical_outcome(
    *,
    manifest: dict[str, Any],
    stage_manifests: list[dict[str, Any]],
    feature_warnings: list[dict[str, Any]],
    missing_artifacts: list[str],
) -> dict[str, Any]:
    run_status = str(manifest.get("status", "")).strip().lower()
    raw_failed_stage = manifest.get("failed_stage")
    failed_stage = str(raw_failed_stage).strip() if raw_failed_stage else ""
    if failed_stage.lower() in ("none", "null"):
        failed_stage = ""
    stage_statuses = [str(row.get("status", "")).strip().lower() for row in stage_manifests]
    failed_stage_count = sum(1 for status in stage_statuses if status == "failed")
    warning_stage_count = sum(1 for status in stage_statuses if status == "warning") + len(
        feature_warnings
    )
    completed_stage_count = sum(1 for status in stage_statuses if status in {"success", "warning"})
    planned_stage_count = len(
        [stage for stage in manifest.get("planned_stages", []) or [] if str(stage).strip()]
    )

    if failed_stage or run_status == "failed" or failed_stage_count > 0:
        outcome = "mechanical_failure"
    elif missing_artifacts:
        outcome = "artifact_contract_failure"
    elif feature_warnings:
        # We downgrade data quality warnings from blocking failure to warning only
        # to allow research to proceed while acknowledging the issues.
        outcome = "warning_only"
    elif warning_stage_count > 0:
        outcome = "warning_only"
    elif stage_manifests and planned_stage_count and completed_stage_count < planned_stage_count:
        outcome = "partial_success"
    else:
        outcome = "success"

    return {
        "run_status": run_status or "unknown",
        "planned_stage_count": planned_stage_count,
        "completed_stage_count": completed_stage_count,
        "warning_stage_count": warning_stage_count,
        "failed_stage": failed_stage,
        "outcome": outcome,
    }


def _classify_statistical_outcome(
    *,
    mechanical_outcome: str,
    promotion_audit: pd.DataFrame,
    edge_candidates: pd.DataFrame,
    phase2_candidates: pd.DataFrame,
    discovery_summary: dict[str, Any],
) -> dict[str, Any]:
    candidate_count = 0
    promoted_count = 0
    primary_fail_gate = ""
    top_event = ""
    sample_floor = 0
    if not promotion_audit.empty:
        candidate_count = len(promotion_audit)
        if "promotion_decision" in promotion_audit.columns:
            promoted_count = int(
                (promotion_audit["promotion_decision"].astype(str) == "promoted").sum()
            )
        fail_col = (
            "promotion_fail_gate_primary"
            if "promotion_fail_gate_primary" in promotion_audit.columns
            else "primary_fail_gate"
        )
        if fail_col in promotion_audit.columns:
            primary_fail_gate = _dominant_value(promotion_audit[fail_col])
        if "event_type" in promotion_audit.columns:
            top_event = _dominant_value(promotion_audit["event_type"])
        for column in ("n_events", "sample_size", "validation_samples", "test_samples"):
            if column in promotion_audit.columns:
                series = pd.to_numeric(promotion_audit[column], errors="coerce").fillna(0)
                if not series.empty:
                    sample_floor = max(sample_floor, int(series.max()))
    elif not edge_candidates.empty:
        candidate_count = len(edge_candidates)
        if "primary_fail_gate" in edge_candidates.columns:
            primary_fail_gate = _dominant_value(edge_candidates["primary_fail_gate"])
        if "event_type" in edge_candidates.columns:
            top_event = _dominant_value(edge_candidates["event_type"])
        for column in ("sample_size", "validation_samples", "test_samples"):
            if column in edge_candidates.columns:
                series = pd.to_numeric(edge_candidates[column], errors="coerce").fillna(0)
                if not series.empty:
                    sample_floor = max(sample_floor, int(series.max()))
    elif not phase2_candidates.empty:
        candidate_count = len(phase2_candidates)
        if "event_type" in phase2_candidates.columns:
            top_event = _dominant_value(phase2_candidates["event_type"])
        if "primary_fail_gate" in phase2_candidates.columns:
            primary_fail_gate = _dominant_value(phase2_candidates["primary_fail_gate"])
        for column in ("n_events", "sample_size", "validation_samples", "test_samples", "n"):
            if column in phase2_candidates.columns:
                series = pd.to_numeric(phase2_candidates[column], errors="coerce").fillna(0)
                if not series.empty:
                    sample_floor = max(sample_floor, int(series.max()))
    elif discovery_summary:
        candidate_count = _int_like(discovery_summary.get("phase2_candidates", 0))

    if (
        mechanical_outcome in {"mechanical_failure", "artifact_contract_failure"}
        and candidate_count == 0
    ):
        outcome = "not_evaluable"
    elif promoted_count > 0:
        outcome = "deploy_promising"
    elif candidate_count == 0:
        outcome = "no_signal"
    elif sample_floor and sample_floor < 20:
        outcome = "inconclusive_due_to_sample"
    elif not promotion_audit.empty and "gate_promo_statistical" in promotion_audit.columns:
        statistical_passes = int((promotion_audit["gate_promo_statistical"] == "pass").sum())
        outcome = "research_promising" if statistical_passes > 0 else "weak_signal"
    elif not edge_candidates.empty and "gate_bridge_tradable" in edge_candidates.columns:
        tradable = int((edge_candidates["gate_bridge_tradable"] == "pass").sum())
        outcome = "research_promising" if tradable > 0 else "weak_signal"
    else:
        outcome = "weak_signal"

    return {
        "outcome": outcome,
        "candidate_count": candidate_count,
        "promoted_count": promoted_count,
        "primary_fail_gate": primary_fail_gate,
        "top_event": top_event,
    }


def _detect_anomalies(
    *,
    manifest: dict[str, Any],
    stage_manifests: list[dict[str, Any]],
    promotion_audit: pd.DataFrame,
    edge_candidates: pd.DataFrame,
    feature_warnings: list[dict[str, Any]],
    missing_artifacts: list[str],
) -> list[dict[str, Any]]:
    anomalies: list[dict[str, Any]] = []
    raw_failed_stage = manifest.get("failed_stage")
    failed_stage = str(raw_failed_stage).strip() if raw_failed_stage else ""
    if failed_stage.lower() in ("none", "null"):
        failed_stage = ""
    run_status = str(manifest.get("status", "")).strip().lower()
    stage_map = {
        str(row.get("stage", "")).strip(): str(row.get("status", "")).strip().lower()
        for row in stage_manifests
        if str(row.get("stage", "")).strip()
    }

    if run_status == "failed" and not promotion_audit.empty:
        anomalies.append(
            {
                "type": "stale_run_manifest",
                "detail": "run manifest is failed but promotion audit exists",
                "severity": "high",
            }
        )
    if failed_stage and stage_map.get(failed_stage) == "success":
        anomalies.append(
            {
                "type": "stage_status_mismatch",
                "detail": f"run manifest failed_stage={failed_stage} but stage manifest is success",
                "severity": "high",
            }
        )
    for artifact_name in missing_artifacts:
        anomalies.append(
            {
                "type": "missing_artifact",
                "detail": artifact_name,
                "severity": "high",
            }
        )
    if feature_warnings:
        anomalies.append(
            {
                "type": "feature_integrity_warning",
                "detail": f"{len(feature_warnings)} feature-integrity warning manifest(s)",
                "severity": "medium",
            }
        )
    if not promotion_audit.empty and "candidate_id" in promotion_audit.columns:
        dupes = int(promotion_audit["candidate_id"].astype(str).duplicated().sum())
        if dupes > 0:
            anomalies.append(
                {
                    "type": "duplicate_candidates",
                    "detail": f"{dupes} duplicate candidate_id rows in promotion audit",
                    "severity": "medium",
                }
            )
    if not edge_candidates.empty and "candidate_id" in edge_candidates.columns:
        dupes = int(edge_candidates["candidate_id"].astype(str).duplicated().sum())
        if dupes > 0:
            anomalies.append(
                {
                    "type": "duplicate_edge_candidates",
                    "detail": f"{dupes} duplicate candidate_id rows in edge candidate export",
                    "severity": "medium",
                }
            )
    return anomalies


def _build_market_findings(
    *,
    statistical: dict[str, Any],
    promotion_audit: pd.DataFrame,
    edge_candidates: pd.DataFrame,
) -> str:
    positive_after_cost = 0
    tradable_candidates = 0
    if not promotion_audit.empty:
        if "gate_bridge_tradable" in promotion_audit.columns:
            tradable_candidates = int((promotion_audit["gate_bridge_tradable"] == "pass").sum())
        if "net_expectancy_bps" in promotion_audit.columns:
            positive_after_cost = int(
                (pd.to_numeric(promotion_audit["net_expectancy_bps"], errors="coerce") > 0).sum()
            )
    elif not edge_candidates.empty:
        if "gate_bridge_tradable" in edge_candidates.columns:
            tradable_candidates = int((edge_candidates["gate_bridge_tradable"] == "pass").sum())
        if "after_cost_expectancy" in edge_candidates.columns:
            positive_after_cost = int(
                (pd.to_numeric(edge_candidates["after_cost_expectancy"], errors="coerce") > 0).sum()
            )
    payload = {
        "candidate_count": int(statistical["candidate_count"]),
        "promoted_count": int(statistical["promoted_count"]),
        "top_event": str(statistical["top_event"] or ""),
        "primary_fail_gate": str(statistical["primary_fail_gate"] or ""),
        "tradable_candidates": tradable_candidates,
        "positive_after_cost_candidates": positive_after_cost,
        "summary": str(statistical["outcome"]),
    }
    return canonical_json(payload)


def _build_system_findings(
    *,
    mechanical: dict[str, Any],
    missing_artifacts: list[str],
    anomalies: list[dict[str, Any]],
) -> str:
    payload = {
        "run_status": mechanical["run_status"],
        "failed_stage": mechanical["failed_stage"],
        "planned_stage_count": int(mechanical["planned_stage_count"]),
        "completed_stage_count": int(mechanical["completed_stage_count"]),
        "warning_stage_count": int(mechanical["warning_stage_count"]),
        "missing_artifacts": list(missing_artifacts),
        "anomaly_count": len(anomalies),
        "summary": str(mechanical["outcome"]),
    }
    return canonical_json(payload)


def _build_belief_update(
    *,
    mechanical_outcome: str,
    statistical_outcome: str,
    primary_fail_gate: str,
    top_event: str,
) -> str:
    if mechanical_outcome in {
        "mechanical_failure",
        "artifact_contract_failure",
        "data_quality_failure",
    }:
        return "system reliability limits interpretation; prioritize repair before updating market beliefs"
    if statistical_outcome == "deploy_promising":
        return f"{top_event or 'top candidate'} has deployable evidence; preserve region and tighten deployment validation"
    if statistical_outcome == "research_promising":
        return f"{top_event or 'top candidate'} remains research-promising; iterate adjacent contexts before broadening"
    if statistical_outcome == "inconclusive_due_to_sample":
        return (
            "sample support is too thin; retain scope but extend history or broaden sample capture"
        )
    if primary_fail_gate:
        return f"candidate quality is limited mainly by {primary_fail_gate}; use that gate as the next search discriminator"
    return "no durable market edge was established in this run"


def _recommend_next_action(
    *,
    mechanical_outcome: str,
    statistical_outcome: str,
) -> str:
    if mechanical_outcome in {
        "mechanical_failure",
        "artifact_contract_failure",
        "data_quality_failure",
    }:
        return "repair_pipeline"
    if statistical_outcome == "deploy_promising":
        return "exploit_promising_region"
    if statistical_outcome == "research_promising":
        return "explore_adjacent_region"
    if statistical_outcome == "inconclusive_due_to_sample":
        return "rerun_same_scope"
    if statistical_outcome == "weak_signal":
        return "explore_adjacent_region"
    return "hold"


def _recommend_next_experiment(
    *,
    manifest: dict[str, Any],
    statistical: dict[str, Any],
    recommended_next_action: str,
) -> str:
    payload = {
        "event_type": str(statistical["top_event"] or ""),
        "primary_fail_gate": str(statistical["primary_fail_gate"] or ""),
        "symbols": str(manifest.get("symbols", "")).strip(),
        "promotion_profile": "deploy"
        if recommended_next_action == "exploit_promising_region"
        else "research",
        "reason": recommended_next_action,
    }
    return canonical_json(payload)


def _confidence(
    *,
    mechanical_outcome: str,
    anomalies: list[dict[str, Any]],
    candidate_count: int,
) -> float:
    score = 0.8
    if mechanical_outcome in {"mechanical_failure", "artifact_contract_failure"}:
        score = 0.25
    elif mechanical_outcome == "data_quality_failure":
        score = 0.4
    elif mechanical_outcome == "warning_only":
        score = 0.6
    if candidate_count == 0:
        score -= 0.1
    score -= min(0.25, 0.05 * len(anomalies))
    return max(0.05, round(score, 3))


def build_run_reflection(
    *,
    run_id: str,
    program_id: str = "",
    data_root: Path | None = None,
) -> dict[str, Any]:
    resolved_data_root = Path(data_root) if data_root is not None else get_data_root()
    run_dir = resolved_data_root / "runs" / run_id
    reports_root = resolved_data_root / "reports"
    manifest = _load_run_manifest_local(run_id, data_root=resolved_data_root)
    stage_manifests = _load_stage_manifests(run_dir)

    discovery_summary = _read_json(
        reports_root / "phase2" / run_id / "discovery_quality_summary.json"
    )
    phase2_candidates = _read_optional_table(
        reports_root / "phase2" / run_id / "phase2_candidates.parquet"
    )
    promotion_audit = _read_optional_table(
        reports_root / "promotions" / run_id / "promotion_statistical_audit.parquet"
    )
    edge_candidates = _read_optional_table(
        reports_root / "edge_candidates" / run_id / "edge_candidates_normalized.parquet"
    )
    feature_manifests = [
        _read_json(path) for path in sorted(run_dir.glob("validate_feature_integrity_*.json"))
    ]
    feature_warnings = [
        payload
        for payload in feature_manifests
        if str(payload.get("status", "")).strip().lower() == "warning"
    ]

    missing_artifacts = [
        name
        for name, path in _expected_artifacts(manifest, data_root=resolved_data_root)
        if not path.exists() and not path.with_suffix(".csv").exists()
    ]
    mechanical = _classify_mechanical_outcome(
        manifest=manifest,
        stage_manifests=stage_manifests,
        feature_warnings=feature_warnings,
        missing_artifacts=missing_artifacts,
    )
    statistical = _classify_statistical_outcome(
        mechanical_outcome=str(mechanical["outcome"]),
        promotion_audit=promotion_audit,
        edge_candidates=edge_candidates,
        phase2_candidates=phase2_candidates,
        discovery_summary=discovery_summary,
    )
    anomalies = _detect_anomalies(
        manifest=manifest,
        stage_manifests=stage_manifests,
        promotion_audit=promotion_audit,
        edge_candidates=edge_candidates,
        feature_warnings=feature_warnings,
        missing_artifacts=missing_artifacts,
    )
    recommended_next_action = _recommend_next_action(
        mechanical_outcome=str(mechanical["outcome"]),
        statistical_outcome=str(statistical["outcome"]),
    )
    reflection = {
        "run_id": run_id,
        "program_id": str(program_id or manifest.get("program_id", "")).strip(),
        "objective": str(manifest.get("objective_name", "")).strip(),
        "executed_scope": canonical_json(
            {
                "symbols": manifest.get("symbols", ""),
                "start": manifest.get("start"),
                "end": manifest.get("end"),
                "mode": manifest.get("run_mode", manifest.get("mode", "")),
            }
        ),
        "run_status": str(mechanical["run_status"]),
        "planned_stage_count": int(mechanical["planned_stage_count"]),
        "completed_stage_count": int(mechanical["completed_stage_count"]),
        "warning_stage_count": int(mechanical["warning_stage_count"]),
        "candidate_count": int(statistical["candidate_count"]),
        "promoted_count": int(statistical["promoted_count"]),
        "primary_fail_gate": str(statistical["primary_fail_gate"] or ""),
        "mechanical_outcome": str(mechanical["outcome"]),
        "statistical_outcome": str(statistical["outcome"]),
        "market_findings": _build_market_findings(
            statistical=statistical,
            promotion_audit=promotion_audit,
            edge_candidates=edge_candidates,
        ),
        "system_findings": _build_system_findings(
            mechanical=mechanical,
            missing_artifacts=missing_artifacts,
            anomalies=anomalies,
        ),
        "anomalies": canonical_json(anomalies),
        "belief_update": _build_belief_update(
            mechanical_outcome=str(mechanical["outcome"]),
            statistical_outcome=str(statistical["outcome"]),
            primary_fail_gate=str(statistical["primary_fail_gate"] or ""),
            top_event=str(statistical["top_event"] or ""),
        ),
        "recommended_next_action": recommended_next_action,
        "recommended_next_experiment": _recommend_next_experiment(
            manifest=manifest,
            statistical=statistical,
            recommended_next_action=recommended_next_action,
        ),
        "confidence": _confidence(
            mechanical_outcome=str(mechanical["outcome"]),
            anomalies=anomalies,
            candidate_count=int(statistical["candidate_count"]),
        ),
        "reflection_version": REFLECTION_VERSION,
        "created_at": datetime.now(UTC).isoformat(),
    }
    return {column: reflection.get(column) for column in REFLECTION_COLUMNS}


__all__ = ["REFLECTION_VERSION", "build_run_reflection"]
