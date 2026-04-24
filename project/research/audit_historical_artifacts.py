"""
Historical artifact audit scanner.

Scans canonical and legacy artifact trees, infers both provenance stamps and
historical trust under the current contract, and emits inventory outputs.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from project.core.config import get_data_root
from project.io.utils import atomic_write_json, atomic_write_text, read_parquet, write_parquet
from project.research.contracts.historical_trust import (
    HISTORICAL_TRUST_LEGACY,
    HISTORICAL_TRUST_REQUIRES_REVALIDATION,
    HISTORICAL_TRUST_TRUSTED,
    HistoricalTrustStamp,
    aggregate_historical_trust,
    requires_revalidation,
)
from project.research.contracts.stat_regime import (
    ARTIFACT_AUDIT_VERSION_PHASE1_V1,
    AUDIT_STATUS_CURRENT,
    AUDIT_STATUS_DEGRADED,
    AUDIT_STATUS_LEGACY,
    AUDIT_STATUS_MANUAL_REVIEW_REQUIRED,
    STAT_REGIME_POST_AUDIT,
    STAT_REGIME_PRE_AUDIT,
    STAT_REGIME_UNKNOWN,
    ArtifactAuditStamp,
    infer_stat_regime_from_artifact_metadata,
)
from project.research.historical_trust import (
    CURRENT_ARTIFACT_FILENAMES,
    LEGACY_ARTIFACT_FILENAMES,
    inspect_artifact_trust,
)

log = logging.getLogger(__name__)

AUDIT_INVENTORY_SCHEMA_VERSION = "audit_inventory_v2"

_DATAFRAME_ARTIFACT_TYPES = {
    "promotion_ready_candidates",
    "promotion_audit",
    "promoted_candidates",
    "evidence_bundle_summary",
}

_JSON_OBJECT_ARTIFACT_TYPES = {
    "run_manifest",
    "validation_bundle",
    "promotion_lineage_audit",
    "promoted_theses",
    "live_thesis_index",
}


@dataclass(frozen=True)
class DiscoveredArtifact:
    artifact_type: str
    artifact_path: Path
    run_hint: str = ""


@dataclass
class ArtifactInventoryRow:
    run_id: str = ""
    candidate_id: str = ""
    hypothesis_id: str = ""
    campaign_id: str = ""
    program_id: str = ""
    artifact_path: str = ""
    artifact_type: str = ""
    created_at: str = ""
    stat_regime: str = ""
    audit_status: str = ""
    audit_reason: str = ""
    requires_repromotion: bool = False
    requires_manual_review: bool = False
    artifact_audit_version: str = ""
    inference_confidence: str = ""
    policy_version: str = ""
    bundle_version: str = ""
    q_value: Optional[float] = None
    q_value_scope: Optional[float] = None
    effective_q_value: Optional[float] = None
    num_tests_scope: Optional[int] = None
    multiplicity_scope_mode: Optional[str] = None
    search_scope_version: Optional[str] = None
    search_burden_estimated: Optional[bool] = None
    historical_trust_status: str = ""
    historical_trust_reason: str = ""
    canonical_reuse_allowed: bool = False
    compat_reuse_allowed: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "candidate_id": self.candidate_id,
            "hypothesis_id": self.hypothesis_id,
            "campaign_id": self.campaign_id,
            "program_id": self.program_id,
            "artifact_path": self.artifact_path,
            "artifact_type": self.artifact_type,
            "created_at": self.created_at,
            "stat_regime": self.stat_regime,
            "audit_status": self.audit_status,
            "audit_reason": self.audit_reason,
            "requires_repromotion": self.requires_repromotion,
            "requires_manual_review": self.requires_manual_review,
            "artifact_audit_version": self.artifact_audit_version,
            "inference_confidence": self.inference_confidence,
            "policy_version": self.policy_version,
            "bundle_version": self.bundle_version,
            "q_value": self.q_value,
            "q_value_scope": self.q_value_scope,
            "effective_q_value": self.effective_q_value,
            "num_tests_scope": self.num_tests_scope,
            "multiplicity_scope_mode": self.multiplicity_scope_mode,
            "search_scope_version": self.search_scope_version,
            "search_burden_estimated": self.search_burden_estimated,
            "historical_trust_status": self.historical_trust_status,
            "historical_trust_reason": self.historical_trust_reason,
            "canonical_reuse_allowed": self.canonical_reuse_allowed,
            "compat_reuse_allowed": self.compat_reuse_allowed,
        }


@dataclass
class AuditInventoryResult:
    rows: List[ArtifactInventoryRow]
    run_id_counts: Dict[str, int]
    stat_regime_counts: Dict[str, int]
    audit_status_counts: Dict[str, int]
    trust_status_counts: Dict[str, int] = field(default_factory=dict)
    requires_repromotion_count: int = 0
    requires_manual_review_count: int = 0
    canonical_reuse_blocked_count: int = 0
    compat_reuse_blocked_count: int = 0
    scanned_artifact_paths: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": AUDIT_INVENTORY_SCHEMA_VERSION,
            "total_rows": len(self.rows),
            "run_id_counts": self.run_id_counts,
            "stat_regime_counts": self.stat_regime_counts,
            "audit_status_counts": self.audit_status_counts,
            "trust_status_counts": self.trust_status_counts,
            "requires_repromotion_count": self.requires_repromotion_count,
            "requires_manual_review_count": self.requires_manual_review_count,
            "canonical_reuse_blocked_count": self.canonical_reuse_blocked_count,
            "compat_reuse_blocked_count": self.compat_reuse_blocked_count,
            "scanned_artifact_count": len(self.scanned_artifact_paths),
            "error_count": len(self.errors),
            "rows": [row.to_dict() for row in self.rows],
        }


def _get_file_created_at(path: Path) -> str:
    try:
        stat = path.stat()
        dt = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    except OSError:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        import math

        out = float(value)
        return out if math.isfinite(out) else None
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes"}:
        return True
    if text in {"false", "0", "no"}:
        return False
    return None


def _parse_created_at(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _read_json_payload(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_dataframe(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return read_parquet(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    return pd.DataFrame()


def _infer_run_hint_from_path(data_root: Path, path: Path) -> str:
    try:
        rel = path.relative_to(data_root)
    except ValueError:
        return ""
    parts = list(rel.parts)
    if len(parts) >= 3 and parts[0] == "reports" and parts[1] in {"validation", "promotions"}:
        return str(parts[2]).strip()
    if len(parts) >= 3 and parts[0] == "live" and parts[1] == "theses" and parts[2] != "index.json":
        return str(parts[2]).strip()
    if len(parts) >= 3 and parts[0] == "runs":
        return str(parts[1]).strip()
    return ""


def _find_historical_artifacts(data_root: Path) -> List[DiscoveredArtifact]:
    paths: dict[tuple[str, str], DiscoveredArtifact] = {}

    def _record(artifact_type: str, candidate: Path) -> None:
        if not candidate.exists() or not candidate.is_file():
            return
        discovered = DiscoveredArtifact(
            artifact_type=artifact_type,
            artifact_path=candidate,
            run_hint=_infer_run_hint_from_path(data_root, candidate),
        )
        paths[(artifact_type, str(candidate))] = discovered

    runs_root = data_root / "runs"
    if runs_root.exists():
        for run_dir in sorted(path for path in runs_root.iterdir() if path.is_dir()):
            for filename in CURRENT_ARTIFACT_FILENAMES.get("run_manifest", set()) | LEGACY_ARTIFACT_FILENAMES.get("run_manifest", set()):
                _record("run_manifest", run_dir / filename)

    validation_root = data_root / "reports" / "validation"
    if validation_root.exists():
        for run_dir in sorted(path for path in validation_root.iterdir() if path.is_dir()):
            for artifact_type in ("validation_bundle", "promotion_ready_candidates"):
                for filename in CURRENT_ARTIFACT_FILENAMES.get(artifact_type, set()):
                    _record(artifact_type, run_dir / filename)

    promotion_root = data_root / "reports" / "promotions"
    if promotion_root.exists():
        for run_dir in sorted(path for path in promotion_root.iterdir() if path.is_dir()):
            for artifact_type in (
                "promotion_audit",
                "promoted_candidates",
                "evidence_bundle_summary",
                "promotion_lineage_audit",
            ):
                filenames = set(CURRENT_ARTIFACT_FILENAMES.get(artifact_type, set())) | set(
                    LEGACY_ARTIFACT_FILENAMES.get(artifact_type, set())
                )
                for filename in filenames:
                    _record(artifact_type, run_dir / filename)

    thesis_root = data_root / "live" / "theses"
    if thesis_root.exists():
        for run_dir in sorted(path for path in thesis_root.iterdir() if path.is_dir()):
            for filename in CURRENT_ARTIFACT_FILENAMES.get("promoted_theses", set()):
                _record("promoted_theses", run_dir / filename)
        for filename in CURRENT_ARTIFACT_FILENAMES.get("live_thesis_index", set()):
            _record("live_thesis_index", thesis_root / filename)

    return sorted(paths.values(), key=lambda item: (item.artifact_type, str(item.artifact_path)))


def _flatten_validation_bundle_records(payload: dict[str, Any]) -> List[Dict[str, Any]]:
    run_id = str(payload.get("run_id", "")).strip()
    program_id = str(payload.get("summary_stats", {}).get("program_id", "")).strip() if isinstance(payload.get("summary_stats", {}), dict) else ""
    candidate_lists = []
    for key in ("validated_candidates", "rejected_candidates", "inconclusive_candidates"):
        values = payload.get(key, [])
        if isinstance(values, list):
            candidate_lists.extend(values)
    rows: List[Dict[str, Any]] = []
    for entry in candidate_lists:
        if not isinstance(entry, dict):
            continue
        decision = entry.get("decision", {}) if isinstance(entry.get("decision"), dict) else {}
        metrics = entry.get("metrics", {}) if isinstance(entry.get("metrics"), dict) else {}
        rows.append(
            {
                "run_id": run_id,
                "candidate_id": str(entry.get("candidate_id", "") or decision.get("candidate_id", "")).strip(),
                "hypothesis_id": str(entry.get("hypothesis_id", "")).strip(),
                "program_id": program_id,
                "policy_version": str(payload.get("summary_stats", {}).get("validation_stage_version", "")).strip()
                if isinstance(payload.get("summary_stats", {}), dict)
                else "",
                "q_value": _safe_float(metrics.get("q_value")),
                "effective_q_value": _safe_float(metrics.get("q_value")),
                "num_tests_scope": _safe_int(metrics.get("sample_count")),
            }
        )
    if rows:
        return rows
    return [{"run_id": run_id, "program_id": program_id}]


def _flatten_promotion_lineage_rows(payload: dict[str, Any]) -> List[Dict[str, Any]]:
    rows = payload.get("rows", [])
    if isinstance(rows, list) and rows:
        return [dict(row) for row in rows if isinstance(row, dict)]
    return [{"run_id": str(payload.get("run_id", "")).strip()}]


def _flatten_promoted_theses_rows(payload: dict[str, Any]) -> List[Dict[str, Any]]:
    run_id = str(payload.get("run_id", "")).strip()
    theses = payload.get("theses", [])
    rows: List[Dict[str, Any]] = []
    if isinstance(theses, list):
        for thesis in theses:
            if not isinstance(thesis, dict):
                continue
            lineage = thesis.get("lineage", {}) if isinstance(thesis.get("lineage"), dict) else {}
            source = thesis.get("source", {}) if isinstance(thesis.get("source"), dict) else {}
            evidence = thesis.get("evidence", {}) if isinstance(thesis.get("evidence"), dict) else {}
            rows.append(
                {
                    "run_id": str(lineage.get("run_id", "")).strip() or run_id,
                    "candidate_id": str(lineage.get("candidate_id", "")).strip(),
                    "hypothesis_id": str(lineage.get("hypothesis_id", "")).strip(),
                    "campaign_id": str(source.get("source_campaign_id", "")).strip(),
                    "program_id": str(source.get("source_program_id", "")).strip(),
                    "policy_version": str(evidence.get("policy_version", "")).strip(),
                    "bundle_version": str(evidence.get("bundle_version", "")).strip(),
                    "q_value": _safe_float(evidence.get("q_value")),
                    "stat_regime": str(evidence.get("stat_regime", "")).strip(),
                    "audit_status": str(evidence.get("audit_status", "")).strip(),
                    "artifact_audit_version": str(evidence.get("artifact_audit_version", "")).strip(),
                }
            )
    if rows:
        return rows
    return [{"run_id": run_id}]


def _flatten_live_index_rows(payload: dict[str, Any]) -> List[Dict[str, Any]]:
    runs = payload.get("runs", {})
    if not isinstance(runs, dict) or not runs:
        return [{"run_id": str(payload.get("latest_run_id", "")).strip()}]
    rows: List[Dict[str, Any]] = []
    for run_id, meta in runs.items():
        meta = meta if isinstance(meta, dict) else {}
        rows.append(
            {
                "run_id": str(run_id).strip(),
                "artifact_audit_version": str(meta.get("artifact_audit_version", "")).strip(),
            }
        )
    return rows


def _flatten_run_manifest_rows(payload: dict[str, Any]) -> List[Dict[str, Any]]:
    return [{"run_id": str(payload.get("run_id", "")).strip(), "program_id": str(payload.get("program_id", "")).strip()}]


def _load_artifact_records(artifact: DiscoveredArtifact) -> List[Dict[str, Any]]:
    path = artifact.artifact_path
    if artifact.artifact_type in _DATAFRAME_ARTIFACT_TYPES:
        return _read_dataframe(path).to_dict(orient="records")
    if artifact.artifact_type not in _JSON_OBJECT_ARTIFACT_TYPES:
        return []
    payload = _read_json_payload(path)
    if not isinstance(payload, dict):
        return []
    if artifact.artifact_type == "validation_bundle":
        return _flatten_validation_bundle_records(payload)
    if artifact.artifact_type == "promotion_lineage_audit":
        return _flatten_promotion_lineage_rows(payload)
    if artifact.artifact_type == "promoted_theses":
        return _flatten_promoted_theses_rows(payload)
    if artifact.artifact_type == "live_thesis_index":
        return _flatten_live_index_rows(payload)
    if artifact.artifact_type == "run_manifest":
        return _flatten_run_manifest_rows(payload)
    return [payload]


def _build_inventory_row(
    row: Dict[str, Any],
    *,
    artifact_path: str,
    artifact_type: str,
    created_at: str,
    trust_stamp: HistoricalTrustStamp,
) -> ArtifactInventoryRow:
    artifact_dt = _parse_created_at(created_at)
    stamp = infer_stat_regime_from_artifact_metadata(row, artifact_timestamp=artifact_dt)
    return ArtifactInventoryRow(
        run_id=str(row.get("run_id", "")).strip(),
        candidate_id=str(row.get("candidate_id", "")).strip(),
        hypothesis_id=str(row.get("hypothesis_id", "")).strip(),
        campaign_id=str(row.get("campaign_id", "")).strip(),
        program_id=str(row.get("program_id", "")).strip(),
        artifact_path=artifact_path,
        artifact_type=artifact_type,
        created_at=created_at,
        stat_regime=stamp.stat_regime,
        audit_status=stamp.audit_status,
        audit_reason=stamp.audit_reason,
        requires_repromotion=stamp.requires_repromotion,
        requires_manual_review=stamp.requires_manual_review,
        artifact_audit_version=stamp.artifact_audit_version,
        inference_confidence=stamp.inference_confidence,
        policy_version=str(row.get("policy_version", "")).strip(),
        bundle_version=str(row.get("bundle_version", "")).strip(),
        q_value=_safe_float(row.get("q_value")),
        q_value_scope=_safe_float(row.get("q_value_scope")),
        effective_q_value=_safe_float(row.get("effective_q_value")),
        num_tests_scope=_safe_int(row.get("num_tests_scope")),
        multiplicity_scope_mode=str(row.get("multiplicity_scope_mode", "")).strip() or None,
        search_scope_version=str(row.get("search_scope_version", "")).strip() or None,
        search_burden_estimated=_safe_bool(row.get("search_burden_estimated")),
        historical_trust_status=trust_stamp.historical_trust_status,
        historical_trust_reason=trust_stamp.historical_trust_reason,
        canonical_reuse_allowed=trust_stamp.canonical_reuse_allowed,
        compat_reuse_allowed=trust_stamp.compat_reuse_allowed,
    )


def scan_historical_artifacts(
    data_root: Optional[Path] = None,
    *,
    run_id: Optional[str] = None,
    since: Optional[str] = None,
) -> AuditInventoryResult:
    resolved_root = Path(data_root) if data_root is not None else get_data_root()
    artifacts = _find_historical_artifacts(resolved_root)

    rows: List[ArtifactInventoryRow] = []
    scanned_paths: List[str] = []
    errors: List[str] = []

    since_dt = _parse_created_at(str(since or ""))

    for artifact in artifacts:
        path = artifact.artifact_path
        try:
            if run_id and artifact.run_hint and artifact.run_hint != str(run_id):
                continue
            created_at = _get_file_created_at(path)
            if since_dt:
                file_dt = _parse_created_at(created_at)
                if file_dt is not None and file_dt < since_dt:
                    continue
            trust_stamp = inspect_artifact_trust(artifact.artifact_type, path)
            records = _load_artifact_records(artifact)
            artifact_rows: List[ArtifactInventoryRow] = []
            for record in records:
                inventory_row = _build_inventory_row(
                    record,
                    artifact_path=str(path),
                    artifact_type=artifact.artifact_type,
                    created_at=created_at,
                    trust_stamp=trust_stamp,
                )
                if run_id and inventory_row.run_id and inventory_row.run_id != str(run_id):
                    continue
                artifact_rows.append(inventory_row)
            if run_id and not artifact_rows:
                continue
            if not artifact_rows:
                artifact_rows.append(
                    _build_inventory_row(
                        {"run_id": artifact.run_hint},
                        artifact_path=str(path),
                        artifact_type=artifact.artifact_type,
                        created_at=created_at,
                        trust_stamp=trust_stamp,
                    )
                )
            rows.extend(artifact_rows)
            scanned_paths.append(str(path))
        except Exception as exc:
            if run_id and artifact.run_hint and artifact.run_hint != str(run_id):
                continue
            errors.append(f"{path}: {exc}")
            log.warning("Failed to scan artifact %s: %s", path, exc)

    run_id_counts: Dict[str, int] = {}
    stat_regime_counts: Dict[str, int] = {}
    audit_status_counts: Dict[str, int] = {}
    trust_status_counts: Dict[str, int] = {}
    requires_repromotion_count = 0
    requires_manual_review_count = 0
    canonical_reuse_blocked_count = 0
    compat_reuse_blocked_count = 0

    for row in rows:
        run_id_counts[row.run_id] = run_id_counts.get(row.run_id, 0) + 1
        stat_regime_counts[row.stat_regime] = stat_regime_counts.get(row.stat_regime, 0) + 1
        audit_status_counts[row.audit_status] = audit_status_counts.get(row.audit_status, 0) + 1
        trust_status_counts[row.historical_trust_status] = (
            trust_status_counts.get(row.historical_trust_status, 0) + 1
        )
        if row.requires_repromotion:
            requires_repromotion_count += 1
        if row.requires_manual_review:
            requires_manual_review_count += 1
        if not row.canonical_reuse_allowed:
            canonical_reuse_blocked_count += 1
        if not row.compat_reuse_allowed:
            compat_reuse_blocked_count += 1

    return AuditInventoryResult(
        rows=rows,
        run_id_counts=run_id_counts,
        stat_regime_counts=stat_regime_counts,
        audit_status_counts=audit_status_counts,
        trust_status_counts=trust_status_counts,
        requires_repromotion_count=requires_repromotion_count,
        requires_manual_review_count=requires_manual_review_count,
        canonical_reuse_blocked_count=canonical_reuse_blocked_count,
        compat_reuse_blocked_count=compat_reuse_blocked_count,
        scanned_artifact_paths=scanned_paths,
        errors=errors,
    )


def build_run_historical_trust_summary(
    *,
    run_id: str,
    data_root: Optional[Path] = None,
    result: AuditInventoryResult | None = None,
) -> Dict[str, Any]:
    resolved = Path(data_root) if data_root is not None else get_data_root()
    inventory = result if result is not None else scan_historical_artifacts(resolved, run_id=run_id)
    run_rows = [row for row in inventory.rows if str(row.run_id).strip() == str(run_id).strip()]
    row_stamps = [
        HistoricalTrustStamp(
            historical_trust_status=row.historical_trust_status,
            historical_trust_reason=row.historical_trust_reason,
            canonical_reuse_allowed=bool(row.canonical_reuse_allowed),
            compat_reuse_allowed=bool(row.compat_reuse_allowed),
            inference_confidence=row.inference_confidence or "high",
        )
        for row in run_rows
        if row.historical_trust_status
    ]
    aggregate = aggregate_historical_trust(row_stamps) if row_stamps else requires_revalidation("no_scannable_artifacts_for_run")
    if inventory.errors:
        aggregate = requires_revalidation("artifact_scan_errors_present")
    status_counts = {
        HISTORICAL_TRUST_TRUSTED: 0,
        HISTORICAL_TRUST_LEGACY: 0,
        HISTORICAL_TRUST_REQUIRES_REVALIDATION: 0,
    }
    for row in run_rows:
        if row.historical_trust_status in status_counts:
            status_counts[row.historical_trust_status] += 1
    artifact_paths = sorted({row.artifact_path for row in run_rows if row.artifact_path})
    artifact_types = sorted({row.artifact_type for row in run_rows if row.artifact_type})
    return {
        "schema_version": "historical_trust_summary_v1",
        "run_id": run_id,
        "historical_trust_status": aggregate.historical_trust_status,
        "historical_trust_reason": aggregate.historical_trust_reason,
        "canonical_reuse_allowed": aggregate.canonical_reuse_allowed,
        "compat_reuse_allowed": aggregate.compat_reuse_allowed,
        "trust_status_counts": status_counts,
        "artifact_count": len(artifact_paths),
        "artifact_paths": artifact_paths,
        "artifact_types": artifact_types,
        "error_count": len(inventory.errors),
        "errors": list(inventory.errors),
    }


def write_artifact_audit_stamp_sidecar(
    artifact_path: Path,
    stamp: ArtifactAuditStamp,
    trust_stamp: HistoricalTrustStamp,
) -> Path:
    sidecar_path = artifact_path.with_suffix(artifact_path.suffix + ".audit_stamp.json")
    payload = {
        "schema_version": "artifact_audit_stamp_v2",
        "stat_regime": stamp.stat_regime,
        "audit_status": stamp.audit_status,
        "artifact_audit_version": stamp.artifact_audit_version,
        "audit_reason": stamp.audit_reason,
        "requires_repromotion": stamp.requires_repromotion,
        "requires_manual_review": stamp.requires_manual_review,
        "inference_confidence": stamp.inference_confidence,
        **trust_stamp.to_dict(),
    }
    atomic_write_json(sidecar_path, payload)
    return sidecar_path


def write_audit_inventory(
    result: AuditInventoryResult,
    output_dir: Path,
) -> Dict[str, Path]:
    ensure_dir(output_dir)
    parquet_path = output_dir / "historical_artifact_audit.parquet"
    json_path = output_dir / "historical_artifact_audit.json"
    md_path = output_dir / "historical_artifact_audit.md"

    if result.rows:
        df = pd.DataFrame([row.to_dict() for row in result.rows])
        write_parquet(df, parquet_path)
    else:
        write_parquet(
            pd.DataFrame(columns=list(ArtifactInventoryRow.__dataclass_fields__.keys())),
            parquet_path,
        )

    atomic_write_json(json_path, result.to_dict())

    md_lines = [
        "# Historical Artifact Audit Inventory",
        "",
        f"- schema_version: `{AUDIT_INVENTORY_SCHEMA_VERSION}`",
        f"- total_rows: `{len(result.rows)}`",
        f"- scanned_artifact_count: `{len(result.scanned_artifact_paths)}`",
        f"- error_count: `{len(result.errors)}`",
        "",
        "## Summary by Historical Trust",
        "",
    ]
    for status, count in sorted(result.trust_status_counts.items()):
        md_lines.append(f"- {status}: `{count}`")
    md_lines.extend(
        [
            "",
            "## Summary by Statistical Regime",
            "",
        ]
    )
    for regime, count in sorted(result.stat_regime_counts.items()):
        md_lines.append(f"- {regime}: `{count}`")
    md_lines.extend(["", "## Summary by Audit Status", ""])
    for status, count in sorted(result.audit_status_counts.items()):
        md_lines.append(f"- {status}: `{count}`")
    md_lines.extend(["", "## Reuse Policy", ""])
    md_lines.append(f"- canonical_reuse_blocked_count: `{result.canonical_reuse_blocked_count}`")
    md_lines.append(f"- compat_reuse_blocked_count: `{result.compat_reuse_blocked_count}`")
    md_lines.extend(["", "## Special Flags", ""])
    md_lines.append(f"- requires_repromotion: `{result.requires_repromotion_count}`")
    md_lines.append(f"- requires_manual_review: `{result.requires_manual_review_count}`")

    if result.errors:
        md_lines.extend(["", "## Errors", ""])
        for error in result.errors[:10]:
            md_lines.append(f"- {error}")
        if len(result.errors) > 10:
            md_lines.append(f"- ... and {len(result.errors) - 10} more errors")

    atomic_write_text(md_path, "\n".join(md_lines) + "\n")

    return {
        "parquet_path": parquet_path,
        "json_path": json_path,
        "md_path": md_path,
    }


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def rewrite_audit_stamp_sidecars(
    result: AuditInventoryResult,
) -> Dict[str, Any]:
    artifact_stamps: Dict[str, List[ArtifactAuditStamp]] = {}
    artifact_trust: Dict[str, List[HistoricalTrustStamp]] = {}
    for row in result.rows:
        path_key = row.artifact_path
        artifact_stamps.setdefault(path_key, []).append(
            ArtifactAuditStamp(
                stat_regime=row.stat_regime,
                audit_status=row.audit_status,
                artifact_audit_version=row.artifact_audit_version,
                audit_reason=row.audit_reason,
                requires_repromotion=row.requires_repromotion,
                requires_manual_review=row.requires_manual_review,
                inference_confidence=row.inference_confidence,
            )
        )
        artifact_trust.setdefault(path_key, []).append(
            HistoricalTrustStamp(
                historical_trust_status=row.historical_trust_status,
                historical_trust_reason=row.historical_trust_reason,
                canonical_reuse_allowed=bool(row.canonical_reuse_allowed),
                compat_reuse_allowed=bool(row.compat_reuse_allowed),
                inference_confidence=row.inference_confidence or "high",
            )
        )

    written_count = 0
    aggregated_stamps: Dict[str, ArtifactAuditStamp] = {}
    aggregated_trust: Dict[str, HistoricalTrustStamp] = {}

    for artifact_path_str, stamps in artifact_stamps.items():
        trust_stamps = artifact_trust.get(artifact_path_str, [])
        has_manual_review = any(
            s.audit_status == AUDIT_STATUS_MANUAL_REVIEW_REQUIRED or s.requires_manual_review
            for s in stamps
        )
        has_pre_audit = any(s.stat_regime == STAT_REGIME_PRE_AUDIT for s in stamps)
        has_degraded = any(s.audit_status == AUDIT_STATUS_DEGRADED for s in stamps)

        if has_manual_review:
            final_status = AUDIT_STATUS_MANUAL_REVIEW_REQUIRED
            final_regime = STAT_REGIME_UNKNOWN
        elif has_pre_audit:
            final_status = AUDIT_STATUS_LEGACY
            final_regime = STAT_REGIME_PRE_AUDIT
        elif has_degraded:
            final_status = AUDIT_STATUS_DEGRADED
            final_regime = STAT_REGIME_POST_AUDIT
        else:
            final_status = AUDIT_STATUS_CURRENT
            final_regime = STAT_REGIME_POST_AUDIT

        final_stamp = ArtifactAuditStamp(
            stat_regime=final_regime,
            audit_status=final_status,
            artifact_audit_version=ARTIFACT_AUDIT_VERSION_PHASE1_V1,
            audit_reason=f"aggregated_from_{len(stamps)}_rows",
            requires_repromotion=has_pre_audit,
            requires_manual_review=has_manual_review,
            inference_confidence="high" if not has_manual_review else "low",
        )
        final_trust = aggregate_historical_trust(trust_stamps)

        artifact_path = Path(artifact_path_str)
        write_artifact_audit_stamp_sidecar(artifact_path, final_stamp, final_trust)
        aggregated_stamps[artifact_path_str] = final_stamp
        aggregated_trust[artifact_path_str] = final_trust
        written_count += 1

    return {
        "sidecars_written": written_count,
        "artifacts_processed": len(artifact_stamps),
        "stamps": aggregated_stamps,
        "trust": aggregated_trust,
    }


__all__ = [
    "ArtifactInventoryRow",
    "AuditInventoryResult",
    "build_run_historical_trust_summary",
    "scan_historical_artifacts",
    "write_artifact_audit_stamp_sidecar",
    "write_audit_inventory",
    "rewrite_audit_stamp_sidecars",
    "AUDIT_INVENTORY_SCHEMA_VERSION",
]
