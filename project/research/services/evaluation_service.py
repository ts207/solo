from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from project.core.config import get_data_root
from project.core.exceptions import MissingArtifactError
from project.io.utils import read_table_auto
from project.research.validation.contracts import (
    ValidationBundle,
    ValidatedCandidateRecord,
    ValidationDecision,
    ValidationMetrics,
    ValidationReasonCodes,
)
from project.research.validation.result_writer import (
    load_validation_bundle,
    write_validation_bundle,
    write_validated_candidate_tables,
)



_DETECTOR_LINEAGE_COLUMNS = {
    "source_event_name",
    "source_event_version",
    "source_detector_class",
    "source_evidence_mode",
    "source_threshold_version",
    "source_calibration_artifact",
}


def extract_detector_lineage(row: pd.Series | Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(row) if isinstance(row, dict) else row.to_dict()
    event_name = str(payload.get("source_event_name") or payload.get("event_type") or payload.get("anchor_event") or "").strip()
    evidence_mode = str(payload.get("source_evidence_mode") or payload.get("evidence_mode") or "").strip()
    out = {
        "source_event_name": event_name,
        "source_event_version": str(payload.get("source_event_version", "")).strip(),
        "source_detector_class": str(payload.get("source_detector_class", "")).strip(),
        "source_evidence_mode": evidence_mode,
        "source_threshold_version": str(payload.get("source_threshold_version", "")).strip(),
        "source_calibration_artifact": str(payload.get("source_calibration_artifact", "")).strip(),
    }
    return {key: value for key, value in out.items() if value not in {"", None}}


@dataclass
class EvaluationSummaryConfig:
    run_id: str
    phase2_root: Optional[Path] = None
    out_path: Optional[Path] = None
    funnel_out_path: Optional[Path] = None
    top_fail_reasons: int = 10


@dataclass
class EvaluationSummaryResult:
    run_id: str
    generated_at: str
    phase2_root: str
    source_files: Dict[str, str]
    primary_event_ids: List[str]
    event_families: List[str]
    total_candidates: int
    gate_pass_count: int
    gate_pass_rate: float
    top_fail_reasons: List[Dict[str, Any]]
    by_primary_event_id: Dict[str, Dict[str, Any]]
    by_event_family: Dict[str, Dict[str, Any]]
    funnel_payload: Dict[str, Any] = field(default_factory=dict)


class EvaluationSummaryService:
    def __init__(self, data_root: Optional[Path] = None):
        self.data_root = data_root or get_data_root()

    def summarize_run(
        self, run_id: str, config: Optional[EvaluationSummaryConfig] = None
    ) -> EvaluationSummaryResult:
        resolved = config or EvaluationSummaryConfig(run_id=run_id)
        phase2_root = resolved.phase2_root or self.data_root / "reports" / "phase2" / run_id
        validation_root = self.data_root / "reports" / "validation" / run_id
        validation_bundle = None
        try:
            validation_bundle = load_validation_bundle(run_id, base_dir=validation_root)
        except Exception:
            validation_bundle = None

        tables = ValidationService(data_root=self.data_root).load_candidate_tables(run_id)
        candidates_df = select_stage_candidate_table(tables)
        promotion_ready_df = self._read_optional_table(validation_root / "promotion_ready_candidates.parquet")
        rejection_df = self._read_optional_table(validation_root / "rejection_reasons.parquet")

        primary_event_col = self._first_present_column(
            candidates_df, "primary_event_id", "event_type", "event_id"
        )
        family_col = self._first_present_column(
            candidates_df, "family", "event_family", "research_family"
        )

        total_candidates = int(len(candidates_df))
        gate_pass_count = (
            int(len(promotion_ready_df))
            if not promotion_ready_df.empty
            else int(len(getattr(validation_bundle, "validated_candidates", [])))
        )
        gate_pass_rate = float(gate_pass_count / total_candidates) if total_candidates else 0.0

        primary_event_ids = self._sorted_unique_values(candidates_df, primary_event_col)
        event_families = self._sorted_unique_values(candidates_df, family_col)

        source_files = {
            "phase2_root": str(phase2_root),
            "validation_root": str(validation_root),
        }
        for label, path in {
            "edge_candidates": self.data_root / "reports" / "edge_candidates" / run_id / "edge_candidates_normalized.parquet",
            "phase2_candidates": phase2_root / "phase2_candidates.parquet",
            "promotion_ready_candidates": validation_root / "promotion_ready_candidates.parquet",
            "rejection_reasons": validation_root / "rejection_reasons.parquet",
            "validation_bundle": validation_root / "validation_bundle.json",
        }.items():
            if path.exists():
                source_files[label] = str(path)

        result = EvaluationSummaryResult(
            run_id=run_id,
            generated_at=datetime.now().isoformat(),
            phase2_root=str(phase2_root),
            source_files=source_files,
            primary_event_ids=primary_event_ids,
            event_families=event_families,
            total_candidates=total_candidates,
            gate_pass_count=gate_pass_count,
            gate_pass_rate=gate_pass_rate,
            top_fail_reasons=self._top_fail_reasons(rejection_df, limit=resolved.top_fail_reasons),
            by_primary_event_id=self._group_counts(
                candidates_df,
                promotion_ready_df,
                key_col=primary_event_col,
            ),
            by_event_family=self._group_counts(
                candidates_df,
                promotion_ready_df,
                key_col=family_col,
            ),
            funnel_payload=self._build_funnel_payload(
                total_candidates=total_candidates,
                promotion_ready_df=promotion_ready_df,
                rejection_df=rejection_df,
                validation_bundle=validation_bundle,
            ),
        )

        if resolved.out_path is not None:
            resolved.out_path.parent.mkdir(parents=True, exist_ok=True)
            resolved.out_path.write_text(json.dumps(result.__dict__, indent=2), encoding="utf-8")
        if resolved.funnel_out_path is not None:
            resolved.funnel_out_path.parent.mkdir(parents=True, exist_ok=True)
            resolved.funnel_out_path.write_text(
                json.dumps(result.funnel_payload, indent=2),
                encoding="utf-8",
            )
        return result

    def _read_optional_table(self, path: Path) -> pd.DataFrame:
        try:
            return read_table_auto(path)
        except Exception:
            return pd.DataFrame()

    def _first_present_column(self, frame: pd.DataFrame, *columns: str) -> str | None:
        for column in columns:
            if column in frame.columns:
                return column
        return None

    def _sorted_unique_values(self, frame: pd.DataFrame, column: str | None) -> List[str]:
        if column is None or frame.empty:
            return []
        values = (
            frame[column]
            .dropna()
            .astype(str)
            .map(str.strip)
        )
        return sorted(value for value in values.unique().tolist() if value)

    def _top_fail_reasons(self, rejection_df: pd.DataFrame, *, limit: int) -> List[Dict[str, Any]]:
        if rejection_df.empty:
            return []
        counts: dict[str, int] = {}
        reason_col = "reason_codes" if "reason_codes" in rejection_df.columns else "validation_reason_codes"
        if reason_col not in rejection_df.columns:
            return []
        for raw in rejection_df[reason_col].dropna().astype(str):
            for token in raw.split("|"):
                reason = token.strip()
                if not reason:
                    continue
                counts[reason] = counts.get(reason, 0) + 1
        ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        return [
            {"reason": reason, "count": count}
            for reason, count in ranked[: max(0, int(limit))]
        ]

    def _group_counts(
        self,
        candidates_df: pd.DataFrame,
        promotion_ready_df: pd.DataFrame,
        *,
        key_col: str | None,
    ) -> Dict[str, Dict[str, Any]]:
        if key_col is None or candidates_df.empty:
            return {}
        promoted_ids = set(
            promotion_ready_df.get("candidate_id", pd.Series(dtype="object"))
            .dropna()
            .astype(str)
            .tolist()
        )
        grouped: Dict[str, Dict[str, Any]] = {}
        for key, group in candidates_df.groupby(key_col):
            label = str(key).strip()
            if not label:
                continue
            candidate_ids = set(group.get("candidate_id", pd.Series(dtype="object")).astype(str).tolist())
            grouped[label] = {
                "candidate_count": int(len(group)),
                "gate_pass_count": int(len(candidate_ids & promoted_ids)),
            }
        return grouped

    def _build_funnel_payload(
        self,
        *,
        total_candidates: int,
        promotion_ready_df: pd.DataFrame,
        rejection_df: pd.DataFrame,
        validation_bundle: Any,
    ) -> Dict[str, Any]:
        if validation_bundle is not None and hasattr(validation_bundle, "summary_stats"):
            payload = dict(getattr(validation_bundle, "summary_stats", {}) or {})
        else:
            payload = {}
        payload.setdefault("total", int(total_candidates))
        payload.setdefault("validated", int(len(promotion_ready_df)))
        payload.setdefault("rejected", int(len(rejection_df)))
        payload.setdefault("inconclusive", 0)
        return payload


STAGE_CANDIDATE_SOURCES = ("edge_candidates", "phase2_candidates")


def select_stage_candidate_table(
    tables: dict[str, pd.DataFrame],
    *,
    sources: tuple[str, ...] = STAGE_CANDIDATE_SOURCES,
) -> pd.DataFrame:
    for source in sources:
        table = tables.get(source, pd.DataFrame())
        if not table.empty:
            return table.copy()
    return pd.DataFrame()


class ValidationService:
    def __init__(self, data_root: Optional[Path] = None):
        self.data_root = data_root or get_data_root()

    def load_candidate_tables(self, run_id: str) -> dict[str, pd.DataFrame]:
        return {
            "promotion_audit": self._read_table(
                self.data_root / "reports" / "promotions" / run_id / "promotion_statistical_audit.parquet"
            ),
            "edge_candidates": self._read_table(
                self.data_root / "reports" / "edge_candidates" / run_id / "edge_candidates_normalized.parquet"
            ),
            "phase2_candidates": self._read_table(
                self.data_root / "reports" / "phase2" / run_id / "phase2_candidates.parquet"
            ),
        }

    def _read_table(self, path: Path) -> pd.DataFrame:
        return read_table_auto(path)

    def create_validation_bundle(
        self, 
        run_id: str, 
        candidates_df: pd.DataFrame, 
        program_id: Optional[str] = None
    ) -> ValidationBundle:
        validated = []
        rejected = []
        inconclusive = []
        
        for _, row in candidates_df.iterrows():
            record = self._map_row_to_validated_record(row, run_id, program_id)
            if record.decision.status == "validated":
                validated.append(record)
            elif record.decision.status == "rejected":
                rejected.append(record)
            else:
                inconclusive.append(record)
        
        # Sprint 4: Add effect stability report
        try:
            from project.operator.stability import build_regime_split_report
            effect_stability_report = build_regime_split_report(run_id=run_id, data_root=self.data_root)
        except Exception:
            effect_stability_report = {}

        bundle = ValidationBundle(
            run_id=run_id,
            program_id=program_id,
            created_at=datetime.now().isoformat(),
            validated_candidates=validated,
            rejected_candidates=rejected,
            inconclusive_candidates=inconclusive,
            summary_stats={
                "total": len(candidates_df),
                "validated": len(validated),
                "rejected": len(rejected),
                "inconclusive": len(inconclusive),
                "rejection_reasons": {
                    reason: sum(1 for c in rejected if reason in c.decision.reason_codes)
                    for reason in set(r for c in rejected for r in c.decision.reason_codes)
                }
            },
            effect_stability_report=effect_stability_report
        )
        return bundle

    def _map_row_to_validated_record(
        self, 
        row: pd.Series | Dict[str, Any], 
        run_id: str, 
        program_id: Optional[str] = None
    ) -> ValidatedCandidateRecord:
        candidate_id = str(row.get("candidate_id", ""))
        
        # Determine status and reasons based on common columns
        # This is a basic mapping, can be expanded
        reasons = []
        
        # Check for common failure gates. OOS failure is a definite validation
        # failure, not an inconclusive state; reserve inconclusive for missing data.
        gates_to_check = (
            ("gate_oos_validation", ValidationReasonCodes.FAILED_OOS_VALIDATION),
            ("gate_after_cost_positive", ValidationReasonCodes.FAILED_COST_SURVIVAL),
            ("gate_after_cost_stressed_positive", ValidationReasonCodes.FAILED_COST_SURVIVAL),
            ("gate_c_regime_stable", ValidationReasonCodes.FAILED_REGIME_SUPPORT),
            ("gate_multiplicity", ValidationReasonCodes.FAILED_MULTIPLICITY_THRESHOLD),
        )
        
        for gate, code in gates_to_check:
            val = row.get(gate)
            if val is not None:
                if not bool(val):
                    reasons.append(code)
        
        # Special check for n_events
        n_events = row.get("n_events", row.get("n_obs", 0))
        if n_events < 20: # Example threshold
            reasons.append(ValidationReasonCodes.INSUFFICIENT_SAMPLE_SUPPORT)

        status = "validated" if not reasons else "rejected"
        
        # If it's missing critical data, it might be inconclusive
        if pd.isna(row.get("p_value")) and pd.isna(row.get("expectancy")):
             status = "inconclusive"
             reasons.append(ValidationReasonCodes.INSUFFICIENT_DATA)

        decision = ValidationDecision(
            status=status,
            candidate_id=candidate_id,
            run_id=run_id,
            program_id=program_id,
            reason_codes=list(dict.fromkeys(reasons)),
            summary=""
        )
        
        metrics = ValidationMetrics(
            sample_count=int(n_events) if not pd.isna(n_events) else None,
            expectancy=float(row.get("expectancy")) if not pd.isna(row.get("expectancy")) else None,
            net_expectancy=float(row.get("net_expectancy_bps")) if not pd.isna(row.get("net_expectancy_bps")) else None,
            p_value=float(row.get("p_value")) if not pd.isna(row.get("p_value")) else None,
            q_value=float(row.get("q_value")) if not pd.isna(row.get("q_value")) else None,
            stability_score=float(row.get("stability_score")) if not pd.isna(row.get("stability_score")) else None,
        )
        
        return ValidatedCandidateRecord(
            candidate_id=candidate_id,
            decision=decision,
            metrics=metrics,
            anchor_summary=str(row.get("anchor_summary", "")),
            template_id=str(row.get("rule_template", row.get("template_id", ""))),
            direction=str(row.get("direction", "")),
            horizon_bars=int(str(row.get("horizon", row.get("horizon_bars", 0))).replace("b", "")),
            detector_lineage=extract_detector_lineage(row),
            validation_stage_version="v1"
        )

    def run_validation_stage(
        self, 
        run_id: str, 
        candidates_df: pd.DataFrame, 
        program_id: Optional[str] = None
    ) -> ValidationBundle:
        bundle = self.create_validation_bundle(run_id, candidates_df, program_id)
        # Sprint 4: Use canonical names via result_writer
        from project.research.validation.result_writer import (
            write_validation_bundle,
            write_validated_candidate_tables,
        )
        base_dir = self.data_root / "reports" / "validation" / run_id
        bundle_path = write_validation_bundle(bundle, base_dir=base_dir)
        table_paths = write_validated_candidate_tables(bundle, base_dir=base_dir)
        required_outputs = {
            "validation_bundle": bundle_path,
            "promotion_ready_candidates": table_paths.get("promotion_ready_candidates"),
        }
        missing_outputs = [
            name
            for name, path in required_outputs.items()
            if path is None or not Path(path).exists()
        ]
        if missing_outputs:
            raise MissingArtifactError(
                "validation stage failed to materialize canonical outputs: "
                + ", ".join(sorted(missing_outputs))
            )
        
        # Sprint 7: Artifact manifest
        from project.research.validation.manifest import RunArtifactManifest
        from datetime import datetime, timezone
        
        manifest = RunArtifactManifest(
            run_id=run_id,
            stage="validate",
            created_at=datetime.now(timezone.utc).isoformat(),
            upstream_run_ids=[run_id], # Discovery run_id is usually same as validation run_id for now
            artifacts={
                "validation_bundle": "validation_bundle.json",
                "validated_candidates": "validated_candidates.parquet",
                "rejection_reasons": "rejection_reasons.parquet",
                "validation_report": "validation_report.json",
                "effect_stability_report": "effect_stability_report.json",
                "promotion_ready_candidates": "promotion_ready_candidates.parquet",
            }
        )
        manifest.persist(base_dir)
        
        return bundle
