from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

RELIABILITY_SCHEMA_VERSION = "phase6_reliability_v1"
SMOKE_DATASET_VERSION = "smoke_dataset_v1"


@dataclass(frozen=True)
class ArtifactSchemaSpec:
    artifact_type: str
    schema_version: str
    required_columns: tuple[str, ...]
    optional_columns: tuple[str, ...] = ()
    non_null_columns: tuple[str, ...] = ()
    monotonic_by: tuple[str, ...] = ()
    unique_by: tuple[str, ...] = ()
    enum_columns: dict[str, tuple[Any, ...]] = field(default_factory=dict)


@dataclass(frozen=True)
class ManifestSchemaSpec:
    manifest_type: str
    schema_version: str
    required_keys: tuple[str, ...]
    artifact_inventory_key: str | None = None


STRATEGY_TRACE_SCHEMA = ArtifactSchemaSpec(
    artifact_type="strategy_trace",
    schema_version="strategy_trace_v1",
    required_columns=(
        "timestamp",
        "strategy",
        "symbol",
        "signal_position",
        "target_position",
        "executed_position",
        "gross_pnl",
        "net_pnl",
    ),
    optional_columns=("fill_mode", "fill_price", "allocation_scale", "allocation_clip_reason"),
    non_null_columns=(
        "timestamp",
        "strategy",
        "symbol",
        "signal_position",
        "target_position",
        "executed_position",
    ),
    monotonic_by=("timestamp",),
)

PORTFOLIO_LEDGER_SCHEMA = ArtifactSchemaSpec(
    artifact_type="portfolio_ledger",
    schema_version="portfolio_frame_v1",
    required_columns=(
        "timestamp",
        "gross_pnl",
        "net_pnl",
        "equity",
        "equity_return",
        "gross_exposure",
        "net_exposure",
        "turnover",
    ),
    non_null_columns=("timestamp", "equity"),
    monotonic_by=("timestamp",),
)

CANDIDATE_TABLE_SCHEMA = ArtifactSchemaSpec(
    artifact_type="phase2_candidates",
    schema_version="phase2_candidates_v2",
    required_columns=(
        "candidate_id",
        "event_type",
        "symbol",
        "run_id",
        "split_scheme_id",
        "estimate_bps",
        "p_value_raw",
        "p_value_adj",
        "correction_family_id",
        "correction_method",
    ),
    optional_columns=(
        "q_value",
        "q_value_by",
        "q_value_cluster",
        "n_obs",
        "n_clusters",
        "hypothesis_id",
    ),
    non_null_columns=("candidate_id", "event_type", "symbol", "run_id", "correction_family_id"),
    unique_by=("candidate_id",),
)

PROMOTION_AUDIT_SCHEMA = ArtifactSchemaSpec(
    artifact_type="promotion_audit",
    schema_version="promotion_audit_v2",
    required_columns=(
        "candidate_id",
        "event_type",
        "promotion_decision",
        "promotion_track",
        "policy_version",
        "bundle_version",
        "is_reduced_evidence",
        "gate_promo_statistical",
        "gate_promo_stability",
        "gate_promo_cost_survival",
        "gate_promo_negative_control",
    ),
    optional_columns=(
        "evidence_bundle_json",
        "bundle_rejection_reasons",
        "promotion_score",
        "q_value",
    ),
    non_null_columns=(
        "candidate_id",
        "event_type",
        "promotion_decision",
        "promotion_track",
        "policy_version",
        "bundle_version",
        "is_reduced_evidence",
    ),
    enum_columns={
        "promotion_decision": ("promoted", "rejected"),
        "gate_promo_statistical": ("pass", "fail", "missing_evidence"),
        "gate_promo_stability": ("pass", "fail", "missing_evidence"),
        "gate_promo_cost_survival": ("pass", "fail", "missing_evidence"),
        "gate_promo_negative_control": ("pass", "fail", "missing_evidence"),
    },
)

EVIDENCE_BUNDLE_SUMMARY_SCHEMA = ArtifactSchemaSpec(
    artifact_type="evidence_bundle_summary",
    schema_version="phase4_bundle_v1",
    required_columns=(
        "candidate_id",
        "event_type",
        "promotion_decision",
        "promotion_track",
        "policy_version",
        "bundle_version",
        "is_reduced_evidence",
    ),
    optional_columns=("rank_score", "rejection_reasons", "q_value", "control_pass_rate"),
    non_null_columns=(
        "candidate_id",
        "event_type",
        "promotion_decision",
        "promotion_track",
        "is_reduced_evidence",
    ),
)

PROMOTION_DECISION_SCHEMA = ArtifactSchemaSpec(
    artifact_type="promotion_decisions",
    schema_version="phase4_bundle_v1",
    required_columns=(
        "candidate_id",
        "event_type",
        "promotion_decision",
        "promotion_track",
        "policy_version",
        "bundle_version",
        "is_reduced_evidence",
    ),
    optional_columns=("rank_score", "rejection_reasons"),
    non_null_columns=(
        "candidate_id",
        "event_type",
        "promotion_decision",
        "promotion_track",
        "is_reduced_evidence",
    ),
)

ENGINE_MANIFEST_SCHEMA = ManifestSchemaSpec(
    manifest_type="engine_run_manifest",
    schema_version="engine_run_manifest_v1",
    required_keys=(
        "manifest_type",
        "manifest_version",
        "run_id",
        "artifacts",
        "schemas",
        "metrics",
    ),
    artifact_inventory_key="artifacts",
)

STAGE_MANIFEST_SCHEMA = ManifestSchemaSpec(
    manifest_type="stage_manifest",
    schema_version="stage_manifest_v1",
    required_keys=(
        "run_id",
        "stage",
        "stage_instance_id",
        "status",
        "parameters",
        "inputs",
        "outputs",
        "ontology_spec_hash",
    ),
    artifact_inventory_key="outputs",
)
