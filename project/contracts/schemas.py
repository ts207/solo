from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List

import pandas as pd


@dataclass(frozen=True)
class DataFrameSchemaContract:
    name: str
    required_columns: tuple[str, ...]
    optional_columns: tuple[str, ...] = ()
    schema_version: str = "phase5_schema_v1"


_SCHEMA_REGISTRY: Dict[str, DataFrameSchemaContract] = {
    "phase2_candidates": DataFrameSchemaContract(
        name="phase2_candidates",
        required_columns=("candidate_id", "hypothesis_id", "event_type", "symbol", "run_id"),
        optional_columns=(
            "estimate_bps",
            "q_value",
            "split_scheme_id",
            "canonical_regime",
            "subtype",
            "phase",
            "evidence_mode",
            "recommended_bucket",
            "regime_bucket",
            "routing_profile_id",
        ),
    ),
    "promotion_audit": DataFrameSchemaContract(
        name="promotion_audit",
        required_columns=("candidate_id", "event_type", "promotion_decision", "promotion_track"),
        optional_columns=(
            "hypothesis_id",
            "q_value",
            "q_value_scope",
            "effective_q_value",
            "num_tests_scope",
            "num_tests_family",
            "num_tests_campaign",
            "num_tests_effective",
            "multiplicity_scope_mode",
            "multiplicity_scope_key",
            "multiplicity_scope_version",
            "multiplicity_scope_degraded",
            "multiplicity_scope_reason",
            "multiplicity_context",
            "promotion_score",
            "bundle_version",
            "policy_version",
            "evidence_bundle_json",
            "canonical_regime",
            "subtype",
            "phase",
            "evidence_mode",
            "recommended_bucket",
            "regime_bucket",
            "routing_profile_id",
            "promotion_class",
            "readiness_status",
            "inventory_reason_code",
            "deployment_state_default",
            "campaign_id",
            "program_id",
            "concept_lineage_key",
            "stat_regime",
            "audit_status",
            "artifact_audit_version",
            "search_proposals_attempted",
            "search_candidates_generated",
            "search_candidates_scored",
            "search_candidates_eligible",
            "search_parameterizations_attempted",
            "search_mutations_attempted",
            "search_directions_tested",
            "search_confirmations_attempted",
            "search_trigger_variants_attempted",
            "search_family_count",
            "search_lineage_count",
            "search_scope_version",
            "search_burden_estimated",
        ),
    ),
    "promoted_candidates": DataFrameSchemaContract(
        name="promoted_candidates",
        required_columns=("candidate_id", "event_type", "status"),
        optional_columns=(
            "hypothesis_id",
            "promotion_track",
            "selection_score",
            "bundle_version",
            "policy_version",
            "canonical_regime",
            "subtype",
            "phase",
            "evidence_mode",
            "recommended_bucket",
            "regime_bucket",
            "routing_profile_id",
            "promotion_class",
            "readiness_status",
            "inventory_reason_code",
            "deployment_state_default",
            "q_value",
            "q_value_scope",
            "effective_q_value",
            "num_tests_scope",
            "num_tests_family",
            "num_tests_campaign",
            "num_tests_effective",
            "multiplicity_scope_mode",
            "multiplicity_scope_key",
            "multiplicity_scope_version",
            "campaign_id",
            "program_id",
            "concept_lineage_key",
            "stat_regime",
            "audit_status",
            "artifact_audit_version",
            "search_proposals_attempted",
            "search_candidates_generated",
            "search_candidates_scored",
            "search_candidates_eligible",
            "search_parameterizations_attempted",
            "search_mutations_attempted",
            "search_directions_tested",
            "search_confirmations_attempted",
            "search_trigger_variants_attempted",
            "search_family_count",
            "search_lineage_count",
            "search_scope_version",
            "search_burden_estimated",
        ),
    ),
    "evidence_bundle_summary": DataFrameSchemaContract(
        name="evidence_bundle_summary",
        required_columns=(
            "candidate_id",
            "event_type",
            "promotion_decision",
            "promotion_track",
            "policy_version",
            "bundle_version",
            "is_reduced_evidence",
        ),
        optional_columns=(
            "hypothesis_id",
            "rank_score",
            "rejection_reasons",
            "canonical_regime",
            "subtype",
            "phase",
            "evidence_mode",
            "recommended_bucket",
            "regime_bucket",
            "routing_profile_id",
            "q_value",
            "q_value_scope",
            "effective_q_value",
            "num_tests_scope",
            "num_tests_family",
            "num_tests_campaign",
            "num_tests_effective",
            "multiplicity_scope_mode",
            "multiplicity_scope_key",
            "multiplicity_scope_version",
            "campaign_id",
            "program_id",
            "concept_lineage_key",
            "stat_regime",
            "audit_status",
            "artifact_audit_version",
            "search_proposals_attempted",
            "search_candidates_generated",
            "search_candidates_scored",
            "search_candidates_eligible",
            "search_parameterizations_attempted",
            "search_mutations_attempted",
            "search_directions_tested",
            "search_confirmations_attempted",
            "search_trigger_variants_attempted",
            "search_family_count",
            "search_lineage_count",
            "search_scope_version",
            "search_burden_estimated",
        ),
    ),
    "promotion_decisions": DataFrameSchemaContract(
        name="promotion_decisions",
        required_columns=(
            "candidate_id",
            "event_type",
            "promotion_decision",
            "promotion_track",
            "policy_version",
            "bundle_version",
            "is_reduced_evidence",
        ),
        optional_columns=(
            "hypothesis_id",
            "rank_score",
            "rejection_reasons",
            "canonical_regime",
            "subtype",
            "phase",
            "evidence_mode",
            "recommended_bucket",
            "regime_bucket",
            "routing_profile_id",
        ),
    ),
}


def get_schema_contract(name: str) -> DataFrameSchemaContract:
    try:
        return _SCHEMA_REGISTRY[str(name)]
    except KeyError as exc:
        raise KeyError(f"unknown dataframe schema: {name}") from exc


def normalize_dataframe_for_schema(df: pd.DataFrame, schema_name: str) -> pd.DataFrame:
    schema = get_schema_contract(schema_name)
    out = df.copy()
    for col in schema.required_columns + schema.optional_columns:
        if col not in out.columns:
            out[col] = pd.NA
    ordered = list(schema.required_columns) + [
        c for c in schema.optional_columns if c in out.columns
    ]
    remainder = [c for c in out.columns if c not in ordered]
    return out[ordered + remainder]


def validate_dataframe_for_schema(
    df: pd.DataFrame, schema_name: str, *, allow_empty: bool = True
) -> pd.DataFrame:
    schema = get_schema_contract(schema_name)
    out = normalize_dataframe_for_schema(df, schema_name)
    if out.empty and allow_empty:
        return out
    missing = [col for col in schema.required_columns if col not in out.columns]
    if missing:
        raise ValueError(
            f"dataframe for schema '{schema_name}' missing required columns: {missing}"
        )
    # Empty values in required columns are allowed only on empty frames.
    if not out.empty:
        bad = [col for col in schema.required_columns if out[col].isna().all()]
        if bad:
            raise ValueError(
                f"dataframe for schema '{schema_name}' has all-null required columns: {bad}"
            )
        if schema_name == "phase2_candidates":
            identical = (
                out["candidate_id"].astype(str).str.strip()
                == out["hypothesis_id"].astype(str).str.strip()
            )
            if bool(identical.all()):
                raise ValueError(
                    "dataframe for schema 'phase2_candidates' has collapsed lineage: "
                    "candidate_id must differ from hypothesis_id for current-format artifacts"
                )
    return out
