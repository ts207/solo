from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

_VALID_STRICTNESS = frozenset({"strict", "transitional", "legacy_compatible", "advisory"})


@dataclass(frozen=True)
class DataFrameSchemaContract:
    name: str
    required_columns: tuple[str, ...]
    optional_columns: tuple[str, ...] = ()
    schema_version: str = "phase5_schema_v1"
    strictness: str = "strict"

    @property
    def schema_id(self) -> str:
        return self.name


@dataclass(frozen=True)
class PayloadSchemaContract:
    name: str
    required_fields: tuple[tuple[str, type], ...]
    optional_fields: tuple[tuple[str, type], ...] = ()
    schema_version: str = "phase5_payload_schema_v1"
    strictness: str = "strict"
    version_field: str | None = None
    version_value: Any = None

    @property
    def schema_id(self) -> str:
        return self.name


_SCHEMA_REGISTRY: dict[str, DataFrameSchemaContract] = {
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
    "promotion_ready_candidates": DataFrameSchemaContract(
        name="promotion_ready_candidates",
        required_columns=(
            "candidate_id",
            "validation_status",
            "validation_run_id",
            "validation_program_id",
            "metric_sample_count",
            "metric_q_value",
            "metric_stability_score",
            "metric_net_expectancy",
        ),
        optional_columns=(
            "anchor_summary",
            "template_id",
            "direction",
            "horizon_bars",
            "validation_stage_version",
            "validation_reason_codes",
            "metric_effective_sample_size",
            "metric_expectancy",
            "metric_hit_rate",
            "metric_p_value",
            "metric_cost_sensitivity",
            "metric_turnover",
            "metric_regime_support_score",
            "metric_time_slice_support_score",
            "metric_negative_control_score",
            "metric_max_drawdown",
            "source_event_name",
            "source_event_version",
            "source_detector_class",
            "source_evidence_mode",
            "source_threshold_version",
            "source_calibration_artifact",
        ),
    ),
}


_PAYLOAD_SCHEMA_REGISTRY: dict[str, PayloadSchemaContract] = {
    "validation_bundle": PayloadSchemaContract(
        name="validation_bundle",
        required_fields=(
            ("run_id", str),
            ("created_at", str),
            ("validated_candidates", list),
            ("rejected_candidates", list),
            ("inconclusive_candidates", list),
            ("summary_stats", dict),
            ("effect_stability_report", dict),
        ),
        schema_version="validation_bundle_v1",
    ),
    "promoted_theses_payload": PayloadSchemaContract(
        name="promoted_theses_payload",
        required_fields=(
            ("schema_version", str),
            ("run_id", str),
            ("generated_at_utc", str),
            ("thesis_count", int),
            ("active_thesis_count", int),
            ("pending_thesis_count", int),
            ("theses", list),
        ),
        schema_version="promoted_theses_v1",
        version_field="schema_version",
        version_value="promoted_theses_v1",
    ),
    "live_thesis_index": PayloadSchemaContract(
        name="live_thesis_index",
        required_fields=(
            ("schema_version", str),
            ("latest_run_id", str),
            ("default_resolution_disabled", bool),
            ("runs", dict),
        ),
        schema_version="promoted_thesis_index_v1",
        version_field="schema_version",
        version_value="promoted_thesis_index_v1",
    ),
    "run_manifest": PayloadSchemaContract(
        name="run_manifest",
        required_fields=(("run_id", str),),
        optional_fields=(
            ("status", str),
            ("started_at", str),
            ("finished_at", str),
            ("planned_stage_instances", list),
            ("stage_timings_sec", dict),
        ),
        schema_version="run_manifest_v1",
    ),
}


def get_schema_contract(name: str) -> DataFrameSchemaContract:
    try:
        return _SCHEMA_REGISTRY[str(name)]
    except KeyError as exc:
        raise KeyError(f"unknown dataframe schema: {name}") from exc


def list_schema_contracts() -> tuple[DataFrameSchemaContract, ...]:
    return tuple(_SCHEMA_REGISTRY[key] for key in sorted(_SCHEMA_REGISTRY))


def get_payload_schema_contract(name: str) -> PayloadSchemaContract:
    try:
        return _PAYLOAD_SCHEMA_REGISTRY[str(name)]
    except KeyError as exc:
        raise KeyError(f"unknown payload schema: {name}") from exc


def list_payload_schema_contracts() -> tuple[PayloadSchemaContract, ...]:
    return tuple(_PAYLOAD_SCHEMA_REGISTRY[key] for key in sorted(_PAYLOAD_SCHEMA_REGISTRY))


def schema_contract_exists(name: str) -> bool:
    token = str(name)
    return token in _SCHEMA_REGISTRY or token in _PAYLOAD_SCHEMA_REGISTRY


def get_any_schema_contract(name: str) -> DataFrameSchemaContract | PayloadSchemaContract:
    token = str(name)
    if token in _SCHEMA_REGISTRY:
        return _SCHEMA_REGISTRY[token]
    if token in _PAYLOAD_SCHEMA_REGISTRY:
        return _PAYLOAD_SCHEMA_REGISTRY[token]
    raise KeyError(f"unknown schema contract: {name}")


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


def validate_schema_at_producer(
    df: pd.DataFrame,
    schema_name: str,
    *,
    context: str = "",
) -> list[str]:
    """Enforce schema contract at the producer boundary.

    Behaviour is governed by the schema's strictness level:
      strict            — raises ContractViolationError on first violation
      transitional      — logs warnings but does not raise
      legacy_compatible — collects issues and returns them without raising
      advisory          — returns issues only, no side-effects

    Returns a (possibly empty) list of issue strings.
    """
    import logging

    from project.core.exceptions import ContractViolationError

    schema = get_schema_contract(schema_name)
    issues: list[str] = []

    if df is None:
        issues.append(f"[{schema_name}] producer passed None instead of DataFrame")
    else:
        missing = [c for c in schema.required_columns if c not in df.columns]
        if missing:
            issues.append(f"[{schema_name}] missing required columns: {missing}")
        if not df.empty:
            all_null = [c for c in schema.required_columns if c in df.columns and df[c].isna().all()]
            if all_null:
                issues.append(f"[{schema_name}] all-null required columns: {all_null}")

    if not issues:
        return []

    tag = f" ({context})" if context else ""
    msg = "; ".join(issues) + tag

    if schema.strictness == "strict":
        raise ContractViolationError(msg)
    if schema.strictness == "transitional":
        logging.getLogger(__name__).warning("schema contract violation: %s", msg)
    elif schema.strictness == "legacy_compatible":
        logging.getLogger(__name__).debug("schema contract violation (legacy_compatible): %s", msg)

    return issues


def validate_payload_for_schema(
    payload: dict[str, Any],
    schema_name: str,
) -> dict[str, Any]:
    schema = get_payload_schema_contract(schema_name)
    if not isinstance(payload, dict):
        raise ValueError(f"payload for schema '{schema_name}' must be a mapping")
    for field_name, field_type in schema.required_fields:
        if field_name not in payload:
            raise ValueError(f"payload for schema '{schema_name}' missing required field {field_name!r}")
        if not isinstance(payload[field_name], field_type):
            raise ValueError(
                f"payload for schema '{schema_name}' field {field_name!r} must be {field_type.__name__}"
            )
    for field_name, field_type in schema.optional_fields:
        if field_name in payload and payload[field_name] is not None and not isinstance(
            payload[field_name], field_type
        ):
            raise ValueError(
                f"payload for schema '{schema_name}' field {field_name!r} must be {field_type.__name__}"
            )
    if schema.version_field is not None:
        actual = payload.get(schema.version_field)
        if actual != schema.version_value:
            raise ValueError(
                f"payload for schema '{schema_name}' field {schema.version_field!r} "
                f"must be {schema.version_value!r}"
            )
    return dict(payload)
