from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from typing import Any

STATIC_ENTITY_COLUMNS = [
    "entity_id",
    "entity_type",
    "name",
    "title",
    "family",
    "enabled",
    "source_path",
    "description",
    "attributes_json",
]

STATIC_RELATION_COLUMNS = [
    "relation_id",
    "from_entity_id",
    "relation_type",
    "to_entity_id",
    "source_path",
    "attributes_json",
]

STATIC_DOCUMENT_COLUMNS = [
    "document_id",
    "entity_id",
    "entity_type",
    "title",
    "content",
    "source_path",
    "metadata_json",
]

KNOB_COLUMNS = [
    "knob_id",
    "scope",
    "group",
    "name",
    "cli_flag",
    "value_type",
    "default_value_json",
    "choices_json",
    "description",
    "source_module",
    "agent_level",
    "mutability",
    "risk",
]

TESTED_REGION_COLUMNS = [
    "region_key",
    "program_id",
    "run_id",
    "hypothesis_id",
    "candidate_id",
    "symbol_scope",
    "event_type",
    "trigger_type",
    "trigger_key",
    "trigger_payload_json",
    "state_id",
    "from_state",
    "to_state",
    "feature",
    "operator",
    "threshold",
    "template_id",
    "direction",
    "horizon",
    "entry_lag",
    "context_hash",
    "context_json",
    "eval_status",
    "train_n_obs",
    "validation_n_obs",
    "test_n_obs",
    "q_value",
    "mean_return_bps",
    "after_cost_expectancy",
    "stressed_after_cost_expectancy",
    "robustness_score",
    "gate_bridge_tradable",
    "gate_promo_statistical",
    "gate_promo_retail_net_expectancy",
    "mechanical_status",
    "primary_fail_gate",
    "warning_count",
    "updated_at",
    # Phase 1.3 additions — failure metadata for probabilistic avoidance
    "failure_confidence",       # float 0.0-1.0: confidence in this failure conclusion
    "failure_cause_class",      # str: mechanical | insufficient_sample | cost | market | overfitting
    "failure_sample_size",      # int: train_n_obs at time of failure (0 means unknown)
]

FAILURE_COLUMNS = [
    "run_id",
    "program_id",
    "stage",
    "failure_class",
    "failure_detail",
    "artifact_path",
    "is_mechanical",
    "is_repeated",
    "superseded_by_run_id",
]

PROPOSAL_AUDIT_COLUMNS = [
    "proposal_id",
    "program_id",
    "run_id",
    "issued_at",
    "proposal_path",
    "experiment_config_path",
    "run_all_overrides_path",
    "status",
    "plan_only",
    "dry_run",
    "returncode",
    "objective_name",
    "promotion_profile",
    "symbols",
    "command_json",
    "validated_plan_json",
    "bounded_json",
    "baseline_run_id",
    "experiment_type",
    "allowed_change_field",
    "campaign_id",
    "cycle_number",
    "branch_id",
    "parent_run_id",
    "mutation_type",
    "branch_depth",
    "decision",
]

EVIDENCE_LEDGER_COLUMNS = [
    "ledger_id",
    "program_id",
    "run_id",
    "baseline_run_id",
    "proposal_id",
    "experiment_type",
    "changed_field",
    "frozen_fields_json",
    "date_range",
    "symbol_scope",
    "event_scope",
    "template_scope",
    "horizon_scope",
    "entry_lag_scope",
    "terminal_status",
    "run_status",
    "mechanical_outcome",
    "statistical_outcome",
    "candidate_count",
    "promoted_count",
    "primary_metric_name",
    "primary_metric_value",
    "top_candidate_label",
    "verdict",
    "recommended_next_action",
    "recommended_next_experiment",
    "negative_diagnosis",
    "regime_classification",
    "summary_path",
    "campaign_id",
    "cycle_number",
    "branch_id",
    "parent_run_id",
    "mutation_type",
    "branch_depth",
    "decision",
    "created_at",
    "updated_at",
]

REFLECTION_COLUMNS = [
    "run_id",
    "program_id",
    "objective",
    "executed_scope",
    "run_status",
    "planned_stage_count",
    "completed_stage_count",
    "warning_stage_count",
    "candidate_count",
    "promoted_count",
    "primary_fail_gate",
    "mechanical_outcome",
    "statistical_outcome",
    "market_findings",
    "system_findings",
    "anomalies",
    "belief_update",
    "recommended_next_action",
    "recommended_next_experiment",
    "confidence",
    "reflection_version",
    "created_at",
]


def canonical_json(value: Any) -> str:
    if value is None:
        return "{}"
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                payload = json.loads(stripped)
                return json.dumps(payload, sort_keys=True, separators=(",", ":"))
            except json.JSONDecodeError:
                return json.dumps({"value": value}, sort_keys=True, separators=(",", ":"))
        return json.dumps({"value": value}, sort_keys=True, separators=(",", ":"))
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def stable_hash(parts: Iterable[Any]) -> str:
    material = "||".join(str(part) for part in parts)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def entity_id(entity_type: str, name: str) -> str:
    return f"{str(entity_type).strip()}::{str(name).strip()}"


def relation_id(from_entity_id: str, relation_type: str, to_entity_id: str) -> str:
    return stable_hash((from_entity_id, relation_type, to_entity_id))


def region_key(payload: dict[str, Any]) -> str:
    return stable_hash(
        (
            payload.get("program_id", ""),
            payload.get("symbol_scope", ""),
            payload.get("event_type", ""),
            payload.get("trigger_type", ""),
            payload.get("template_id", ""),
            payload.get("direction", ""),
            payload.get("horizon", ""),
            payload.get("entry_lag", ""),
            payload.get("context_hash", ""),
        )
    )
