from __future__ import annotations

import hashlib
import json
import logging
import shutil
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from project.domain.hypotheses import HypothesisSpec
from project.io.utils import write_parquet
from project.research.context_labels import canonicalize_contexts
from project.research.experiment_engine_schema import (
    AgentExperimentRequest,
    ContextSelection,
    EvaluationConfig,
    InstrumentScope,
    PromotionConfig,
    RegistryBundle,
    SearchControl,
    TemplateSelection,
    TriggerSpace,
    ValidatedExperimentPlan,
)
from project.research.experiment_engine_validators import (
    _validate_campaign_status,
    _validate_contexts,
    _validate_event_trigger,
    _validate_feature_predicate_trigger,
    _validate_instrument_compatibility,
    _validate_interaction_trigger,
    _validate_proposal_quality,
    _validate_search_limits,
    _validate_sequence_trigger,
    _validate_state_trigger,
    _validate_templates,
    _validate_transition_trigger,
    expand_hypotheses,
)
from project.research.search.feasibility import FeasibilityError, FeasibilityReport

_LOG = logging.getLogger(__name__)

__all__ = [
    "AgentExperimentRequest",
    "ContextSelection",
    "EvaluationConfig",
    "InstrumentScope",
    "PromotionConfig",
    "RegistryBundle",
    "SearchControl",
    "TemplateSelection",
    "TriggerSpace",
    "ValidatedExperimentPlan",
    "build_experiment_plan",
    "expand_hypotheses",
    "export_experiment_artifacts",
    "load_agent_experiment_config",
    "resolve_required_detectors",
    "resolve_required_features",
    "resolve_required_states",
    "validate_agent_request",
    "validate_registry_consistency",
]


def _repo_relative_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(Path(__file__).resolve().parents[2]).as_posix()
    except Exception:
        return path.as_posix()


def load_agent_experiment_config(path: Path) -> AgentExperimentRequest:
    raw = yaml.safe_load(path.read_text())
    raw_contexts = dict(raw.get("contexts") or {})
    raw_contexts["include"] = canonicalize_contexts(raw_contexts.get("include", {}))
    return AgentExperimentRequest(
        program_id=raw["program_id"],
        run_mode=raw["run_mode"],
        description=raw.get("description", ""),
        instrument_scope=InstrumentScope(**raw["instrument_scope"]),
        trigger_space=TriggerSpace(**raw["trigger_space"]),
        templates=TemplateSelection(**raw["templates"]),
        evaluation=EvaluationConfig(**raw["evaluation"]),
        contexts=ContextSelection(**raw_contexts),
        avoid_region_keys=[str(value).strip() for value in list(raw.get("avoid_region_keys") or []) if str(value).strip()],
        search_control=SearchControl(**raw["search_control"]),
        promotion=PromotionConfig(**raw["promotion"]),
        artifacts=raw.get("artifacts", {}),
    )


def validate_agent_request(
    request: AgentExperimentRequest,
    registries: RegistryBundle,
    *,
    data_root: Path | None = None,
) -> None:
    _validate_templates(request, registries)
    _validate_instrument_compatibility(request, registries)
    _validate_contexts(request, registries)
    _validate_search_limits(request, registries)
    _validate_campaign_status(request, registries, data_root=data_root)
    _validate_proposal_quality(request, registries, data_root=data_root)

    for trigger_type in request.trigger_space.allowed_trigger_types:
        trigger_type_upper = trigger_type.upper()
        if trigger_type_upper == "EVENT":
            _validate_event_trigger(request, registries)
        elif trigger_type_upper == "STATE":
            _validate_state_trigger(request, registries)
        elif trigger_type_upper == "TRANSITION":
            _validate_transition_trigger(request, registries)
        elif trigger_type_upper == "SEQUENCE":
            _validate_sequence_trigger(request, registries)
        elif trigger_type_upper == "FEATURE_PREDICATE":
            _validate_feature_predicate_trigger(request, registries)
        elif trigger_type_upper == "INTERACTION":
            _validate_interaction_trigger(request, registries)
        else:
            raise ValueError(
                f"Unsupported trigger type in experiment config: {trigger_type}"
            )




def _validate_feasibility_threshold(
    *,
    feasible_count: int,
    min_feasible: int,
    report: FeasibilityReport | None = None,
) -> None:
    if int(feasible_count) >= int(min_feasible):
        return
    summary = report.summary(limit=5) if report is not None else {}
    counts = summary.get("counts_by_reason", {}) if isinstance(summary, dict) else {}
    reason_text = ", ".join(f"{k}={v}" for k, v in sorted(counts.items())) or "no detailed reasons"
    raise FeasibilityError(
        "Experiment plan has fewer feasible hypotheses than required "
        f"(feasible={int(feasible_count)}, min_feasible={int(min_feasible)}; {reason_text}).",
        report=report,
    )


def resolve_required_detectors(
    hypotheses: list[HypothesisSpec],
    registries: RegistryBundle,
) -> list[str]:
    detector_map = registries.detectors.get("detector_ownership", {})
    required = set()
    for hypothesis in hypotheses:
        trigger = hypothesis.trigger
        if trigger.trigger_type == "event":
            detector = detector_map.get(trigger.event_id)
            if detector:
                required.add(detector)
        elif trigger.trigger_type == "sequence":
            required.add("EventSequenceDetector")
            if trigger.events:
                for event_id in trigger.events:
                    detector = detector_map.get(event_id)
                    if detector:
                        required.add(detector)
        elif trigger.trigger_type == "interaction":
            required.add("EventInteractionDetector")
            for operand in [trigger.left, trigger.right]:
                detector = detector_map.get(operand)
                if detector:
                    required.add(detector)
    return sorted(required)


def resolve_required_features(
    hypotheses: list[HypothesisSpec],
    registries: RegistryBundle,
) -> list[str]:
    required = set()
    event_meta = registries.events.get("events", {})

    for hypothesis in hypotheses:
        trigger = hypothesis.trigger
        if trigger.trigger_type == "feature_predicate" and trigger.feature:
            required.add(trigger.feature)

        if trigger.trigger_type == "event":
            meta = event_meta.get(trigger.event_id, {})
            required.update(meta.get("requires_features", []))

        if trigger.trigger_type == "sequence" and trigger.events:
            for event_id in trigger.events:
                meta = event_meta.get(event_id, {})
                required.update(meta.get("requires_features", []))

        if trigger.trigger_type == "interaction":
            for operand in [trigger.left, trigger.right]:
                meta = event_meta.get(operand, {})
                required.update(meta.get("requires_features", []))

    return sorted(required)


def resolve_required_states(
    hypotheses: list[HypothesisSpec],
    registries: RegistryBundle,
) -> list[str]:
    required = set()
    state_registry = registries.states.get("states", {})

    for hypothesis in hypotheses:
        trigger = hypothesis.trigger
        if trigger.trigger_type == "state" and trigger.state_id:
            required.add(trigger.state_id)
        elif trigger.trigger_type == "transition":
            if trigger.from_state:
                required.add(trigger.from_state)
            if trigger.to_state:
                required.add(trigger.to_state)
        elif trigger.trigger_type == "interaction":
            for operand in [trigger.left, trigger.right]:
                if operand in state_registry:
                    required.add(operand)

    return sorted(required)


def export_experiment_artifacts(
    plan: ValidatedExperimentPlan,
    config_path: Path,
    registries: RegistryBundle,
    out_dir: Path,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    shutil.copy(config_path, out_dir / "request.yaml")

    request_bytes = (out_dir / "request.yaml").read_bytes()
    (out_dir / "request_hash.txt").write_text(hashlib.sha256(request_bytes).hexdigest())

    registry_hash = hashlib.sha256()
    registry_sources: dict[str, list[str]] = {}
    for category, paths in sorted(registries.registry_source_paths().items()):
        normalized_paths: list[str] = []
        for path in sorted(paths):
            normalized = _repo_relative_path(path)
            normalized_paths.append(normalized)
            registry_hash.update(category.encode("utf-8"))
            registry_hash.update(normalized.encode("utf-8"))
            if path.exists():
                registry_hash.update(path.read_bytes())
        registry_sources[category] = normalized_paths
    (out_dir / "registry_hash.txt").write_text(registry_hash.hexdigest())
    (out_dir / "registry_sources.json").write_text(
        json.dumps(registry_sources, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    plan_dict = {
        "program_id": plan.program_id,
        "estimated_hypothesis_count": plan.estimated_hypothesis_count,
        "required_detectors": plan.required_detectors,
        "required_features": plan.required_features,
        "required_states": plan.required_states,
        "feasibility_summary": dict(plan.feasibility_summary or {}),
    }
    (out_dir / "validated_plan.json").write_text(json.dumps(plan_dict, indent=2))

    requirements = {
        "detectors": plan.required_detectors,
        "features": plan.required_features,
        "state_engines": plan.required_states,
    }
    (out_dir / "execution_requirements.json").write_text(json.dumps(requirements, indent=2))

    rows = []
    for hypothesis in plan.hypotheses:
        row = hypothesis.to_dict()
        row["hypothesis_id"] = hypothesis.hypothesis_id()
        row["trigger_type"] = hypothesis.trigger.trigger_type
        row["context_slice"] = json.dumps(hypothesis.context) if hypothesis.context else None
        row["trigger_payload"] = json.dumps(hypothesis.trigger.to_dict())
        row.pop("trigger", None)
        row.pop("feature_condition", None)
        row.pop("context", None)
        rows.append(row)

    frame = pd.DataFrame(rows)
    for column in [
        "hypothesis_id",
        "trigger_type",
        "trigger_payload",
        "template_id",
        "horizon",
        "direction",
        "entry_lag",
        "context_slice",
    ]:
        if column not in frame.columns:
            frame[column] = None
    for column in frame.columns:
        if frame[column].dtype == "object":
            frame[column] = frame[column].map(
                lambda value: json.dumps(value, sort_keys=True)
                if isinstance(value, (dict, list))
                else value
            )
    write_parquet(frame, out_dir / "expanded_hypotheses.parquet")


def validate_registry_consistency(registries: RegistryBundle) -> None:
    template_families: dict[str, Any] = registries.templates.get("families", {})
    family_families: dict[str, Any] = registries.events.get("event_families", {})
    if not template_families or not family_families:
        return

    mismatches: list[str] = []
    all_families = set(template_families) | set(family_families)
    for family_name in sorted(all_families):
        template_allowed = sorted(
            template_families.get(family_name, {}).get("allowed_templates", [])
        )
        family_allowed = sorted(
            family_families.get(family_name, {}).get("allowed_templates", [])
        )
        if template_allowed == family_allowed:
            continue

        detail_parts = []
        only_in_template = set(template_allowed) - set(family_allowed)
        only_in_family = set(family_allowed) - set(template_allowed)
        if only_in_template:
            detail_parts.append(
                f"only in template_registry: {sorted(only_in_template)}"
            )
        if only_in_family:
            detail_parts.append(
                f"only in family_registry: {sorted(only_in_family)}"
            )
        mismatches.append(f"  {family_name}: {'; '.join(detail_parts)}")

    if mismatches:
        mismatch_report = "\n".join(mismatches)
        raise ValueError(
            "Registry consistency check failed — family_registry.yaml and "
            "template_registry.yaml disagree on allowed_templates for the following "
            f"families:\n{mismatch_report}\n\n"
            "Fix: update spec/grammar/family_registry.yaml to match "
            "spec/templates/registry.yaml (the authoritative source). "
            "Run validate_registry_consistency() to confirm the fix."
        )
    _LOG.debug("validate_registry_consistency: all families consistent.")


def build_experiment_plan(
    config_path: Path,
    registry_root: Path,
    out_dir: Path | None = None,
    data_root: Path | None = None,
) -> ValidatedExperimentPlan:
    registries = RegistryBundle(registry_root)
    validate_registry_consistency(registries)
    request = load_agent_experiment_config(config_path)
    validate_agent_request(request, registries, data_root=data_root)
    hypotheses = expand_hypotheses(request, registries)
    feasibility_report = getattr(expand_hypotheses, "last_feasibility_report", None)
    min_feasible = int(getattr(request.search_control, "min_feasible", 1) or 0)
    _validate_feasibility_threshold(
        feasible_count=len(hypotheses),
        min_feasible=min_feasible,
        report=feasibility_report,
    )

    plan = ValidatedExperimentPlan(
        program_id=request.program_id,
        hypotheses=hypotheses,
        required_detectors=resolve_required_detectors(hypotheses, registries),
        required_features=resolve_required_features(hypotheses, registries),
        required_states=resolve_required_states(hypotheses, registries),
        estimated_hypothesis_count=len(hypotheses),
        feasibility_summary=(
            feasibility_report.summary(limit=5)
            if feasibility_report is not None
            else {"generated": len(hypotheses), "feasible": len(hypotheses), "dropped": 0}
        ),
    )

    if out_dir is not None:
        export_experiment_artifacts(plan, config_path, registries, out_dir)

    return plan
