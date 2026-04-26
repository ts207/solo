"""Validation and expansion helpers for experiment engine."""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

from project.domain.compiled_registry import get_domain_registry
from project.domain.hypotheses import HypothesisSpec, TriggerSpec
from project.events.event_aliases import resolve_executable_event_alias
from project.events.governance import event_matches_filters, get_event_governance_metadata
from project.io.utils import read_parquet
from project.research.context_labels import canonicalize_context_label
from project.research.experiment_engine_schema import (
    AgentExperimentRequest,
    RegistryBundle,
)
from project.research.search.feasibility import check_hypothesis_feasibility, filter_hypotheses_with_report

log = logging.getLogger(__name__)

def _resolve_experiment_data_root(data_root: Path | None) -> Path:
    from project.core.config import get_data_root

    return Path(data_root) if data_root is not None else get_data_root()


def _ordered_run_ids(df) -> list[str]:
    import pandas as pd

    if df.empty or "run_id" not in df.columns:
        return []

    ordering = df.copy()
    ordering["run_id"] = ordering["run_id"].astype(str).str.strip()
    ordering = ordering[ordering["run_id"] != ""].copy()
    if ordering.empty:
        return []

    ordering["_row_order"] = range(len(ordering))
    if "created_at" in ordering.columns:
        ordering["created_at"] = pd.to_datetime(ordering["created_at"], utc=True, errors="coerce")
        summary = (
            ordering.groupby("run_id", as_index=False)
            .agg(last_created_at=("created_at", "max"), last_row=("_row_order", "max"))
            .sort_values(["last_created_at", "last_row", "run_id"], kind="stable")
        )
    else:
        summary = (
            ordering.groupby("run_id", as_index=False)
            .agg(last_row=("_row_order", "max"))
            .sort_values(["last_row", "run_id"], kind="stable")
        )
    return summary["run_id"].tolist()


def _validate_campaign_status(
    request: AgentExperimentRequest,
    registries: RegistryBundle,
    *,
    data_root: Path | None = None,
) -> None:
    data_root = _resolve_experiment_data_root(data_root)
    campaign_dir = data_root / "artifacts" / "experiments" / request.program_id
    ledger_path = campaign_dir / "tested_ledger.parquet"
    state_path = campaign_dir / "campaign_state.json"

    # Load state
    current_state = "active"
    if state_path.exists():
        current_state = json.loads(state_path.read_text()).get("state", "active")

    if current_state != "active":
        raise ValueError(
            f"Campaign '{request.program_id}' is in state '{current_state}' and cannot accept new proposals."
        )

    if ledger_path.exists():
        df = read_parquet(ledger_path)

        # Cumulative Budget
        limit_total = registries.limits.get("limits", {}).get(
            "max_hypotheses_total_per_campaign", 5000
        )
        if len(df) > limit_total:
            # Auto-transition state
            state_path.write_text(json.dumps({"state": "budget_exhausted"}))
            raise ValueError(
                f"Campaign '{request.program_id}' has exceeded the cumulative limit of {limit_total} hypotheses."
            )

        # Check for failure rates in last 2 runs
        runs = _ordered_run_ids(df)
        if len(runs) >= 2:
            last_runs = runs[-2:]
            recent = df[df["run_id"].isin(last_runs)]

            empty_rate = len(recent[recent["eval_status"] == "empty_sample"]) / len(recent)
            if empty_rate > 0.9:
                state_path.write_text(json.dumps({"state": "halted_empty_sample"}))
                raise ValueError(
                    f"Campaign '{request.program_id}' halted due to excessive empty sample rate."
                )

            unsupported_rate = len(
                recent[recent["eval_status"] == "unsupported_trigger_evaluator"]
            ) / len(recent)
            if unsupported_rate > 0.5:
                state_path.write_text(json.dumps({"state": "halted_unsupported"}))
                raise ValueError(
                    f"Campaign '{request.program_id}' halted due to excessive unsupported trigger share."
                )


def _validate_proposal_quality(
    request: AgentExperimentRequest,
    registries: RegistryBundle,
    *,
    data_root: Path | None = None,
) -> None:
    # Penalize redundancy and low-diversity
    data_root = _resolve_experiment_data_root(data_root)
    ledger_path = (
        data_root / "artifacts" / "experiments" / request.program_id / "tested_ledger.parquet"
    )

    if not ledger_path.exists():
        return  # First run always accepted

    df = read_parquet(ledger_path)

    # 1. Retesting exhausted regions
    def get_eid(payload):
        try:
            return json.loads(payload).get("event_id")
        except Exception:
            return None

    df["eid"] = df["trigger_payload"].apply(get_eid)

    fail_counts = (
        df[df["eval_status"].isin(["empty_sample", "insufficient_sample"])].groupby("eid").size()
    )
    exhausted = set(fail_counts[fail_counts >= 3].index)

    requested_events = set(_resolve_requested_event_ids(request, registries))
    exhausted_overlap = requested_events.intersection(exhausted)
    if len(exhausted_overlap) > len(requested_events) * 0.5:
        raise ValueError(
            f"Proposal quality rejection: >50% of requested events are already exhausted: {exhausted_overlap}"
        )

    # 2. Material difference from previous run
    runs = _ordered_run_ids(df)
    if len(runs) > 0:
        last_run = df[df["run_id"] == runs[-1]]
        last_events = set(last_run["eid"].unique())
        if requested_events == last_events:
            log.warning(
                "Proposal quality warning: Requested events are identical to the previous run."
            )


def _validate_templates(request: AgentExperimentRequest, registries: RegistryBundle) -> None:
    allowed_templates = registries.templates.get("templates", {})
    allowed_trigger_types = set(request.trigger_space.allowed_trigger_types)

    for tpl in request.templates.include:
        if tpl not in allowed_templates:
            raise ValueError(f"Template '{tpl}' is not in the authoritative registry.")
        tpl_meta = allowed_templates[tpl]
        if not tpl_meta.get("enabled", True):
            raise ValueError(f"Template '{tpl}' is disabled in the registry.")

        # Check trigger type compatibility
        tpl_supported = set(tpl_meta.get("supports_trigger_types", []))
        for t_type in allowed_trigger_types:
            if t_type.upper() not in [s.upper() for s in tpl_supported]:
                raise ValueError(f"Template '{tpl}' does not support trigger type '{t_type}'.")


def _validate_instrument_compatibility(
    request: AgentExperimentRequest, registries: RegistryBundle
) -> None:
    requested_ics = request.instrument_scope.instrument_classes

    # Check events
    allowed_events = registries.events.get("events", {})
    for event_id in _resolve_requested_event_ids(request, registries):
        if event_id not in allowed_events:
            continue  # Let _validate_event_trigger handle it
        event_meta = allowed_events.get(event_id, {})
        event_ics = [str(ic).strip() for ic in event_meta.get("instrument_classes", []) if str(ic).strip()]
        if not event_ics:
            continue
        for ic in requested_ics:
            if ic not in event_ics:
                raise ValueError(f"Event '{event_id}' is not allowed for instrument class '{ic}'.")

    # Check states
    allowed_states = registries.states.get("states", {})
    for state_id in request.trigger_space.states.get("include", []):
        if state_id not in allowed_states:
            continue  # Let _validate_state_trigger handle it
        state_meta = allowed_states.get(state_id, {})
        state_ics = [str(ic).strip() for ic in state_meta.get("instrument_classes", []) if str(ic).strip()]
        if not state_ics:
            continue
        for ic in requested_ics:
            if ic not in state_ics:
                raise ValueError(f"State '{state_id}' is not allowed for instrument class '{ic}'.")


def _validate_contexts(request: AgentExperimentRequest, registries: RegistryBundle) -> None:
    allowed_contexts = registries.contexts.get("context_dimensions", {})
    for dim, values in request.contexts.include.items():
        if dim not in allowed_contexts:
            raise ValueError(f"Context dimension '{dim}' is not in the authoritative registry.")
        for val in values:
            canonical_value = canonicalize_context_label(dim, val)
            if canonical_value not in allowed_contexts[dim].get("allowed_values", []):
                raise ValueError(f"Value '{val}' is not allowed for context dimension '{dim}'.")


def _validate_search_limits(request: AgentExperimentRequest, registries: RegistryBundle) -> None:
    limits = registries.limits.get("limits", {})

    if len(_resolve_requested_event_ids(request, registries)) > limits.get("max_events_per_run", 100):
        raise ValueError("Exceeded max_events_per_run limit.")
    if len(request.templates.include) > limits.get("max_templates_per_run", 100):
        raise ValueError("Exceeded max_templates_per_run limit.")
    if len(request.evaluation.horizons_bars) > limits.get("max_horizons_per_run", 10):
        raise ValueError("Exceeded max_horizons_per_run limit.")
    if len(request.evaluation.directions) > limits.get("max_directions_per_run", 2):
        raise ValueError("Exceeded max_directions_per_run limit.")
    invalid_entry_lags = [int(lag) for lag in request.evaluation.entry_lags if int(lag) < 1]
    if invalid_entry_lags:
        raise ValueError("entry_lags must be >= 1 to prevent same-bar entry leakage.")


def _validate_event_trigger(request: AgentExperimentRequest, registries: RegistryBundle) -> None:
    allowed_events = registries.events.get("events", {})
    requested = _resolve_requested_event_ids(request, registries)
    if not requested:
        raise ValueError("Trigger type EVENT enabled but no events included.")
    for event_id in requested:
        if event_id not in allowed_events:
            raise ValueError(f"Event ID '{event_id}' is not in the authoritative registry.")
        ev = allowed_events[event_id]
        if not ev.get("enabled", True) and not ev.get("planning_eligible", False):
            raise ValueError(f"Event ID '{event_id}' is disabled.")


def _validate_state_trigger(request: AgentExperimentRequest, registries: RegistryBundle) -> None:
    allowed_states = registries.states.get("states", {})
    requested = request.trigger_space.states.get("include", [])
    if not requested:
        raise ValueError("Trigger type STATE enabled but no states included.")
    for state_id in requested:
        if state_id not in allowed_states:
            raise ValueError(f"State ID '{state_id}' is not in the authoritative registry.")
        if not allowed_states[state_id].get("enabled", True):
            raise ValueError(f"State ID '{state_id}' is disabled.")


def _validate_transition_trigger(
    request: AgentExperimentRequest, registries: RegistryBundle
) -> None:
    allowed_states = registries.states.get("states", {})
    requested = request.trigger_space.transitions.get("include", [])
    if not requested:
        raise ValueError("Trigger type TRANSITION enabled but no transitions included.")
    for trans in requested:
        from_s = trans.get("from_state")
        to_s = trans.get("to_state")
        if not from_s or not to_s:
            raise ValueError("Transition must specify from_state and to_state.")
        if from_s not in allowed_states:
            raise ValueError(f"Transition from_state '{from_s}' unknown.")
        if to_s not in allowed_states:
            raise ValueError(f"Transition to_state '{to_s}' unknown.")


def _validate_sequence_trigger(request: AgentExperimentRequest, registries: RegistryBundle) -> None:
    allowed_events = registries.events.get("events", {})
    seq_config = request.trigger_space.sequences
    requested = seq_config.get("include", [])
    if not requested:
        raise ValueError("Trigger type SEQUENCE enabled but no sequences included.")

    max_len = registries.limits.get("limits", {}).get("max_sequence_length", 5)

    for seq in requested:
        if not isinstance(seq, list):
            raise ValueError("Sequence inclusion must be a list of event IDs.")
        if len(seq) > max_len:
            raise ValueError(f"Sequence length {len(seq)} exceeds limit {max_len}.")
        for event_id in seq:
            if event_id not in allowed_events:
                raise ValueError(f"Sequence contains unknown event ID '{event_id}'.")
            if not allowed_events[event_id].get("sequence_eligible", True):
                raise ValueError(f"Event '{event_id}' is not sequence-eligible.")


def _validate_feature_predicate_trigger(
    request: AgentExperimentRequest, registries: RegistryBundle
) -> None:
    allowed_features = registries.features.get("features", {})
    requested = request.trigger_space.feature_predicates.get("include", [])
    if not requested:
        raise ValueError("Trigger type FEATURE_PREDICATE enabled but no predicates included.")

    for pred in requested:
        feat_id = pred.get("feature")
        op = pred.get("operator")
        if not feat_id or not op:
            raise ValueError("Feature predicate must specify feature and operator.")
        if feat_id not in allowed_features:
            raise ValueError(f"Feature '{feat_id}' is not in the authoritative registry.")

        feat_meta = allowed_features[feat_id]
        if op not in feat_meta.get("allowed_operators", []):
            raise ValueError(f"Operator '{op}' not allowed for feature '{feat_id}'.")


def _validate_interaction_trigger(
    request: AgentExperimentRequest, registries: RegistryBundle
) -> None:
    allowed_events = registries.events.get("events", {})
    allowed_states = registries.states.get("states", {})
    requested = request.trigger_space.interactions.get("include", [])
    if not requested:
        raise ValueError("Trigger type INTERACTION enabled but no interactions included.")

    for inter in requested:
        left = inter.get("left")
        right = inter.get("right")
        op = inter.get("op")
        left_direction = str(inter.get("left_direction", "") or "").strip().lower()
        right_direction = str(inter.get("right_direction", "") or "").strip().lower()
        if not left or not right or not op:
            raise ValueError("Interaction must specify left, right, and op.")

        if op.upper() not in ["AND", "CONFIRM", "EXCLUDE"]:
            raise ValueError(f"Unsupported interaction operator '{op}'.")

        # Binary interaction validation: depth = 1 check (operands must be events or states)
        for operand in [left, right]:
            if operand not in allowed_events and operand not in allowed_states:
                raise ValueError(f"Interaction operand '{operand}' must be a known EVENT or STATE.")

        for side_name, operand, direction_value in (
            ("left", left, left_direction),
            ("right", right, right_direction),
        ):
            if not direction_value:
                continue
            if direction_value not in {"up", "down", "non_directional"}:
                raise ValueError(
                    f"Unsupported {side_name}_direction '{direction_value}'. "
                    "Expected one of: up, down, non_directional."
                )
            if operand not in allowed_events:
                raise ValueError(
                    f"{side_name}_direction is only valid for EVENT operands; got {operand!r}."
                )


def expand_hypotheses(
    request: AgentExperimentRequest,
    registries: RegistryBundle,
) -> List[HypothesisSpec]:
    hypotheses = []

    # Resolve context slices (Cartesian product of selected values)
    import itertools

    context_keys = sorted(request.contexts.include.keys())
    context_values = [request.contexts.include[k] for k in context_keys]
    context_slices = [dict(zip(context_keys, v)) for v in itertools.product(*context_values)]
    if not context_slices:
        context_slices = [None]

    for t_type in request.trigger_space.allowed_trigger_types:
        t_type_upper = t_type.upper()
        if t_type_upper == "EVENT":
            hypotheses.extend(_expand_event_triggers(request, registries, context_slices))
        elif t_type_upper == "STATE":
            hypotheses.extend(_expand_state_triggers(request, context_slices))
        elif t_type_upper == "TRANSITION":
            hypotheses.extend(_expand_transition_triggers(request, context_slices))
        elif t_type_upper == "SEQUENCE":
            hypotheses.extend(_expand_sequence_triggers(request, context_slices))
        elif t_type_upper == "FEATURE_PREDICATE":
            hypotheses.extend(_expand_feature_predicate_triggers(request, context_slices))
        elif t_type_upper == "INTERACTION":
            hypotheses.extend(_expand_interaction_triggers(request, context_slices))

    # Filter infeasible hypotheses with a structured report.  Older versions only
    # logged template-family incompatibilities; the report is consumed by
    # build_experiment_plan() to fail zero-feasible plans and persist drop reasons.
    registry = get_domain_registry()
    hypotheses, feasibility_report = filter_hypotheses_with_report(hypotheses, registry=registry)
    expand_hypotheses.last_feasibility_report = feasibility_report  # type: ignore[attr-defined]
    for reason, count in feasibility_report.counts_by_reason().items():
        log.warning("Dropped %d infeasible hypotheses during plan expansion: %s", count, reason)

    # Apply search budget
    max_total = request.search_control.max_hypotheses_total
    if len(hypotheses) > max_total:
        log.warning(f"Truncating hypotheses expansion from {len(hypotheses)} to {max_total}")
        from collections import defaultdict
        by_template = defaultdict(list)
        for hyp in hypotheses:
            by_template[hyp.template_id].append(hyp)
        balanced_hypotheses = []
        while len(balanced_hypotheses) < max_total and by_template:
            for template_id in sorted(by_template.keys()):
                if by_template[template_id]:
                    balanced_hypotheses.append(by_template[template_id].pop(0))
                    if len(balanced_hypotheses) == max_total:
                        break
            by_template = {k: v for k, v in by_template.items() if v}
        hypotheses = balanced_hypotheses

    return hypotheses


def _resolve_requested_event_ids(
    request: AgentExperimentRequest,
    registries: RegistryBundle,
) -> List[str]:
    allowed_events = registries.events.get("events", {})
    explicit_events = []
    for raw_event in request.trigger_space.events.get("include", []):
        if isinstance(raw_event, dict):
            event_id = str(
                raw_event.get("event_id", raw_event.get("id", raw_event.get("event", ""))) or ""
            ).strip()
        else:
            event_id = str(raw_event).strip()
        if event_id:
            explicit_events.append(event_id)
    requested_regimes = [
        str(regime).strip().upper()
        for regime in getattr(request.trigger_space, "canonical_regimes", [])
        if str(regime).strip()
    ]
    subtypes = {
        str(value).strip().lower()
        for value in getattr(request.trigger_space, "subtypes", [])
        if str(value).strip()
    }
    phases = {
        str(value).strip().lower()
        for value in getattr(request.trigger_space, "phases", [])
        if str(value).strip()
    }
    evidence_modes = {
        str(value).strip().lower()
        for value in getattr(request.trigger_space, "evidence_modes", [])
        if str(value).strip()
    }
    tiers = {
        str(value).strip().upper()
        for value in getattr(request.trigger_space, "tiers", [])
        if str(value).strip()
    }
    operational_roles = {
        str(value).strip().lower()
        for value in getattr(request.trigger_space, "operational_roles", [])
        if str(value).strip()
    }
    deployment_dispositions = {
        str(value).strip().lower()
        for value in getattr(request.trigger_space, "deployment_dispositions", [])
        if str(value).strip()
    }

    registry = get_domain_registry()

    def _normalize_event_id(event_id: str) -> str:
        token = str(event_id).strip()
        if token not in allowed_events:
            executable_token = resolve_executable_event_alias(token)
            if executable_token in allowed_events:
                token = executable_token
        return token

    def _validate_event_constraints(event_id: str) -> None:
        spec = registry.get_event(event_id)
        if spec is None:
            return
        canonical_regime = str(getattr(spec, "canonical_regime", "") or "").strip().upper()
        research_family = str(
            getattr(spec, "research_family", "") or getattr(spec, "canonical_family", "") or ""
        ).strip().upper()
        subtype = str(getattr(spec, "subtype", "") or "").strip().lower()
        phase = str(getattr(spec, "phase", "") or "").strip().lower()
        evidence_mode = str(getattr(spec, "evidence_mode", "") or "").strip().lower()
        if requested_regimes and not ({canonical_regime, research_family} & set(requested_regimes)):
            raise ValueError(
                "Explicit event "
                f"'{event_id}' does not belong to requested canonical_regimes {requested_regimes}; "
                f"registry canonical_regime is '{canonical_regime or '?'}'."
            )
        if subtypes and subtype not in subtypes:
            raise ValueError(
                f"Explicit event '{event_id}' does not match requested subtypes {sorted(subtypes)}; "
                f"registry subtype is '{subtype or '?'}'."
            )
        if phases and phase not in phases:
            raise ValueError(
                f"Explicit event '{event_id}' does not match requested phases {sorted(phases)}; "
                f"registry phase is '{phase or '?'}'."
            )
        if evidence_modes and evidence_mode not in evidence_modes:
            raise ValueError(
                f"Explicit event '{event_id}' does not match requested evidence_modes {sorted(evidence_modes)}; "
                f"registry evidence_mode is '{evidence_mode or '?'}'."
            )
        if not event_matches_filters(
            event_id,
            tiers=tiers,
            roles=operational_roles,
            deployment_dispositions=deployment_dispositions,
        ):
            raise ValueError(
                f"Explicit event '{event_id}' does not satisfy requested governance filters "
                f"tiers={sorted(tiers)} roles={sorted(operational_roles)} dispositions={sorted(deployment_dispositions)}."
            )
        governance = get_event_governance_metadata(event_id)
        deployment_like_mode = str(getattr(request, "run_mode", "") or "").strip().lower() in {
            "production",
            "promotion",
            "deploy",
            "certification",
        } or bool(getattr(getattr(request, "promotion", None), "enabled", False))
        if deployment_like_mode and not bool(governance.get("trade_trigger_eligible", False)):
            raise ValueError(
                f"Explicit event '{event_id}' is not deployment eligible under current governance: "
                f"{governance.get('promotion_block_reason', 'blocked')}"
            )

    ordered: list[str] = []
    seen: set[str] = set()

    if explicit_events:
        for event_id in explicit_events:
            token = _normalize_event_id(event_id)
            _validate_event_constraints(token)
            if token and token not in seen:
                ordered.append(token)
                seen.add(token)
        return ordered

    if not requested_regimes:
        return ordered

    governance_filtered = bool(tiers or operational_roles or deployment_dispositions)

    def _requested_event_ids_for_regime(regime: str) -> tuple[str, ...]:
        matches = registry.get_event_ids_for_regime(regime, executable_only=not governance_filtered)
        if matches:
            return matches
        matches = registry.get_event_ids_for_family(regime)
        if matches:
            return matches
        runtime_matches = [
            event_id
            for event_id, meta in allowed_events.items()
            if str(meta.get("family", "") or "").strip().upper() == regime
        ]
        return tuple(sorted(runtime_matches))

    for regime in requested_regimes:
        for event_id in _requested_event_ids_for_regime(regime):
            spec = registry.get_event(event_id)
            if spec is None:
                continue
            if not spec.enabled:
                continue
            if subtypes and str(spec.subtype).strip().lower() not in subtypes:
                continue
            if phases and str(spec.phase).strip().lower() not in phases:
                continue
            if evidence_modes and str(spec.evidence_mode).strip().lower() not in evidence_modes:
                continue
            if not event_matches_filters(
                event_id,
                tiers=tiers,
                roles=operational_roles,
                deployment_dispositions=deployment_dispositions,
            ):
                continue
            token = _normalize_event_id(event_id)
            if token not in allowed_events:
                log.debug(
                    "Skipping non-authoritative regime-expanded event %r during request resolution",
                    event_id,
                )
                continue
            if token and token not in seen:
                ordered.append(token)
                seen.add(token)
    return ordered


def _expand_event_triggers(
    request: AgentExperimentRequest,
    registries: RegistryBundle,
    context_slices: List[Optional[Dict[str, str]]],
) -> List[HypothesisSpec]:
    hyps = []
    explicit_event_specs: list[tuple[str, str | None]] = []
    for raw_event in request.trigger_space.events.get("include", []):
        if isinstance(raw_event, dict):
            event_id = str(
                raw_event.get("event_id", raw_event.get("id", raw_event.get("event", ""))) or ""
            ).strip()
            event_direction = str(raw_event.get("event_direction", "") or "").strip().lower() or None
        else:
            event_id = str(raw_event).strip()
            event_direction = None
        if event_id:
            explicit_event_specs.append((event_id, event_direction))
    if explicit_event_specs:
        requested_events = explicit_event_specs
    else:
        requested_events = [(event_id, None) for event_id in _resolve_requested_event_ids(request, registries)]
    # Translate first feature predicate into a feature_condition for evaluation-time filtering.
    feature_predicates_include = request.trigger_space.feature_predicates.get("include", [])
    feature_condition: Optional[TriggerSpec] = None
    if feature_predicates_include:
        if len(feature_predicates_include) > 1:
            log.warning(
                "Multiple feature_predicates provided; only the first will be applied as feature_condition"
            )
        fp = feature_predicates_include[0]
        try:
            feature_condition = TriggerSpec.feature_predicate(
                feature=str(fp["feature"]),
                operator=str(fp["operator"]),
                threshold=float(fp["threshold"]),
            )
        except (KeyError, ValueError, TypeError) as exc:
            log.warning("Could not build feature_condition from predicate %r: %s", fp, exc)

    for event_id, event_direction in requested_events:
        for tpl in request.templates.include:
            for horizon in request.evaluation.horizons_bars:
                for direction in request.evaluation.directions:
                    for lag in request.evaluation.entry_lags:
                        for ctx in context_slices:
                            trigger = TriggerSpec.event(event_id, event_direction=event_direction)
                            hyps.append(
                                HypothesisSpec(
                                    trigger=trigger,
                                    direction=direction,
                                    horizon=f"{horizon}b",
                                    template_id=tpl,
                                    entry_lag=lag,
                                    context=ctx,
                                    feature_condition=feature_condition,
                                )
                            )
    return hyps


def _expand_state_triggers(
    request: AgentExperimentRequest, context_slices: List[Optional[Dict[str, str]]]
) -> List[HypothesisSpec]:
    hyps = []
    requested_states = request.trigger_space.states.get("include", [])
    for state_id in requested_states:
        for tpl in request.templates.include:
            for horizon in request.evaluation.horizons_bars:
                for direction in request.evaluation.directions:
                    for lag in request.evaluation.entry_lags:
                        for ctx in context_slices:
                            trigger = TriggerSpec.state(state_id)
                            hyps.append(
                                HypothesisSpec(
                                    trigger=trigger,
                                    direction=direction,
                                    horizon=f"{horizon}b",
                                    template_id=tpl,
                                    entry_lag=lag,
                                    context=ctx,
                                )
                            )
    return hyps


def _expand_transition_triggers(
    request: AgentExperimentRequest, context_slices: List[Optional[Dict[str, str]]]
) -> List[HypothesisSpec]:
    hyps = []
    requested_transitions = request.trigger_space.transitions.get("include", [])
    for trans in requested_transitions:
        from_s = trans["from_state"]
        to_s = trans["to_state"]
        for tpl in request.templates.include:
            for horizon in request.evaluation.horizons_bars:
                for direction in request.evaluation.directions:
                    for lag in request.evaluation.entry_lags:
                        for ctx in context_slices:
                            trigger = TriggerSpec.transition(from_s, to_s)
                            hyps.append(
                                HypothesisSpec(
                                    trigger=trigger,
                                    direction=direction,
                                    horizon=f"{horizon}b",
                                    template_id=tpl,
                                    entry_lag=lag,
                                    context=ctx,
                                )
                            )
    return hyps


def _expand_sequence_triggers(
    request: AgentExperimentRequest, context_slices: List[Optional[Dict[str, str]]]
) -> List[HypothesisSpec]:
    hyps = []
    seq_config = request.trigger_space.sequences
    requested_sequences = seq_config.get("include", [])
    max_gaps = seq_config.get("max_gaps_bars", [1])

    for seq_events in requested_sequences:
        for gap in max_gaps:
            # Generate deterministic sequence ID
            import hashlib

            payload = "|".join(seq_events) + f"|gap={gap}"
            seq_id = "SEQ_" + hashlib.sha256(payload.encode()).hexdigest()[:12].upper()

            # Domain TriggerSpec.sequence takes List[int] for max_gap if length matches
            gaps_list = [gap] * (len(seq_events) - 1)

            for tpl in request.templates.include:
                for horizon in request.evaluation.horizons_bars:
                    for direction in request.evaluation.directions:
                        for lag in request.evaluation.entry_lags:
                            for ctx in context_slices:
                                trigger = TriggerSpec.sequence(seq_id, seq_events, gaps_list)
                                hyps.append(
                                    HypothesisSpec(
                                        trigger=trigger,
                                        direction=direction,
                                        horizon=f"{horizon}b",
                                        template_id=tpl,
                                        entry_lag=lag,
                                        context=ctx,
                                    )
                                )
    return hyps


def _expand_feature_predicate_triggers(
    request: AgentExperimentRequest, context_slices: List[Optional[Dict[str, str]]]
) -> List[HypothesisSpec]:
    hyps = []
    requested_preds = request.trigger_space.feature_predicates.get("include", [])
    for pred in requested_preds:
        feat = pred["feature"]
        op = pred["operator"]
        threshold = pred["threshold"]
        for tpl in request.templates.include:
            for horizon in request.evaluation.horizons_bars:
                for direction in request.evaluation.directions:
                    for lag in request.evaluation.entry_lags:
                        for ctx in context_slices:
                            trigger = TriggerSpec.feature_predicate(feat, op, threshold)
                            hyps.append(
                                HypothesisSpec(
                                    trigger=trigger,
                                    direction=direction,
                                    horizon=f"{horizon}b",
                                    template_id=tpl,
                                    entry_lag=lag,
                                    context=ctx,
                                )
                            )
    return hyps


def _expand_interaction_triggers(
    request: AgentExperimentRequest, context_slices: List[Optional[Dict[str, str]]]
) -> List[HypothesisSpec]:
    hyps = []
    requested_inters = request.trigger_space.interactions.get("include", [])
    for inter in requested_inters:
        left = inter["left"]
        right = inter["right"]
        op = inter["op"]
        lag = inter.get("lag", 6)
        left_direction = str(inter.get("left_direction", "") or "").strip().lower() or None
        right_direction = str(inter.get("right_direction", "") or "").strip().lower() or None

        # Generate deterministic interaction ID
        import hashlib

        payload = (
            f"{left}|{op}|{right}|lag={lag}|"
            f"left_direction={left_direction or ''}|right_direction={right_direction or ''}"
        )
        int_id = "INT_" + hashlib.sha256(payload.encode()).hexdigest()[:12].upper()

        for tpl in request.templates.include:
            for horizon in request.evaluation.horizons_bars:
                for direction in request.evaluation.directions:
                    for lag_e in request.evaluation.entry_lags:
                        for ctx in context_slices:
                            trigger = TriggerSpec.interaction(
                                int_id,
                                left,
                                right,
                                op,
                                lag,
                                left_direction=left_direction,
                                right_direction=right_direction,
                            )
                            hyps.append(
                                HypothesisSpec(
                                    trigger=trigger,
                                    direction=direction,
                                    horizon=f"{horizon}b",
                                    template_id=tpl,
                                    entry_lag=lag_e,
                                    context=ctx,
                                )
                            )
    return hyps
