"""
Hypothesis generator.

Refactored to support phased search specs, family-based expansion,
sequences, and interactions.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from itertools import product
from pathlib import Path
from typing import Any

from project.domain.compiled_registry import get_domain_registry
from project.domain.hypotheses import HypothesisSpec, TriggerSpec, TriggerType
from project.research.search.feasibility import FeasibilityResult, check_hypothesis_feasibility
from project.research.search.stage_models import CandidateHypothesis, FeasibilityCheckedHypothesis
from project.research.search.validation import validate_hypothesis_spec
from project.spec_validation import (
    expand_triggers,
    loaders,
    resolve_entry_lags,
    resolve_filter_template_names,
    resolve_filter_templates,
    resolve_templates,
    validate_search_spec_doc,
)

log = logging.getLogger(__name__)


def _candidate_row(spec: HypothesisSpec, *, search_spec_name: str) -> dict[str, Any]:
    return CandidateHypothesis(spec=spec, search_spec_name=search_spec_name).to_record()


def _accept_unique_spec(
    spec: HypothesisSpec,
    *,
    seen_ids: set[str],
    seen_branch_hashes: set[str],
) -> bool:
    branch_hash = spec.semantic_branch_hash()
    if branch_hash in seen_branch_hashes:
        return False
    hid = spec.hypothesis_id()
    if hid in seen_ids:
        return False
    seen_ids.add(hid)
    seen_branch_hashes.add(branch_hash)
    return True


def _context_combinations(contexts: dict[str, Any]) -> list[dict[str, str] | None]:
    """
    Expand contexts dict into a list of conditioning dicts.
    Supports '*' wildcard for expanding all labels in a family.
    """
    if not contexts:
        return [None]

    registry = get_domain_registry()

    keys = list(contexts.keys())
    active_keys = []
    values = []
    for k in keys:
        v = contexts[k]
        if v == "*":
            labels = registry.context_labels_for_family(k)
            if labels:
                active_keys.append(k)
                values.append(list(labels))
            else:
                log.warning(
                    "Family %r not found in compiled domain registry context labels. Skipping wildcard expansion for this family.",
                    k,
                )
                # Ensure we don't invent "unknown" labels. Just skip this family from combinations.
                continue
        else:
            active_keys.append(k)
            values.append(v if isinstance(v, list) else [v])

    if not values:
        return [None]

    combos: list[dict[str, str] | None] = []
    for combo in product(*values):
        combos.append(dict(zip(active_keys, combo)))
    return combos


def _edge_cell_contexts(contexts: dict[str, Any]) -> list[dict[str, str] | None]:
    """
    Edge-cell discovery treats each context family independently (1D contexts),
    plus an unconditional baseline. It intentionally does not emit cartesian
    products across multiple context families.
    """
    if not contexts:
        return [None]

    registry = get_domain_registry()
    combos: list[dict[str, str] | None] = [None]
    for family, raw in contexts.items():
        if raw == "*":
            values = list(registry.context_labels_for_family(family))
        else:
            values = raw if isinstance(raw, list) else [raw]
        for label in values:
            token = str(label).strip()
            fam = str(family).strip()
            if not fam or not token:
                continue
            combos.append({fam: token})
    return combos


def _build_hypotheses(
    trigger_type: str,
    ids_or_configs: list[Any],
    horizons: list[str],
    directions: list[str],
    entry_lags: list[int],
    contexts: list[dict[str, str] | None],
    templates: list[str],
) -> Iterable[HypothesisSpec]:
    for item in ids_or_configs:
        if trigger_type == TriggerType.EVENT:
            trigger = TriggerSpec.event(item)
        elif trigger_type == TriggerType.STATE:
            trigger = TriggerSpec.state(item)
        elif trigger_type == TriggerType.TRANSITION:
            trigger = TriggerSpec.transition(from_state=item["from"], to_state=item["to"])
        elif trigger_type == TriggerType.FEATURE_PREDICATE:
            trigger = TriggerSpec.feature_predicate(
                feature=item["feature"], operator=item["operator"], threshold=item["threshold"]
            )
        elif trigger_type == TriggerType.SEQUENCE:
            trigger = TriggerSpec.sequence(
                sequence_id=item["name"],
                events=item["events"],
                max_gap=item.get("max_gap", [6] * (len(item["events"]) - 1)),
            )
        elif trigger_type == TriggerType.INTERACTION:
            trigger = TriggerSpec.interaction(
                interaction_id=item["name"],
                left=item["left"],
                right=item["right"],
                op=item["op"],
                lag=item.get("lag", 6),
            )
        else:
            log.warning("Unsupported trigger_type in _build_hypotheses: %s", trigger_type)
            continue

        for template in templates:
            operator = get_domain_registry().get_operator(template)
            operator_raw = operator.raw if operator is not None and isinstance(operator.raw, dict) else {}
            requires_direction = bool(operator_raw.get("requires_direction", True))
            template_directions = directions if requires_direction else directions[:1]

            for horizon, direction, lag, ctx in product(
                horizons, template_directions, entry_lags, contexts
            ):
                yield HypothesisSpec(
                    trigger=trigger,
                    direction=direction,
                    horizon=horizon,
                    template_id=template,
                    context=ctx,
                    entry_lag=lag,
                )


def _event_default_templates(
    event_id: str,
    *,
    registry,
    fallback_templates: list[str],
) -> list[str]:
    event_def = registry.get_event(event_id)
    if event_def is None:
        return list(fallback_templates)

    raw = event_def.raw if isinstance(event_def.raw, dict) else {}
    raw_templates = raw.get("templates")
    if isinstance(raw_templates, (list, tuple)):
        templates = [
            str(item).strip()
            for item in raw_templates
            if str(item).strip() and registry.is_hypothesis_template(str(item).strip())
        ]
        if templates:
            return templates

    research_family = None
    parameters = raw.get("parameters")
    if isinstance(parameters, dict):
        research_family = parameters.get("research_family") or parameters.get("canonical_family")
    if research_family:
        templates = list(registry.family_hypothesis_templates(str(research_family)))
        if templates:
            return templates

    templates = list(
        registry.family_hypothesis_templates(event_def.research_family or event_def.canonical_family)
    )
    if templates:
        return templates
    return list(fallback_templates)


def load_sequence_registry() -> list[dict[str, Any]]:
    return get_domain_registry().sequence_rows()


def load_interaction_registry() -> list[dict[str, Any]]:
    return get_domain_registry().interaction_rows()


def generate_hypotheses_with_audit(
    search_spec_name: str = "full",
    *,
    max_hypotheses: int | None = None,
    skip_invalid: bool = True,
    search_space_path: Path | str | None = None,
    features=None,
) -> tuple[list[HypothesisSpec], dict[str, Any]]:
    """
    Generate all hypothesis candidates from a phased search spec.
    """
    if search_space_path:
        from project.spec_registry import load_yaml_path

        doc = load_yaml_path(Path(search_space_path))
    else:
        doc = loaders.load_search_spec(search_spec_name)
    validate_search_spec_doc(doc, source=str(search_spec_name))

    # Expand triggers from families and explicit lists
    expanded = expand_triggers(doc)
    events = expanded.get("events", [])
    states = expanded.get("states", [])
    transitions = expanded.get("transitions", [])
    feature_predicates = expanded.get("feature_predicates", [])
    event_family_map: dict[str, str] = expanded.get("event_family_map", {})

    # Resolve wildcards
    horizons = [str(h) for h in doc.get("horizons", ["15m"])]
    directions = [str(d) for d in doc.get("directions", ["long", "short"])]
    entry_lags = resolve_entry_lags(doc)
    templates_raw = doc.get("templates", ["base"])
    use_event_templates = templates_raw == "*"
    templates = resolve_templates(doc)
    selected_filter_templates = resolve_filter_template_names(doc)

    raw_contexts = doc.get("contexts", {})
    source_mode = str((doc.get("metadata", {}) or {}).get("source_discovery_mode", "") or "").strip()
    if source_mode == "edge_cells":
        contexts = _edge_cell_contexts(raw_contexts)
    else:
        contexts = _context_combinations(raw_contexts)

    # Budgets and Quotas
    quotas = doc.get("quotas", {})
    template_budgets = doc.get("template_budgets", {})

    type_counts: dict[str, int] = {}
    template_counts: dict[str, int] = {}

    hypotheses: list[HypothesisSpec] = []
    seen_ids: set = set()
    seen_branch_hashes: set = set()
    seen_branch_hashes: set = set()
    skipped_invalid = 0
    skipped_dup = 0
    skipped_quota = 0
    skipped_budget = 0
    skipped_cap = 0
    rejection_reason_counts: dict[str, int] = {}
    generated_rows: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []
    feasible_rows: list[dict[str, Any]] = []

    def _add(spec: HypothesisSpec) -> None:
        nonlocal skipped_invalid, skipped_dup, skipped_quota, skipped_budget, skipped_cap
        generated_rows.append(_candidate_row(spec, search_spec_name=search_spec_name))

        def _record_rejection(reason: str, details: dict[str, Any] | None = None) -> None:
            rejection_reason_counts[reason] = rejection_reason_counts.get(reason, 0) + 1
            candidate = CandidateHypothesis(spec=spec, search_spec_name=search_spec_name)
            rejected_rows.append(
                FeasibilityCheckedHypothesis(
                    candidate=candidate,
                    feasibility=FeasibilityResult(
                        valid=False,
                        reasons=(reason,),
                        details=dict(details or {}),
                    ),
                ).to_record()
            )

        # 1. Global cap
        if max_hypotheses is not None and len(hypotheses) >= max_hypotheses:
            skipped_cap += 1
            _record_rejection("max_hypotheses_cap")
            return

        # 2. Type Quota
        ttype = spec.trigger.trigger_type
        if ttype in quotas and type_counts.get(ttype, 0) >= quotas[ttype]:
            skipped_quota += 1
            _record_rejection("type_quota")
            return

        # 2b. Template Budget
        tid = spec.template_id
        if tid in template_budgets and template_counts.get(tid, 0) >= template_budgets[tid]:
            skipped_budget += 1
            _record_rejection("template_budget")
            return

        # 3. Validation
        errors = validate_hypothesis_spec(spec)
        if errors:
            if skip_invalid:
                skipped_invalid += 1
                log.debug("Rejecting invalid spec %s: %s", spec.label(), errors)
                _record_rejection("validation_error", {"errors": list(errors)})
                return
            raise ValueError(f"Invalid HypothesisSpec {spec.label()!r}: {errors}")

        feasibility = check_hypothesis_feasibility(spec, features=features)
        if not feasibility.valid:
            if skip_invalid:
                skipped_invalid += 1
                log.debug("Rejecting infeasible spec %s: %s", spec.label(), feasibility.reasons)
                _record_rejection(
                    feasibility.primary_reason or "infeasible",
                    {"reasons": list(feasibility.reasons), **dict(feasibility.details)},
                )
                return
            raise ValueError(
                f"Infeasible HypothesisSpec {spec.label()!r}: {list(feasibility.reasons)}"
            )

        # 4. Deduplication
        if spec.semantic_branch_hash() in seen_branch_hashes:
            skipped_dup += 1
            _record_rejection(
                "duplicate_semantic_branch",
                {"branch_hash": spec.semantic_branch_hash()},
            )
            return

        if spec.hypothesis_id() in seen_ids:
            skipped_dup += 1
            _record_rejection("duplicate_hypothesis_id")
            return

        # Success - add
        _accept_unique_spec(spec, seen_ids=seen_ids, seen_branch_hashes=seen_branch_hashes)
        hypotheses.append(spec)
        feasible_rows.append(
            FeasibilityCheckedHypothesis(
                candidate=CandidateHypothesis(spec=spec, search_spec_name=search_spec_name),
                feasibility=FeasibilityResult(valid=True),
            ).to_record()
        )
        type_counts[ttype] = type_counts.get(ttype, 0) + 1
        template_counts[tid] = template_counts.get(tid, 0) + 1

    registry = get_domain_registry()

    # Build event-led
    if use_event_templates:
        for event_id in events:
            event_templates = _event_default_templates(
                event_id,
                registry=registry,
                fallback_templates=templates,
            )
            for spec in _build_hypotheses(
                TriggerType.EVENT,
                [event_id],
                horizons,
                directions,
                entry_lags,
                contexts,
                event_templates,
            ):
                _add(spec)
    else:
        for spec in _build_hypotheses(
            TriggerType.EVENT, events, horizons, directions, entry_lags, contexts, templates
        ):
            _add(spec)

    # Build state-led
    for spec in _build_hypotheses(
        TriggerType.STATE, states, horizons, directions, entry_lags, contexts, templates
    ):
        _add(spec)

    # Build transitions
    for spec in _build_hypotheses(
        TriggerType.TRANSITION, transitions, horizons, directions, entry_lags, contexts, templates
    ):
        _add(spec)

    # Build feature predicates
    for spec in _build_hypotheses(
        TriggerType.FEATURE_PREDICATE,
        feature_predicates,
        horizons,
        directions,
        entry_lags,
        contexts,
        templates,
    ):
        _add(spec)

    # Build sequences if requested
    if doc.get("include_sequences", False) or (search_spec_name == "full" and search_space_path is None):
        sequences = load_sequence_registry()
        for spec in _build_hypotheses(
            TriggerType.SEQUENCE, sequences, horizons, directions, entry_lags, contexts, templates
        ):
            _add(spec)

    # Build interactions if requested
    if doc.get("include_interactions", False) or (search_spec_name == "full" and search_space_path is None):
        interactions = load_interaction_registry()
        for spec in _build_hypotheses(
            TriggerType.INTERACTION,
            interactions,
            horizons,
            directions,
            entry_lags,
            contexts,
            templates,
        ):
            _add(spec)

    # Pass 2 — optional filter overlays for event-led expression hypotheses.
    # The primary search unit remains trigger × expression template. When a search spec
    # requests filter_templates, they are attached as feature_condition overlays instead
    # of being emitted as standalone filter-only hypotheses.
    if selected_filter_templates:
        for event_id in events:
            family = event_family_map.get(event_id)
            if not family:
                continue
            available_filters = resolve_filter_templates(family)
            if not available_filters:
                continue
            if selected_filter_templates == ["*"]:
                overlay_filters = available_filters
            else:
                allowed = set(selected_filter_templates)
                overlay_filters = [ft for ft in available_filters if ft["name"] in allowed]
            if not overlay_filters:
                continue
            event_templates = (
                _event_default_templates(event_id, registry=registry, fallback_templates=templates)
                if use_event_templates
                else list(templates)
            )
            trigger = TriggerSpec.event(event_id)
            for ft in overlay_filters:
                fc = TriggerSpec.feature_predicate(
                    feature=ft["feature"],
                    operator=ft["operator"],
                    threshold=ft["threshold"],
                )
                for horizon, direction, lag, ctx, template in product(
                    horizons, directions, entry_lags, contexts, event_templates
                ):
                    _add(
                        HypothesisSpec(
                            trigger=trigger,
                            direction=direction,
                            horizon=horizon,
                            template_id=template,
                            context=ctx,
                            entry_lag=lag,
                            feature_condition=fc,
                            filter_template_id=ft["name"],
                        )
                    )

    if skipped_invalid:
        log.warning("Skipped %d invalid HypothesisSpec objects during generation", skipped_invalid)
    if rejection_reason_counts:
        log.warning("Generation rejections by reason: %s", rejection_reason_counts)

    log.info(
        "Generated %d hypotheses from search spec '%s' (events=%d states=%d transitions=%d features=%d). "
        "Audit: skipped_cap=%d, skipped_quota=%d, skipped_budget=%d, skipped_dup=%d, skipped_invalid=%d",
        len(hypotheses),
        search_spec_name,
        len(events),
        len(states),
        len(transitions),
        len(feature_predicates),
        skipped_cap,
        skipped_quota,
        skipped_budget,
        skipped_dup,
        skipped_invalid,
    )

    audit = {
        "search_spec_name": search_spec_name,
        "generated_rows": generated_rows,
        "rejected_rows": rejected_rows,
        "feasible_rows": feasible_rows,
        "counts": {
            "generated": len(generated_rows),
            "rejected": len(rejected_rows),
            "feasible": len(feasible_rows),
            "skipped_cap": int(skipped_cap),
            "skipped_quota": int(skipped_quota),
            "skipped_budget": int(skipped_budget),
            "skipped_dup": int(skipped_dup),
            "skipped_invalid": int(skipped_invalid),
        },
        "rejection_reason_counts": dict(rejection_reason_counts),
    }
    return hypotheses, audit


def generate_hypotheses(
    search_spec_name: str = "full",
    *,
    max_hypotheses: int | None = None,
    skip_invalid: bool = True,
    search_space_path: Path | str | None = None,
) -> list[HypothesisSpec]:
    hypotheses, _ = generate_hypotheses_with_audit(
        search_spec_name,
        max_hypotheses=max_hypotheses,
        skip_invalid=skip_invalid,
        search_space_path=search_space_path,
    )
    return hypotheses


# ---------------------------------------------------------------------------
# Phase 4 — Hierarchical stage generators
#
# All four functions delegate to the existing _build_hypotheses() primitive
# with deliberately constrained parameter lists.  No new generation logic —
# just bounded sub-slices of the full flat search space.
# ---------------------------------------------------------------------------

def _first_n(items: list, n: int) -> list:
    """Return the first n items of a list (or all if len < n)."""
    return items[:max(1, int(n))]


def _canonical_templates_for_event(
    event_id: str,
    *,
    fallback_templates: list[str],
    registry,
) -> list[str]:
    """Return templates for an event, falling back to spec templates."""
    return _event_default_templates(
        event_id, registry=registry, fallback_templates=fallback_templates
    )


def generate_trigger_probe_candidates(
    events: list[str],
    search_spec_doc: dict,
    hierarchical_config: dict,
    *,
    features=None,
) -> list[HypothesisSpec]:
    """Stage A — Generate a minimal canonical probe set per trigger.

    Per event: one canonical template, one canonical horizon, one entry lag.
    Directions: both if ``allow_both_directions=True`` (default), else ["long"].
    No context expansion.
    """
    stage_cfg = hierarchical_config.get("trigger_viability", {})
    max_templates = int(stage_cfg.get("max_templates", 1))
    max_horizons = int(stage_cfg.get("max_horizons", 1))
    max_entry_lags = int(stage_cfg.get("max_entry_lags", 1))
    allow_both = bool(stage_cfg.get("allow_both_directions", True))

    horizons_all = [str(h) for h in search_spec_doc.get("horizons", ["24b"])]
    entry_lags_all = resolve_entry_lags(search_spec_doc)
    directions_all = [str(d) for d in search_spec_doc.get("directions", ["long", "short"])]
    templates_all = resolve_templates(search_spec_doc)

    probe_horizons = _first_n(horizons_all, max_horizons)
    probe_lags = _first_n(entry_lags_all, max_entry_lags)
    probe_directions = directions_all if allow_both else [directions_all[0]]
    # No context for Stage A
    probe_contexts: list[dict[str, Any] | None] = [None]

    registry = get_domain_registry()
    hypotheses: list[HypothesisSpec] = []
    seen_ids: set = set()
    seen_branch_hashes: set = set()

    for event_id in events:
        # One canonical template per trigger
        event_templates = _canonical_templates_for_event(
            event_id, fallback_templates=templates_all, registry=registry
        )
        probe_templates = _first_n(event_templates, max_templates)

        for spec in _build_hypotheses(
            TriggerType.EVENT,
            [event_id],
            probe_horizons,
            probe_directions,
            probe_lags,
            probe_contexts,
            probe_templates,
        ):
            errors = validate_hypothesis_spec(spec)
            if errors:
                continue
            feasibility = check_hypothesis_feasibility(spec, features=features)
            if not feasibility.valid:
                continue
            if not _accept_unique_spec(
                spec,
                seen_ids=seen_ids,
                seen_branch_hashes=seen_branch_hashes,
            ):
                continue
            hypotheses.append(spec)

    log.info(
        "Stage A probes: %d hypotheses from %d triggers (no context, up to %d templates each)",
        len(hypotheses),
        len(events),
        max_templates,
    )
    return hypotheses


def generate_template_refinement_candidates(
    surviving_trigger_events: list[str],
    search_spec_doc: dict,
    hierarchical_config: dict,
    *,
    features=None,
) -> list[HypothesisSpec]:
    """Stage B — Expand templates for surviving triggers only.

    Uses canonical horizon and canonical lag (Stage A anchors).
    No context expansion.
    """
    if not surviving_trigger_events:
        return []

    stage_cfg = hierarchical_config.get("template_refinement", {})
    top_k_templates = int(stage_cfg.get("top_k_templates_per_trigger", 3))
    max_horizons = 1  # Stay at canonical anchor
    max_entry_lags = 1

    horizons_all = [str(h) for h in search_spec_doc.get("horizons", ["24b"])]
    entry_lags_all = resolve_entry_lags(search_spec_doc)
    directions_all = [str(d) for d in search_spec_doc.get("directions", ["long", "short"])]
    templates_all = resolve_templates(search_spec_doc)

    probe_horizons = _first_n(horizons_all, max_horizons)
    probe_lags = _first_n(entry_lags_all, max_entry_lags)
    probe_contexts: list[dict[str, Any] | None] = [None]

    registry = get_domain_registry()
    hypotheses: list[HypothesisSpec] = []
    seen_ids: set = set()
    seen_branch_hashes: set = set()

    for event_id in surviving_trigger_events:
        event_templates = _canonical_templates_for_event(
            event_id, fallback_templates=templates_all, registry=registry
        )
        # Respect top_k_templates cap
        probe_templates = _first_n(event_templates, top_k_templates)

        for spec in _build_hypotheses(
            TriggerType.EVENT,
            [event_id],
            probe_horizons,
            directions_all,
            probe_lags,
            probe_contexts,
            probe_templates,
        ):
            errors = validate_hypothesis_spec(spec)
            if errors:
                continue
            feasibility = check_hypothesis_feasibility(spec, features=features)
            if not feasibility.valid:
                continue
            if not _accept_unique_spec(
                spec,
                seen_ids=seen_ids,
                seen_branch_hashes=seen_branch_hashes,
            ):
                continue
            hypotheses.append(spec)

    log.info(
        "Stage B: %d hypotheses from %d surviving triggers (top %d templates each)",
        len(hypotheses),
        len(surviving_trigger_events),
        top_k_templates,
    )
    return hypotheses


def generate_execution_refinement_candidates(
    surviving_trigger_templates: list[tuple[str, str]],
    search_spec_doc: dict,
    hierarchical_config: dict,
    *,
    features=None,
) -> list[HypothesisSpec]:
    """Stage C — Refine execution shape (direction × lag) for surviving pairs.

    Input: list of (event_id, template_id) tuples.
    Expands: all directions × all spec lags × first 2 horizons.
    No context expansion.
    """
    if not surviving_trigger_templates:
        return []

    stage_cfg = hierarchical_config.get("execution_refinement", {})
    max_horizons = int(stage_cfg.get("max_horizons", 2))
    max_entry_lags = int(stage_cfg.get("max_entry_lags", max(1, len(resolve_entry_lags(search_spec_doc)))))

    horizons_all = [str(h) for h in search_spec_doc.get("horizons", ["24b"])]
    entry_lags_all = resolve_entry_lags(search_spec_doc)
    directions_all = [str(d) for d in search_spec_doc.get("directions", ["long", "short"])]

    exec_horizons = _first_n(horizons_all, max_horizons)
    exec_lags = _first_n(entry_lags_all, max_entry_lags)
    exec_contexts: list[dict[str, Any] | None] = [None]

    hypotheses: list[HypothesisSpec] = []
    seen_ids: set = set()
    seen_branch_hashes: set = set()

    for event_id, template_id in surviving_trigger_templates:
        for spec in _build_hypotheses(
            TriggerType.EVENT,
            [event_id],
            exec_horizons,
            directions_all,
            exec_lags,
            exec_contexts,
            [template_id],
        ):
            errors = validate_hypothesis_spec(spec)
            if errors:
                continue
            feasibility = check_hypothesis_feasibility(spec, features=features)
            if not feasibility.valid:
                continue
            if not _accept_unique_spec(
                spec,
                seen_ids=seen_ids,
                seen_branch_hashes=seen_branch_hashes,
            ):
                continue
            hypotheses.append(spec)

    log.info(
        "Stage C: %d hypotheses from %d trigger-template pairs (dir=%d, lags=%d, horizons=%d)",
        len(hypotheses),
        len(surviving_trigger_templates),
        len(directions_all),
        len(exec_lags),
        len(exec_horizons),
    )
    return hypotheses


def generate_context_refinement_candidates(
    surviving_specs: list[HypothesisSpec],
    search_spec_doc: dict,
    hierarchical_config: dict,
    *,
    features=None,
) -> tuple[list[HypothesisSpec], list[HypothesisSpec]]:
    """Stage D — Sparse context refinement for surviving execution shapes.

    Returns (baseline_specs, context_specs) where baseline_specs are the
    same specs with no context (unconditional anchor) and context_specs are
    one-dimensional context variants.

    Hard constraint: max_context_dims=1, no conjunctions.
    """
    if not surviving_specs:
        return [], []

    stage_cfg = hierarchical_config.get("context_refinement", {})
    max_context_dims = int(stage_cfg.get("max_context_dims", 1))  # always 1 in v1
    top_k_contexts = int(stage_cfg.get("top_k_contexts_per_candidate", 2))

    raw_contexts = search_spec_doc.get("contexts", {})
    # Expand only single-dimension context variants
    all_1d_contexts: list[dict[str, str]] = []
    registry = get_domain_registry()
    for family, value in raw_contexts.items():
        labels = []
        if value == "*":
            labels = list(registry.context_labels_for_family(family))
        elif isinstance(value, list):
            labels = [str(v) for v in value]
        else:
            labels = [str(value)]
        for label in labels:
            all_1d_contexts.append({str(family): str(label)})

    # Hard cap: enforce max_context_dims=1 (no multi-dim conjunctions)
    if max_context_dims < 1:
        all_1d_contexts = []

    # Apply top_k cap per parent spec
    capped_contexts = _first_n(all_1d_contexts, top_k_contexts)

    seen_ids: set = set()
    seen_branch_hashes: set = set()
    baseline_specs: list[HypothesisSpec] = []
    context_specs: list[HypothesisSpec] = []

    for parent_spec in surviving_specs:
        # Baseline: same spec, no context
        baseline = HypothesisSpec(
            trigger=parent_spec.trigger,
            direction=parent_spec.direction,
            horizon=parent_spec.horizon,
            template_id=parent_spec.template_id,
            context=None,
            entry_lag=parent_spec.entry_lag,
        )
        if baseline.hypothesis_id() not in seen_ids:
            errors = validate_hypothesis_spec(baseline)
            if not errors:
                feasibility = check_hypothesis_feasibility(baseline, features=features)
                if feasibility.valid and _accept_unique_spec(
                    baseline,
                    seen_ids=seen_ids,
                    seen_branch_hashes=seen_branch_hashes,
                ):
                    baseline_specs.append(baseline)

        # Context variants
        for ctx in capped_contexts:
            spec = HypothesisSpec(
                trigger=parent_spec.trigger,
                direction=parent_spec.direction,
                horizon=parent_spec.horizon,
                template_id=parent_spec.template_id,
                context=ctx,
                entry_lag=parent_spec.entry_lag,
            )
            errors = validate_hypothesis_spec(spec)
            if errors:
                continue
            feasibility = check_hypothesis_feasibility(spec, features=features)
            if not feasibility.valid:
                continue
            if not _accept_unique_spec(
                spec,
                seen_ids=seen_ids,
                seen_branch_hashes=seen_branch_hashes,
            ):
                continue
            context_specs.append(spec)

    log.info(
        "Stage D: %d baseline + %d context variants from %d surviving specs "
        "(max_context_dims=%d, top_k=%d)",
        len(baseline_specs),
        len(context_specs),
        len(surviving_specs),
        max_context_dims,
        top_k_contexts,
    )
    return baseline_specs, context_specs
