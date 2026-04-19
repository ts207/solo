from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from project import PROJECT_ROOT
from project.domain.models import (
    DomainRegistry,
    EventDefinition,
    RegimeDefinition,
    StateDefinition,
    ThesisDefinition,
    TemplateOperatorDefinition,
)
from project.spec_registry import (
    load_regime_registry,
    load_state_registry,
    load_template_registry,
    load_thesis_registry,
    load_unified_event_registry,
    load_yaml_relative,
    load_yaml_path,
    resolve_relative_spec_path,
)


_SPECIAL_EVENT_SPEC_KINDS = {
    "canonical_event_registry",
    "event_config_defaults",
    "event_family_defaults",
    "event_unified_registry",
}
_DOMAIN_GRAPH_RELATIVE_PATH = "spec/domain/domain_graph.yaml"
_EVENT_RUNTIME_RAW_KEYS = (
    "templates",
    "horizons",
    "conditioning_cols",
    "max_candidates_per_run",
    "state_overrides",
    "precedence_reason",
)
_OPERATOR_RUNTIME_RAW_KEYS = (
    "side_policy",
    "label_target",
    "requires_direction",
    "supports_trigger_types",
)


def _event_spec_dir() -> Path:
    return resolve_relative_spec_path("spec/events", repo_root=PROJECT_ROOT.parent)


def domain_graph_path() -> Path:
    return resolve_relative_spec_path(_DOMAIN_GRAPH_RELATIVE_PATH, repo_root=PROJECT_ROOT.parent)


def _repo_relative_path(path: str | Path) -> str:
    candidate = Path(path)
    if not candidate.is_absolute():
        return candidate.as_posix()
    try:
        return candidate.resolve().relative_to(PROJECT_ROOT.parent.resolve()).as_posix()
    except Exception:
        return candidate.as_posix()


def _graph_spec_path(relative_path: str) -> str:
    return _repo_relative_path(resolve_relative_spec_path(relative_path, repo_root=PROJECT_ROOT.parent))


def _inflate_graph_path(value: Any) -> str:
    text = str(value or '').strip()
    if not text:
        return ''
    path = Path(text)
    if path.is_absolute():
        return path.as_posix()
    return (PROJECT_ROOT.parent / path).resolve().as_posix()


def _generated_at_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def _detector_ownership() -> Dict[str, str]:
    payload = load_yaml_relative("project/configs/registries/detectors.yaml")
    raw = payload.get("detector_ownership", {}) if isinstance(payload, dict) else {}
    if not isinstance(raw, dict):
        return {}
    return {
        str(event_type).strip().upper(): str(detector_name).strip()
        for event_type, detector_name in raw.items()
        if str(event_type).strip() and str(detector_name).strip()
    }


def _merge_event_rows(unified: Dict[str, Any]) -> Dict[str, EventDefinition]:
    defaults = unified.get("defaults", {})
    families = unified.get("families", {})
    unified_events = unified.get("events", {})
    detector_ownership = _detector_ownership()
    out: Dict[str, EventDefinition] = {}

    event_types = set()
    if isinstance(unified_events, dict):
        event_types.update(str(k).strip().upper() for k in unified_events.keys())

    for event_type in sorted(event_types):
        unified_row = unified_events.get(event_type, {}) if isinstance(unified_events, dict) else {}
        row: Dict[str, Any] = {}
        if isinstance(defaults, dict):
            row.update(defaults)
        research_family = str(
            unified_row.get("research_family", unified_row.get("canonical_family", ""))
        ).strip().upper()
        canonical_regime = str(unified_row.get("canonical_regime", "")).strip().upper()
        if (
            research_family
            and isinstance(families, dict)
            and isinstance(families.get(research_family), dict)
        ):
            row.update(families[research_family])
        
        if isinstance(unified_row, dict):
            row.update(unified_row)
        parameters = {}
        default_params = defaults.get("parameters", {}) if isinstance(defaults, dict) else {}
        family_params = {}
        if (
            research_family
            and isinstance(families, dict)
            and isinstance(families.get(research_family), dict)
        ):
            family_params = families[research_family].get("parameters", {})
            
        if isinstance(default_params, dict):
            parameters.update(default_params)
        if isinstance(family_params, dict):
            parameters.update(family_params)
        if isinstance(row.get("parameters"), dict):
            parameters.update(row["parameters"])

        row["parameters"] = parameters
        spec_path = _graph_spec_path(f"spec/events/{event_type}.yaml")
        
        out[event_type] = EventDefinition(
            event_type=event_type,
            research_family=research_family or str(row.get("canonical_family", "")).strip().upper(),
            canonical_family=research_family or str(row.get("canonical_family", "")).strip().upper(),
            canonical_regime=canonical_regime or str(row.get("canonical_regime", "")).strip().upper(),
            event_kind=str(row.get("event_kind", "market_event")).strip() or "market_event",
            reports_dir=str(row.get("reports_dir", event_type.lower())),
            events_file=str(row.get("events_file", f"{event_type.lower()}_events.parquet")),
            signal_column=str(row.get("signal_column", f"{event_type.lower()}_event")),
            subtype=str(row.get("subtype", "")).strip(),
            phase=str(row.get("phase", "")).strip(),
            evidence_mode=str(row.get("evidence_mode", "")).strip(),
            asset_scope=str(row.get("asset_scope", "")).strip(),
            venue_scope=str(row.get("venue_scope", "")).strip(),
            is_composite=bool(row.get("is_composite", False)),
            is_context_tag=bool(row.get("is_context_tag", False)),
            is_strategy_construct=bool(row.get("is_strategy_construct", False)),
            research_only=bool(row.get("research_only", False)),
            strategy_only=bool(row.get("strategy_only", False)),
            deconflict_priority=int(row.get("deconflict_priority", 0) or 0),
            disposition=str(row.get("disposition", "")).strip(),
            layer=str(row.get("layer", "")).strip(),
            notes=str(row.get("notes", "")).strip(),
            tier=str(row.get("tier", "")).strip().upper(),
            operational_role=str(row.get("operational_role", "")).strip(),
            deployment_disposition=str(row.get("deployment_disposition", "")).strip(),
            runtime_category=str(row.get("runtime_category", "active_runtime_event")).strip() or "active_runtime_event",
            maturity=str(row.get("maturity", "")).strip(),
            default_executable=bool(row.get("default_executable", True)),
            detector_band=str(row.get("detector_band", "")).strip(),
            planning_eligible=bool(row.get("planning_eligible", False)),
            runtime_eligible=bool(row.get("runtime_eligible", False)),
            promotion_eligible=bool(row.get("promotion_eligible", False)),
            primary_anchor_eligible=bool(row.get("primary_anchor_eligible", False)),
            enabled=bool(row.get("enabled", True)),
            detector_name=(
                str(row.get("detector_name", "")).strip()
                or detector_ownership.get(event_type, "")
            ),
            instrument_classes=tuple(str(item).strip() for item in row.get("instrument_classes", []) if str(item).strip()),
            requires_features=tuple(str(item).strip() for item in row.get("requires_features", []) if str(item).strip()),
            runtime_tags=tuple(str(item).strip() for item in row.get("runtime_tags", []) if str(item).strip()),
            sequence_eligible=bool(row.get("sequence_eligible", True)),
            cluster_id=str(row.get("cluster_id", "")).strip(),
            collapse_target=str(row.get("collapse_target", "")).strip(),
            overlap_group=str(row.get("overlap_group", "")).strip(),
            precedence_rank=int(row.get("precedence_rank", 0) or 0),
            routing_profile_ref=str(row.get("routing_profile_ref", "")).strip(),
            suppresses=tuple(row.get("suppresses", []) if isinstance(row.get("suppresses"), (list, tuple)) else ()),
            suppressed_by=tuple(row.get("suppressed_by", []) if isinstance(row.get("suppressed_by"), (list, tuple)) else ()),
            maturity_scores=dict(row.get("maturity_scores", {})) if isinstance(row.get("maturity_scores"), dict) else {},
            parameters=dict(parameters),
            raw=dict(row),
            spec_path=spec_path,
            source_kind="unified_registry",
        )
    return out


def _load_states() -> Dict[str, StateDefinition]:
    payload = load_state_registry()
    out: Dict[str, StateDefinition] = {}
    defaults = payload.get("defaults", {}) if isinstance(payload, dict) else {}
    if not isinstance(defaults, dict):
        defaults = {}
    runtime_defaults = defaults.get("runtime", {})
    if not isinstance(runtime_defaults, dict):
        runtime_defaults = {}
    default_scope = str(defaults.get("state_scope", "source_only")).strip().lower() or "source_only"
    default_min_events = int(defaults.get("min_events", 200) or 200)
    rows = payload.get("states", []) if isinstance(payload, dict) else []
    for row in rows:
        if not isinstance(row, dict):
            continue
        state_id = str(row.get("state_id", "")).strip().upper()
        if not state_id:
            continue
        spec_path = _graph_spec_path(f"spec/states/{state_id}.yaml")
        runtime = row.get("runtime", {})
        if not isinstance(runtime, dict):
            runtime = {}
        context = row.get("context", {})
        if not isinstance(context, dict):
            context = {}
        merged_runtime = dict(runtime_defaults)
        merged_runtime.update(runtime)
        state_scope = str(row.get("state_scope", default_scope)).strip().lower() or default_scope
        if state_scope not in {"source_only", "family_safe", "global"}:
            state_scope = default_scope
        out[state_id] = StateDefinition(
            state_id=state_id,
            family=str(row.get("family", "")).strip().upper(),
            source_event_type=str(row.get("source_event_type", "")).strip().upper(),
            state_scope=state_scope,
            min_events=int(row.get("min_events", default_min_events) or default_min_events),
            activation_rule=str(row.get("activation_rule", "")).strip(),
            decay_rule=str(row.get("decay_rule", "")).strip(),
            max_duration=row.get("max_duration"),
            features_required=tuple(
                str(item).strip()
                for item in row.get("features_required", [])
                if str(item).strip()
            ),
            allowed_templates=tuple(
                str(item).strip()
                for item in row.get("allowed_templates", [])
                if str(item).strip()
            ),
            enabled=bool(merged_runtime.get("enabled", True)),
            state_engine=str(merged_runtime.get("state_engine", "")).strip(),
            instrument_classes=tuple(
                str(item).strip()
                for item in merged_runtime.get("instrument_classes", [])
                if str(item).strip()
            ),
            runtime_tags=tuple(
                str(item).strip()
                for item in merged_runtime.get("tags", merged_runtime.get("runtime_tags", []))
                if str(item).strip()
            ),
            description=str(
                row.get("description", merged_runtime.get("description", ""))
            ).strip(),
            context_family=str(context.get("family", "")).strip(),
            context_label=str(context.get("label", "")).strip(),
            raw=dict(row),
            spec_path=spec_path,
            source_kind="state_registry",
        )
    return out


def _state_context_dimensions() -> Dict[str, Dict[str, Any]]:
    payload = load_state_registry()
    raw = payload.get("context_dimensions", {}) if isinstance(payload, dict) else {}
    return dict(raw) if isinstance(raw, dict) else {}


def _load_context_state_map() -> Dict[tuple[str, str], str]:
    out: Dict[tuple[str, str], str] = {}
    for family, cfg in _state_context_dimensions().items():
        if not isinstance(cfg, dict):
            continue
        labels = cfg.get("mapping", {})
        if not isinstance(labels, dict):
            continue
        for label, state_id in labels.items():
            fam = str(family).strip()
            lab = str(label).strip()
            sid = str(state_id).strip().upper()
            if fam and lab and sid:
                out[(fam, lab)] = sid
    return out


def _load_state_aliases() -> tuple[str, ...]:
    out: set[str] = set()
    for cfg in _state_context_dimensions().values():
        if not isinstance(cfg, dict):
            continue
        labels = cfg.get("allowed_values", [])
        if isinstance(labels, list):
            out.update(str(label).strip().upper() for label in labels if str(label).strip())
        mapping = cfg.get("mapping", {})
        if isinstance(mapping, dict):
            out.update(
                str(state_id).strip().upper()
                for state_id in mapping.values()
                if str(state_id).strip()
            )
    return tuple(sorted(out))


def _load_searchable_families() -> tuple[tuple[str, ...], tuple[str, ...]]:
    payload = _load_family_registry_payload()
    event_families = payload.get("event_families", {}) if isinstance(payload, dict) else {}
    state_families = payload.get("state_families", {}) if isinstance(payload, dict) else {}
    searchable_events = tuple(
        sorted(
            str(name).strip().upper()
            for name, cfg in event_families.items()
            if isinstance(cfg, dict) and bool(cfg.get("searchable", False))
        )
    )
    searchable_states = tuple(
        sorted(
            str(name).strip().upper()
            for name, cfg in state_families.items()
            if isinstance(cfg, dict) and bool(cfg.get("searchable", False))
        )
    )
    return searchable_events, searchable_states


def _load_family_registry_payload(
    template_registry_payload: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    canonical = (
        dict(template_registry_payload)
        if isinstance(template_registry_payload, dict)
        else load_template_registry()
    )
    authored = load_yaml_relative("spec/grammar/family_registry.yaml")
    event_families = authored.get("event_families", {}) if isinstance(authored, dict) else {}
    state_families = authored.get("state_families", {}) if isinstance(authored, dict) else {}
    template_families = canonical.get("families", {}) if isinstance(canonical, dict) else {}

    merged_event_families: Dict[str, Dict[str, Any]] = {}
    all_event_family_names = {
        str(name).strip().upper()
        for name in (
            list(event_families.keys()) if isinstance(event_families, dict) else []
        )
        + (list(template_families.keys()) if isinstance(template_families, dict) else [])
        if str(name).strip()
    }
    for family_name in sorted(all_event_family_names):
        authored_row = (
            event_families.get(family_name, {})
            if isinstance(event_families, dict)
            else {}
        )
        canonical_row = (
            template_families.get(family_name, {})
            if isinstance(template_families, dict)
            else {}
        )
        merged_row: Dict[str, Any] = {}
        if isinstance(authored_row, dict):
            merged_row.update(dict(authored_row))
        if isinstance(canonical_row, dict):
            templates = canonical_row.get("templates", canonical_row.get("allowed_templates", []))
            merged_row["allowed_templates"] = [
                str(item).strip()
                for item in templates
                if str(item).strip()
            ]
        merged_event_families[family_name] = merged_row

    return {
        "version": int(authored.get("version", 1) or 1) if isinstance(authored, dict) else 1,
        "kind": str(authored.get("kind", "family_registry")).strip() if isinstance(authored, dict) else "family_registry",
        "event_families": merged_event_families,
        "state_families": dict(state_families) if isinstance(state_families, dict) else {},
    }


def _load_stress_scenarios() -> tuple[Dict[str, Any], ...]:
    payload = load_yaml_relative("spec/grammar/stress_scenarios.yaml")
    rows = payload.get("scenarios", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        return ()
    out: list[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name", "")).strip()
        feature = str(row.get("feature", "")).strip()
        operator = str(row.get("operator", "")).strip()
        if not (name and feature and operator):
            continue
        out.append(dict(row))
    return tuple(out)


def _load_kill_switch_candidate_features() -> tuple[str, ...]:
    payload = load_yaml_relative("spec/grammar/kill_switch_config.yaml")
    rows = payload.get("candidates", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        return ()
    return tuple(str(value).strip() for value in rows if str(value).strip())


def _load_sequence_definitions() -> tuple[Dict[str, Any], ...]:
    payload = load_yaml_relative("spec/grammar/sequence_registry.yaml")
    rows = payload.get("sequences", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        return ()
    out: list[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name", "")).strip()
        events = row.get("events", [])
        if not (name and isinstance(events, list) and events):
            continue
        out.append(dict(row))
    return tuple(out)


def _load_interaction_definitions() -> tuple[Dict[str, Any], ...]:
    payload = load_yaml_relative("spec/grammar/interaction_registry.yaml")
    rows = payload.get("motifs", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        return ()
    out: list[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name", "")).strip()
        left = str(row.get("left", "")).strip()
        right = str(row.get("right", "")).strip()
        op = str(row.get("op", "")).strip()
        if not (name and left and right and op):
            continue
        out.append(dict(row))
    return tuple(out)


def _load_operators() -> Dict[str, TemplateOperatorDefinition]:
    template_registry = load_template_registry()
    operators = template_registry.get("operators", {})
    out: Dict[str, TemplateOperatorDefinition] = {}
    if not isinstance(operators, dict):
        return out
    for template_id, row in operators.items():
        if not isinstance(row, dict):
            continue
        out[str(template_id).strip()] = TemplateOperatorDefinition(
            template_id=str(template_id).strip(),
            compatible_families=tuple(
                str(x).strip().upper() for x in row.get("compatible_families", []) or []
            ),
            template_kind=str(row.get("template_kind", "")).strip().lower(),
            raw=dict(row),
        )
    return out


def _load_regimes() -> Dict[str, RegimeDefinition]:
    payload = load_regime_registry()
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    regimes = payload.get("regimes", {}) if isinstance(payload, dict) else {}
    if not isinstance(metadata, dict):
        metadata = {}
    if not isinstance(regimes, dict):
        return {}
    routing_profile_id = str(metadata.get("routing_profile_id", "regime_routing")).strip()
    scorecard_version = str(metadata.get("scorecard_version", "")).strip()
    scorecard_source_run = str(metadata.get("scorecard_source_run", "")).strip()
    spec_path = _graph_spec_path("spec/regimes/registry.yaml")
    out: Dict[str, RegimeDefinition] = {}
    for canonical_regime, row in sorted(regimes.items()):
        if not isinstance(row, dict):
            continue
        normalized = str(canonical_regime).strip().upper()
        if not normalized:
            continue
        out[normalized] = RegimeDefinition(
            canonical_regime=normalized,
            bucket=str(row.get("bucket", "")).strip(),
            eligible_templates=tuple(
                str(item).strip()
                for item in row.get("eligible_templates", [])
                if str(item).strip()
            ),
            forbidden_templates=tuple(
                str(item).strip()
                for item in row.get("forbidden_templates", [])
                if str(item).strip()
            ),
            risk_posture=str(row.get("risk_posture", "")).strip(),
            execution_style=str(row.get("execution_style", "")).strip(),
            holding_horizon_profile=str(row.get("holding_horizon_profile", "")).strip(),
            stop_logic_profile=str(row.get("stop_logic_profile", "")).strip(),
            profit_taking_profile=str(row.get("profit_taking_profile", "")).strip(),
            overrides=dict(row.get("overrides", {}) or {}),
            routing_profile_id=routing_profile_id,
            scorecard_version=scorecard_version,
            scorecard_source_run=scorecard_source_run,
            raw=dict(row),
            spec_path=spec_path,
            source_kind="regime_registry",
        )
    return out


def _normalize_tokens(values: Any, *, uppercase: bool = True) -> tuple[str, ...]:
    if values in (None, ""):
        return ()
    if isinstance(values, str):
        values = [values]
    if not isinstance(values, (list, tuple, set)):
        return ()
    out: list[str] = []
    seen: set[str] = set()
    for item in values:
        token = str(item or "").strip()
        if uppercase:
            token = token.upper()
        if token and token not in seen:
            out.append(token)
            seen.add(token)
    return tuple(out)


def _load_theses() -> Dict[str, ThesisDefinition]:
    payload = load_thesis_registry()
    defaults = payload.get("defaults", {}) if isinstance(payload, dict) else {}
    rows = payload.get("theses", {}) if isinstance(payload, dict) else {}
    if not isinstance(defaults, dict):
        defaults = {}
    if not isinstance(rows, dict):
        return {}

    out: Dict[str, ThesisDefinition] = {}
    spec_path = _graph_spec_path("spec/theses/thesis_registry.yaml")
    default_required_context = defaults.get("required_context", {})
    default_supportive_context = defaults.get("supportive_context", {})
    default_expected_response = defaults.get("expected_response", {})
    default_invalidation = defaults.get("invalidation", {})
    default_freshness_policy = defaults.get("freshness_policy", {})
    default_governance = defaults.get("governance", {})
    default_symbol_scope = defaults.get("symbol_scope", {})
    default_detection = defaults.get("detection", {})

    for thesis_id, raw in sorted(rows.items()):
        if not isinstance(raw, dict):
            continue
        identity = raw.get("identity", {})
        if not isinstance(identity, dict):
            identity = {}
        clauses = raw.get("clauses", {})
        if not isinstance(clauses, dict):
            clauses = {}
        source = raw.get("source", {})
        if not isinstance(source, dict):
            source = {}
        governance = _merge_mapping(default_governance, raw.get("governance", {}))
        symbol_scope = _merge_mapping(default_symbol_scope, raw.get("symbol_scope", {}))
        detection = _merge_mapping(default_detection, raw.get("detection", {}))
        required_context = _merge_mapping(default_required_context, raw.get("required_context", {}))
        supportive_context = _merge_mapping(default_supportive_context, raw.get("supportive_context", {}))
        expected_response = _merge_mapping(default_expected_response, raw.get("expected_response", {}))
        invalidation = _merge_mapping(default_invalidation, raw.get("invalidation", {}))
        freshness_policy = _merge_mapping(default_freshness_policy, raw.get("freshness_policy", {}))

        normalized_id = str(identity.get("thesis_id", thesis_id)).strip().upper()
        if not normalized_id:
            continue
        source_event_contract_ids = _normalize_tokens(source.get("event_contract_ids", []))
        trigger_events = _normalize_tokens(clauses.get("trigger_events", []))
        primary_event_id = (
            source_event_contract_ids[0]
            if source_event_contract_ids
            else (trigger_events[0] if trigger_events else str(identity.get("event_family", raw.get("event_family", ""))).strip().upper())
        )
        canonical_regime = str(
            raw.get(
                "canonical_regime",
                supportive_context.get("canonical_regime", ""),
            )
        ).strip().upper()

        out[normalized_id] = ThesisDefinition(
            thesis_id=normalized_id,
            thesis_kind=str(identity.get("thesis_kind", raw.get("thesis_kind", "standalone_event"))).strip().lower() or "standalone_event",
            event_family=str(identity.get("event_family", raw.get("event_family", ""))).strip().upper(),
            primary_event_id=str(primary_event_id).strip().upper(),
            canonical_regime=canonical_regime,
            timeframe=str(identity.get("timeframe", defaults.get("timeframe", ""))).strip(),
            event_side=str(identity.get("event_side", defaults.get("event_side", "unknown"))).strip().lower() or "unknown",
            promotion_class=str(raw.get("promotion_class", defaults.get("promotion_class", ""))).strip(),
            deployment_state=str(raw.get("deployment_state", defaults.get("deployment_state", ""))).strip(),
            trigger_events=trigger_events,
            confirmation_events=_normalize_tokens(clauses.get("confirmation_events", [])),
            required_episodes=_normalize_tokens(clauses.get("required_episodes", [])),
            disallowed_regimes=_normalize_tokens(clauses.get("disallowed_regimes", [])),
            source_event_contract_ids=source_event_contract_ids,
            source_episode_contract_ids=_normalize_tokens(source.get("episode_contract_ids", [])),
            required_context=required_context,
            supportive_context=supportive_context,
            expected_response=expected_response,
            invalidation=invalidation,
            freshness_policy=freshness_policy,
            governance=governance,
            symbol_scope=symbol_scope,
            detection=detection,
            notes=str(raw.get("notes", "")).strip(),
            raw=dict(raw),
            spec_path=spec_path,
            source_kind="thesis_registry",
        )
    return out


def _merge_mapping(base: Any, override: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = dict(base) if isinstance(base, dict) else {}
    if isinstance(override, dict):
        out.update(override)
    return out


def _slim_mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _slim_event_raw(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, Any] = {}
    for key in _EVENT_RUNTIME_RAW_KEYS:
        value = raw.get(key)
        if value in (None, "", [], {}, ()):
            continue
        if isinstance(value, dict):
            out[key] = dict(value)
        elif isinstance(value, (list, tuple)):
            out[key] = list(value)
        else:
            out[key] = value
    return out


def _slim_operator_raw(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, Any] = {}
    for key in _OPERATOR_RUNTIME_RAW_KEYS:
        value = raw.get(key)
        if value in (None, "", [], {}, ()):
            continue
        if isinstance(value, (list, tuple)):
            out[key] = [str(item).strip() for item in value if str(item).strip()]
        else:
            out[key] = value
    return out


def _build_domain_registry_from_sources() -> DomainRegistry:
    unified = load_unified_event_registry()
    if not unified:
        raise FileNotFoundError("Unified event registry is missing or empty")
    template_registry_payload = load_template_registry()
    family_registry_payload = _load_family_registry_payload(template_registry_payload)
    event_definitions = _merge_event_rows(unified)
    state_definitions = _load_states()
    template_operator_definitions = _load_operators()
    regime_definitions = _load_regimes()
    thesis_definitions = _load_theses()
    context_state_map = _load_context_state_map()
    state_aliases = _load_state_aliases()
    searchable_event_families, searchable_state_families = _load_searchable_families()
    stress_scenarios = _load_stress_scenarios()
    kill_switch_candidate_features = _load_kill_switch_candidate_features()
    sequence_definitions = _load_sequence_definitions()
    interaction_definitions = _load_interaction_definitions()
    return DomainRegistry(
        unified_payload=dict(unified),
        event_definitions=event_definitions,
        state_definitions=state_definitions,
        template_operator_definitions=template_operator_definitions,
        regime_definitions=regime_definitions,
        gates_spec={},
        unified_registry_path="",
        template_registry_payload=dict(template_registry_payload) if isinstance(template_registry_payload, dict) else {},
        family_registry_payload=dict(family_registry_payload) if isinstance(family_registry_payload, dict) else {},
        thesis_definitions=thesis_definitions,
        context_state_map=context_state_map,
        searchable_event_families=searchable_event_families,
        searchable_state_families=searchable_state_families,
        state_aliases=state_aliases,
        stress_scenarios=stress_scenarios,
        kill_switch_candidate_features=kill_switch_candidate_features,
        sequence_definitions=sequence_definitions,
        interaction_definitions=interaction_definitions,
    )


def _event_definition_payload(spec: EventDefinition) -> Dict[str, Any]:
    return {
        "event_type": spec.event_type,
        "research_family": spec.research_family,
        "canonical_family": spec.canonical_family,
        "canonical_regime": spec.canonical_regime,
        "event_kind": spec.event_kind,
        "reports_dir": spec.reports_dir,
        "events_file": spec.events_file,
        "signal_column": spec.signal_column,
        "subtype": spec.subtype,
        "phase": spec.phase,
        "evidence_mode": spec.evidence_mode,
        "asset_scope": spec.asset_scope,
        "venue_scope": spec.venue_scope,
        "is_composite": spec.is_composite,
        "is_context_tag": spec.is_context_tag,
        "is_strategy_construct": spec.is_strategy_construct,
        "research_only": spec.research_only,
        "strategy_only": spec.strategy_only,
        "deconflict_priority": spec.deconflict_priority,
        "disposition": spec.disposition,
        "layer": spec.layer,
        "notes": spec.notes,
        "tier": spec.tier,
        "operational_role": spec.operational_role,
        "deployment_disposition": spec.deployment_disposition,
        "runtime_category": spec.runtime_category,
        "maturity": spec.maturity,
        "default_executable": spec.default_executable,
        "detector_band": spec.detector_band,
        "planning_eligible": spec.planning_eligible,
        "runtime_eligible": spec.runtime_eligible,
        "promotion_eligible": spec.promotion_eligible,
        "primary_anchor_eligible": spec.primary_anchor_eligible,
        "enabled": spec.enabled,
        "detector_name": spec.detector_name,
        "instrument_classes": list(spec.instrument_classes),
        "requires_features": list(spec.requires_features),
        "runtime_tags": list(spec.runtime_tags),
        "sequence_eligible": spec.sequence_eligible,
        "cluster_id": spec.cluster_id,
        "collapse_target": spec.collapse_target,
        "overlap_group": spec.overlap_group,
        "precedence_rank": spec.precedence_rank,
        "routing_profile_ref": spec.routing_profile_ref,
        "suppresses": list(spec.suppresses),
        "suppressed_by": list(spec.suppressed_by),
        "maturity_scores": dict(spec.maturity_scores),
        "parameters": dict(spec.parameters),
        "runtime": _slim_event_raw(spec.raw),
        "spec_path": _repo_relative_path(spec.spec_path),
    }


def _state_definition_payload(spec: StateDefinition) -> Dict[str, Any]:
    return {
        "state_id": spec.state_id,
        "family": spec.family,
        "source_event_type": spec.source_event_type,
        "state_scope": spec.state_scope,
        "min_events": spec.min_events,
        "activation_rule": spec.activation_rule,
        "decay_rule": spec.decay_rule,
        "max_duration": spec.max_duration,
        "features_required": list(spec.features_required),
        "allowed_templates": list(spec.allowed_templates),
        "enabled": spec.enabled,
        "state_engine": spec.state_engine,
        "instrument_classes": list(spec.instrument_classes),
        "runtime_tags": list(spec.runtime_tags),
        "description": spec.description,
        "context_family": spec.context_family,
        "context_label": spec.context_label,
        "spec_path": _repo_relative_path(spec.spec_path),
    }


def _operator_definition_payload(spec: TemplateOperatorDefinition) -> Dict[str, Any]:
    raw = _slim_operator_raw(spec.raw)
    return {
        "template_id": spec.template_id,
        "compatible_families": list(spec.compatible_families),
        "template_kind": spec.template_kind,
        "side_policy": str(raw.get("side_policy", "both")).strip().lower() or "both",
        "label_target": str(raw.get("label_target", "fwd_return_h")).strip().lower() or "fwd_return_h",
        "requires_direction": bool(raw.get("requires_direction", True)),
        "supports_trigger_types": [
            str(item).strip().upper()
            for item in raw.get("supports_trigger_types", [])
            if str(item).strip()
        ],
    }


def _regime_definition_payload(spec: RegimeDefinition) -> Dict[str, Any]:
    return {
        "canonical_regime": spec.canonical_regime,
        "bucket": spec.bucket,
        "eligible_templates": list(spec.eligible_templates),
        "forbidden_templates": list(spec.forbidden_templates),
        "risk_posture": spec.risk_posture,
        "execution_style": spec.execution_style,
        "holding_horizon_profile": spec.holding_horizon_profile,
        "stop_logic_profile": spec.stop_logic_profile,
        "profit_taking_profile": spec.profit_taking_profile,
        "overrides": dict(spec.overrides),
        "routing_profile_id": spec.routing_profile_id,
        "scorecard_version": spec.scorecard_version,
        "scorecard_source_run": spec.scorecard_source_run,
        "spec_path": _repo_relative_path(spec.spec_path),
    }


def _thesis_definition_payload(spec: ThesisDefinition) -> Dict[str, Any]:
    return {
        "thesis_id": spec.thesis_id,
        "thesis_kind": spec.thesis_kind,
        "event_family": spec.event_family,
        "primary_event_id": spec.primary_event_id,
        "canonical_regime": spec.canonical_regime,
        "timeframe": spec.timeframe,
        "event_side": spec.event_side,
        "promotion_class": spec.promotion_class,
        "deployment_state": spec.deployment_state,
        "trigger_events": list(spec.trigger_events),
        "confirmation_events": list(spec.confirmation_events),
        "required_episodes": list(spec.required_episodes),
        "disallowed_regimes": list(spec.disallowed_regimes),
        "source_event_contract_ids": list(spec.source_event_contract_ids),
        "source_episode_contract_ids": list(spec.source_episode_contract_ids),
        "required_context": dict(spec.required_context),
        "supportive_context": dict(spec.supportive_context),
        "expected_response": dict(spec.expected_response),
        "invalidation": dict(spec.invalidation),
        "freshness_policy": dict(spec.freshness_policy),
        "governance": dict(spec.governance),
        "symbol_scope": dict(spec.symbol_scope),
        "detection": dict(spec.detection),
        "notes": spec.notes,
        "spec_path": _repo_relative_path(spec.spec_path),
    }


def _event_runtime_view(registry: DomainRegistry) -> Dict[str, Any]:
    payload = registry.unified_payload if isinstance(registry.unified_payload, dict) else {}
    families = payload.get("families", {})
    defaults = payload.get("defaults", {})
    return {
        "kind": "event_runtime_defaults",
        "defaults": dict(defaults) if isinstance(defaults, dict) else {},
        "families": dict(families) if isinstance(families, dict) else {},
        "events": {
            event_type: _slim_event_raw(spec.raw)
            for event_type, spec in sorted(registry.event_definitions.items())
            if _slim_event_raw(spec.raw)
        },
    }


def _template_runtime_view(registry: DomainRegistry) -> Dict[str, Any]:
    payload = registry.template_registry_payload if isinstance(registry.template_registry_payload, dict) else {}
    defaults = payload.get("defaults", {})
    families = payload.get("families", {})
    filter_templates = payload.get("filter_templates", {})
    return {
        "kind": "template_runtime_defaults",
        "defaults": dict(defaults) if isinstance(defaults, dict) else {},
        "families": dict(families) if isinstance(families, dict) else {},
        "filter_templates": dict(filter_templates) if isinstance(filter_templates, dict) else {},
    }


def _runtime_payload(registry: DomainRegistry) -> Dict[str, Any]:
    return {
        "event_registry": _event_runtime_view(registry),
        "template_registry": _template_runtime_view(registry),
        "context_state_map": [
            {"family": family, "label": label, "state_id": state_id}
            for (family, label), state_id in sorted(registry.context_state_map.items())
        ],
        "searchable_event_families": list(registry.searchable_event_families),
        "searchable_state_families": list(registry.searchable_state_families),
        "stress_scenarios": [dict(row) for row in registry.stress_scenarios],
        "kill_switch_candidate_features": list(registry.kill_switch_candidate_features),
        "sequence_definitions": [dict(row) for row in registry.sequence_definitions],
        "interaction_definitions": [dict(row) for row in registry.interaction_definitions],
    }


def _domain_registry_payload(registry: DomainRegistry) -> Dict[str, Any]:
    return {
        "version": 2,
        "kind": "domain_graph",
        "metadata": {
            "status": "generated",
            "graph_role": "runtime_read_model",
            "schema_version": 2,
            "generated_at_utc": _generated_at_utc(),
            "spec_sources_digest": spec_sources_digest(),
            "notes": "Compiled runtime read model. Semantic nodes are normalized for direct runtime/search loading.",
        },
        "events": {
            event_type: _event_definition_payload(spec)
            for event_type, spec in sorted(registry.event_definitions.items())
        },
        "states": {
            state_id: _state_definition_payload(spec)
            for state_id, spec in sorted(registry.state_definitions.items())
        },
        "templates": {
            template_id: _operator_definition_payload(spec)
            for template_id, spec in sorted(registry.template_operator_definitions.items())
        },
        "regimes": {
            canonical_regime: _regime_definition_payload(spec)
            for canonical_regime, spec in sorted(registry.regime_definitions.items())
        },
        "theses": {
            thesis_id: _thesis_definition_payload(spec)
            for thesis_id, spec in sorted(registry.thesis_definitions.items())
        },
        "runtime": _runtime_payload(registry),
    }


def build_domain_graph_payload() -> Dict[str, Any]:
    return _domain_registry_payload(_build_domain_registry_from_sources())


def compile_domain_registry_from_sources() -> DomainRegistry:
    return _build_domain_registry_from_sources()


def _event_definition_from_payload(row: Dict[str, Any]) -> EventDefinition:
    return EventDefinition(
        event_type=str(row.get("event_type", "")).strip().upper(),
        research_family=str(row.get("research_family", row.get("canonical_family", ""))).strip().upper(),
        canonical_family=str(row.get("canonical_family", row.get("research_family", ""))).strip().upper(),
        canonical_regime=str(row.get("canonical_regime", "")).strip().upper(),
        event_kind=str(row.get("event_kind", "market_event")).strip() or "market_event",
        reports_dir=str(row.get("reports_dir", "")).strip(),
        events_file=str(row.get("events_file", "")).strip(),
        signal_column=str(row.get("signal_column", "")).strip(),
        subtype=str(row.get("subtype", "")).strip(),
        phase=str(row.get("phase", "")).strip(),
        evidence_mode=str(row.get("evidence_mode", "")).strip(),
        asset_scope=str(row.get("asset_scope", "")).strip(),
        venue_scope=str(row.get("venue_scope", "")).strip(),
        is_composite=bool(row.get("is_composite", False)),
        is_context_tag=bool(row.get("is_context_tag", False)),
        is_strategy_construct=bool(row.get("is_strategy_construct", False)),
        research_only=bool(row.get("research_only", False)),
        strategy_only=bool(row.get("strategy_only", False)),
        deconflict_priority=int(row.get("deconflict_priority", 0) or 0),
        disposition=str(row.get("disposition", "")).strip(),
        layer=str(row.get("layer", "")).strip(),
        notes=str(row.get("notes", "")).strip(),
        tier=str(row.get("tier", "")).strip().upper(),
        operational_role=str(row.get("operational_role", "")).strip(),
        deployment_disposition=str(row.get("deployment_disposition", "")).strip(),
        runtime_category=str(row.get("runtime_category", "active_runtime_event")).strip() or "active_runtime_event",
        maturity=str(row.get("maturity", "")).strip(),
        default_executable=bool(row.get("default_executable", True)),
        detector_band=str(row.get("detector_band", "")).strip(),
        planning_eligible=bool(row.get("planning_eligible", False)),
        runtime_eligible=bool(row.get("runtime_eligible", False)),
        promotion_eligible=bool(row.get("promotion_eligible", False)),
        primary_anchor_eligible=bool(row.get("primary_anchor_eligible", False)),
        enabled=bool(row.get("enabled", True)),
        detector_name=str(row.get("detector_name", "")).strip(),
        instrument_classes=tuple(str(item).strip() for item in row.get("instrument_classes", []) if str(item).strip()),
        requires_features=tuple(str(item).strip() for item in row.get("requires_features", []) if str(item).strip()),
        runtime_tags=tuple(str(item).strip() for item in row.get("runtime_tags", []) if str(item).strip()),
        sequence_eligible=bool(row.get("sequence_eligible", True)),
        cluster_id=str(row.get("cluster_id", "")).strip(),
        collapse_target=str(row.get("collapse_target", "")).strip(),
        overlap_group=str(row.get("overlap_group", "")).strip(),
        precedence_rank=int(row.get("precedence_rank", 0) or 0),
        routing_profile_ref=str(row.get("routing_profile_ref", "")).strip(),
        suppresses=tuple(row.get("suppresses", []) if isinstance(row.get("suppresses"), (list, tuple)) else ()),
        suppressed_by=tuple(row.get("suppressed_by", []) if isinstance(row.get("suppressed_by"), (list, tuple)) else ()),
        maturity_scores=dict(row.get("maturity_scores", {})) if isinstance(row.get("maturity_scores"), dict) else {},
        parameters=dict(row.get("parameters", {})) if isinstance(row.get("parameters"), dict) else {},
        raw=_slim_event_raw(row.get("runtime", row.get("raw", {}))),
        spec_path=_inflate_graph_path(row.get("spec_path", "")),
        source_kind="domain_graph",
    )


def _state_definition_from_payload(row: Dict[str, Any]) -> StateDefinition:
    return StateDefinition(
        state_id=str(row.get("state_id", "")).strip().upper(),
        family=str(row.get("family", "")).strip().upper(),
        source_event_type=str(row.get("source_event_type", "")).strip().upper(),
        raw={},
        state_scope=str(row.get("state_scope", "source_only")).strip().lower() or "source_only",
        min_events=int(row.get("min_events", 200) or 200),
        activation_rule=str(row.get("activation_rule", "")).strip(),
        decay_rule=str(row.get("decay_rule", "")).strip(),
        max_duration=row.get("max_duration"),
        features_required=tuple(
            str(item).strip() for item in row.get("features_required", []) if str(item).strip()
        ),
        allowed_templates=tuple(
            str(item).strip() for item in row.get("allowed_templates", []) if str(item).strip()
        ),
        enabled=bool(row.get("enabled", True)),
        state_engine=str(row.get("state_engine", "")).strip(),
        instrument_classes=tuple(
            str(item).strip() for item in row.get("instrument_classes", []) if str(item).strip()
        ),
        runtime_tags=tuple(
            str(item).strip() for item in row.get("runtime_tags", []) if str(item).strip()
        ),
        description=str(row.get("description", "")).strip(),
        context_family=str(row.get("context_family", "")).strip(),
        context_label=str(row.get("context_label", "")).strip(),
        spec_path=_inflate_graph_path(row.get("spec_path", "")),
        source_kind="domain_graph",
    )


def _operator_definition_from_payload(row: Dict[str, Any]) -> TemplateOperatorDefinition:
    raw = {
        "template_kind": str(row.get("template_kind", "")).strip().lower(),
        "side_policy": str(row.get("side_policy", "both")).strip().lower() or "both",
        "label_target": str(row.get("label_target", "fwd_return_h")).strip().lower()
        or "fwd_return_h",
        "requires_direction": bool(row.get("requires_direction", True)),
        "supports_trigger_types": [
            str(item).strip().upper()
            for item in row.get("supports_trigger_types", [])
            if str(item).strip()
        ],
    }
    return TemplateOperatorDefinition(
        template_id=str(row.get("template_id", "")).strip(),
        compatible_families=tuple(
            str(item).strip().upper() for item in row.get("compatible_families", []) if str(item).strip()
        ),
        template_kind=str(row.get("template_kind", "")).strip().lower(),
        raw=raw,
    )


def _thesis_definition_from_payload(row: Dict[str, Any]) -> ThesisDefinition:
    return ThesisDefinition(
        thesis_id=str(row.get("thesis_id", "")).strip().upper(),
        thesis_kind=str(row.get("thesis_kind", "standalone_event")).strip().lower() or "standalone_event",
        event_family=str(row.get("event_family", "")).strip().upper(),
        primary_event_id=str(row.get("primary_event_id", row.get("event_family", ""))).strip().upper(),
        canonical_regime=str(
            row.get(
                "canonical_regime",
                row.get("supportive_context", {}).get("canonical_regime", "")
                if isinstance(row.get("supportive_context"), dict)
                else "",
            )
        ).strip().upper(),
        timeframe=str(row.get("timeframe", "")).strip(),
        event_side=str(row.get("event_side", "unknown")).strip().lower() or "unknown",
        promotion_class=str(row.get("promotion_class", "")).strip(),
        deployment_state=str(row.get("deployment_state", "")).strip(),
        trigger_events=_normalize_tokens(row.get("trigger_events", [])),
        confirmation_events=_normalize_tokens(row.get("confirmation_events", [])),
        required_episodes=_normalize_tokens(row.get("required_episodes", [])),
        disallowed_regimes=_normalize_tokens(row.get("disallowed_regimes", [])),
        source_event_contract_ids=_normalize_tokens(row.get("source_event_contract_ids", [])),
        source_episode_contract_ids=_normalize_tokens(row.get("source_episode_contract_ids", [])),
        required_context=dict(row.get("required_context", {})) if isinstance(row.get("required_context"), dict) else {},
        supportive_context=dict(row.get("supportive_context", {})) if isinstance(row.get("supportive_context"), dict) else {},
        expected_response=dict(row.get("expected_response", {})) if isinstance(row.get("expected_response"), dict) else {},
        invalidation=dict(row.get("invalidation", {})) if isinstance(row.get("invalidation"), dict) else {},
        freshness_policy=dict(row.get("freshness_policy", {})) if isinstance(row.get("freshness_policy"), dict) else {},
        governance=dict(row.get("governance", {})) if isinstance(row.get("governance"), dict) else {},
        symbol_scope=dict(row.get("symbol_scope", {})) if isinstance(row.get("symbol_scope"), dict) else {},
        detection=dict(row.get("detection", {})) if isinstance(row.get("detection"), dict) else {},
        notes=str(row.get("notes", "")).strip(),
        raw={},
        spec_path=_inflate_graph_path(row.get("spec_path", "")),
        source_kind="domain_graph",
    )


def _regime_definition_from_payload(row: Dict[str, Any]) -> RegimeDefinition:
    return RegimeDefinition(
        canonical_regime=str(row.get("canonical_regime", "")).strip().upper(),
        bucket=str(row.get("bucket", "")).strip(),
        eligible_templates=tuple(
            str(item).strip() for item in row.get("eligible_templates", []) if str(item).strip()
        ),
        forbidden_templates=tuple(
            str(item).strip() for item in row.get("forbidden_templates", []) if str(item).strip()
        ),
        risk_posture=str(row.get("risk_posture", "")).strip(),
        execution_style=str(row.get("execution_style", "")).strip(),
        holding_horizon_profile=str(row.get("holding_horizon_profile", "")).strip(),
        stop_logic_profile=str(row.get("stop_logic_profile", "")).strip(),
        profit_taking_profile=str(row.get("profit_taking_profile", "")).strip(),
        overrides=dict(row.get("overrides", {})) if isinstance(row.get("overrides"), dict) else {},
        routing_profile_id=str(row.get("routing_profile_id", "")).strip(),
        scorecard_version=str(row.get("scorecard_version", "")).strip(),
        scorecard_source_run=str(row.get("scorecard_source_run", "")).strip(),
        raw={},
        spec_path=_inflate_graph_path(row.get("spec_path", "")),
        source_kind="domain_graph",
    )


def _context_state_map_from_payload(rows: Any) -> Dict[tuple[str, str], str]:
    out: Dict[tuple[str, str], str] = {}
    if not isinstance(rows, list):
        return out
    for row in rows:
        if not isinstance(row, dict):
            continue
        family = str(row.get("family", "")).strip()
        label = str(row.get("label", "")).strip()
        state_id = str(row.get("state_id", "")).strip().upper()
        if family and label and state_id:
            out[(family, label)] = state_id
    return out


def _load_domain_registry_from_graph() -> DomainRegistry | None:
    path = domain_graph_path()
    if not path.exists():
        return None
    payload = load_yaml_path(path)
    if not isinstance(payload, dict):
        return None
    if str(payload.get("kind", "")).strip() != "domain_graph":
        return None

    if "events" in payload:
        event_rows = payload.get("events", {})
        state_rows = payload.get("states", {})
        operator_rows = payload.get("templates", {})
        regime_rows = payload.get("regimes", {})
        thesis_rows = payload.get("theses", {})
        compatibility = payload.get("compatibility", {}) if isinstance(payload.get("compatibility"), dict) else {}
        runtime = payload.get("runtime", {}) if isinstance(payload.get("runtime"), dict) else {}
        event_runtime = runtime.get("event_registry", {}) if isinstance(runtime.get("event_registry"), dict) else {}
        template_runtime = runtime.get("template_registry", {}) if isinstance(runtime.get("template_registry"), dict) else {}
        family_registry_payload = {
            "event_families": dict(compatibility.get("event_families", {})) if isinstance(compatibility.get("event_families"), dict) else {},
            "state_families": dict(compatibility.get("state_families", {})) if isinstance(compatibility.get("state_families"), dict) else {},
        }
        state_aliases = tuple(
            str(item).strip().upper()
            for item in compatibility.get("state_aliases", [])
            if str(item).strip()
        )
        context_rows = runtime.get("context_state_map", [])
        searchable_event_families = tuple(
            str(item).strip().upper()
            for item in runtime.get("searchable_event_families", [])
            if str(item).strip()
        )
        searchable_state_families = tuple(
            str(item).strip().upper()
            for item in runtime.get("searchable_state_families", [])
            if str(item).strip()
        )
        stress_scenarios = tuple(
            dict(row) for row in runtime.get("stress_scenarios", []) if isinstance(row, dict)
        )
        kill_switch_candidate_features = tuple(
            str(item).strip()
            for item in runtime.get("kill_switch_candidate_features", [])
            if str(item).strip()
        )
        sequence_definitions = tuple(
            dict(row) for row in runtime.get("sequence_definitions", []) if isinstance(row, dict)
        )
        interaction_definitions = tuple(
            dict(row) for row in runtime.get("interaction_definitions", []) if isinstance(row, dict)
        )
        unified_payload = {
            "kind": str(event_runtime.get("kind", "event_runtime_defaults")).strip() or "event_runtime_defaults",
            "defaults": dict(event_runtime.get("defaults", {})) if isinstance(event_runtime.get("defaults"), dict) else {},
            "families": dict(event_runtime.get("families", {})) if isinstance(event_runtime.get("families"), dict) else {},
            "events": dict(event_runtime.get("events", {})) if isinstance(event_runtime.get("events"), dict) else {},
        }
        template_registry_payload = {
            "kind": str(template_runtime.get("kind", "template_runtime_defaults")).strip() or "template_runtime_defaults",
            "defaults": dict(template_runtime.get("defaults", {})) if isinstance(template_runtime.get("defaults"), dict) else {},
            "families": dict(template_runtime.get("families", {})) if isinstance(template_runtime.get("families"), dict) else {},
            "filter_templates": dict(template_runtime.get("filter_templates", {})) if isinstance(template_runtime.get("filter_templates"), dict) else {},
        }
        gates_spec = {}
        unified_registry_path = ""
    else:
        event_rows = payload.get("event_definitions", {})
        state_rows = payload.get("state_definitions", {})
        operator_rows = payload.get("template_operator_definitions", {})
        regime_rows = payload.get("regime_definitions", {})
        thesis_rows = payload.get("thesis_definitions", {})
        if not isinstance(event_rows, dict) or not isinstance(state_rows, dict) or not isinstance(operator_rows, dict) or not isinstance(regime_rows, dict) or not isinstance(thesis_rows, dict):
            return None
        unified_payload = dict(payload.get("unified_payload", {})) if isinstance(payload.get("unified_payload"), dict) else {}
        template_registry_payload = dict(payload.get("template_registry_payload", {})) if isinstance(payload.get("template_registry_payload"), dict) else {}
        family_registry_payload = dict(payload.get("family_registry_payload", {})) if isinstance(payload.get("family_registry_payload"), dict) else {}
        context_rows = payload.get("context_state_map", [])
        searchable_event_families = tuple(
            str(item).strip().upper() for item in payload.get("searchable_event_families", []) if str(item).strip()
        )
        searchable_state_families = tuple(
            str(item).strip().upper() for item in payload.get("searchable_state_families", []) if str(item).strip()
        )
        state_aliases = tuple(
            str(item).strip().upper() for item in payload.get("state_aliases", []) if str(item).strip()
        )
        stress_scenarios = tuple(
            dict(row) for row in payload.get("stress_scenarios", []) if isinstance(row, dict)
        )
        kill_switch_candidate_features = tuple(
            str(item).strip() for item in payload.get("kill_switch_candidate_features", []) if str(item).strip()
        )
        sequence_definitions = tuple(
            dict(row) for row in payload.get("sequence_definitions", []) if isinstance(row, dict)
        )
        interaction_definitions = tuple(
            dict(row) for row in payload.get("interaction_definitions", []) if isinstance(row, dict)
        )
        gates_spec = dict(payload.get("gates_spec", {})) if isinstance(payload.get("gates_spec"), dict) else {}
        unified_registry_path = str(payload.get("unified_registry_path", "")).strip()

    if not isinstance(event_rows, dict) or not isinstance(state_rows, dict) or not isinstance(operator_rows, dict) or not isinstance(regime_rows, dict) or not isinstance(thesis_rows, dict):
        return None

    return DomainRegistry(
        unified_payload=unified_payload,
        event_definitions={
            event_type: _event_definition_from_payload(dict(row))
            for event_type, row in sorted(event_rows.items())
            if isinstance(row, dict)
        },
        state_definitions={
            state_id: _state_definition_from_payload(dict(row))
            for state_id, row in sorted(state_rows.items())
            if isinstance(row, dict)
        },
        template_operator_definitions={
            template_id: _operator_definition_from_payload(dict(row))
            for template_id, row in sorted(operator_rows.items())
            if isinstance(row, dict)
        },
        regime_definitions={
            canonical_regime: _regime_definition_from_payload(dict(row))
            for canonical_regime, row in sorted(regime_rows.items())
            if isinstance(row, dict)
        },
        thesis_definitions={
            thesis_id: _thesis_definition_from_payload(dict(row))
            for thesis_id, row in sorted(thesis_rows.items())
            if isinstance(row, dict)
        },
        gates_spec=gates_spec,
        unified_registry_path=unified_registry_path,
        template_registry_payload=template_registry_payload,
        family_registry_payload=family_registry_payload,
        context_state_map=_context_state_map_from_payload(context_rows),
        searchable_event_families=searchable_event_families,
        searchable_state_families=searchable_state_families,
        state_aliases=state_aliases,
        stress_scenarios=stress_scenarios,
        kill_switch_candidate_features=kill_switch_candidate_features,
        sequence_definitions=sequence_definitions,
        interaction_definitions=interaction_definitions,
    )


def load_domain_registry_from_graph(*, required: bool = True) -> DomainRegistry | None:
    graph = _load_domain_registry_from_graph()
    if graph is not None:
        return graph
    if not required:
        return None
    path = domain_graph_path()
    raise FileNotFoundError(
        "Compiled domain graph is missing or invalid at "
        f"{path}. Rebuild it with `python3 project/scripts/build_domain_graph.py`."
    )


def compile_domain_registry(*, allow_source_fallback: bool = False) -> DomainRegistry:
    graph = load_domain_registry_from_graph(required=not allow_source_fallback)
    if graph is not None:
        return graph
    if allow_source_fallback:
        return _build_domain_registry_from_sources()
    raise AssertionError("unreachable: required graph load must raise before this point")


def domain_graph_digest() -> str:
    """Return a deterministic SHA-256 hex digest of the compiled domain graph file.

    Use this to detect whether the graph is stale relative to spec sources, or to
    record the exact compiled snapshot pinned by a run artifact.
    """
    import hashlib
    path = domain_graph_path()
    if not path.exists():
        return ""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def spec_sources_digest() -> str:
    """Return a deterministic SHA-256 hex digest over all authored spec YAML files.

    Comparing this against the digest stored in domain_graph.yaml metadata detects
    whether the graph needs a rebuild after spec edits.
    """
    import hashlib
    from project.spec_registry import iter_spec_yaml_files
    h = hashlib.sha256()
    for path in sorted(iter_spec_yaml_files()):
        rel = _repo_relative_path(path)
        if rel == _DOMAIN_GRAPH_RELATIVE_PATH:
            continue
        h.update(rel.encode())
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(65536), b""):
                h.update(chunk)
    return h.hexdigest()
