from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class EventDefinition:
    event_type: str
    canonical_family: str
    canonical_regime: str
    event_kind: str
    reports_dir: str
    events_file: str
    signal_column: str
    research_family: str = ""
    subtype: str = ""
    phase: str = ""
    evidence_mode: str = ""
    asset_scope: str = ""
    venue_scope: str = ""
    is_composite: bool = False
    is_context_tag: bool = False
    is_strategy_construct: bool = False
    research_only: bool = False
    strategy_only: bool = False
    deconflict_priority: int = 0
    disposition: str = ""
    layer: str = ""
    notes: str = ""
    tier: str = ""
    operational_role: str = ""
    deployment_disposition: str = ""
    runtime_category: str = "active_runtime_event"
    maturity: str = ""
    default_executable: bool = True
    detector_band: str = ""
    planning_eligible: bool = False
    runtime_eligible: bool = False
    promotion_eligible: bool = False
    primary_anchor_eligible: bool = False
    enabled: bool = True
    detector_name: str = ""
    instrument_classes: tuple[str, ...] = ()
    requires_features: tuple[str, ...] = ()
    runtime_tags: tuple[str, ...] = ()
    sequence_eligible: bool = True
    cluster_id: str = ""
    collapse_target: str = ""
    overlap_group: str = ""
    precedence_rank: int = 0
    routing_profile_ref: str = ""
    suppresses: tuple[Any, ...] = ()
    suppressed_by: tuple[Any, ...] = ()
    maturity_scores: dict[str, Any] = field(default_factory=dict)
    parameters: dict[str, Any] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)
    spec_path: str = ""
    source_kind: str = "unified_registry"


@dataclass(frozen=True)
class StateDefinition:
    state_id: str
    family: str
    source_event_type: str
    raw: dict[str, Any] = field(default_factory=dict)
    state_scope: str = "source_only"
    min_events: int = 200
    activation_rule: str = ""
    decay_rule: str = ""
    max_duration: Any = None
    features_required: tuple[str, ...] = ()
    allowed_templates: tuple[str, ...] = ()
    enabled: bool = True
    state_engine: str = ""
    instrument_classes: tuple[str, ...] = ()
    runtime_tags: tuple[str, ...] = ()
    description: str = ""
    context_family: str = ""
    context_label: str = ""
    spec_path: str = ""
    source_kind: str = "state_registry"


@dataclass(frozen=True)
class TemplateOperatorDefinition:
    template_id: str
    compatible_families: tuple[str, ...]
    template_kind: str = ""
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RegimeDefinition:
    canonical_regime: str
    bucket: str
    eligible_templates: tuple[str, ...] = ()
    forbidden_templates: tuple[str, ...] = ()
    risk_posture: str = ""
    execution_style: str = ""
    holding_horizon_profile: str = ""
    stop_logic_profile: str = ""
    profit_taking_profile: str = ""
    overrides: dict[str, Any] = field(default_factory=dict)
    routing_profile_id: str = ""
    scorecard_version: str = ""
    scorecard_source_run: str = ""
    raw: dict[str, Any] = field(default_factory=dict)
    spec_path: str = ""
    source_kind: str = "regime_registry"


@dataclass(frozen=True)
class ThesisDefinition:
    thesis_id: str
    thesis_kind: str
    event_family: str
    timeframe: str
    primary_event_id: str = ""
    canonical_regime: str = ""
    event_side: str = "unknown"
    promotion_class: str = ""
    deployment_state: str = ""
    trigger_events: tuple[str, ...] = ()
    confirmation_events: tuple[str, ...] = ()
    required_episodes: tuple[str, ...] = ()
    disallowed_regimes: tuple[str, ...] = ()
    required_states: tuple[str, ...] = ()
    supportive_states: tuple[str, ...] = ()
    source_event_contract_ids: tuple[str, ...] = ()
    source_episode_contract_ids: tuple[str, ...] = ()
    required_context: dict[str, Any] = field(default_factory=dict)
    supportive_context: dict[str, Any] = field(default_factory=dict)
    expected_response: dict[str, Any] = field(default_factory=dict)
    invalidation: dict[str, Any] = field(default_factory=dict)
    freshness_policy: dict[str, Any] = field(default_factory=dict)
    governance: dict[str, Any] = field(default_factory=dict)
    symbol_scope: dict[str, Any] = field(default_factory=dict)
    detection: dict[str, Any] = field(default_factory=dict)
    notes: str = ""
    raw: dict[str, Any] = field(default_factory=dict)
    spec_path: str = ""
    source_kind: str = "thesis_registry"


@dataclass(frozen=True)
class DomainRegistry:
    unified_payload: dict[str, Any] = field(default_factory=dict)
    event_definitions: dict[str, EventDefinition] = field(default_factory=dict)
    state_definitions: dict[str, StateDefinition] = field(default_factory=dict)
    template_operator_definitions: dict[str, TemplateOperatorDefinition] = field(default_factory=dict)
    regime_definitions: dict[str, RegimeDefinition] = field(default_factory=dict)
    gates_spec: dict[str, Any] = field(default_factory=dict)
    unified_registry_path: str = ""
    template_registry_payload: dict[str, Any] = field(default_factory=dict)
    family_registry_payload: dict[str, Any] = field(default_factory=dict)
    thesis_definitions: dict[str, ThesisDefinition] = field(default_factory=dict)
    context_state_map: dict[tuple[str, str], str] = field(default_factory=dict)
    searchable_event_families: tuple[str, ...] = ()
    searchable_state_families: tuple[str, ...] = ()
    state_aliases: tuple[str, ...] = ()
    stress_scenarios: tuple[dict[str, Any], ...] = ()
    kill_switch_candidate_features: tuple[str, ...] = ()
    sequence_definitions: tuple[dict[str, Any], ...] = ()
    interaction_definitions: tuple[dict[str, Any], ...] = ()

    def has_event(self, event_type: str) -> bool:
        normalized = str(event_type).strip().upper()
        return normalized in self.event_definitions

    def get_event(self, event_type: str) -> EventDefinition | None:
        normalized = str(event_type).strip().upper()
        return self.event_definitions.get(normalized)

    def has_state(self, state_id: str) -> bool:
        return str(state_id).strip().upper() in self.state_definitions

    def get_state(self, state_id: str) -> StateDefinition | None:
        return self.state_definitions.get(str(state_id).strip().upper())

    def get_operator(self, template_id: str) -> TemplateOperatorDefinition | None:
        return self.template_operator_definitions.get(str(template_id).strip())

    def template_kind(self, template_id: str) -> str:
        operator = self.get_operator(template_id)
        if operator is None:
            return ""
        token = str(operator.template_kind).strip().lower()
        if token:
            return token
        raw = operator.raw if isinstance(operator.raw, dict) else {}
        return str(raw.get("template_kind", "")).strip().lower()

    def is_filter_template(self, template_id: str) -> bool:
        return self.template_kind(template_id) == "filter_template"

    def is_expression_template(self, template_id: str) -> bool:
        return self.template_kind(template_id) in {"", "expression_template"}

    def is_execution_template(self, template_id: str) -> bool:
        return self.template_kind(template_id) == "execution_template"

    def is_hypothesis_template(self, template_id: str) -> bool:
        return self.is_expression_template(template_id)

    def has_regime(self, canonical_regime: str) -> bool:
        return str(canonical_regime).strip().upper() in self.regime_definitions

    def get_regime(self, canonical_regime: str) -> RegimeDefinition | None:
        return self.regime_definitions.get(str(canonical_regime).strip().upper())

    def has_thesis(self, thesis_id: str) -> bool:
        return str(thesis_id).strip().upper() in self.thesis_definitions

    def get_thesis(self, thesis_id: str) -> ThesisDefinition | None:
        return self.thesis_definitions.get(str(thesis_id).strip().upper())

    def operator_rows(self) -> dict[str, dict[str, Any]]:
        rows: dict[str, dict[str, Any]] = {}
        for name, spec in self.template_operator_definitions.items():
            row = dict(spec.raw)
            row.setdefault("template_id", spec.template_id)
            row.setdefault("compatible_families", list(spec.compatible_families))
            row.setdefault("template_kind", spec.template_kind)
            rows[name] = row
        return rows

    def thesis_rows(self) -> dict[str, dict[str, Any]]:
        rows: dict[str, dict[str, Any]] = {}
        for name, spec in self.thesis_definitions.items():
            row = dict(spec.raw)
            row.setdefault("thesis_id", spec.thesis_id)
            row.setdefault("thesis_kind", spec.thesis_kind)
            row.setdefault("event_family", spec.event_family)
            row.setdefault("primary_event_id", spec.primary_event_id)
            row.setdefault("canonical_regime", spec.canonical_regime)
            row.setdefault("timeframe", spec.timeframe)
            row.setdefault("event_side", spec.event_side)
            row.setdefault("promotion_class", spec.promotion_class)
            row.setdefault("deployment_state", spec.deployment_state)
            row.setdefault("trigger_events", list(spec.trigger_events))
            row.setdefault("confirmation_events", list(spec.confirmation_events))
            row.setdefault("required_episodes", list(spec.required_episodes))
            row.setdefault("disallowed_regimes", list(spec.disallowed_regimes))
            row.setdefault("source_event_contract_ids", list(spec.source_event_contract_ids))
            row.setdefault("source_episode_contract_ids", list(spec.source_episode_contract_ids))
            row.setdefault("required_context", dict(spec.required_context))
            row.setdefault("supportive_context", dict(spec.supportive_context))
            row.setdefault("expected_response", dict(spec.expected_response))
            row.setdefault("invalidation", dict(spec.invalidation))
            row.setdefault("freshness_policy", dict(spec.freshness_policy))
            row.setdefault("governance", dict(spec.governance))
            row.setdefault("symbol_scope", dict(spec.symbol_scope))
            row.setdefault("detection", dict(spec.detection))
            row.setdefault("notes", spec.notes)
            rows[name] = row
        return rows

    def family_templates(self, family_name: str) -> tuple[str, ...]:
        template_families = self.template_registry_payload.get("families", {})
        if isinstance(template_families, Mapping):
            family_row = template_families.get(str(family_name).strip().upper(), {})
            if isinstance(family_row, Mapping):
                templates = family_row.get("templates", family_row.get("allowed_templates", []))
                if isinstance(templates, (list, tuple)):
                    seen: set[str] = set()
                    out: list[str] = []
                    for item in templates:
                        token = str(item).strip()
                        if token and token not in seen:
                            out.append(token)
                            seen.add(token)
                    if out:
                        return tuple(out)
        row = self.family_defaults(family_name)
        templates = row.get("templates", [])
        if not isinstance(templates, (list, tuple)):
            return ()
        seen: set[str] = set()
        out: list[str] = []
        for item in templates:
            token = str(item).strip()
            if token and token not in seen:
                out.append(token)
                seen.add(token)
        return tuple(out)

    def family_defaults(self, family: str) -> dict[str, Any]:
        families = self.unified_payload.get("families", {})
        if not isinstance(families, Mapping):
            return {}
        row = families.get(str(family).strip().upper(), {})
        return dict(row) if isinstance(row, Mapping) else {}

    def defaults(self) -> dict[str, Any]:
        payload = self.unified_payload.get("defaults", {})
        return dict(payload) if isinstance(payload, Mapping) else {}

    def template_registry(self) -> dict[str, Any]:
        return dict(self.template_registry_payload)

    def template_defaults(self) -> dict[str, Any]:
        payload = self.template_registry_payload.get("defaults", {})
        return dict(payload) if isinstance(payload, Mapping) else {}

    def family_registry(self) -> dict[str, Any]:
        return dict(self.family_registry_payload)

    def event_family_rows(self) -> dict[str, Any]:
        payload = self.family_registry_payload.get("event_families", {})
        return dict(payload) if isinstance(payload, Mapping) else {}

    def state_family_rows(self) -> dict[str, Any]:
        payload = self.family_registry_payload.get("state_families", {})
        return dict(payload) if isinstance(payload, Mapping) else {}

    def event_row(self, event_type: str) -> dict[str, Any]:
        event = self.get_event(event_type)
        if event is None:
            return {}
        row = dict(event.raw)
        row.setdefault("event_type", event.event_type)
        row.setdefault("research_family", event.research_family)
        row.setdefault("canonical_family", event.canonical_family)
        row.setdefault("canonical_regime", event.canonical_regime)
        row.setdefault("event_kind", event.event_kind)
        row.setdefault("reports_dir", event.reports_dir)
        row.setdefault("events_file", event.events_file)
        row.setdefault("signal_column", event.signal_column)
        row.setdefault("subtype", event.subtype)
        row.setdefault("phase", event.phase)
        row.setdefault("evidence_mode", event.evidence_mode)
        row.setdefault("asset_scope", event.asset_scope)
        row.setdefault("venue_scope", event.venue_scope)
        row.setdefault("layer", event.layer)
        row.setdefault("enabled", event.enabled)
        row.setdefault("default_executable", event.default_executable)
        row.setdefault("research_only", event.research_only)
        row.setdefault("strategy_only", event.strategy_only)
        row["planning_eligible"] = event.planning_eligible
        row["runtime_eligible"] = event.runtime_eligible
        row["promotion_eligible"] = event.promotion_eligible
        row["primary_anchor_eligible"] = event.primary_anchor_eligible
        row["detector_band"] = event.detector_band
        row.setdefault("notes", event.notes)
        row.setdefault("routing_profile_ref", event.routing_profile_ref)
        row.setdefault("parameters", dict(event.parameters))
        return row

    def event_spec_path(self, event_type: str) -> str:
        event = self.get_event(event_type)
        return str(event.spec_path) if event is not None else ""

    def get_event_ids_for_family(self, family_name: str) -> tuple[str, ...]:
        family = str(family_name).strip().upper()
        return tuple(
            sorted(
                event_type
                for event_type, spec in self.event_definitions.items()
                if spec.research_family == family
                or spec.canonical_family == family
                or spec.canonical_regime == family
            )
        )

    def get_event_ids_for_regime(
        self,
        regime_name: str,
        *,
        executable_only: bool = False,
    ) -> tuple[str, ...]:
        regime = str(regime_name).strip().upper()
        return tuple(
            sorted(
                event_type
                for event_type, spec in self.event_definitions.items()
                if spec.canonical_regime == regime
                and (
                    not executable_only
                    or (
                        not spec.is_composite
                        and not spec.is_context_tag
                        and not spec.is_strategy_construct
                    )
                )
            )
        )

    def canonical_regime_rows(self) -> dict[str, tuple[str, ...]]:
        regimes = {
            spec.canonical_regime
            for spec in self.event_definitions.values()
            if spec.canonical_regime
        }
        return {
            regime: self.get_event_ids_for_regime(regime)
            for regime in sorted(regimes)
        }

    def default_executable_event_ids(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                event_type
                for event_type, spec in self.event_definitions.items()
                if spec.default_executable
                and spec.runtime_category == "active_runtime_event"
                and not spec.is_composite
                and not spec.is_context_tag
                and not spec.is_strategy_construct
            )
        )

    def planning_eligible_event_ids(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                event_type
                for event_type, spec in self.event_definitions.items()
                if spec.planning_eligible
                and spec.runtime_category == "active_runtime_event"
                and not spec.is_composite
                and not spec.is_context_tag
                and not spec.is_strategy_construct
            )
        )

    def runtime_eligible_event_ids(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                event_type
                for event_type, spec in self.event_definitions.items()
                if spec.runtime_eligible
                and spec.detector_band == "deployable_core"
                and spec.runtime_category == "active_runtime_event"
                and not spec.is_composite
                and not spec.is_context_tag
                and not spec.is_strategy_construct
            )
        )

    def promotion_eligible_event_ids(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                event_type
                for event_type, spec in self.event_definitions.items()
                if spec.promotion_eligible
                and spec.detector_band in {"deployable_core", "research_trigger"}
                and not spec.is_composite
                and not spec.is_context_tag
                and not spec.is_strategy_construct
            )
        )

    def get_event_ids_for_tier(self, tier: str) -> tuple[str, ...]:
        normalized = str(tier).strip().upper()
        return tuple(sorted(event_type for event_type, spec in self.event_definitions.items() if str(spec.tier).upper() == normalized))

    def get_event_ids_for_role(self, role: str) -> tuple[str, ...]:
        normalized = str(role).strip().lower()
        return tuple(sorted(event_type for event_type, spec in self.event_definitions.items() if str(spec.operational_role).strip().lower() == normalized))

    def get_state_ids_for_family(self, family_name: str) -> tuple[str, ...]:
        family = str(family_name).strip().upper()
        return tuple(
            sorted(
                state_id
                for state_id, spec in self.state_definitions.items()
                if spec.family == family
            )
        )

    def resolve_context_state(self, family: str, label: str) -> str | None:
        return self.context_state_map.get((str(family).strip(), str(label).strip()))

    def context_labels_for_family(self, family: str) -> tuple[str, ...]:
        normalized = str(family).strip()
        return tuple(
            sorted(label for fam, label in self.context_state_map.keys() if fam == normalized)
        )

    @property
    def state_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self.state_definitions.keys()))

    @property
    def event_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self.event_definitions.keys()))

    @property
    def thesis_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self.thesis_definitions.keys()))

    @property
    def valid_state_ids(self) -> tuple[str, ...]:
        return tuple(sorted({*self.state_definitions.keys(), *self.state_aliases}))

    def default_templates(self) -> tuple[str, ...]:
        defaults = self.template_defaults()
        templates = defaults.get("templates", [])
        if not isinstance(templates, (list, tuple)):
            return ()
        out: list[str] = []
        seen: set[str] = set()
        for item in templates:
            token = str(item).strip()
            if token and token not in seen:
                out.append(token)
                seen.add(token)
        return tuple(out)

    def default_hypothesis_templates(self) -> tuple[str, ...]:
        return tuple(
            template_id
            for template_id in self.default_templates()
            if self.is_hypothesis_template(template_id)
        )

    def default_expression_templates(self) -> tuple[str, ...]:
        return self.default_hypothesis_templates()

    def family_filter_templates(self, family_name: str) -> tuple[dict[str, Any], ...]:
        allowed = set(self.family_templates(family_name))
        registry_filters = self.template_registry_payload.get("filter_templates", {})
        if not isinstance(registry_filters, Mapping) or not registry_filters:
            try:
                from project.spec_registry import load_template_registry

                canonical_registry = load_template_registry()
                fallback_filters = canonical_registry.get("filter_templates", {}) if isinstance(canonical_registry, Mapping) else {}
                registry_filters = fallback_filters if isinstance(fallback_filters, Mapping) else {}
            except Exception:
                registry_filters = {}
        out: list[dict[str, Any]] = []
        for name in sorted(allowed):
            operator = self.get_operator(name)
            if operator is None or operator.template_kind != "filter_template":
                continue
            cond = operator.raw if isinstance(operator.raw, dict) else {}
            if not isinstance(cond, Mapping) or not {"feature", "operator", "threshold"}.issubset(cond.keys()):
                sidecar_cond = registry_filters.get(str(name), {}) if isinstance(registry_filters, Mapping) else {}
                cond = sidecar_cond if isinstance(sidecar_cond, Mapping) else cond
            if isinstance(cond, Mapping) and {"feature", "operator", "threshold"}.issubset(cond.keys()):
                out.append(
                    {
                        "name": str(name),
                        "feature": cond["feature"],
                        "operator": cond["operator"],
                        "threshold": float(cond["threshold"]),
                    }
                )
        return tuple(out)

    def family_expression_templates(self, family_name: str) -> tuple[str, ...]:
        return self.family_hypothesis_templates(family_name)

    def family_execution_templates(self, family_name: str) -> tuple[str, ...]:
        allowed = self.family_templates(family_name)
        if not allowed:
            return ()
        return tuple(name for name in allowed if self.is_execution_template(name))

    def family_hypothesis_templates(self, family_name: str) -> tuple[str, ...]:
        allowed = self.family_templates(family_name)
        if not allowed:
            allowed = self.default_hypothesis_templates()
        if not allowed:
            return ()
        return tuple(name for name in allowed if self.is_hypothesis_template(name))

    def default_entry_lags(self) -> tuple[int, ...]:
        defaults = self.template_defaults()
        grids = defaults.get("template_param_grid_defaults", {})
        if not isinstance(grids, Mapping):
            return (1, 2)
        common = grids.get("common", {})
        if not isinstance(common, Mapping):
            return (1, 2)
        values = common.get("entry_lag_bars", [1, 2])
        if not isinstance(values, (list, tuple)):
            return (1, 2)
        out: list[int] = []
        for value in values:
            try:
                out.append(int(value))
            except (TypeError, ValueError):
                continue
        return tuple(out or [1, 2])

    def stress_scenario_rows(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.stress_scenarios]

    def kill_switch_candidates(self) -> list[str]:
        return list(self.kill_switch_candidate_features)

    def sequence_rows(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.sequence_definitions]

    def interaction_rows(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.interaction_definitions]
