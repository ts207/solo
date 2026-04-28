from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from project.core.coercion import as_bool


@dataclass(frozen=True)
class EventRegistrySpec:
    event_type: str
    reports_dir: str
    events_file: str
    signal_column: str
    research_family: str = ""
    canonical_regime: str = ""
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
    merge_gap_bars: int = 1
    cooldown_bars: int = 0
    anchor_rule: str = "max_intensity"
    min_occurrences: int = 0
    is_descriptive: bool = False
    is_trade_trigger: bool = True
    requires_confirmation: bool = False
    allowed_templates: Sequence[str] = ("all",)
    disallowed_states: Sequence[str] = ()
    synthetic_coverage: str = "uncovered"

    @property
    def canonical_family(self) -> str:
        return self.research_family


def _load_event_specs() -> dict[str, EventRegistrySpec]:
    from project.domain.compiled_registry import get_domain_registry

    registry = get_domain_registry()
    specs: dict[str, EventRegistrySpec] = {}
    for event_type in registry.event_ids:
        event_def = registry.get_event(event_type)
        if event_def is None or not event_def.enabled:
            continue

        params = dict(event_def.parameters) if isinstance(event_def.parameters, dict) else {}
        raw = dict(event_def.raw)
        runtime_params = raw.get("parameters", {})
        if isinstance(runtime_params, dict):
            params.update(runtime_params)

        def _param(name: str, default, *, params=params, raw=raw):
            if name in params:
                return params[name]
            if name in raw:
                return raw[name]
            return default

        is_context_or_proxy = bool(
            event_def.is_context_tag
            or event_def.operational_role.lower().strip() in ("context", "filter", "research_only", "sequence_component", "composite")
            or event_def.evidence_mode.lower().strip() in ("proxy", "indirect", "derived", "inferred", "inferred_cross_asset")
        )
        is_trade_trigger = bool(
            not is_context_or_proxy
            and event_def.operational_role.lower().strip() in ("trigger", "confirm", "")
            and event_def.promotion_eligible
        )

        spec = EventRegistrySpec(
            event_type=event_def.event_type,
            reports_dir=event_def.reports_dir,
            events_file=event_def.events_file,
            signal_column=event_def.signal_column,
            research_family=event_def.research_family,
            canonical_regime=event_def.canonical_regime,
            subtype=event_def.subtype,
            phase=event_def.phase,
            evidence_mode=event_def.evidence_mode,
            asset_scope=event_def.asset_scope,
            venue_scope=event_def.venue_scope,
            is_composite=event_def.is_composite,
            is_context_tag=event_def.is_context_tag,
            is_strategy_construct=event_def.is_strategy_construct,
            research_only=event_def.research_only,
            strategy_only=event_def.strategy_only,
            deconflict_priority=event_def.deconflict_priority,
            disposition=event_def.disposition,
            layer=event_def.layer,
            notes=event_def.notes,
            merge_gap_bars=int(_param("merge_gap_bars", 1)),
            cooldown_bars=int(_param("cooldown_bars", 0)),
            anchor_rule=str(_param("anchor_rule", "max_intensity")),
            min_occurrences=int(_param("min_occurrences", 0)),
            is_descriptive=as_bool(_param("is_descriptive", is_context_or_proxy)),
            is_trade_trigger=as_bool(_param("is_trade_trigger", is_trade_trigger)),
            requires_confirmation=as_bool(_param("requires_confirmation", False)),
            allowed_templates=list(_param("allowed_templates", _param("templates", ["all"]))),
            disallowed_states=list(_param("disallowed_states", [])),
            synthetic_coverage=str(_param("synthetic_coverage", "uncovered")),
        )
        specs[spec.event_type] = spec
    return specs


def assert_event_specs_available() -> None:
    specs = _load_event_specs()
    if not specs:
        raise FileNotFoundError(
            "No active event registry specifications found under spec/events; "
            "ensure the analyzer specs are present before running phase1/registry."
        )


EVENT_REGISTRY_SPECS: dict[str, EventRegistrySpec] = _load_event_specs()

SIGNAL_TO_EVENT_TYPE: dict[str, str] = {
    spec.signal_column: event_type for event_type, spec in EVENT_REGISTRY_SPECS.items()
}
REGISTRY_BACKED_SIGNALS = set(SIGNAL_TO_EVENT_TYPE.keys())

REGISTRY_EVENT_COLUMNS = [
    "run_id",
    "event_type",
    "signal_column",
    "timestamp",
    "event_ts_raw",
    "event_ts_snapped",
    "signal_bar_open_time",
    "first_tradable_bar_open_time",
    "active_start_time",
    "active_end_time",
    "effective_entry_bar_open_time",
    "phenom_enter_ts",
    "eval_bar_ts",
    "detected_ts",
    "signal_ts",
    "enter_ts",
    "exit_ts",
    "event_idx",
    "year",
    "event_score",
    "evt_signal_intensity",
    "composite_source",
    "symbol",
    "event_id",
    "direction",
    "sign",
    "severity_bucket",
    "vol_regime",
    "carry_state",
    "ms_trend_state",
    "ms_spread_state",
    "split_label",
    "features_at_event",
    "is_observational",
    "is_signal_eligible",
    "is_tradable_now",
    "is_tradable_next_bar",
]

AGGREGATE_EVENT_TYPE_UNIONS: dict[str, Sequence[str]] = {}


def expected_event_types_for_spec(event_type: str) -> Sequence[str]:
    normalized = str(event_type).strip().upper()
    if not normalized:
        return ()
    return AGGREGATE_EVENT_TYPE_UNIONS.get(normalized, (normalized,))


VALID_DIRECTIONS = frozenset({"long", "short", "neutral", "non_directional"})
_DIRECTION_DEFAULT = "non_directional"
