from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Sequence

from project.spec_registry import load_yaml_path, resolve_relative_spec_path
from project import PROJECT_ROOT
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


def _load_event_specs() -> Dict[str, EventRegistrySpec]:
    from project.spec_registry import load_unified_event_registry
    unified = load_unified_event_registry()
    if not unified:
        return {}

    events_payload = unified.get("events", {})
    if not isinstance(events_payload, dict):
        return {}

    defaults = unified.get("defaults", {})
    default_params = defaults.get("parameters", {}) if isinstance(defaults, dict) else {}
    families = unified.get("families", {})

    specs = {}
    for event_type, row in events_payload.items():
        if not isinstance(row, dict):
            continue
        if bool(row.get("deprecated", False)) or not bool(row.get("active", True)):
            continue

        family_name = str(row.get("research_family", row.get("canonical_family", ""))).strip().upper()
        family_params = {}
        if family_name and isinstance(families, dict):
            family_info = families.get(family_name)
            if isinstance(family_info, dict):
                family_params = family_info.get("parameters", {})

        parameters = {}
        if isinstance(default_params, dict):
            parameters.update(default_params)
        if isinstance(family_params, dict):
            parameters.update(family_params)
        if isinstance(row.get("parameters"), dict):
            parameters.update(row["parameters"])

        def _canon_param(name: str, default: int | str | Sequence[str] | bool):
            if name in row:
                return row.get(name, default)
            if name in parameters:
                return parameters.get(name, default)
            if isinstance(defaults, dict) and name in defaults:
                return defaults.get(name, default)
            return default

        def _coalesce_text(value: object, default: str) -> str:
            text = str(value or "").strip()
            return text or default

        reports_dir = _coalesce_text(row.get("reports_dir"), event_type.lower())
        events_file = _coalesce_text(
            row.get("events_file"),
            f"{event_type.lower()}_events.parquet",
        )
        default_signal_column = (
            event_type.lower()
            if str(event_type).strip().lower().endswith("_event")
            else f"{event_type.lower()}_event"
        )
        signal_column = _coalesce_text(row.get("signal_column"), default_signal_column)

        spec = EventRegistrySpec(
            event_type=event_type,
            reports_dir=reports_dir,
            events_file=events_file,
            signal_column=signal_column,
            research_family=str(
                row.get("research_family", row.get("canonical_family", ""))
            ).strip().upper(),
            canonical_regime=str(
                row.get("canonical_regime", row.get("canonical_family", ""))
            ).strip().upper(),
            subtype=str(row.get("subtype", "")).strip(),
            phase=str(row.get("phase", "")).strip(),
            evidence_mode=str(row.get("evidence_mode", "")).strip(),
            asset_scope=str(row.get("asset_scope", "")).strip(),
            venue_scope=str(row.get("venue_scope", "")).strip(),
            is_composite=as_bool(row.get("is_composite", False)),
            is_context_tag=as_bool(row.get("is_context_tag", False)),
            is_strategy_construct=as_bool(row.get("is_strategy_construct", False)),
            research_only=as_bool(row.get("research_only", False)),
            strategy_only=as_bool(row.get("strategy_only", False)),
            deconflict_priority=int(row.get("deconflict_priority", 0) or 0),
            disposition=str(row.get("disposition", "")).strip(),
            layer=str(row.get("layer", "")).strip(),
            notes=str(row.get("notes", "")).strip(),
            merge_gap_bars=int(_canon_param("merge_gap_bars", 1)),
            cooldown_bars=int(_canon_param("cooldown_bars", 0)),
            anchor_rule=str(_canon_param("anchor_rule", "max_intensity")),
            min_occurrences=int(_canon_param("min_occurrences", 0)),
            is_descriptive=as_bool(_canon_param("is_descriptive", False)),
            is_trade_trigger=as_bool(_canon_param("is_trade_trigger", True)),
            requires_confirmation=as_bool(_canon_param("requires_confirmation", False)),
            allowed_templates=list(_canon_param("allowed_templates", ["all"])),
            disallowed_states=list(_canon_param("disallowed_states", [])),
            synthetic_coverage=str(_canon_param("synthetic_coverage", "uncovered")),
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


EVENT_REGISTRY_SPECS: Dict[str, EventRegistrySpec] = _load_event_specs()

SIGNAL_TO_EVENT_TYPE: Dict[str, str] = {
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

AGGREGATE_EVENT_TYPE_UNIONS: Dict[str, Sequence[str]] = {}


def expected_event_types_for_spec(event_type: str) -> Sequence[str]:
    normalized = str(event_type).strip().upper()
    if not normalized:
        return ()
    return AGGREGATE_EVENT_TYPE_UNIONS.get(normalized, (normalized,))


VALID_DIRECTIONS = frozenset({"long", "short", "neutral", "non_directional"})
_DIRECTION_DEFAULT = "non_directional"
