from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from functools import cache, lru_cache
from pathlib import Path
from typing import Any

from project import PROJECT_ROOT
from project.domain.compiled_registry import get_domain_registry
from project.spec_registry import (
    load_yaml_path,
)

_LOG = logging.getLogger(__name__)

REPO_ROOT = PROJECT_ROOT.parent
SPEC_DIR = REPO_ROOT / "spec" / "ontology" / "events"
RUNTIME_SPEC_DIR = REPO_ROOT / "spec" / "events"
UNIFIED_REGISTRY_PATH = REPO_ROOT / "spec" / "events" / "event_registry_unified.yaml"

_CORE_KEYS = {"event_type", "reports_dir", "events_file", "signal_column", "parameters"}
_META_KEYS = {
    "active",
    "status",
    "description",
    "provenance",
    "deprecated",
    "kind",
    "version",
}
_UNIFIED_NON_EVENT_PARAMETER_KEYS = {
    "canonical_family",
    "canonical_regime",
    "subtype",
    "phase",
    "evidence_mode",
    "asset_scope",
    "venue_scope",
    "is_composite",
    "is_context_tag",
    "is_strategy_construct",
    "research_only",
    "strategy_only",
    "deconflict_priority",
    "disposition",
    "layer",
    "notes",
    "templates",
    "horizons",
    "conditioning_cols",
    "max_candidates_per_run",
    "state_overrides",
}


@dataclass(frozen=True)
class ComposedConfig:
    """Unified configuration for event detection and template discovery."""

    event_type: str
    family: str
    canonical_regime: str
    reports_dir: str
    events_file: str
    signal_column: str
    parameters: dict[str, Any]
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

    # Template related fields
    templates: tuple[str, ...] = ()
    horizons: tuple[str, ...] = ()
    conditioning_cols: tuple[str, ...] = ()
    max_candidates_per_run: int = 1000
    state_id: str | None = None

    # Metadata
    config_hash: str = ""
    normalized_json: str = ""
    source_layers: dict[str, str] = field(default_factory=dict)

    @property
    def canonical_family(self) -> str:
        return self.family


# Re-alias for backward compatibility if needed, though we should migrate
ComposedEventConfig = ComposedConfig
ComposedTemplateConfig = ComposedConfig


def _event_spec_candidates(normalized: str) -> tuple[Path, Path]:
    return SPEC_DIR / f"{normalized}.yaml", RUNTIME_SPEC_DIR / f"{normalized}.yaml"


def _to_str_tuple(value: Any) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    out: list[str] = []
    seen: set[str] = set()
    for item in value:
        token = str(item).strip()
        if token and token not in seen:
            out.append(token)
            seen.add(token)
    return tuple(out)


def _coalesce_text(value: Any, default: str) -> str:
    text = str(value or "").strip()
    return text or default


@lru_cache(maxsize=1)
def _unified_registry() -> dict[str, Any]:
    payload = get_domain_registry().unified_payload
    if not payload:
        return {}
    kind = str(payload.get("kind", "")).strip().lower()
    if kind not in {"event_unified_registry", "event_runtime_defaults"}:
        raise ValueError(f"Malformed unified registry kind at {UNIFIED_REGISTRY_PATH}")
    return payload


_registry = _unified_registry


@lru_cache(maxsize=1)
def _family_by_event() -> dict[str, str]:
    registry = get_domain_registry()
    return {
        event_type: spec.research_family or spec.canonical_family or spec.canonical_regime
        for event_type, spec in registry.event_definitions.items()
        if spec.research_family or spec.canonical_family or spec.canonical_regime
    }


@lru_cache(maxsize=1)
def _operator_registry() -> dict[str, dict[str, Any]]:
    registry = get_domain_registry()
    return registry.operator_rows()


@cache
def _detector_default_parameters(event_type: str) -> dict[str, Any]:
    from project.events.detectors.registry import get_detector, load_all_detectors

    normalized = str(event_type).strip().upper()
    if not normalized:
        return {}
    load_all_detectors()
    detector = get_detector(normalized)
    defaults = getattr(detector, "defaults", {}) if detector is not None else {}
    return dict(defaults) if isinstance(defaults, dict) else {}


def compose_config(
    event_type: str,
    *,
    state_id: str | None = None,
    runtime_overrides: Mapping[str, Any] | None = None,
    **kwargs,
) -> ComposedConfig:
    """Universal configuration resolver."""
    normalized = str(event_type).strip().upper()
    if not normalized:
        raise ValueError("event_type must be non-empty")

    domain_registry = get_domain_registry()
    unified = _registry()
    events = unified.get("events", {})
    selected_spec_path = Path(
        domain_registry.event_spec_path(normalized) or (RUNTIME_SPEC_DIR / f"{normalized}.yaml")
    )
    event_def = domain_registry.get_event(normalized)
    if isinstance(events, dict) and normalized in events:
        row = dict(event_def.raw) if event_def is not None else {}
        if event_def is not None and isinstance(event_def.parameters, dict) and event_def.parameters:
            merged_parameters = {}
            if isinstance(row.get("parameters"), dict):
                merged_parameters.update(row["parameters"])
            merged_parameters.update(event_def.parameters)
            row["parameters"] = merged_parameters
        row.update(dict(events[normalized]))
    elif normalized.startswith("TEST_") and not domain_registry.has_event(normalized):
        row = {
            "reports_dir": "test_reports",
            "events_file": "test_events.parquet",
            "signal_column": f"{normalized.lower()}_event",
            "parameters": {},
        }
    else:
        if event_def is None:
            ontology_path, runtime_path = _event_spec_candidates(normalized)
            raise KeyError(
                f"event_type {normalized} missing in compiled domain registry and spec paths "
                f"{ontology_path} / {runtime_path}"
            )
        row = dict(event_def.raw)
        if isinstance(event_def.parameters, dict) and event_def.parameters:
            merged_parameters = {}
            if isinstance(row.get("parameters"), dict):
                merged_parameters.update(row["parameters"])
            merged_parameters.update(event_def.parameters)
            row["parameters"] = merged_parameters
    source_spec = load_yaml_path(selected_spec_path) if selected_spec_path.exists() else {}
    if isinstance(source_spec, dict) and source_spec:
        source_parameters = source_spec.get("parameters", {})
        if isinstance(source_parameters, dict) and source_parameters:
            merged_parameters = {}
            if isinstance(row.get("parameters"), dict):
                merged_parameters.update(row["parameters"])
            merged_parameters.update(source_parameters)
            row["parameters"] = merged_parameters

    defaults = unified.get("defaults", {})
    family_name = (
        str(
            row.get(
                "research_family",
                row.get("canonical_family", _family_by_event().get(normalized, "UNSPECIFIED")),
            )
        )
        .strip()
        .upper()
    )
    regime_name = (
        str(getattr(event_def, "canonical_regime", "") or row.get("canonical_regime", family_name))
        .strip()
        .upper()
    )
    event_parameters = row.get("parameters", {})
    if not isinstance(event_parameters, dict):
        event_parameters = {}
    family_defaults_all = unified.get("families", {})
    family_defaults = (
        family_defaults_all.get(family_name, {}) if isinstance(family_defaults_all, dict) else {}
    )

    overrides = dict(runtime_overrides or {})
    normalized_state = str(state_id).strip().upper() if state_id else ""

    state_defaults: dict[str, Any] = {}
    if normalized_state:
        # State overrides from event level
        event_state_overrides = row.get("state_overrides", {})
        if isinstance(event_state_overrides, dict):
            state_defaults.update(event_state_overrides.get(normalized_state, {}))
        # State overrides from global level
        global_state_overrides = unified.get("state_overrides", {})
        if isinstance(global_state_overrides, dict):
            state_defaults.update(
                global_state_overrides.get(normalized, {}).get(normalized_state, {})
            )

    # Extract legacy top-level keys that aren't core or unified
    legacy_parameters = {
        k: v
        for k, v in row.items()
        if k not in _CORE_KEYS
        and k not in _META_KEYS
        and k not in _UNIFIED_NON_EVENT_PARAMETER_KEYS
    }

    effective_parameters = {}
    effective_parameters.update(defaults.get("parameters", {}))
    effective_parameters.update(family_defaults.get("parameters", {}))
    effective_parameters.update(_detector_default_parameters(normalized))
    effective_parameters.update(legacy_parameters)
    effective_parameters.update(event_parameters)
    effective_parameters.update(overrides)

    # Compose Template fields
    def _get_field(key: str, default: Any = None) -> Any:
        val = defaults.get(key, default)
        if key in family_defaults:
            val = family_defaults[key]
        if key in row:
            val = row[key]
        if key in state_defaults:
            val = state_defaults[key]
        if key in overrides:
            val = overrides[key]
        return val

    templates = _to_str_tuple(_get_field("templates", ()))
    horizons = _to_str_tuple(_get_field("horizons", ()))
    conditioning_cols = _to_str_tuple(_get_field("conditioning_cols", ()))
    max_candidates = int(_get_field("max_candidates_per_run", 1000))

    # Sequence detectors are analyzer-only contracts; they should not be blocked
    # by template discovery compatibility rules that apply to event families used
    # in strategy generation.
    if normalized.startswith("SEQ_"):
        templates = ()

    # FORCED_FLOW_AND_EXHAUSTION family events are post-hoc patterns that don't
    # support standard template-based discovery - treat as analyzer-only.
    if family_name == "FORCED_FLOW_AND_EXHAUSTION" or normalized.startswith("POST_DELEVERAGING"):
        templates = ()
    # Validation
    operators = _operator_registry()
    compatible_templates: list[str] = []
    family_candidates = {
        token
        for token in (
            family_name,
            event_parameters.get("research_family"),
            event_parameters.get("canonical_family"),
        )
        if isinstance(token, str) and token.strip()
    }
    for t_name in templates:
        if t_name in operators:
            compat = operators[t_name].get("compatible_families", [])
            compat_families = {str(c).upper() for c in compat}
            if compat_families and not family_candidates.intersection(compat_families):
                continue
        compatible_templates.append(t_name)
    templates = tuple(compatible_templates)

    # Hash for discovery uniqueness
    payload_for_hash = {
        "event_type": normalized,
        "family": family_name,
        "state_id": normalized_state or None,
        "templates": list(templates),
        "horizons": list(horizons),
        "conditioning_cols": list(conditioning_cols),
    }
    h_json = json.dumps(payload_for_hash, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(h_json.encode("utf-8")).hexdigest()

    return ComposedConfig(
        event_type=normalized,
        family=family_name,
        canonical_regime=regime_name,
        reports_dir=_coalesce_text(
            row.get("reports_dir"),
            getattr(event_def, "reports_dir", normalized.lower()),
        ),
        events_file=_coalesce_text(
            row.get("events_file"),
            getattr(event_def, "events_file", f"{normalized.lower()}_events.parquet"),
        ),
        signal_column=_coalesce_text(
            row.get("signal_column"),
            getattr(
                event_def,
                "signal_column",
                normalized.lower()
                if normalized.lower().endswith("_event")
                else f"{normalized.lower()}_event",
            ),
        ),
        parameters=effective_parameters,
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
        templates=templates,
        horizons=horizons,
        conditioning_cols=conditioning_cols,
        max_candidates_per_run=max_candidates,
        state_id=normalized_state or None,
        config_hash=f"sha256:{digest}",
        normalized_json=h_json,
        source_layers={
            "unified_registry": str(UNIFIED_REGISTRY_PATH.resolve()),
            "registry": str(UNIFIED_REGISTRY_PATH.resolve()),
            "event_spec": str(selected_spec_path.resolve()),
        },
    )


def compose_event_config(event_type: str, **kwargs) -> ComposedConfig:
    return compose_config(event_type, **kwargs)


def compose_template_config(event_type: str, **kwargs) -> ComposedConfig:
    return compose_config(event_type, **kwargs)


def bootstrap_event_registry() -> None:
    _unified_registry.cache_clear()
    _family_by_event.cache_clear()
    _operator_registry.cache_clear()


# Internal aliases for backward compatibility with old tests
_registry = _unified_registry
_legacy_taxonomy_families = lambda: {}
