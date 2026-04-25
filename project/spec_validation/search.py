from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence
from pathlib import Path
import yaml

from project.domain.compiled_registry import get_domain_registry
from project.spec_validation.ontology import get_event_ids_for_family, get_event_ids_for_regime

def _load_yaml_at(root: Path, relative_path: str) -> Dict[str, Any]:
    path = root / relative_path
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def validate_search_spec_doc(search_cfg: Dict[str, Any], source: str = "unknown", root: Path = Path(".")) -> None:
    if search_cfg.get("kind") not in ("search_spec", "search_space"):
        return

    # This still uses get_domain_registry() which is global
    registry = get_domain_registry()
    
    resolve_templates(search_cfg)
    
    if "entry_lag" in search_cfg:
        val = search_cfg["entry_lag"]
        if isinstance(val, (int, str)) and str(val) != "*":
            if int(val) < 1:
                raise ValueError("entry_lag must be >= 1 to prevent same-bar entry leakage")

    if "entry_lags" in search_cfg:
        lags = search_cfg["entry_lags"]
        if lags == "*":
            pass
        elif isinstance(lags, (list, tuple)):
            for lag in lags:
                if isinstance(lag, (int, str)) and str(lag) != "*":
                    if int(lag) < 1:
                        raise ValueError("entry_lags must be >= 1 to prevent same-bar entry leakage")

    if "cost_profiles" in search_cfg:
        profiles = search_cfg["cost_profiles"]
        if isinstance(profiles, (list, tuple)):
            supported = {"standard", "aggressive", "conservative", "market_maker"}
            unsupported = [p for p in profiles if p not in supported]
            if unsupported:
                raise ValueError(f"Unsupported cost_profiles entries: {', '.join(unsupported)}")

def resolve_templates(search_cfg: Dict[str, Any]) -> List[str]:
    registry = get_domain_registry()
    templates = search_cfg.get("expression_templates", search_cfg.get("templates", []))
    if templates == "*":
        return list(registry.default_hypothesis_templates())

    policy = search_cfg.get("template_policy", {})
    generic_allowed = policy.get("generic_templates_allowed", False)
    reason = policy.get("reason", "")

    # Templates requiring explicit policy acknowledgment — either unregistered or broad-scope generics.
    _truly_abstract = {
        "generic_continuation", "generic_mean_reversion", "unconditioned_mean_reversion",
        "continuation", "mean_reversion",
    }

    resolved = [templates] if isinstance(templates, str) else list(templates)
    normalized: List[str] = []
    seen: set[str] = set()
    invalid_filter_templates: List[str] = []
    invalid_execution_templates: List[str] = []
    invalid_abstract_templates: List[str] = []

    for raw in resolved:
        token = str(raw).strip()
        if not token:
            continue
        is_unresolvable = (
            not registry.is_expression_template(token)
            and not registry.is_filter_template(token)
            and not registry.is_execution_template(token)
        )
        needs_policy = token in _truly_abstract
        if is_unresolvable:
            invalid_abstract_templates.append(token)
            continue
        if needs_policy:
            if not generic_allowed:
                invalid_abstract_templates.append(token)
                continue
            elif not reason:
                raise ValueError(f"Template policy allows generic templates but missing 'reason' for {token}")

        if registry.is_filter_template(token):
            invalid_filter_templates.append(token)
            continue
        if registry.is_execution_template(token):
            invalid_execution_templates.append(token)
            continue
        if token not in seen:
            normalized.append(token)
            seen.add(token)

    if invalid_abstract_templates:
        raise ValueError(
            "Search spec templates must be concrete expression templates; "
            "generic abstract templates are forbidden unless explicitly allowed by template_policy and reason is present: "
            + ", ".join(sorted(set(invalid_abstract_templates)))
        )
    if invalid_filter_templates:
        raise ValueError(
            "Search spec templates must be expression templates; "
            "filter templates belong in optional filter-template overlays, not top-level templates: "
            + ", ".join(sorted(set(invalid_filter_templates)))
        )
    if invalid_execution_templates:
        raise ValueError(
            "Search spec templates must be expression templates; "
            "execution templates cannot be emitted as standalone top-level search units: "
            + ", ".join(sorted(set(invalid_execution_templates)))
        )
    return normalized

def resolve_entry_lags(search_cfg: Dict[str, Any]) -> List[int]:
    lags = search_cfg.get("entry_lags", [search_cfg.get("entry_lag", 1)])
    if lags == "*":
         # Fallback to default
         return [1]
    if isinstance(lags, int):
        lags = [lags]
    normalized = []
    seen = set()
    for raw in lags:
        if str(raw) == "*":
            continue
        lag = int(raw)
        if lag < 1:
             raise ValueError("entry_lag/entry_lags must be >= 1 to prevent same-bar entry leakage")
        if lag not in seen:
            normalized.append(lag)
            seen.add(lag)
    return normalized if normalized else [1]

def expand_triggers(triggers: Dict[str, Any]) -> Dict[str, Any]:
    # Support being passed either the triggers dict or the whole spec
    actual_triggers = triggers.get("triggers", triggers) if "triggers" in triggers else triggers

    event_ids: set[str] = set()
    if "events" in actual_triggers:
        for ev in actual_triggers["events"]:
            event_ids.add(ev)
    if "families" in actual_triggers:
        for fam in actual_triggers["families"]:
            event_ids.update(get_event_ids_for_family(fam))
    if "regimes" in actual_triggers:
        for reg in actual_triggers["regimes"]:
            event_ids.update(get_event_ids_for_regime(reg, executable_only=True))

    # Also support canonical_regimes as requested by test
    if "canonical_regimes" in actual_triggers:
        for reg in actual_triggers["canonical_regimes"]:
            event_ids.update(get_event_ids_for_regime(reg, executable_only=True))

    # Build event_family_map so the generator can look up filter templates per event
    registry = get_domain_registry()
    event_family_map: Dict[str, str] = {}
    for eid in event_ids:
        spec = registry.get_event(eid)
        if spec is not None:
            family = str(spec.research_family or spec.canonical_family or spec.canonical_regime).strip().upper()
            if family:
                event_family_map[eid] = family

    return {"events": sorted(list(event_ids)), "event_family_map": event_family_map}

def resolve_filter_template_names(search_cfg: Dict[str, Any]) -> List[str]:
    raw = search_cfg.get("filter_templates", [])
    if raw == "*":
        registry = get_domain_registry()
        return [name for name, op in registry.template_operator_definitions.items() if op.template_kind == "filter_template"]
    if not isinstance(raw, list):
        return []
    return [str(t).strip() for t in raw if str(t).strip()]

def resolve_execution_template_names(search_cfg: Dict[str, Any]) -> List[str]:
    raw = search_cfg.get("execution_templates", [])
    if not isinstance(raw, list):
        return []
    return [str(t).strip() for t in raw if str(t).strip()]

def resolve_filter_templates(family_name: str) -> List[Dict[str, Any]]:
    """Return filter template dicts for a given event family name."""
    registry = get_domain_registry()
    return list(registry.family_filter_templates(family_name))

def resolve_execution_templates(search_cfg: Dict[str, Any]) -> List[str]:
    return resolve_execution_template_names(search_cfg)
