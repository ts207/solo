from typing import Any, Dict, List

from project.domain.compiled_registry import get_domain_registry
from project.spec_validation.ontology import (
    get_event_family,
    get_event_ids_for_family,
    get_event_ids_for_regime,
    get_searchable_event_families,
    get_searchable_state_families,
    get_state_ids_for_family,
)

_SUPPORTED_COST_PROFILES = frozenset({"standard"})


def validate_search_spec_doc(search_cfg: Dict[str, Any], *, source: str = "<memory>") -> None:
    if not isinstance(search_cfg, dict):
        raise ValueError(f"Search spec must resolve to a mapping: {source}")

    # Validate optional search-surface controls eagerly so spec docs and runtime contract stay aligned.
    resolve_cost_profiles(search_cfg)
    resolve_conditioning_intersections(search_cfg)
    resolve_filter_template_names(search_cfg)
    resolve_execution_template_names(search_cfg)

    # Resolve and validate entry lags eagerly so stale same-bar configs fail before generation.
    resolve_entry_lags(search_cfg)
    resolve_templates(search_cfg)


def expand_triggers(search_cfg: Dict[str, Any]) -> Dict[str, Any]:
    triggers = search_cfg.get("triggers", {})

    # 1. Expand events — also build event_id → family map
    event_ids: set = set()
    event_family_map: Dict[str, str] = {}
    # From families
    raw_event_fams = triggers.get("event_families", [])
    if raw_event_fams == "*":
        raw_event_fams = get_searchable_event_families()
    for fam in raw_event_fams:
        for eid in get_event_ids_for_family(fam):
            event_ids.add(eid)
            event_family_map[eid] = fam
    raw_regimes = triggers.get("canonical_regimes", [])
    for regime in raw_regimes:
        for eid in get_event_ids_for_regime(regime, executable_only=True):
            event_ids.add(eid)
            event_family_map[eid] = regime
    # From explicit list
    for eid in triggers.get("events", []):
        event_ids.add(eid)
        if eid not in event_family_map:
            fam = get_event_family(eid)
            if fam:
                event_family_map[eid] = fam

    # 2. Expand states
    state_ids: set = set()
    # From families
    raw_state_fams = triggers.get("state_families", [])
    if raw_state_fams == "*":
        raw_state_fams = get_searchable_state_families()
    for fam in raw_state_fams:
        state_ids.update(get_state_ids_for_family(fam))
    # From explicit list
    state_ids.update(triggers.get("states", []))

    # 3. Transitions
    transition_ids = triggers.get("transitions", [])

    # 4. Feature predicates
    feature_predicates = triggers.get("feature_predicates", [])

    return {
        "events": sorted(list(event_ids)),
        "states": sorted(list(state_ids)),
        "transitions": transition_ids,
        "feature_predicates": feature_predicates,
        "event_family_map": event_family_map,
    }


def resolve_templates(search_cfg: Dict[str, Any]) -> List[str]:
    registry = get_domain_registry()
    templates = search_cfg.get("expression_templates", search_cfg.get("templates", []))
    if templates == "*":
        return list(registry.default_hypothesis_templates())

    resolved = [templates] if isinstance(templates, str) else list(templates)
    normalized: List[str] = []
    seen: set[str] = set()
    invalid_filter_templates: List[str] = []
    invalid_execution_templates: List[str] = []
    for raw in resolved:
        token = str(raw).strip()
        if not token:
            continue
        if registry.is_filter_template(token):
            invalid_filter_templates.append(token)
            continue
        if registry.is_execution_template(token):
            invalid_execution_templates.append(token)
            continue
        if token not in seen:
            normalized.append(token)
            seen.add(token)
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




def resolve_filter_template_names(search_cfg: Dict[str, Any]) -> List[str]:
    registry = get_domain_registry()
    templates = search_cfg.get("filter_templates", [])
    if templates in (None, "", [], ()):
        return []
    if templates == "*":
        return ["*"]
    resolved = [templates] if isinstance(templates, str) else list(templates)
    normalized: List[str] = []
    seen: set[str] = set()
    invalid: List[str] = []
    for raw in resolved:
        token = str(raw).strip()
        if not token:
            raise ValueError("filter_templates entries must be non-empty strings")
        if not registry.is_filter_template(token):
            invalid.append(token)
            continue
        if token not in seen:
            normalized.append(token)
            seen.add(token)
    if invalid:
        raise ValueError(
            "filter_templates entries must resolve to filter_template operators: "
            + ", ".join(sorted(set(invalid)))
        )
    return normalized


def resolve_execution_template_names(search_cfg: Dict[str, Any]) -> List[str]:
    registry = get_domain_registry()
    templates = search_cfg.get("execution_templates", [])
    if templates in (None, "", [], ()):
        return []
    if templates == "*":
        return ["*"]
    resolved = [templates] if isinstance(templates, str) else list(templates)
    normalized: List[str] = []
    seen: set[str] = set()
    invalid: List[str] = []
    for raw in resolved:
        token = str(raw).strip()
        if not token:
            raise ValueError("execution_templates entries must be non-empty strings")
        if not registry.is_execution_template(token):
            invalid.append(token)
            continue
        if token not in seen:
            normalized.append(token)
            seen.add(token)
    if invalid:
        raise ValueError(
            "execution_templates entries must resolve to execution_template operators: "
            + ", ".join(sorted(set(invalid)))
        )
    return normalized
def resolve_execution_templates(family: str) -> List[str]:
    """
    Return execution template names for a family — allowed_templates minus filter_templates.
    Falls back to the registry defaults if the family has no config.
    """
    return list(get_domain_registry().family_execution_templates(family))


def resolve_filter_templates(family: str) -> List[Dict[str, Any]]:
    """
    Return filter template definitions applicable to a family.
    A filter template is one whose name appears in the family's allowed_templates
    AND has an entry in the registry's filter_templates block.
    Returns list of dicts: {name, feature, operator, threshold}.
    """
    return list(get_domain_registry().family_filter_templates(family))


def resolve_cost_profiles(search_cfg: Dict[str, Any]) -> List[str]:
    profiles = search_cfg.get("cost_profiles", ["standard"])
    if profiles == "*":
        resolved = ["standard"]
    elif profiles is None or profiles == "" or profiles == [] or profiles == ():
        resolved = ["standard"]
    else:
        resolved = [profiles] if isinstance(profiles, str) else list(profiles)

    normalized: List[str] = []
    seen: set[str] = set()
    invalid: List[str] = []
    for raw in resolved:
        profile = str(raw).strip().lower()
        if not profile:
            raise ValueError("cost_profiles entries must be non-empty strings")
        if profile not in _SUPPORTED_COST_PROFILES:
            invalid.append(profile)
            continue
        if profile not in seen:
            normalized.append(profile)
            seen.add(profile)
    if invalid:
        raise ValueError(
            "Unsupported cost_profiles entries: " + ", ".join(sorted(set(invalid)))
        )
    return normalized


def resolve_conditioning_intersections(search_cfg: Dict[str, Any]) -> List[str]:
    intersections = search_cfg.get("conditioning_intersections", [])
    if intersections == "*":
        return ["*"]
    if intersections is None or intersections == "" or intersections == [] or intersections == ():
        return []

    resolved = [intersections] if isinstance(intersections, str) else list(intersections)
    normalized: List[str] = []
    seen: set[str] = set()
    for raw in resolved:
        value = str(raw).strip()
        if not value:
            raise ValueError("conditioning_intersections entries must be non-empty strings")
        if value not in seen:
            normalized.append(value)
            seen.add(value)
    return normalized


def resolve_entry_lags(search_cfg: Dict[str, Any]) -> List[int]:
    # Support both 'entry_lag' and legacy 'entry_lags'
    lags = search_cfg.get("entry_lag", search_cfg.get("entry_lags", []))
    if lags == "*":
        resolved = list(get_domain_registry().default_entry_lags())
    elif lags is None or lags == "" or lags == [] or lags == ():
        resolved = [1]  # Default to 1 bar lag
    else:
        resolved = [lags] if isinstance(lags, int) else list(lags)

    normalized: List[int] = []
    seen: set[int] = set()
    for raw in resolved:
        lag = int(raw)
        if lag < 1:
            raise ValueError(
                "entry_lag/entry_lags must be >= 1 to prevent same-bar entry leakage"
            )
        if lag not in seen:
            normalized.append(lag)
            seen.add(lag)
    return normalized
