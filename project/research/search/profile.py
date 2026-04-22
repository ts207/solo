from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict


DEFAULT_SEARCH_SPEC = "spec/search_space.yaml"
DEFAULT_SEARCH_MIN_N = 30
DEFAULT_MIN_T_STAT = 1.5
_EXPLORATORY_SEARCH_MIN_N = 24
_EXPLORATORY_MIN_T_STAT = 1.0

_EXPLORATORY_HIERARCHICAL_OVERRIDES: Dict[str, Any] = {
    "trigger_viability": {
        "max_templates": 2,
        "max_horizons": 2,
        "max_entry_lags": 2,
        "allow_both_directions": True,
    },
    "template_refinement": {
        "top_k_templates_per_trigger": 5,
    },
    "execution_refinement": {
        "top_k_shapes_per_template": 6,
        "max_horizons": 3,
        "max_entry_lags": 4,
    },
    "context_refinement": {
        "top_k_contexts_per_candidate": 4,
    },
}


def _clone_profile_overrides(profile: str) -> Dict[str, Any]:
    profile_name = str(profile or "standard").strip().lower()
    if profile_name == "exploratory":
        return deepcopy(_EXPLORATORY_HIERARCHICAL_OVERRIDES)
    return {}


def resolve_search_profile(
    *,
    discovery_profile: str,
    search_spec: str,
    min_n: int,
    min_t_stat: float | None,
) -> Dict[str, Any]:
    profile = str(discovery_profile or "standard").strip().lower()
    resolved_search_spec = str(search_spec or DEFAULT_SEARCH_SPEC).strip() or DEFAULT_SEARCH_SPEC
    resolved_min_n = int(min_n)
    resolved_min_t_stat = (
        DEFAULT_MIN_T_STAT if min_t_stat is None else float(min_t_stat)
    )

    if profile == "synthetic":
        if resolved_search_spec in {"", DEFAULT_SEARCH_SPEC, "spec/search_space.yaml", "search_space.yaml"}:
            resolved_search_spec = "synthetic_truth"
        if resolved_min_n >= DEFAULT_SEARCH_MIN_N:
            resolved_min_n = 8
        if resolved_min_t_stat >= DEFAULT_MIN_T_STAT:
            resolved_min_t_stat = 0.25
    elif profile == "exploratory":
        if resolved_min_n >= DEFAULT_SEARCH_MIN_N:
            resolved_min_n = _EXPLORATORY_SEARCH_MIN_N
        if resolved_min_t_stat >= DEFAULT_MIN_T_STAT:
            resolved_min_t_stat = _EXPLORATORY_MIN_T_STAT

    return {
        "discovery_profile": profile,
        "search_spec": resolved_search_spec,
        "min_n": resolved_min_n,
        "min_t_stat": resolved_min_t_stat,
        "hierarchical_overrides": _clone_profile_overrides(profile),
    }
