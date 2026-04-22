from __future__ import annotations

from project.research.search.profile import DEFAULT_SEARCH_SPEC, resolve_search_profile


def test_default_search_profile_uses_tier1_search_space() -> None:
    assert DEFAULT_SEARCH_SPEC == "spec/search_space.yaml"
    resolved = resolve_search_profile(
        discovery_profile="standard",
        search_spec="",
        min_n=30,
        min_t_stat=None,
    )
    assert resolved["search_spec"] == "spec/search_space.yaml"


def test_synthetic_profile_still_redirects_default_search_to_synthetic_truth() -> None:
    resolved = resolve_search_profile(
        discovery_profile="synthetic",
        search_spec="spec/search_space.yaml",
        min_n=30,
        min_t_stat=None,
    )
    assert resolved["search_spec"] == "synthetic_truth"
    assert resolved["min_n"] == 8
    assert resolved["min_t_stat"] == 0.25


def test_exploratory_profile_relaxes_discovery_thresholds_and_exposes_overrides() -> None:
    resolved = resolve_search_profile(
        discovery_profile="exploratory",
        search_spec="spec/search_space.yaml",
        min_n=30,
        min_t_stat=None,
    )
    assert resolved["min_n"] == 24
    assert resolved["min_t_stat"] == 1.0
    assert resolved["hierarchical_overrides"]["trigger_viability"]["max_templates"] == 2
    assert resolved["hierarchical_overrides"]["execution_refinement"]["max_horizons"] == 3
