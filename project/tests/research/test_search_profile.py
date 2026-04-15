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
