from __future__ import annotations

from project.scripts.check_domain_graph_freshness import check_domain_graph_freshness


def test_domain_graph_freshness_passes_for_checked_in_graph() -> None:
    fresh, details = check_domain_graph_freshness()

    assert fresh is True
    assert details["recorded_spec_sources_digest"] == details["current_spec_sources_digest"]
