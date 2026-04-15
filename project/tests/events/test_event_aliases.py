from project.events.event_aliases import (
    resolve_event_alias,
    resolve_executable_event_alias,
)


def test_resolve_executable_event_alias_maps_proxy_surfaces_to_authoritative_ids():
    assert resolve_event_alias("ABSORPTION_EVENT") == "ABSORPTION_PROXY"
    assert resolve_executable_event_alias("ABSORPTION_PROXY") == "ABSORPTION_EVENT"
    assert resolve_executable_event_alias("DEPTH_STRESS_PROXY") == "DEPTH_COLLAPSE"
    assert resolve_executable_event_alias("LIQUIDITY_STRESS_DIRECT") == "LIQUIDITY_SHOCK"
