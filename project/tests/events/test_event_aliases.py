from project.events.event_aliases import (
    EVENT_ALIASES,
    compatibility_event_aliases,
    event_alias_policy_rows,
    resolve_event_alias,
    resolve_executable_event_alias,
)


def test_event_aliases_are_compatibility_only_legacy_tokens():
    assert set(compatibility_event_aliases()) == {"ABSORPTION_EVENT", "BASIS_DISLOCATION", "VOL_REGIME_SHIFT"}
    assert EVENT_ALIASES["BASIS_DISLOCATION"] == "BASIS_DISLOC"
    assert resolve_event_alias("ABSORPTION_EVENT") == "ABSORPTION_PROXY"
    assert resolve_event_alias("DEPTH_COLLAPSE") == "DEPTH_COLLAPSE"
    assert resolve_event_alias("LIQUIDITY_STRESS_DIRECT") == "LIQUIDITY_STRESS_DIRECT"
    assert resolve_executable_event_alias("ABSORPTION_EVENT") == "ABSORPTION_PROXY"
    assert resolve_executable_event_alias("DEPTH_STRESS_PROXY") == "DEPTH_STRESS_PROXY"
    assert resolve_executable_event_alias("LIQUIDITY_STRESS_DIRECT") == "LIQUIDITY_STRESS_DIRECT"


def test_alias_policy_cannot_create_runtime_planning_or_promotion_identity():
    rows = event_alias_policy_rows()
    assert rows
    assert all(row["scope"] == "load_time_compatibility" for row in rows)
    assert all(row["planning_identity"] is False for row in rows)
    assert all(row["runtime_identity"] is False for row in rows)
    assert all(row["promotion_identity"] is False for row in rows)
