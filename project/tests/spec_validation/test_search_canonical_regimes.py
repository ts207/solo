from __future__ import annotations

from project.spec_validation.search import expand_triggers


def test_expand_triggers_supports_canonical_regimes_and_excludes_non_canonical_layers():
    expanded = expand_triggers({"triggers": {"canonical_regimes": ["LIQUIDITY_STRESS"]}})
    events = set(expanded["events"])
    assert "LIQUIDITY_VACUUM" in events
    assert "LIQUIDITY_STRESS_DIRECT" in events
    assert "LIQUIDITY_STRESS_PROXY" in events
    assert "SEQ_LIQ_VACUUM_THEN_DEPTH_RECOVERY" not in events
    assert "SESSION_OPEN_EVENT" not in events
    assert "COPULA_PAIRS_TRADING" not in events
