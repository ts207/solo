from __future__ import annotations

from project.events.event_aliases import resolve_event_alias
from project.events.event_specs import EVENT_REGISTRY_SPECS
from project.events.registry import get_event_definition, load_milestone_event_registry
from project.events.validate import validate_event_payload


def test_milestone_registry_loads_generated_yaml():
    registry = load_milestone_event_registry()
    assert "FUNDING_EXTREME_ONSET" in registry
    assert "LIQUIDATION_CASCADE" in registry


def test_event_aliases_resolve_before_registry_lookup():
    assert resolve_event_alias("basis_dislocation") == "BASIS_DISLOC"
    assert resolve_event_alias("vol_regime_shift") == "VOL_REGIME_SHIFT_EVENT"
    assert get_event_definition("BASIS_DISLOC")["event_type"] == "BASIS_DISLOC"
    assert get_event_definition("BASIS_DISLOCATION") is None


def test_promoted_proxy_and_direct_events_are_active_specs():
    for event_type in (
        "ABSORPTION_PROXY",
        "DEPTH_STRESS_PROXY",
        "FLOW_EXHAUSTION_PROXY",
        "POST_DELEVERAGING_REBOUND",
        "LIQUIDITY_STRESS_DIRECT",
        "LIQUIDITY_STRESS_PROXY",
        "PRICE_VOL_IMBALANCE_PROXY",
        "WICK_REVERSAL_PROXY",
    ):
        assert event_type in EVENT_REGISTRY_SPECS


def test_validate_event_payload_accepts_canonical_shape():
    payload = {
        "event_id": "e1",
        "event_family": "basis",
        "event_type": "BASIS_DISLOC",
        "observable_type": "cross_market_price_dislocation",
        "interpretation": "basis dislocation",
        "asset": "BTCUSDT",
        "bar_type": "5m",
        "eval_bar_ts": "2026-01-01T00:00:00Z",
        "detected_ts": "2026-01-01T00:00:00Z",
        "signal_ts": "2026-01-01T00:05:00Z",
        "intensity": 2.5,
        "severity": 2,
        "episode_id": None,
        "attribution_id": None,
        "detector_version": "v1",
        "event_version": "v1",
        "meta": {},
    }
    validate_event_payload(payload)
