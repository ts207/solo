from __future__ import annotations

from project.scripts.build_regime_registry_sidecars import build_regime_routing_payload
from project.spec_registry import load_regime_registry


def test_regime_routing_payload_is_generated_from_canonical_regime_registry() -> None:
    canonical = load_regime_registry()
    routing = build_regime_routing_payload()

    assert canonical["metadata"]["status"] == "authoritative"
    assert routing["kind"] == "regime_routing"
    assert routing["metadata"]["status"] == "generated"
    assert routing["metadata"]["authored_source"] == "spec/regimes/registry.yaml"
    assert routing["regimes"]["LIQUIDITY_STRESS"]["execution_style"] == "spread_aware"
