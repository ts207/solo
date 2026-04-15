from __future__ import annotations

from project.events.canonical_registry_sidecars import (
    canonical_event_registry_payload,
    event_contract_overrides_payload,
    event_ontology_mapping_payload,
)


def test_event_ontology_mapping_payload_uses_compiled_registry_rows() -> None:
    payload = event_ontology_mapping_payload()
    events = payload["events"]

    depth_collapse = events["DEPTH_COLLAPSE"]
    assert depth_collapse["canonical_regime"] == "LIQUIDITY_STRESS"
    assert depth_collapse["phase"] == "collapse"
    assert depth_collapse["evidence_mode"] in {"direct", "hybrid"}


def test_event_contract_overrides_payload_uses_compiled_contracts() -> None:
    payload = event_contract_overrides_payload()
    events = payload["events"]

    liquidity_shock = events["LIQUIDITY_SHOCK"]
    assert liquidity_shock["tier"] == "A"
    assert liquidity_shock["operational_role"] == "trigger"
    assert liquidity_shock["runtime_category"] == "active_runtime_event"


def test_canonical_event_registry_payload_preserves_proxy_tier_compatibility() -> None:
    payload = canonical_event_registry_payload()
    meta = payload["event_metadata"]

    for event_type in (
        "ABSORPTION_EVENT",
        "DEPTH_COLLAPSE",
        "ORDERFLOW_IMBALANCE_SHOCK",
        "SWEEP_STOPRUN",
        "FORCED_FLOW_EXHAUSTION",
    ):
        assert meta[event_type]["evidence_tier"] == "proxy"
