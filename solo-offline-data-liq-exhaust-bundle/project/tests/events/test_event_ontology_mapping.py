from __future__ import annotations

from project.domain.compiled_registry import get_domain_registry
from project.events.event_specs import EVENT_REGISTRY_SPECS
from project.events.ontology_mapping import (
    allowed_dispositions,
    allowed_evidence_modes,
    allowed_ontology_layers,
    ontology_rows_by_event,
)


def test_event_ontology_mapping_covers_all_active_events_exactly_once():
    mapped = ontology_rows_by_event()
    registry = get_domain_registry()
    assert set(mapped) == set(registry.event_ids)


def test_event_ontology_mapping_uses_allowed_enum_values():
    mapped = ontology_rows_by_event()
    allowed_layers = set(allowed_ontology_layers())
    disposition_values = set(allowed_dispositions())
    allowed_evidence = set(allowed_evidence_modes())
    for event_type, row in mapped.items():
        assert row["layer"] in allowed_layers, event_type
        assert row["disposition"] in disposition_values, event_type
        assert row["evidence_mode"] in allowed_evidence, event_type


def test_compiled_registry_exposes_canonical_ontology_fields():
    registry = get_domain_registry()
    spec = registry.get_event("LIQUIDITY_STRESS_DIRECT")
    assert spec is not None
    assert spec.research_family == "LIQUIDITY_DISLOCATION"
    assert spec.canonical_regime == "LIQUIDITY_STRESS"
    assert spec.canonical_family == spec.research_family
    assert spec.evidence_mode == "direct"


def test_direct_and_hybrid_variants_share_canonical_regime_but_keep_distinct_evidence_modes():
    registry = get_domain_registry()
    direct = registry.get_event("LIQUIDITY_STRESS_DIRECT")
    hybrid = registry.get_event("LIQUIDITY_STRESS_PROXY")
    assert direct is not None and hybrid is not None
    assert direct.canonical_regime == hybrid.canonical_regime == "LIQUIDITY_STRESS"
    assert direct.evidence_mode == "direct"
    assert hybrid.evidence_mode == "hybrid"


def test_compatibility_events_with_strengthened_runtime_evidence_are_promoted_to_hybrid() -> None:
    registry = get_domain_registry()
    for event_type in (
        "ABSORPTION_PROXY",
        "DEPTH_STRESS_PROXY",
        "FLOW_EXHAUSTION_PROXY",
        "PRICE_VOL_IMBALANCE_PROXY",
        "LIQUIDITY_STRESS_PROXY",
        "WICK_REVERSAL_PROXY",
    ):
        spec = registry.get_event(event_type)
        assert spec is not None
        assert spec.evidence_mode == "hybrid"

def test_non_canonical_layers_are_flagged_in_registry_specs():
    seq = EVENT_REGISTRY_SPECS["SEQ_VOL_COMP_THEN_BREAKOUT"]
    context = EVENT_REGISTRY_SPECS["SESSION_OPEN_EVENT"]
    strategy = EVENT_REGISTRY_SPECS["COPULA_PAIRS_TRADING"]
    assert seq.is_composite is True
    assert context.is_context_tag is True
    assert strategy.is_strategy_construct is True
    assert strategy.strategy_only is True
