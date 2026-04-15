from __future__ import annotations

from project.spec_registry import load_template_registry
from project.scripts.build_template_registry_sidecars import (
    build_template_registry_compat_payload,
    build_ontology_template_registry_payload,
    build_runtime_template_registry_payload,
)


def test_template_registry_compat_payload_is_generated_from_canonical_source() -> None:
    canonical = load_template_registry()
    compat = build_template_registry_compat_payload()

    assert canonical["metadata"]["status"] == "authoritative"
    assert compat["metadata"]["status"] == "generated"
    assert compat["metadata"]["authored_source"] == "spec/templates/registry.yaml"
    assert canonical["events"]["VOL_SHOCK"]["research_family"] == "VOLATILITY_TRANSITION"
    assert "canonical_family" not in canonical["events"]["VOL_SHOCK"]
    assert compat["events"]["VOL_SHOCK"]["canonical_family"] == "VOLATILITY_TRANSITION"
    assert compat["operators"]["continuation"]["supports_trigger_types"] == canonical["operators"]["continuation"]["supports_trigger_types"]


def test_runtime_template_registry_payload_uses_canonical_operator_runtime_fields() -> None:
    payload = build_runtime_template_registry_payload()

    assert payload["metadata"]["status"] == "generated"
    assert payload["metadata"]["authored_source"] == "spec/templates/registry.yaml"
    continuation = payload["templates"]["continuation"]
    assert continuation["enabled"] is True
    assert continuation["template_kind"] == "expression_template"
    assert continuation["supports_contexts"] is True
    assert continuation["supports_trigger_types"] == ["EVENT", "STATE", "SEQUENCE", "INTERACTION"]


def test_ontology_template_registry_payload_uses_canonical_family_and_filter_fields() -> None:
    payload = build_ontology_template_registry_payload()

    assert payload["metadata"]["status"] == "generated"
    families = payload["families"]
    assert "LIQUIDITY_DISLOCATION" in families
    assert "mean_reversion" in families["LIQUIDITY_DISLOCATION"]["allowed_templates"]

    filters = payload["filter_templates"]
    assert filters["only_if_regime"]["feature"] == "rv_pct_17280"
    assert filters["only_if_regime"]["operator"] == ">"
