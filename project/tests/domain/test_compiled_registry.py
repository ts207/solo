from __future__ import annotations

import pytest

from project.domain.compiled_registry import get_domain_registry, refresh_domain_registry
from project.domain.hypotheses import HypothesisSpec, TriggerSpec
from project.domain.registry_loader import (
    compile_domain_registry,
    domain_graph_path,
    load_domain_registry_from_graph,
)
from project.research.search.feasibility import check_hypothesis_feasibility


def test_domain_registry_compiles_core_event_state_and_template_views():
    registry = get_domain_registry()

    assert registry.has_event("VOL_SHOCK")
    assert registry.has_state("LOW_LIQUIDITY_STATE")
    assert registry.get_operator("mean_reversion") is not None
    assert registry.has_thesis("THESIS_VOL_SHOCK")

    event = registry.get_event("VOL_SHOCK")
    assert event is not None
    assert event.event_type == "VOL_SHOCK"
    assert event.research_family == "VOLATILITY_TRANSITION"
    assert event.canonical_family == "VOLATILITY_TRANSITION"
    assert event.canonical_regime == "VOLATILITY_TRANSITION"
    assert event.signal_column
    assert event.spec_path.endswith("VOL_SHOCK.yaml")
    assert registry.get_event("BASIS_DISLOCATION") is None
    assert domain_graph_path().exists()


def test_vol_shock_is_feasible_for_continuation_template_family():
    registry = get_domain_registry()
    spec = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="60m",
        template_id="continuation",
    )

    result = check_hypothesis_feasibility(spec, registry=registry)

    assert result.valid is True
    assert "incompatible_template_family" not in result.reasons


def test_domain_registry_includes_runtime_promoted_event_specs():
    registry = get_domain_registry()

    promoted = registry.get_event("LIQUIDITY_STRESS_DIRECT")
    assert promoted is not None
    assert promoted.signal_column == "liquidity_stress_direct_event"
    assert promoted.spec_path.endswith("LIQUIDITY_STRESS_DIRECT.yaml")


def test_domain_registry_exposes_runtime_metadata_from_event_specs():
    registry = get_domain_registry()

    depth_collapse = registry.event_definitions["DEPTH_COLLAPSE"]
    assert depth_collapse.detector_name == "DepthCollapseDetector"
    assert depth_collapse.enabled is True
    assert depth_collapse.instrument_classes == ("crypto", "futures")
    assert depth_collapse.runtime_tags == ("liquidity_stress",)
    assert depth_collapse.sequence_eligible is True
    assert depth_collapse.event_kind == "market_event"
    assert depth_collapse.default_executable is True

    absorption_proxy = registry.event_definitions["ABSORPTION_PROXY"]
    assert absorption_proxy.detector_name == "AbsorptionProxyDetector"

    high_vol = registry.state_definitions["HIGH_VOL_REGIME"]
    assert high_vol.state_engine == "VolatilityRegimeEngine"
    assert high_vol.instrument_classes == ("crypto", "equities", "futures")
    assert high_vol.runtime_tags == ("volatility",)
    assert high_vol.description == "Market is in a high volatility state."
    assert high_vol.spec_path.endswith("HIGH_VOL_REGIME.yaml")

    continuation = registry.get_operator("continuation")
    assert continuation is not None
    assert "LIQUIDITY_DISLOCATION" in continuation.compatible_families
    assert continuation.raw["supports_trigger_types"] == [
        "EVENT",
        "STATE",
        "SEQUENCE",
        "INTERACTION",
    ]
    thesis = registry.get_thesis("THESIS_VOL_SHOCK_LIQUIDITY_CONFIRM")
    assert thesis is not None
    assert thesis.primary_event_id == "VOL_SHOCK"
    assert thesis.canonical_regime == "VOLATILITY_TRANSITION"
    assert thesis.event_family == "VOL_SHOCK"
    assert thesis.trigger_events == ("VOL_SHOCK",)
    assert thesis.confirmation_events == ("LIQUIDITY_VACUUM",)
    assert thesis.freshness_policy["allowed_staleness_classes"] == ["fresh", "watch"]
    assert thesis.governance["operational_role"] == "confirm"
    regime = registry.get_regime("LIQUIDITY_STRESS")
    assert regime is not None
    assert regime.execution_style == "spread_aware"
    assert regime.spec_path.endswith("spec/regimes/registry.yaml")


def test_domain_registry_event_row_exposes_routing_profile_ref():
    registry = refresh_domain_registry()

    row = registry.event_row("LIQUIDATION_CASCADE_PROXY")

    assert row["research_family"] == "POSITIONING_EXTREMES"
    assert row["canonical_regime"] == "LIQUIDATION_CASCADE"
    assert row["routing_profile_ref"] == "LIQUIDATION_CASCADE"


def test_domain_registry_exposes_research_family_separately_from_canonical_regime():
    registry = refresh_domain_registry(rebuild_from_sources=True)

    row = registry.event_row("OI_FLUSH")

    assert row["research_family"] == "POSITIONING_EXTREMES"
    assert row["canonical_family"] == "POSITIONING_EXTREMES"
    assert row["canonical_regime"] == "POSITIONING_UNWIND_DELEVERAGING"


def test_domain_registry_derives_family_allowed_templates_from_template_registry(monkeypatch):
    from project.domain import registry_loader

    original = registry_loader.load_yaml_relative

    def patched(relative_path: str):
        payload = original(relative_path)
        if relative_path == "spec/grammar/family_registry.yaml":
            poisoned = dict(payload)
            event_families = dict(poisoned.get("event_families", {}))
            liquidity = dict(event_families.get("LIQUIDITY_DISLOCATION", {}))
            liquidity["allowed_templates"] = ["poisoned_template"]
            event_families["LIQUIDITY_DISLOCATION"] = liquidity
            poisoned["event_families"] = event_families
            return poisoned
        return payload

    monkeypatch.setattr(registry_loader, "load_yaml_relative", patched)

    registry = registry_loader.compile_domain_registry_from_sources()

    assert "poisoned_template" not in registry.event_family_rows()["LIQUIDITY_DISLOCATION"]["allowed_templates"]
    assert "liquidity_refill_repair" in registry.event_family_rows()["LIQUIDITY_DISLOCATION"]["allowed_templates"]


def test_domain_registry_exposes_context_and_searchable_family_views():
    registry = get_domain_registry()

    assert registry.resolve_context_state("vol_regime", "high") == "HIGH_VOL_REGIME"
    assert registry.resolve_context_state("carry_state", "funding_pos") == "FUNDING_POSITIVE"
    assert "low" in registry.context_labels_for_family("vol_regime")
    assert registry.context_labels_for_family("carry_state") == ("funding_neg", "funding_pos")
    assert "VOLATILITY_TRANSITION" in registry.searchable_event_families
    assert "TREND_STRUCTURE" in registry.searchable_state_families
    assert "AFTERSHOCK_STATE" in registry.valid_state_ids


def test_domain_registry_exposes_robustness_runtime_config():
    registry = get_domain_registry()

    assert len(registry.stress_scenarios) >= 3
    assert registry.stress_scenarios[0]["name"]
    assert registry.stress_scenarios[0]["feature"]

    assert len(registry.kill_switch_candidate_features) >= 5
    assert "rv_pct_17280" in registry.kill_switch_candidate_features


def test_domain_registry_exposes_sequence_and_interaction_runtime_config():
    registry = get_domain_registry()

    assert len(registry.sequence_definitions) >= 1
    assert registry.sequence_definitions[0]["name"]
    assert registry.sequence_definitions[0]["events"]

    assert len(registry.interaction_definitions) >= 1
    assert registry.interaction_definitions[0]["name"]
    assert registry.interaction_definitions[0]["left"]
    assert registry.interaction_definitions[0]["right"]


def test_domain_registry_loads_from_generated_domain_graph():
    registry = refresh_domain_registry()
    assert registry.unified_payload.get("kind") in {"event_runtime_defaults", "event_unified_registry"}
    assert registry.unified_registry_path in {"", "spec/events/event_registry_unified.yaml"}
    assert registry.event_definitions["DEPTH_COLLAPSE"].canonical_regime == "LIQUIDITY_STRESS"




def test_domain_graph_payload_omits_legacy_compatibility_surfaces():
    from project.domain.registry_loader import build_domain_graph_payload

    payload = build_domain_graph_payload()

    assert "compatibility" not in payload
    first_event = next(iter(payload["events"].values()))
    assert "legacy_family" not in first_event

def test_compile_domain_registry_requires_generated_graph_by_default(monkeypatch):
    monkeypatch.setattr(
        "project.domain.registry_loader._load_domain_registry_from_graph",
        lambda: None,
    )

    def fail_on_source_compile():
        raise AssertionError("source compilation should not be used implicitly")

    monkeypatch.setattr(
        "project.domain.registry_loader._build_domain_registry_from_sources",
        fail_on_source_compile,
    )

    with pytest.raises(FileNotFoundError, match="Compiled domain graph is missing or invalid"):
        compile_domain_registry()


def test_load_domain_registry_from_graph_requires_generated_graph_by_default(monkeypatch):
    monkeypatch.setattr(
        "project.domain.registry_loader._load_domain_registry_from_graph",
        lambda: None,
    )

    with pytest.raises(FileNotFoundError, match="Compiled domain graph is missing or invalid"):
        load_domain_registry_from_graph()


def test_refresh_domain_registry_can_explicitly_rebuild_from_sources(monkeypatch):
    registry = get_domain_registry()
    called = {"n": 0}

    def fake_source_compile():
        called["n"] += 1
        return registry

    monkeypatch.setattr(
        "project.domain.compiled_registry.compile_domain_registry_from_sources",
        fake_source_compile,
    )
    monkeypatch.setattr("project.domain.compiled_registry.clear_caches", lambda: None)

    refreshed = refresh_domain_registry(rebuild_from_sources=True)

    assert refreshed is registry
    assert called["n"] == 1


def test_domain_graph_digest_returns_stable_hex_string():
    from project.domain.registry_loader import domain_graph_digest
    digest = domain_graph_digest()
    assert len(digest) == 64
    assert all(c in "0123456789abcdef" for c in digest)
    assert domain_graph_digest() == digest


def test_spec_sources_digest_returns_stable_hex_string():
    from project.domain.registry_loader import spec_sources_digest
    digest = spec_sources_digest()
    assert len(digest) == 64
    assert all(c in "0123456789abcdef" for c in digest)


def test_domain_graph_records_current_spec_sources_digest():
    import yaml

    from project.domain.registry_loader import domain_graph_path, spec_sources_digest

    payload = yaml.safe_load(domain_graph_path().read_text(encoding="utf-8"))
    metadata = payload.get("metadata", {})
    assert metadata.get("spec_sources_digest") == spec_sources_digest()


def test_domain_init_exposes_compiled_registry_api():
    import project.domain as domain
    assert callable(domain.get_domain_registry)
    assert callable(domain.domain_graph_digest)
    assert callable(domain.spec_sources_digest)
    registry = domain.get_domain_registry()
    assert isinstance(registry, domain.DomainRegistry)


def test_event_row_governance_fields_are_not_overridden_by_raw_blank_values():
    """event_row() must emit computed governance values, not raw blank/None from the YAML."""
    from project.domain.models import EventDefinition, DomainRegistry

    # Build a synthetic EventDefinition with well-known governance values and a raw
    # dict that contains blank/None for those same fields (simulating stale YAML rows).
    raw_with_blanks = {
        "planning_eligible": None,
        "runtime_eligible": "",
        "promotion_eligible": None,
        "primary_anchor_eligible": "",
        "detector_band": "",
        # keep a non-governance field in raw so row is non-trivial
        "notes": "from raw",
    }
    spec = EventDefinition(
        event_type="SYNTHETIC_TEST_EVENT",
        canonical_family="TEST_FAMILY",
        canonical_regime="TEST_REGIME",
        event_kind="market_event",
        reports_dir="test",
        events_file="test.parquet",
        signal_column="synthetic_test_event",
        planning_eligible=True,
        runtime_eligible=True,
        promotion_eligible=True,
        primary_anchor_eligible=True,
        detector_band="deployable_core",
        raw=raw_with_blanks,
    )

    # Build a minimal registry that contains only this synthetic event.
    registry = DomainRegistry(
        event_definitions={"SYNTHETIC_TEST_EVENT": spec},
        state_definitions={},
        template_registry_payload={},
        family_registry_payload={},
        thesis_definitions={},
        context_state_map={},
        searchable_event_families=(),
        searchable_state_families=(),
        state_aliases=(),
        stress_scenarios=(),
        kill_switch_candidate_features=(),
        sequence_definitions=(),
        interaction_definitions=(),
        gates_spec={},
        unified_registry_path="",
    )

    row = registry.event_row("SYNTHETIC_TEST_EVENT")

    # Governance fields must reflect computed EventDefinition values, not raw blanks.
    assert row["planning_eligible"] is True, "planning_eligible was overridden by raw None"
    assert row["runtime_eligible"] is True, "runtime_eligible was overridden by raw blank"
    assert row["promotion_eligible"] is True, "promotion_eligible was overridden by raw None"
    assert row["primary_anchor_eligible"] is True, "primary_anchor_eligible was overridden by raw blank"
    assert row["detector_band"] == "deployable_core", "detector_band was overridden by raw blank"

    # Non-governance field from raw should still be preserved via setdefault.
    assert row["notes"] == "from raw"


def test_event_row_governance_fields_populated_for_live_events():
    """Spot-check that key real events have non-blank governance fields in event_row()."""
    registry = get_domain_registry()
    checks = {
        "VOL_SHOCK": {"detector_band": "research_trigger", "planning_eligible": False},
        "LIQUIDATION_CASCADE": {"detector_band": "deployable_core", "planning_eligible": True},
        "LIQUIDITY_VACUUM": {"detector_band": "deployable_core", "promotion_eligible": True},
        "OI_SPIKE_NEGATIVE": {"detector_band": "deployable_core", "primary_anchor_eligible": True},
        "FUNDING_PERSISTENCE_TRIGGER": {"detector_band": "research_trigger", "planning_eligible": True},
    }
    for event_type, expected in checks.items():
        row = registry.event_row(event_type)
        assert row, f"event_row({event_type!r}) returned empty"
        for field, value in expected.items():
            assert row[field] == value, (
                f"{event_type}.{field}: expected {value!r}, got {row[field]!r}"
            )


def test_events_contracts_delegates_to_compiled_domain():
    from project.domain.models import EventDefinition
    from project.events.contracts import (
        get_event_spec,
        get_event_type_from_signal,
        is_registry_backed_signal,
    )

    spec = get_event_spec("VOL_SPIKE")
    assert spec is not None
    assert isinstance(spec, EventDefinition)
    assert spec.signal_column

    assert get_event_spec("NONEXISTENT_XYZ_999") is None

    signal = spec.signal_column
    assert get_event_type_from_signal(signal) == "VOL_SPIKE"
    assert is_registry_backed_signal(signal) is True
    assert is_registry_backed_signal("not_a_real_signal_xyz") is False
