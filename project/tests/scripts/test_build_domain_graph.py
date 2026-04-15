from __future__ import annotations

from project.domain.registry_loader import build_domain_graph_payload


def test_domain_graph_payload_contains_clean_runtime_sections() -> None:
    payload = build_domain_graph_payload()

    assert payload["kind"] == "domain_graph"
    assert payload["metadata"]["graph_role"] == "runtime_read_model"
    assert "events" in payload
    assert "states" in payload
    assert "templates" in payload
    assert "regimes" in payload
    assert "theses" in payload
    assert "compatibility" in payload
    assert "runtime" in payload


def test_domain_graph_payload_carries_runtime_and_state_metadata() -> None:
    payload = build_domain_graph_payload()

    depth_collapse = payload["events"]["DEPTH_COLLAPSE"]
    assert depth_collapse["research_family"] == "LIQUIDITY_DISLOCATION"
    assert depth_collapse["canonical_family"] == "LIQUIDITY_DISLOCATION"
    assert depth_collapse["detector_name"] == "DepthCollapseDetector"
    assert depth_collapse["canonical_regime"] == "LIQUIDITY_STRESS"

    assert "LOW_LIQUIDITY_STATE" in payload["states"]
    assert payload["states"]["HIGH_VOL_REGIME"]["state_engine"] == "VolatilityRegimeEngine"
    assert payload["states"]["HIGH_VOL_REGIME"]["instrument_classes"] == [
        "crypto",
        "equities",
        "futures",
    ]
    assert "mean_reversion" in payload["templates"]
    assert payload["templates"]["mean_reversion"]["template_kind"] == "expression_template"
    assert payload["templates"]["mean_reversion"]["side_policy"] == "contrarian"
    assert payload["templates"]["continuation"]["supports_trigger_types"] == [
        "EVENT",
        "STATE",
        "SEQUENCE",
        "INTERACTION",
    ]
    assert payload["regimes"]["LIQUIDITY_STRESS"]["execution_style"] == "spread_aware"
    assert payload["theses"]["THESIS_VOL_SHOCK"]["trigger_events"] == ["VOL_SHOCK"]
    assert payload["theses"]["THESIS_VOL_SHOCK_LIQUIDITY_CONFIRM"]["confirmation_events"] == [
        "LIQUIDITY_VACUUM"
    ]


def test_domain_graph_payload_is_a_slim_runtime_read_model() -> None:
    payload = build_domain_graph_payload()

    depth_collapse = payload["events"]["DEPTH_COLLAPSE"]
    assert "source_kind" not in depth_collapse
    assert set(depth_collapse["runtime"].keys()) <= {
        "templates",
        "horizons",
        "conditioning_cols",
        "max_candidates_per_run",
        "state_overrides",
        "precedence_reason",
    }

    assert "raw" not in payload["states"]["HIGH_VOL_REGIME"]
    assert "source_kind" not in payload["states"]["HIGH_VOL_REGIME"]
    assert "raw" not in payload["regimes"]["LIQUIDITY_STRESS"]
    assert "source_kind" not in payload["regimes"]["LIQUIDITY_STRESS"]
    assert "raw" not in payload["theses"]["THESIS_VOL_SHOCK"]
    assert "source_kind" not in payload["theses"]["THESIS_VOL_SHOCK"]
    assert "raw" not in payload["templates"]["continuation"]

    assert "unified_payload" not in payload
    assert "template_registry_payload" not in payload
    assert "family_registry_payload" not in payload
    assert "gates_spec" not in payload
    assert "unified_registry_path" not in payload

    runtime = payload["runtime"]
    assert runtime["event_registry"]["kind"] == "event_runtime_defaults"
    assert runtime["template_registry"]["kind"] == "template_runtime_defaults"
    assert isinstance(payload["compatibility"]["event_families"], dict)
    assert isinstance(payload["compatibility"]["state_families"], dict)
