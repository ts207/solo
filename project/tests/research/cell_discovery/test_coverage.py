from __future__ import annotations

from project.research.cell_discovery.coverage import build_cell_coverage_audit
from project.research.cell_discovery.registry import load_registry
from project.research.cell_discovery.spec_audit import build_spec_audit


def test_cell_coverage_audit_reports_authored_tier2_surfaces() -> None:
    payload = build_cell_coverage_audit()

    assert payload["status"] == "ok"
    assert payload["registry_event_count"] == 83
    assert payload["default_search_event_count"] == 60
    assert payload["cell_event_count"] == 60
    assert payload["cell_coverage_fraction_of_default_search"] == 1.0
    assert payload["cell_surfaces"]["tier2_liquidity_stress_v1"]["events"] == [
        "DEPTH_COLLAPSE",
        "LIQUIDITY_GAP_PRINT",
        "LIQUIDITY_SHOCK",
        "ORDERFLOW_IMBALANCE_SHOCK",
        "SPREAD_BLOWOUT",
        "SWEEP_STOPRUN",
    ]
    assert payload["cell_surfaces"]["tier2_trend_failure_v1"]["events"] == [
        "CLIMAX_VOLUME_BAR",
        "FAILED_CONTINUATION",
        "FALSE_BREAKOUT",
        "LIQUIDATION_EXHAUSTION_REVERSAL",
        "MOMENTUM_DIVERGENCE_TRIGGER",
        "TREND_EXHAUSTION_TRIGGER",
    ]
    assert payload["cell_surfaces"]["tier2_basis_funding_runtime_v1"]["events"] == [
        "BASIS_DISLOC",
        "CROSS_VENUE_DESYNC",
        "FND_DISLOC",
        "FUNDING_FLIP",
        "SPOT_PERP_BASIS_SHOCK",
    ]
    assert payload["cell_surfaces"]["tier2_liquidation_positioning_runtime_v1"]["events"] == [
        "DELEVERAGING_WAVE",
        "LIQUIDATION_CASCADE",
        "LIQUIDATION_CASCADE_PROXY",
        "OI_FLUSH",
        "OI_SPIKE_NEGATIVE",
        "OI_SPIKE_POSITIVE",
        "POST_DELEVERAGING_REBOUND",
    ]
    assert payload["default_search_events_missing_from_cell"] == []
    assert payload["cell_events_not_in_default_search"] == []


def test_tier2_cell_specs_load_through_registry() -> None:
    liquidity = load_registry("spec/discovery/tier2_liquidity_stress_v1")
    trend_failure = load_registry("spec/discovery/tier2_trend_failure_v1")

    assert {atom.event_type for atom in liquidity.event_atoms} == {
        "DEPTH_COLLAPSE",
        "LIQUIDITY_GAP_PRINT",
        "LIQUIDITY_SHOCK",
        "ORDERFLOW_IMBALANCE_SHOCK",
        "SPREAD_BLOWOUT",
        "SWEEP_STOPRUN",
    }
    assert {atom.event_type for atom in trend_failure.event_atoms} == {
        "CLIMAX_VOLUME_BAR",
        "FAILED_CONTINUATION",
        "FALSE_BREAKOUT",
        "LIQUIDATION_EXHAUSTION_REVERSAL",
        "MOMENTUM_DIVERGENCE_TRIGGER",
        "TREND_EXHAUSTION_TRIGGER",
    }


def test_runtime_and_repair_specs_are_template_clean() -> None:
    surfaces = [
        "tier2_basis_funding_runtime_v1",
        "tier2_desync_runtime_v1",
        "tier2_guard_filter_v1",
        "tier2_liquidation_exhaustion_focused_v1",
        "tier2_liquidation_positioning_runtime_v1",
        "tier2_liquidity_proxy_repair_v1",
        "tier2_liquidity_repair_v1",
        "tier2_regime_transition_runtime_v1",
        "tier2_statistical_stretch_repair_v1",
        "tier2_temporal_execution_guard_v1",
        "tier2_trend_continuation_runtime_v1",
        "tier2_trend_failure_residual_runtime_v1",
        "tier2_trend_failure_runtime_v1",
        "tier2_volatility_transition_runtime_v1",
    ]

    for surface in surfaces:
        payload = build_spec_audit(spec_dir=f"spec/discovery/{surface}")
        assert payload["status"] == "ok", surface
        assert payload["template_issue_count"] == 0, surface
        assert all(not row["unsupported_templates"] for row in payload["events"]), surface
