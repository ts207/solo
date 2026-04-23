from __future__ import annotations

from project.core.feature_registry import (
    ensure_core_feature_definitions_registered,
    ensure_market_context_feature_definitions_registered,
    get_feature_definition,
    has_feature_definition,
    list_feature_definitions,
)


def test_core_feature_registry_contains_expected_definitions():
    ensure_core_feature_definitions_registered()

    assert has_feature_definition("basis_zscore")
    assert has_feature_definition("funding_abs_pct")
    assert has_feature_definition("micro_depth_depletion")

    basis = get_feature_definition("basis_zscore")
    assert basis is not None
    assert basis.units == "zscore"
    assert "basis_bps" in basis.dependencies

    names = [definition.name for definition in list_feature_definitions()]
    assert names == sorted(names)


def test_market_context_registry_contains_state_features():
    ensure_core_feature_definitions_registered()
    ensure_market_context_feature_definitions_registered()

    assert has_feature_definition("high_vol_regime")
    assert has_feature_definition("carry_state_code")
    assert has_feature_definition("ms_liquidation_state")
    assert has_feature_definition("ms_context_state_code")
    assert has_feature_definition("fp_active")
    assert has_feature_definition("fp_severity")
    assert has_feature_definition("prob_vol_high")
    assert has_feature_definition("ms_vol_confidence")
    assert has_feature_definition("prob_trend_bull")
    assert has_feature_definition("ms_spread_entropy")

    definition = get_feature_definition("high_vol_regime")
    assert definition is not None
    assert definition.source_stage == "build_market_context"
    assert "rv_pct_17280" in definition.dependencies

    prob_definition = get_feature_definition("prob_vol_high")
    assert prob_definition is not None
    assert prob_definition.units == "probability"

    entropy_definition = get_feature_definition("ms_spread_entropy")
    assert entropy_definition is not None
    assert entropy_definition.units == "entropy"
