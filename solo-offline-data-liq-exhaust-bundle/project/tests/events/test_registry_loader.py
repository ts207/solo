from project.events.config import compose_event_config
from project.events.registry import _load_event_specs, EVENT_REGISTRY_SPECS


def test_dynamic_loading():
    # Ensure loaded specs match the hardcoded baseline (sanity check)
    loaded = _load_event_specs()
    assert "VOL_SHOCK" in loaded
    assert loaded["VOL_SHOCK"].signal_column == "vol_shock_relaxation_event"
    assert len(loaded) >= 11


def test_compose_event_config_uses_registry_core_artifact_fields_for_vol_shock():
    cfg = compose_event_config("VOL_SHOCK")

    assert cfg.reports_dir == "vol_shock_relaxation"
    assert cfg.events_file == "vol_shock_relaxation_events.parquet"
    assert cfg.signal_column == "vol_shock_relaxation_event"


def test_blank_registry_fields_fall_back_to_canonical_defaults():
    loaded = _load_event_specs()
    spec = loaded["CROSS_ASSET_DESYNC_EVENT"]

    assert spec.reports_dir == "cross_asset_desync_event"
    assert spec.events_file == "cross_asset_desync_event_events.parquet"
    assert spec.signal_column == "cross_asset_desync_event"

    cfg = compose_event_config("CROSS_ASSET_DESYNC_EVENT")
    assert cfg.reports_dir == "cross_asset_desync_event"
    assert cfg.events_file == "cross_asset_desync_event_events.parquet"
    assert cfg.signal_column == "cross_asset_desync_event"


def test_compose_event_config_filters_templates_against_research_family():
    cfg = compose_event_config(
        "CROSS_ASSET_DESYNC_EVENT",
        runtime_overrides={"templates": ("desync_repair", "mean_reversion")},
    )

    assert cfg.family == "INFORMATION_DESYNC"
    assert cfg.canonical_regime == "CROSS_ASSET_DESYNCHRONIZATION"
    assert cfg.templates == ("desync_repair",)


def test_compose_event_config_drops_incompatible_liquidity_template():
    cfg = compose_event_config(
        "LIQUIDITY_SHOCK",
        runtime_overrides={"templates": ("stop_run_repair", "reversal_or_squeeze")},
    )

    assert cfg.family == "LIQUIDITY_DISLOCATION"
    assert cfg.canonical_regime == "LIQUIDITY_STRESS"
    assert cfg.templates == ("stop_run_repair",)


def test_compose_event_config_preserves_source_spec_parameters_missing_from_unified_registry():
    depth_cfg = compose_event_config("DEPTH_COLLAPSE")
    assert depth_cfg.parameters["spread_weight"] == 0.45
    assert depth_cfg.parameters["rv_weight"] == 0.35
    assert depth_cfg.parameters["depth_weight"] == 0.2

    funding_cfg = compose_event_config("FUNDING_NORMALIZATION_TRIGGER")
    assert funding_cfg.parameters["min_prior_extreme_abs"] == 0.0004


def test_compose_event_config_surfaces_detector_defaults_and_runtime_contract_fields():
    flow_cfg = compose_event_config("FORCED_FLOW_EXHAUSTION")
    assert flow_cfg.parameters["oi_drop_quantile"] == 0.88
    assert flow_cfg.parameters["liquidation_quantile"] == 0.92
    assert flow_cfg.parameters["rv_decay_ratio"] == 0.99
    assert flow_cfg.parameters["min_spacing"] == 32

    rebound_cfg = compose_event_config("POST_DELEVERAGING_REBOUND")
    assert rebound_cfg.parameters["wick_quantile"] == 0.70
    assert rebound_cfg.parameters["cluster_window"] == 12
    assert rebound_cfg.parameters["wick_ratio_min"] == 0.55

    liquidity_cfg = compose_event_config("LIQUIDITY_VACUUM")
    assert liquidity_cfg.parameters["shock_quantile"] == 0.99
    assert liquidity_cfg.parameters["shock_threshold_mode"] == "rolling"
    assert liquidity_cfg.parameters["max_vacuum_bars"] == 96
