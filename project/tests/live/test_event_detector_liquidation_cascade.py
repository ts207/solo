from project.live.event_detector import detect_live_event


def test_detect_live_event_supports_liquidation_cascade_when_market_features_present():
    event = detect_live_event(
        symbol="BTCUSDT",
        timeframe="5m",
        current_close=105.0,
        previous_close=100.0,
        market_features={
            "open_interest_delta_fraction": -0.08,
            "funding_rate": 0.0012,
        },
        supported_event_ids=["LIQUIDATION_CASCADE"],
        detector_config={
            "liquidation_cascade_min_abs_move_bps": 300.0,
            "liquidation_cascade_min_abs_oi_drop_fraction": 0.03,
            "liquidation_cascade_min_abs_funding_rate": 0.0005,
        },
    )
    assert event is not None
    assert event.event_id == "LIQUIDATION_CASCADE"
    assert event.event_family == "LIQUIDATION_CASCADE"
    assert event.canonical_regime == "LIQUIDATION_CASCADE"
    assert event.event_side == "long"
    assert event.features["open_interest_delta_fraction"] == -0.08


def test_detect_live_event_skips_liquidation_cascade_without_forced_flow_confirmation():
    event = detect_live_event(
        symbol="BTCUSDT",
        timeframe="5m",
        current_close=105.0,
        previous_close=100.0,
        market_features={
            "open_interest_delta_fraction": -0.01,
            "funding_rate": 0.0001,
        },
        supported_event_ids=["LIQUIDATION_CASCADE"],
        detector_config={
            "liquidation_cascade_min_abs_move_bps": 300.0,
            "liquidation_cascade_min_abs_oi_drop_fraction": 0.03,
            "liquidation_cascade_min_abs_funding_rate": 0.0005,
        },
    )
    assert event is None
