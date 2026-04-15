from __future__ import annotations

from project.live.event_detector import detect_live_event


def test_detect_live_event_supports_liquidity_vacuum() -> None:
    detected = detect_live_event(
        symbol="BTCUSDT",
        timeframe="5m",
        current_close=100.0,
        previous_close=99.5,
        volume=40_000.0,
        market_features={"spread_bps": 7.5, "depth_usd": 15_000.0},
        supported_event_ids=["LIQUIDITY_VACUUM", "VOL_SHOCK"],
        detector_config={},
    )

    assert detected is not None
    assert detected.event_id == "LIQUIDITY_VACUUM"
    assert detected.event_family == "LIQUIDITY_VACUUM"
    assert detected.canonical_regime == "LIQUIDITY_STRESS"


def test_detect_live_event_supports_vol_spike() -> None:
    detected = detect_live_event(
        symbol="BTCUSDT",
        timeframe="5m",
        current_close=101.0,
        previous_close=100.0,
        volume=120_000.0,
        market_features={"spread_bps": 2.0, "depth_usd": 100_000.0},
        supported_event_ids=["VOL_SPIKE", "VOL_SHOCK"],
        detector_config={},
    )

    assert detected is not None
    assert detected.event_id == "VOL_SPIKE"
    assert detected.event_family == "VOL_SPIKE"
    assert detected.canonical_regime == "VOLATILITY_EXPANSION"
