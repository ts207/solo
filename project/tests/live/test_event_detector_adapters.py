from __future__ import annotations

from project.live.context_builder import build_runtime_core_detector_input_surface
from project.live.event_detector import (
    GovernedRuntimeCoreEventDetectionAdapter,
    HeuristicLiveEventDetectionAdapter,
    build_live_event_detection_adapter,
    detect_live_event,
)


def test_build_live_event_detection_adapter_defaults_to_governed_runtime_core() -> None:
    adapter = build_live_event_detection_adapter({})
    assert isinstance(adapter, GovernedRuntimeCoreEventDetectionAdapter)


def test_build_live_event_detection_adapter_selects_governed_runtime_core() -> None:
    adapter = build_live_event_detection_adapter({"adapter": "governed"})
    assert isinstance(adapter, GovernedRuntimeCoreEventDetectionAdapter)


def test_detect_live_event_no_longer_defaults_to_heuristic() -> None:
    detected = detect_live_event(
        symbol="BTCUSDT",
        timeframe="5m",
        current_close=101.0,
        previous_close=100.0,
        volume=120_000.0,
        market_features={"spread_bps": 2.0, "depth_usd": 100_000.0},
        supported_event_ids=["VOL_SPIKE"],
        detector_config={"threshold_version": "2.1.0"},
    )

    assert detected is None


def test_detect_live_event_heuristic_requires_explicit_legacy_config() -> None:
    try:
        detect_live_event(
            symbol="BTCUSDT",
            timeframe="5m",
            current_close=101.0,
            previous_close=100.0,
            volume=120_000.0,
            market_features={"spread_bps": 2.0, "depth_usd": 100_000.0},
            supported_event_ids=["VOL_SPIKE"],
            detector_config={"adapter": "heuristic"},
        )
    except ValueError as exc:
        assert "legacy_heuristic_enabled=true" in str(exc)
    else:
        raise AssertionError("expected ValueError")

    detected = detect_live_event(
        symbol="BTCUSDT",
        timeframe="5m",
        current_close=101.0,
        previous_close=100.0,
        volume=120_000.0,
        market_features={"spread_bps": 2.0, "depth_usd": 100_000.0},
        supported_event_ids=["VOL_SPIKE"],
        detector_config={
            "adapter": "heuristic",
            "legacy_heuristic_enabled": True,
            "threshold_version": "2.1.0",
        },
    )

    assert detected is not None
    assert detected.event_id == "VOL_SPIKE"


def test_build_live_event_detection_adapter_requires_explicit_legacy_flag_for_heuristic() -> None:
    try:
        build_live_event_detection_adapter({"adapter": "heuristic"})
    except ValueError as exc:
        assert "legacy_heuristic_enabled=true" in str(exc)
    else:
        raise AssertionError("expected ValueError")

    adapter = build_live_event_detection_adapter(
        {"adapter": "heuristic", "legacy_heuristic_enabled": True}
    )
    assert isinstance(adapter, HeuristicLiveEventDetectionAdapter)


def test_governed_runtime_core_adapter_detects_liquidity_stress_direct() -> None:
    adapter = GovernedRuntimeCoreEventDetectionAdapter(
        {"median_window": 3, "min_periods": 1}
    )

    for minute in range(3):
        events = adapter.detect_events(
            symbol="BTCUSDT",
            timeframe="5m",
            current_close=100.0,
            previous_close=100.0,
            volume=50_000.0,
            market_features={
                "timestamp": f"2026-04-18T00:{minute:02d}:00Z",
                "open": 100.0,
                "high": 100.3,
                "low": 99.7,
                "spread_bps": 1.0,
                "depth_usd": 100_000.0,
            },
            supported_event_ids=["VOL_CLUSTER_SHIFT", "LIQUIDITY_STRESS_DIRECT"],
        )
        assert events == []

    events = adapter.detect_events(
        symbol="BTCUSDT",
        timeframe="5m",
        current_close=99.0,
        previous_close=100.0,
        volume=75_000.0,
        market_features={
            "timestamp": "2026-04-18T00:03:00Z",
            "open": 100.0,
            "high": 100.1,
            "low": 98.8,
            "spread_bps": 5.0,
            "depth_usd": 10_000.0,
        },
        supported_event_ids=["VOL_CLUSTER_SHIFT", "LIQUIDITY_STRESS_DIRECT"],
    )

    assert [event.event_id for event in events] == ["LIQUIDITY_STRESS_DIRECT"]
    detected = events[0]
    assert detected.event_family == "LIQUIDITY_STRESS_DIRECT"
    assert detected.event_confidence is not None
    assert detected.event_severity is not None
    assert detected.data_quality_flag == "ok"
    assert detected.features["threshold_snapshot"]["version"] == "2.0"
    assert detected.features["detector_input_status"]["adapter"] == "governed_runtime_core"
    assert detected.features["required_context_present"] is True


def test_runtime_core_input_surface_marks_basis_spot_feed_missing() -> None:
    surface = build_runtime_core_detector_input_surface(
        symbol="BTCUSDT",
        timeframe="5m",
        current_close=100.0,
        previous_close=99.0,
        volume=10_000.0,
        market_features={"timestamp": "2026-04-18T00:00:00Z"},
        supported_event_ids=["BASIS_DISLOC"],
    )

    status = surface.detector_input_status["per_event"]["BASIS_DISLOC"]
    assert "close_spot" in status["missing_inputs"]
    assert status["input_mapping"]["close_spot"]["source"] == "current_close_fallback"
