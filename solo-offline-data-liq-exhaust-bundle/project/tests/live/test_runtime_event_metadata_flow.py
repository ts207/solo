from __future__ import annotations

from project.live.context_builder import build_live_trade_context
from project.live.event_detector import detect_live_event


def test_runtime_event_metadata_flows_into_context() -> None:
    event = detect_live_event(
        symbol='BTCUSDT',
        timeframe='5m',
        current_close=105.0,
        previous_close=100.0,
        volume=100000.0,
        market_features={'spread_bps': 2.0, 'depth_usd': 50000.0},
        supported_event_ids=['VOL_SPIKE'],
        detector_config={'threshold_version': '2.1.0'},
    )
    assert event is not None
    assert event.event_confidence is not None
    assert event.event_severity is not None
    assert event.threshold_version == '2.1.0'

    context = build_live_trade_context(
        timestamp='2026-04-02T00:00:00Z',
        symbol='BTCUSDT',
        timeframe='5m',
        detected_event=event,
        market_features=event.features,
        portfolio_state={},
        execution_env={},
    )
    assert context.event_confidence == event.event_confidence
    assert context.event_severity == event.event_severity
    assert context.data_quality_flag == event.data_quality_flag
    assert context.event_version == event.event_version
    assert context.threshold_version == event.threshold_version
