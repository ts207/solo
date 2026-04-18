from __future__ import annotations

from project.live.contracts.live_trade_context import LiveTradeContext
from project.live.scoring import _event_quality_adjustment


def _ctx(**overrides):
    payload = dict(
        timestamp='2026-04-01T00:00:00Z',
        symbol='BTCUSDT',
        timeframe='5m',
        primary_event_id='VOL_SHOCK',
        event_family='VOL_SHOCK',
        canonical_regime='VOLATILITY_TRANSITION',
        event_side='long',
        event_confidence=0.85,
        event_severity=0.9,
        data_quality_flag='ok',
        event_version='v2',
        threshold_version='2.0',
        event_evidence_mode='direct',
        event_role='trigger',
        threshold_snapshot={},
    )
    payload.update(overrides)
    return LiveTradeContext(**payload)


def test_event_quality_adjustment_rewards_high_quality_trigger() -> None:
    add, penalty, reasons_for, reasons_against = _event_quality_adjustment(_ctx())
    assert add > 0
    assert penalty == 0
    assert 'event_quality_ok' in reasons_for


def test_event_quality_adjustment_penalizes_proxy_degraded_context() -> None:
    add, penalty, reasons_for, reasons_against = _event_quality_adjustment(
        _ctx(data_quality_flag='degraded', event_confidence=0.2, event_evidence_mode='proxy', event_role='context')
    )
    assert add >= 0
    assert penalty > 0.4
    assert 'proxy_evidence_penalty' in reasons_against
    assert 'event_role_not_primary_trigger' in reasons_against
