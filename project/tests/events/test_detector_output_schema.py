from __future__ import annotations

from datetime import datetime, timezone

import pytest

from project.events.event_output_schema import DetectedEvent


def test_detected_event_bounds_and_serialization() -> None:
    event = DetectedEvent(
        event_name='VOL_SPIKE',
        event_version='v2',
        detector_class='VolSpikeDetectorV2',
        symbol='BTCUSDT',
        timeframe='5m',
        ts_start=datetime.now(timezone.utc),
        ts_end=datetime.now(timezone.utc),
        canonical_family='VOLATILITY_TRANSITION',
        subtype='vol_spike',
        phase='shock',
        evidence_mode='direct',
        role='trigger',
        confidence=1.2,
        severity=-0.3,
        trigger_value=3.4,
        threshold_snapshot={'version': '2.0'},
        source_features={'rv_z': 2.4},
        detector_metadata={'cluster_id': 'vol_regime'},
        required_context_present=True,
        data_quality_flag='ok',
        merge_key='BTCUSDT:vol_regime',
        cooldown_until=None,
    )
    payload = event.as_dict()
    assert payload['confidence'] == 1.0
    assert payload['severity'] == 0.0
    assert isinstance(payload['ts_start'], str)


def test_detected_event_rejects_invalid_quality_flag() -> None:
    with pytest.raises(ValueError):
        DetectedEvent(
            event_name='VOL_SPIKE',
            event_version='v2',
            detector_class='VolSpikeDetectorV2',
            symbol='BTCUSDT',
            timeframe='5m',
            ts_start=datetime.now(timezone.utc),
            ts_end=datetime.now(timezone.utc),
            canonical_family='VOLATILITY_TRANSITION',
            subtype='vol_spike',
            phase='shock',
            evidence_mode='direct',
            role='trigger',
            confidence=0.5,
            severity=0.5,
            trigger_value=1.0,
            threshold_snapshot={},
            source_features={},
            detector_metadata={},
            required_context_present=True,
            data_quality_flag='bad',
            merge_key=None,
            cooldown_until=None,
        )
