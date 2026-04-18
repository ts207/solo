from __future__ import annotations

from project.events.calibration.registry import build_calibration_matrix_rows, latest_calibration_artifact
from project.events.registry import get_detector_contract


def test_wave3_calibration_artifacts_exist() -> None:
    artifact = latest_calibration_artifact('CROSS_VENUE_DESYNC', preferred_version='v2')
    assert artifact is not None
    assert artifact.threshold_version == '2.0'
    rows = build_calibration_matrix_rows(['CROSS_VENUE_DESYNC', 'BETA_SPIKE_EVENT'])
    assert {row['event_name'] for row in rows} == {'CROSS_VENUE_DESYNC', 'BETA_SPIKE_EVENT'}


def test_wave3_governance_policy_is_applied() -> None:
    trigger = get_detector_contract('CROSS_VENUE_DESYNC')
    context = get_detector_contract('CROSS_ASSET_DESYNC_EVENT')
    regime = get_detector_contract('CORRELATION_BREAKDOWN_EVENT')
    assert trigger.runtime_default is True
    assert trigger.promotion_eligible is True
    assert context.role == 'context'
    assert context.runtime_default is True
    assert context.promotion_eligible is False
    assert context.primary_anchor_eligible is False
    assert regime.runtime_default is True
    assert regime.promotion_eligible is True
