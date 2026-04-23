from __future__ import annotations

from project.events.calibration.registry import build_calibration_matrix_rows, latest_calibration_artifact
from project.events.registry import get_detector_contract


def test_wave2_calibration_artifacts_exist() -> None:
    artifact = latest_calibration_artifact('BASIS_DISLOC', preferred_version='v2')
    assert artifact is not None
    assert artifact.threshold_version == '2.0'
    rows = build_calibration_matrix_rows(['BASIS_DISLOC', 'OI_FLUSH'])
    assert {row['event_name'] for row in rows} == {'BASIS_DISLOC', 'OI_FLUSH'}


def test_wave2_governance_policy_is_applied() -> None:
    strong = get_detector_contract('FND_DISLOC')
    conservative = get_detector_contract('FUNDING_NORMALIZATION_TRIGGER')
    assert strong.runtime_default is True
    assert strong.promotion_eligible is True
    assert conservative.runtime_default is False
    assert conservative.promotion_eligible is True
    assert conservative.primary_anchor_eligible is False
