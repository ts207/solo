from __future__ import annotations

from project.events.calibration.registry import build_calibration_matrix_rows, latest_calibration_artifact


def test_wave1_calibration_artifacts_exist() -> None:
    artifact = latest_calibration_artifact('LIQUIDITY_VACUUM', preferred_version='v2')
    assert artifact is not None
    assert artifact.threshold_version == '2.0'
    rows = build_calibration_matrix_rows(['LIQUIDITY_VACUUM', 'VOL_SHOCK'])
    assert {row['event_name'] for row in rows} == {'LIQUIDITY_VACUUM', 'VOL_SHOCK'}
