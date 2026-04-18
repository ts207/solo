from __future__ import annotations

from project.events.calibration.registry import (
    build_calibration_matrix_rows,
    find_duplicate_calibration_keys,
    latest_calibration_artifact,
)
from project.events.registry import list_v2_detectors


def test_wave1_calibration_artifacts_exist() -> None:
    artifact = latest_calibration_artifact('LIQUIDITY_VACUUM', preferred_version='v2')
    assert artifact is not None
    assert artifact.threshold_version == '2.0'
    rows = build_calibration_matrix_rows(['LIQUIDITY_VACUUM', 'VOL_SHOCK'])
    assert {row['event_name'] for row in rows} == {'LIQUIDITY_VACUUM', 'VOL_SHOCK'}


def test_all_v2_detectors_have_calibration_artifacts() -> None:
    contracts = list_v2_detectors()
    rows = build_calibration_matrix_rows([contract.event_name for contract in contracts])
    calibrated = {row['event_name'] for row in rows if row['event_version'] == 'v2'}
    expected = {contract.event_name for contract in contracts}
    assert calibrated == expected
    assert len(rows) == len(expected)
    assert find_duplicate_calibration_keys(expected) == {}
    for row in rows:
        assert row['threshold_version'] == '2.0'
        assert row['symbol_group'] == 'major_crypto'
        assert row['timeframe_group'] == '5m'
        assert row['dataset_lineage']['calibration_input_dataset']
        assert row['training_period']['start']
        assert row['validation_period']['start']
        assert row['robustness']['status'] == 'baseline_fixture'
