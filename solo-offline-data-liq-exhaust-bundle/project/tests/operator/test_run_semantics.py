
from __future__ import annotations

import json
from pathlib import Path

from project.operator.run_semantics import classify_terminal_status


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding='utf-8')


def test_classify_terminal_status_mechanical_failure(tmp_path):
    data_root = tmp_path / 'data'
    run_id = 'run1'
    _write_json(data_root / 'runs' / run_id / 'run_manifest.json', {
        'run_id': run_id,
        'status': 'failed',
        'planned_stages': ['build_features'],
        'failed_stage': 'build_features',
        'symbols': 'BTCUSDT',
    })
    _write_json(data_root / 'runs' / run_id / 'build_features.json', {
        'run_id': run_id,
        'stage': 'build_features',
        'stage_instance_id': 'build_features',
        'started_at': '2026-03-31T00:00:00+00:00',
        'finished_at': '2026-03-31T00:00:02+00:00',
        'status': 'failed',
        'parameters': {}, 'inputs': [], 'outputs': [], 'spec_hashes': {}, 'ontology_spec_hash': 'abc'
    })
    payload = classify_terminal_status(run_id=run_id, manifest={'run_id': run_id, 'status': 'failed', 'failed_stage': 'build_features'}, data_root=data_root)
    assert payload['terminal_status'] == 'failed_mechanical'
    assert payload['resume_recommended'] is True
