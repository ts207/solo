
from __future__ import annotations

import json
from pathlib import Path

from project.pipelines import run_all_finalize


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding='utf-8')


def test_finalize_successful_run_stamps_terminal_status(tmp_path):
    data_root = tmp_path / 'data'
    run_id = 'ok_run'
    _write_json(data_root / 'runs' / run_id / 'run_manifest.json', {
        'run_id': run_id,
        'status': 'running',
        'planned_stage_instances': ['build_features'],
        'planned_stages': ['build_features'],
        'symbols': 'BTCUSDT',
        'stage_timings_sec': {},
        'stage_instance_timings_sec': {},
    })
    out = tmp_path / 'features.parquet'
    out.write_text('x', encoding='utf-8')
    _write_json(data_root / 'runs' / run_id / 'build_features.json', {
        'run_id': run_id,
        'stage': 'build_features',
        'stage_instance_id': 'build_features',
        'started_at': '2026-03-29T00:00:00+00:00',
        'finished_at': '2026-03-29T00:00:10+00:00',
        'status': 'success',
        'parameters': {},
        'inputs': [],
        'outputs': [{'path': str(out)}],
        'spec_hashes': {},
        'ontology_spec_hash': 'sha256:abc',
    })
    captured = {}
    rc = run_all_finalize.finalize_successful_run(
        run_manifest={'run_id': run_id, 'status': 'running', 'symbols': 'BTCUSDT'},
        run_id=run_id,
        preflight={'emit_run_hash_requested': False, 'research_compare_baseline_run_id': ''},
        stage_execution={'checklist_decision': None, 'auto_continue_applied': False, 'auto_continue_reason': '', 'non_production_overrides': []},
        stage_timings=[],
        stage_instance_timings=[],
        finalize_run_manifest=lambda run_manifest, status, **kwargs: run_manifest.update({'status': status, **({'terminal_status': kwargs['terminal_status']} if 'terminal_status' in kwargs else {})}),
        apply_run_terminal_audit=lambda *_args, **_kwargs: None,
        maybe_emit_run_hash=lambda *_args, **_kwargs: None,
        write_run_manifest=lambda _run_id, manifest: captured.update(manifest),
        write_run_kpi_scorecard=lambda *_args, **_kwargs: None,
        print_artifact_summary=lambda *_args, **_kwargs: None,
        data_root=data_root,
    )
    assert rc == 0
    assert captured['terminal_status'] == 'completed'
