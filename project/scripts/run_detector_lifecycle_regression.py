from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from project.reliability.smoke_data import (
    build_smoke_dataset,
    materialize_smoke_promotion_inputs,
    run_engine_smoke,
    run_promotion_smoke,
    run_research_smoke,
)
from project.scripts.build_detector_governance_artifacts import build_governance_artifacts


def run_regression(root: Path) -> dict[str, object]:
    root.mkdir(parents=True, exist_ok=True)
    governance_dir = root / 'governance_artifacts'
    governance_summary = build_governance_artifacts(governance_dir)

    dataset = build_smoke_dataset(root / 'smoke_data', seed=20260101, storage_mode='auto')
    research_result = run_research_smoke(dataset)
    materialize_smoke_promotion_inputs(dataset, research_result)
    promotion_result = run_promotion_smoke(dataset, research_result)
    engine_result = run_engine_smoke(dataset)

    env = os.environ.copy()
    env['PYTHONPATH'] = str(REPO_ROOT) + (os.pathsep + env['PYTHONPATH'] if env.get('PYTHONPATH') else '')
    env['BACKTEST_DATA_ROOT'] = str(dataset.root)
    dry_run_id = 'detector_lifecycle_dry_run'
    dry_run_cmd = [sys.executable, '-m', 'project.pipelines.run_all', '--run_id', dry_run_id, '--symbols', 'BTCUSDT', '--start', '2024-01-01', '--end', '2024-01-02', '--dry_run', '1']
    dry_run = subprocess.run(dry_run_cmd, cwd=REPO_ROOT, env=env, capture_output=True, text=True)
    manifest_path = dataset.root / 'runs' / dry_run_id / 'run_manifest.json'

    summary = {
        'governance_summary': governance_summary,
        'research_output_dir': str(research_result['output_dir']),
        'promotion_output_dir': str(promotion_result['output_dir']),
        'engine_keys': sorted(engine_result.keys()) if isinstance(engine_result, dict) else [],
        'dry_run_returncode': int(dry_run.returncode),
        'dry_run_manifest_exists': manifest_path.exists(),
        'dry_run_stdout_snippet': dry_run.stdout[:500],
    }
    summary['status'] = 'success' if summary.get('dry_run_returncode') == 0 and summary.get('dry_run_manifest_exists') else 'failure'
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', default='')
    parser.add_argument('--json-out', default='')
    args = parser.parse_args()
    root = Path(args.root) if args.root else Path(tempfile.mkdtemp(prefix='detector-lifecycle-'))
    summary = run_regression(root)
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(summary, indent=2, sort_keys=True), encoding='utf-8')
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary.get('status') == 'success' else 1


if __name__ == '__main__':
    raise SystemExit(main())
