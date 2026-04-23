from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from project.tests.conftest import REPO_ROOT


def test_detector_lifecycle_regression(tmp_path: Path):
    json_out = tmp_path / 'summary.json'
    cmd = [sys.executable, 'project/scripts/run_detector_lifecycle_regression.py', '--root', str(tmp_path / 'run'), '--json-out', str(json_out)]
    result = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    summary = json.loads(json_out.read_text(encoding='utf-8'))
    assert summary['status'] == 'success'
    assert summary['governance_summary']['runtime_non_v2'] == 0
    assert summary['dry_run_manifest_exists'] is True
