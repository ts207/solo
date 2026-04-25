from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from project.tests.conftest import REPO_ROOT


def test_build_detector_governance_artifacts(tmp_path: Path):
    out_dir = tmp_path / 'generated'
    cmd = [sys.executable, 'project/scripts/build_detector_governance_artifacts.py', '--output-dir', str(out_dir)]
    result = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    summary = json.loads((out_dir / 'detector_governance_summary.json').read_text(encoding='utf-8'))
    assert summary['governed_detectors'] == 71
    assert summary['runtime_non_v2'] == 0
    assert summary['runtime_v2'] == 3

    assert summary['alias_count'] == 3
    assert summary['band_counts']['deployable_core'] == 3
    assert summary['band_counts']['context_only'] == 5
    assert summary['migration_bucket_counts']['runtime_core_first'] == 3
    assert summary['migration_target_counts']['migrate_to_v2'] == 28
    assert summary['migration_owner_counts']['workstream_c'] == 28
    assert (out_dir / 'detector_version_coverage.md').exists()
    assert (out_dir / 'detector_alias_policy.md').exists()
    assert (out_dir / 'detector_alias_policy.json').exists()
    assert (out_dir / 'detector_eligibility_matrix.md').exists()
    assert (out_dir / 'detector_eligibility_matrix.json').exists()
    assert (out_dir / 'detector_migration_ledger.md').exists()
    assert (out_dir / 'detector_migration_ledger.json').exists()
    assert (out_dir / 'legacy_detector_retirement.md').exists()
