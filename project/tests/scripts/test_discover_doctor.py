from __future__ import annotations

import subprocess
import sys

from project.scripts.discover_doctor import build_discover_doctor_report


def test_discover_doctor_missing_run_blocks(tmp_path):
    report = build_discover_doctor_report(
        run_id="missing_run",
        data_root=tmp_path,
    )

    assert report["kind"] == "discover_doctor"
    assert report["run_id"] == "missing_run"
    assert report["status"] == "blocked"
    assert "edge validate run" in report["forbidden_actions"]


def test_discover_doctor_cli_missing_run_exits_nonzero(tmp_path):
    result = subprocess.run(
        [
            sys.executable,
            "project/scripts/discover_doctor.py",
            "--run_id",
            "missing_run",
            "--data_root",
            str(tmp_path),
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert '"kind": "discover_doctor"' in result.stdout
    assert '"status": "blocked"' in result.stdout
