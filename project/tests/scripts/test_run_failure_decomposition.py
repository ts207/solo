import subprocess
import sys
from pathlib import Path

def test_run_failure_decomposition_cli_help():
    result = subprocess.run(
        [sys.executable, "project/scripts/run_failure_decomposition.py", "--help"],
        capture_output=True,
        text=True
    )
    assert result.returncode == 0
    assert "--source-run-id" in result.stdout
