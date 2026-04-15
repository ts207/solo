import subprocess
import sys


def test_cli_help():
    cmds = ["discover", "validate", "promote", "deploy"]
    for cmd in cmds:
        result = subprocess.run(
            [sys.executable, "-m", "project.cli", cmd, "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert cmd in result.stdout


def test_legacy_operator_command_removed():
    result = subprocess.run(
        [sys.executable, "-m", "project.cli", "operator", "preflight", "--proposal", "fake.yaml"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2
    assert "invalid choice: 'operator'" in result.stderr
