import subprocess
from pathlib import Path

import pytest


def test_cli_help_contains_trigger_discovery_warnings():
    # Verify 'edge discover --help' shows 'triggers' subgroup
    result = subprocess.run(
        ["python3", "-m", "project.cli", "discover", "--help"], capture_output=True, text=True
    )
    assert result.returncode == 0
    assert "triggers" in result.stdout.lower()
    # Check for keywords instead of exact string
    assert "internal" in result.stdout.lower()
    assert "research" in result.stdout.lower()
    assert "lane" in result.stdout.lower()


def test_cli_triggers_help_contains_safety_language():
    # Verify 'edge discover triggers --help'
    result = subprocess.run(
        ["python3", "-m", "project.cli", "discover", "triggers", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    # Search for keywords due to line-wrapping in argparse output
    out = result.stdout.lower()
    assert "advanced" in out
    assert "internal" in out
    assert "trigger" in out
    assert "discovery" in out
    assert "lane" in out
    assert "proposal-generating" in out
    assert "no runtime effect" in out
    assert "manual review required" in out


def test_makefile_contains_advanced_targets():
    makefile_content = Path("Makefile").read_text()
    assert "# Advanced/Internal trigger discovery" in makefile_content
    assert "advanced-discover-triggers-parameter:" in makefile_content
    assert "advanced-discover-triggers-cluster:" in makefile_content


@pytest.mark.parametrize("cmd", ["parameter-sweep", "feature-cluster", "emit-registry-payload"])
def test_cli_triggers_subcommands_dispatch(cmd):
    # Just a smoke test for dispatch wiring (using --help for each)
    result = subprocess.run(
        ["python3", "-m", "project.cli", "discover", "triggers", cmd, "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "show this help" in result.stdout.lower()
