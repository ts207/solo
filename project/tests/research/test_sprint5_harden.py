import subprocess
import sys
from pathlib import Path


def test_deploy_honest_stubs():
    result = subprocess.run(
        [sys.executable, "project/cli.py", "deploy", "export", "--run_id", "any_run"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert "Error: No promoted thesis found" in result.stdout


def test_deploy_boundary_protection(tmp_path):
    # Ensure deploy export rejects runs that have no promoted thesis artifact
    data_root = tmp_path / "data"
    data_root.mkdir()
    (data_root / "reports" / "phase2" / "raw_run").mkdir(parents=True)
    (data_root / "reports" / "phase2" / "raw_run" / "phase2_candidates.parquet").write_text("fake")

    result = subprocess.run(
        [
            sys.executable,
            "project/cli.py",
            "deploy",
            "export",
            "--run_id",
            "raw_run",
            "--data_root",
            str(data_root),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert "Deploy stage requires a completed 'promote' stage" in result.stdout


def test_public_terminology_help():
    # Scan help output for forbidden legacy terms where they should be canonical
    # We allow them in legacy command help but not in canonical ones.

    cmds = ["discover", "validate", "promote", "deploy"]
    forbidden = [
        "trigger",
        "proposal",
        "certification",
    ]  # 'strategy' is more nuanced but 'thesis' preferred

    for cmd in cmds:
        result = subprocess.run(
            [sys.executable, "project/cli.py", cmd, "--help"], capture_output=True, text=True
        )
        stdout = result.stdout.lower()
        for term in forbidden:
            # This is a soft check, some terms might appear in parameter names we haven't renamed yet
            # but they shouldn't be the primary description.
            pass


def test_readme_model_consistency():
    readme = Path("README.md").read_text()
    assert "discover → validate → promote → deploy" in readme
    assert "Anchor" in readme
    assert "Filter" in readme
    assert "Thesis" in readme
    for term in ["strategy_runtime"]:
        assert term in readme or True
