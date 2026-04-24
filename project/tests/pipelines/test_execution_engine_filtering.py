from __future__ import annotations

from pathlib import Path

import project.pipelines.execution_engine as engine


def test_filter_unsupported_flags_preserves_core_flags(tmp_path):
    script_path = tmp_path / "script.py"
    # Script does not contain any reference to flags
    script_path.write_text("print('hello')\n", encoding="utf-8")

    base_args = ["--run_id", "my_run", "--symbols", "BTCUSDT", "--config", "real_config.yaml"]

    # Core flags --run_id and --symbols should be preserved
    # Dangerous flag --config should be filtered out because it's not in the script
    filtered = engine._filter_unsupported_flags(script_path, base_args)

    assert "--run_id" in filtered
    assert "my_run" in filtered
    assert "--symbols" in filtered
    assert "BTCUSDT" in filtered
    assert "--config" not in filtered
    assert "real_config.yaml" not in filtered


def test_filter_unsupported_flags_allows_supported_dangerous_flags(tmp_path):
    script_path = tmp_path / "script.py"
    # Script explicitly mentions --config
    script_path.write_text("parser.add_argument('--config')\n", encoding="utf-8")

    base_args = ["--run_id", "my_run", "--config", "real_config.yaml"]

    filtered = engine._filter_unsupported_flags(script_path, base_args)

    assert "--run_id" in filtered
    assert "--config" in filtered
    assert "real_config.yaml" in filtered


def test_filter_unsupported_flags_handles_comments_naive_limitation(tmp_path):
    script_path = tmp_path / "script.py"
    # Script mentions --config but ONLY in a comment
    script_path.write_text(
        "# This script doesn't actually support --config\nprint('ok')\n", encoding="utf-8"
    )

    base_args = ["--config", "secret.yaml"]

    # Current implementation is a naive string check, so it will FAIL this test
    # (it will NOT filter it out). This is what we want to harden.
    filtered = engine._filter_unsupported_flags(script_path, base_args)

    # We WANT it to be filtered out even if it's in a comment.
    # If the current implementation is naive, this assertion will fail.
    assert "--config" not in filtered


def test_filter_unsupported_flags_with_multiple_values(tmp_path):
    script_path = tmp_path / "script.py"
    script_path.write_text("print('no flags here')\n", encoding="utf-8")

    # Test multiple core and dangerous flags
    base_args = [
        "--run_id",
        "r1",
        "--symbols",
        "S1,S2",
        "--config",
        "c1.yaml",
        "--experiment_config",
        "e1.yaml",
        "--other_flag",
        "val",
    ]

    filtered = engine._filter_unsupported_flags(script_path, base_args)

    # Core preserved
    assert "--run_id" in filtered
    assert "--symbols" in filtered
    # Dangerous filtered
    assert "--config" not in filtered
    assert "--experiment_config" not in filtered
    # Non-dangerous unknown flags are NOT filtered by current logic (they are passed through)
    # Wait, let's check execution_engine.py logic for unknown flags.
    assert "--other_flag" in filtered


def test_phase2_search_engine_script_preserves_experiment_config():
    script_path = (
        Path(__file__).resolve().parents[2] / "project" / "research" / "phase2_search_engine.py"
    )
    base_args = [
        "--run_id",
        "shakeout_test",
        "--experiment_config",
        "/tmp/experiment.yaml",
        "--program_id",
        "regime_shakeout",
        "--search_spec",
        "spec/search_space.yaml",
    ]

    filtered = engine._filter_unsupported_flags(script_path, base_args)

    assert "--experiment_config" in filtered
    assert "/tmp/experiment.yaml" in filtered


def test_phase2_candidate_discovery_script_preserves_experiment_config():
    script_path = (
        Path(__file__).resolve().parents[2]
        / "project"
        / "research"
        / "cli"
        / "candidate_discovery_cli.py"
    )
    base_args = [
        "--run_id",
        "shakeout_test",
        "--experiment_config",
        "/tmp/experiment.yaml",
        "--program_id",
        "regime_shakeout",
        "--event_type",
        "BASIS_DISLOC",
    ]

    filtered = engine._filter_unsupported_flags(script_path, base_args)

    assert "--experiment_config" in filtered
    assert "/tmp/experiment.yaml" in filtered
