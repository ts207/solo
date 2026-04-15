from __future__ import annotations

from pathlib import Path

from project.scripts import spec_qa_linter


def test_check_artifacts_reports_runtime_artifacts_separately(tmp_path: Path, capsys) -> None:
    specs = {
        "C_TEST": {
            "artifacts": [
                {"path": "spec/cost_model.yaml"},
                {"path": "data/runs/<run_id>/run_manifest.json"},
            ]
        }
    }

    spec_qa_linter.check_artifacts(specs, tmp_path)

    captured = capsys.readouterr().out
    assert "REPORT: Missing authored artifacts:" in captured
    assert "C_TEST: spec/cost_model.yaml" in captured
    assert "REPORT: Runtime artifacts not statically checked:" in captured
    assert "C_TEST: data/runs/<run_id>/run_manifest.json" in captured


def test_check_artifacts_success_still_mentions_runtime_skip(tmp_path: Path, capsys) -> None:
    authored = tmp_path / "spec" / "cost_model.yaml"
    authored.parent.mkdir(parents=True, exist_ok=True)
    authored.write_text("version: 1\n", encoding="utf-8")
    specs = {
        "C_TEST": {
            "artifacts": [
                {"path": "spec/cost_model.yaml"},
                {"path": "data/lake/events/events.parquet"},
            ]
        }
    }

    spec_qa_linter.check_artifacts(specs, tmp_path)

    captured = capsys.readouterr().out
    assert "SUCCESS: All authored artifacts exist." in captured
    assert "REPORT: Runtime artifacts not statically checked:" in captured


def test_check_artifacts_strict_runtime_mode_fails_on_missing_runtime_artifact(
    tmp_path: Path, capsys
) -> None:
    specs = {
        "C_TEST": {
            "artifacts": [
                {"path": "data/runs/<run_id>/run_manifest.json"},
            ]
        }
    }

    try:
        spec_qa_linter.check_artifacts(
            specs,
            tmp_path,
            strict_runtime_artifacts=True,
            runtime_placeholder_values={"run_id": "strict_run"},
        )
    except SystemExit as exc:
        assert exc.code == 1
    else:
        raise AssertionError("Expected strict runtime artifact enforcement to fail.")

    captured = capsys.readouterr().out
    assert "ERROR: Missing runtime artifacts under strict enforcement:" in captured
    assert "strict_run" in captured


def test_check_artifacts_strict_runtime_mode_accepts_existing_runtime_artifact(
    tmp_path: Path, capsys
) -> None:
    runtime_artifact = tmp_path / "data" / "runs" / "strict_run" / "run_manifest.json"
    runtime_artifact.parent.mkdir(parents=True, exist_ok=True)
    runtime_artifact.write_text("{}", encoding="utf-8")
    specs = {
        "C_TEST": {
            "artifacts": [
                {"path": "data/runs/<run_id>/run_manifest.json"},
            ]
        }
    }

    spec_qa_linter.check_artifacts(
        specs,
        tmp_path,
        strict_runtime_artifacts=True,
        runtime_placeholder_values={"run_id": "strict_run"},
    )

    captured = capsys.readouterr().out
    assert "SUCCESS: All authored artifacts exist." in captured
    assert "SUCCESS: All runtime artifacts resolved under strict enforcement." in captured
