from __future__ import annotations

import json
import subprocess

from project.scripts.check_calibration_version_bumps import (
    calibration_change_requires_version_bump,
    changed_versioned_fields,
    find_calibration_version_bump_violations,
    version_changed,
)


def _artifact(**overrides):
    payload = {
        "event_name": "VOL_SHOCK",
        "event_version": "v2",
        "threshold_version": "2.0",
        "calibration_mode": "rolling_quantile",
        "symbol_group": "major_crypto",
        "timeframe_group": "5m",
        "dataset_lineage": {"calibration_input_dataset": "baseline"},
        "training_period": {"start": "2023-01-01", "end": "2023-12-31"},
        "validation_period": {"start": "2024-01-01", "end": "2024-12-31"},
        "parameters": {"shock_quantile": 0.99},
        "robustness": {"status": "baseline_fixture"},
        "failure_notes": ["baseline"],
        "notes": "existing notes",
    }
    payload.update(overrides)
    return payload


def test_parameter_change_requires_threshold_or_detector_version_bump() -> None:
    old = _artifact()
    new = _artifact(parameters={"shock_quantile": 0.995})

    assert calibration_change_requires_version_bump(old, new) == ("parameters",)


def test_parameter_change_with_threshold_version_bump_is_allowed() -> None:
    old = _artifact()
    new = _artifact(threshold_version="2.1", parameters={"shock_quantile": 0.995})

    assert version_changed(old, new) is True
    assert calibration_change_requires_version_bump(old, new) == ()


def test_lineage_change_with_detector_version_bump_is_allowed() -> None:
    old = _artifact()
    new = _artifact(
        event_version="v3",
        dataset_lineage={"calibration_input_dataset": "empirical_recalibration"},
    )

    assert version_changed(old, new) is True
    assert calibration_change_requires_version_bump(old, new) == ()


def test_notes_and_robustness_review_changes_do_not_require_version_bump() -> None:
    old = _artifact()
    new = _artifact(
        notes="operator review updated",
        robustness={"status": "reviewed_baseline"},
        failure_notes=["baseline", "reviewed false positive cluster"],
    )

    assert changed_versioned_fields(old, new) == ()
    assert calibration_change_requires_version_bump(old, new) == ()


def test_git_diff_checker_flags_unversioned_parameter_change(tmp_path) -> None:
    repo = tmp_path / "repo"
    artifact_path = (
        repo
        / "project/events/calibration/artifacts/detectors/VOL_SHOCK/v2/default_5m/calibration.json"
    )
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_text(json.dumps(_artifact(), indent=2, sort_keys=True), encoding="utf-8")

    subprocess.run(["git", "init", "--initial-branch", "main"], cwd=repo, check=True, stdout=subprocess.PIPE)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=repo, check=True)
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-m", "baseline"], cwd=repo, check=True, stdout=subprocess.PIPE)

    artifact_path.write_text(
        json.dumps(_artifact(parameters={"shock_quantile": 0.995}), indent=2, sort_keys=True),
        encoding="utf-8",
    )

    violations = find_calibration_version_bump_violations(repo_root=repo, base_ref="HEAD")

    assert len(violations) == 1
    assert "VOL_SHOCK" in violations[0]
    assert "parameters" in violations[0]
