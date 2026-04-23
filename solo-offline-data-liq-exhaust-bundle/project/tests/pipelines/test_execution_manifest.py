from __future__ import annotations

import json

import pytest

from project.core.exceptions import DataIntegrityError
from project.pipelines import execution_manifest as manifest


def test_validate_stage_manifest_on_disk_returns_false_for_invalid_json(tmp_path) -> None:
    manifest_path = tmp_path / "stage.json"
    manifest_path.write_text("{bad json", encoding="utf-8")

    ok, message = manifest.validate_stage_manifest_on_disk(
        manifest_path, allow_failed_minimal=False
    )

    assert ok is False
    assert "invalid manifest JSON" in message


def test_validate_stage_manifest_on_disk_propagates_unexpected_runtime_errors(
    monkeypatch, tmp_path
) -> None:
    manifest_path = tmp_path / "stage.json"
    manifest_path.write_text("{}", encoding="utf-8")

    def _boom(_text):
        raise RuntimeError("unexpected parse failure")

    monkeypatch.setattr(manifest.json, "loads", _boom)

    with pytest.raises(RuntimeError, match="unexpected parse failure"):
        manifest.validate_stage_manifest_on_disk(manifest_path, allow_failed_minimal=False)


def test_validate_stage_manifest_on_disk_raises_data_integrity_error_for_schema_violation(
    tmp_path,
) -> None:
    manifest_path = tmp_path / "stage.json"
    manifest_path.write_text(json.dumps({"status": "success"}), encoding="utf-8")

    with pytest.raises(DataIntegrityError, match="manifest schema validation failed"):
        manifest.validate_stage_manifest_on_disk(manifest_path, allow_failed_minimal=False)
