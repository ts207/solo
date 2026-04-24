from __future__ import annotations

import json

from project.tests.conftest import REPO_ROOT


def test_generated_docs_inventory_exists() -> None:
    generated = REPO_ROOT / "docs" / "generated"
    assert (generated / "repo_metrics.md").exists()
    assert (generated / "repo_metrics.json").exists()
    assert (generated / "system_map.md").exists()
    assert (generated / "contract_strictness_inventory.md").exists()
    assert (generated / "detector_governance_summary.json").exists()
    assert (generated / "legacy_surface_inventory.md").exists()
    assert (generated / "legacy_surface_inventory.json").exists()


def test_generated_repo_metrics_json_matches_expected_schema() -> None:
    payload = json.loads(
        (REPO_ROOT / "docs" / "generated" / "repo_metrics.json").read_text(encoding="utf-8")
    )
    assert payload["schema_version"] == "repo_metrics_v1"
    assert payload["project_python_files"] >= 1000
    assert payload["test_python_files"] >= 500
    assert payload["spec_yaml_files"] >= 300
    assert payload["top_packages"]


def test_docs_readme_points_to_generated_refresh_command() -> None:
    text = (REPO_ROOT / "docs" / "README.md").read_text(encoding="utf-8")
    assert "docs/generated/" in text
    assert "project/scripts/refresh_docs_governance.py" in text


def test_generated_legacy_surface_inventory_matches_expected_schema() -> None:
    payload = json.loads(
        (REPO_ROOT / "docs" / "generated" / "legacy_surface_inventory.json").read_text(
            encoding="utf-8"
        )
    )
    assert payload["schema_version"] == "legacy_surface_inventory_v1"
    assert payload["legacy_detector_count"] >= 1
    assert payload["legacy_detectors_retired_safe"] <= payload["legacy_detector_count"]
    assert payload["file_count"] >= 1
    assert payload["category_counts"]
