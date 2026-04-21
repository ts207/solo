from __future__ import annotations

import re

from project.scripts.build_repo_metrics import (
    collect_repo_metrics,
    render_repo_metrics_markdown,
    update_root_readme_metrics,
)
from project.tests.conftest import REPO_ROOT


def test_repo_metrics_match_repo_surface() -> None:
    metrics = collect_repo_metrics(REPO_ROOT)
    assert metrics["project_python_files"] >= 1000
    assert metrics["test_python_files"] >= 500
    assert metrics["spec_yaml_files"] >= 300
    assert metrics["docs_markdown_files"] >= 10
    assert metrics["top_packages"]


def test_repo_metrics_markdown_renders_key_sections() -> None:
    metrics = collect_repo_metrics(REPO_ROOT)
    markdown = render_repo_metrics_markdown(metrics)
    assert "# Repository Metrics" in markdown
    assert "## Largest Packages" in markdown
    assert "`research`" in markdown


def test_root_readme_metrics_can_be_refreshed_from_code_truth() -> None:
    metrics = collect_repo_metrics(REPO_ROOT)
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    refreshed = update_root_readme_metrics(readme, metrics)
    assert refreshed == readme
    assert re.search(rf"- {metrics['project_python_files']} Python modules under `project/`", refreshed)
    assert re.search(rf"- {metrics['test_python_files']} test files under `project/tests/`", refreshed)
    assert re.search(rf"- {metrics['spec_yaml_files']} YAML spec files under `spec/`", refreshed)
