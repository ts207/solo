from __future__ import annotations

import json

from project.scripts import build_event_deep_analysis_suite as suite


EXPECTED_TASK_IDS = [
    "01_event_universe",
    "02_event_contracts",
    "03_detector_fidelity",
    "04_maturity_tiers",
    "05_threshold_calibration",
    "06_overlap_collisions",
    "07_regime_restrictions",
    "08_data_dependencies",
    "09_ci_event_guards",
    "10_synthesis",
]


def test_build_report_returns_ten_ordered_tasks() -> None:
    report = suite.build_report()

    assert report["summary"]["task_count"] == 10
    assert [task["id"] for task in report["tasks"]] == EXPECTED_TASK_IDS
    assert all("title" in task for task in report["tasks"])
    assert all("status" in task for task in report["tasks"])
    assert all("summary" in task for task in report["tasks"])
    assert all("details" in task for task in report["tasks"])
    assert all("verification_commands" in task for task in report["tasks"])


def test_render_markdown_contains_all_task_titles() -> None:
    report = suite.build_report()
    markdown = suite.render_markdown(report)

    assert markdown.startswith("# Event Deep Analysis Suite")
    for task in report["tasks"]:
        assert task["title"] in markdown


def test_main_writes_expected_outputs(tmp_path) -> None:
    rc = suite.main(["--base-dir", str(tmp_path)])
    assert rc == 0

    payload = json.loads((tmp_path / "event_deep_analysis_suite.json").read_text(encoding="utf-8"))
    assert payload["summary"]["task_count"] == 10
    assert (tmp_path / "event_deep_analysis_suite.md").read_text(encoding="utf-8").startswith(
        "# Event Deep Analysis Suite"
    )
