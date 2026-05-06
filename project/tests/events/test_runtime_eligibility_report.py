from __future__ import annotations

from pathlib import Path

import yaml

from project.events.runtime_eligibility_report import build_runtime_eligibility_report, render_runtime_eligibility_markdown


def test_runtime_eligibility_report_explains_blocking_reasons(tmp_path: Path) -> None:
    graph = {
        "metadata": {"generated_at_utc": "2026-01-01T00:00:00Z"},
        "events": {
            "BLOCKED_EVENT": {
                "canonical_family": "TEST_FAMILY",
                "detector_name": "BlockedDetector",
                "operational_role": "trigger",
                "promotion_eligible": False,
                "runtime_eligible": False,
                "primary_anchor_eligible": False,
                "deployment_disposition": "secondary_or_confirm",
                "detector_band": "context_only",
                "eligibility": {
                    "research_planning_allowed": True,
                    "paper_anchor_allowed": False,
                    "shadow_runtime_allowed": False,
                    "micro_live_allowed": False,
                    "scaled_live_allowed": False,
                },
            },
            "LIVE_EVENT": {
                "canonical_family": "TEST_FAMILY",
                "detector_name": "LiveDetector",
                "operational_role": "trigger",
                "promotion_eligible": True,
                "runtime_eligible": True,
                "primary_anchor_eligible": True,
                "deployment_disposition": "live_eligible",
                "eligibility": {
                    "research_planning_allowed": True,
                    "paper_anchor_allowed": True,
                    "shadow_runtime_allowed": True,
                    "micro_live_allowed": True,
                    "scaled_live_allowed": False,
                },
            },
        },
    }
    path = tmp_path / "domain_graph.yaml"
    path.write_text(yaml.safe_dump(graph), encoding="utf-8")

    report = build_runtime_eligibility_report(path)

    assert report["totals"]["events"] == 2
    assert report["totals"]["runtime_eligible"] == 1
    blocked = next(row for row in report["rows"] if row["event_id"] == "BLOCKED_EVENT")
    assert "runtime_eligible=false" in blocked["blocking_reason"]
    assert "detector_band=context_only" in blocked["blocking_reason"]


def test_runtime_eligibility_markdown_contains_operator_columns(tmp_path: Path) -> None:
    graph = {"metadata": {}, "events": {"A": {"canonical_family": "F", "detector_name": "D", "promotion_eligible": False, "runtime_eligible": False, "eligibility": {}}}}
    path = tmp_path / "domain_graph.yaml"
    path.write_text(yaml.safe_dump(graph), encoding="utf-8")

    markdown = render_runtime_eligibility_markdown(build_runtime_eligibility_report(path))

    assert "| Event | Family | Detector | Role | Promotion | Runtime | Paper | Live | Blocking reason |" in markdown
    assert "A" in markdown
