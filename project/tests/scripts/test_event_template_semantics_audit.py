from __future__ import annotations

import json

from project.scripts import event_template_semantics_audit as audit


def test_run_audit_reports_key_event_template_findings() -> None:
    report = audit.run_audit()

    assert report["summary"]["active_event_count"] == 70
    assert report["summary"]["status"] == "passed"
    assert report["summary"]["default_template_event_count"] == 0
    assert report["summary"]["missing_event_template_row_count"] == 0
    assert report["summary"]["unregistered_template_event_count"] == 0
    assert report["summary"]["runtime_template_drop_event_count"] == 0
    assert report["summary"]["operator_compatibility_override_count"] == 0

    assert report["events_using_default_template_set"] == []
    assert report["events_missing_event_template_row"] == []
    assert report["events_with_unregistered_templates"] == {}
    assert "POST_DELEVERAGING_REBOUND" in report["intentional_runtime_template_suppression"]
    assert "SEQ_VOL_COMP_THEN_BREAKOUT" in report["intentional_runtime_template_suppression"]


def test_render_markdown_includes_runtime_sections() -> None:
    report = audit.run_audit()

    markdown = audit.render_markdown(report)

    assert "# Event Template Semantics Audit" in markdown
    assert "## Events With Unregistered Templates" in markdown
    assert "## Runtime Template Drops" in markdown
    assert "## Intentional Runtime Suppression" in markdown
    assert "## Operator Compatibility Overrides" in markdown


def test_main_writes_outputs(tmp_path) -> None:
    json_out = tmp_path / "event_template_semantics_audit.json"
    md_out = tmp_path / "event_template_semantics_audit.md"

    rc = audit.main(["--json-out", str(json_out), "--md-out", str(md_out)])

    assert rc == 0
    payload = json.loads(json_out.read_text(encoding="utf-8"))
    assert payload["summary"]["active_event_count"] == 70
    assert md_out.read_text(encoding="utf-8").startswith("# Event Template Semantics Audit")
