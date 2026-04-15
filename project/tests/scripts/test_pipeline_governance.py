from __future__ import annotations

import json

from project.scripts import pipeline_governance as gov


def test_run_audit_returns_structured_report() -> None:
    report = gov.run_audit()
    assert report["summary"]["schema_version"] == "pipeline_governance_audit_v1"
    assert isinstance(report["checks"], list)
    assert isinstance(report["issues"], list)
    check_names = {check["name"] for check in report["checks"]}
    assert {"features", "events", "contracts"}.issubset(check_names)


def test_render_markdown_contains_check_sections() -> None:
    report = {
        "summary": {
            "schema_version": "pipeline_governance_audit_v1",
            "status": "passed",
            "check_count": 1,
            "issue_count": 0,
            "error_count": 0,
            "warning_count": 0,
        },
        "checks": [{"name": "contracts", "issues": [], "spec_count": 0}],
        "issues": [],
    }
    markdown = gov.render_markdown(report)
    assert "# Pipeline Governance Audit" in markdown
    assert "### contracts" in markdown
    assert "No issues." in markdown


def test_main_writes_json_and_markdown_outputs(tmp_path) -> None:
    json_out = tmp_path / "audit.json"
    md_out = tmp_path / "audit.md"
    rc = gov.main(["--audit", "--json-out", str(json_out), "--md-out", str(md_out)])
    assert rc in {0, 1}
    payload = json.loads(json_out.read_text(encoding="utf-8"))
    assert payload["summary"]["schema_version"] == "pipeline_governance_audit_v1"
    assert md_out.read_text(encoding="utf-8").startswith("# Pipeline Governance Audit")


def test_main_sync_writes_feature_catalog(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(gov, "PROJECT_DIR", tmp_path / "project")
    monkeypatch.setattr(gov, "SPEC_ROOT", tmp_path / "spec")
    (gov.SPEC_ROOT / "features").mkdir(parents=True, exist_ok=True)
    (gov.SPEC_ROOT / "features" / "demo.yaml").write_text(
        "feature_family: demo\nparams:\n  lookback: 5\n",
        encoding="utf-8",
    )
    rc = gov.main(["--sync"])
    assert rc == 0
    catalog = json.loads(
        (gov.PROJECT_DIR / "schemas" / "feature_catalog.json").read_text(encoding="utf-8")
    )
    assert catalog["features"]["demo"]["family"] == "demo"
