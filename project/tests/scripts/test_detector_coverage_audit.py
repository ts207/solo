from __future__ import annotations

import inspect
import json

from project.events.detectors.exhaustion import (
    FlowExhaustionDetector,
    PostDeleveragingReboundDetector,
)
from project.events.detectors.funding import (
    FundingExtremeOnsetDetector,
    FundingNormalizationDetector,
    FundingPersistenceDetector,
)
from project.events.detectors.liquidity import (
    DirectLiquidityStressDetector,
    LiquidityStressDetector,
    ProxyLiquidityStressDetector,
)
from project.events.families.liquidation import LiquidationCascadeDetector
from project.events.families.temporal import SessionCloseDetector, SessionOpenDetector
from project.scripts import detector_coverage_audit as audit


def test_run_audit_returns_structured_report() -> None:
    report = audit.run_audit()

    assert report["summary"]["schema_version"] == "detector_coverage_audit_v2"
    assert isinstance(report["detectors"], list)
    assert isinstance(report["issues"], list)
    assert report["summary"]["active_event_count"] == len(report["detectors"])
    assert report["summary"]["registered_event_count"] == len(report["detectors"])
    assert (
        report["summary"]["registered_detector_entry_count"]
        >= report["summary"]["registered_event_count"]
    )
    assert "maturity_counts" in report["summary"]


def test_render_markdown_contains_sections() -> None:
    report = {
        "summary": {
            "schema_version": "detector_coverage_audit_v2",
            "status": "passed",
            "active_event_count": 2,
            "registered_event_count": 2,
            "registered_detector_entry_count": 3,
            "issue_count": 0,
            "error_count": 0,
            "warning_count": 0,
            "maturity_counts": {"production": 1, "standard": 1},
            "evidence_tier_counts": {"direct": 1, "proxy": 1},
        },
        "detectors": [
            {
                "event_type": "A",
                "class_name": "DetectorA",
                "maturity_tier": "production",
                "evidence_tier": "direct",
            },
            {
                "event_type": "B",
                "class_name": "DetectorB",
                "maturity_tier": "standard",
                "evidence_tier": "proxy",
            },
        ],
        "issues": [],
    }

    markdown = audit.render_markdown(report)
    assert "# Detector Coverage Audit" in markdown
    assert "## Maturity Counts" in markdown
    assert "## Evidence Tier Counts" in markdown
    assert "## Detector Inventory" in markdown
    assert "- None" in markdown


def test_main_writes_json_and_markdown_outputs(tmp_path) -> None:
    json_out = tmp_path / "detector_audit.json"
    md_out = tmp_path / "detector_audit.md"

    rc = audit.main(["--json-out", str(json_out), "--md-out", str(md_out)])

    assert rc in {0, 1}
    payload = json.loads(json_out.read_text(encoding="utf-8"))
    assert payload["summary"]["schema_version"] == "detector_coverage_audit_v2"
    assert md_out.read_text(encoding="utf-8").startswith("# Detector Coverage Audit")


def test_registered_alias_without_spec_is_not_reported_as_gap() -> None:
    assert (
        audit._is_registered_alias_without_spec("VOL_REGIME_SHIFT", {"VOL_REGIME_SHIFT_EVENT"})
        is True
    )
    assert (
        audit._is_registered_alias_without_spec("LIQUIDITY_STRESS_DIRECT", {"LIQUIDITY_SHOCK"})
        is False
    )


def test_parameterized_detector_families_are_not_flagged_as_hardcoded() -> None:
    detector_classes = [
        FundingExtremeOnsetDetector,
        FundingNormalizationDetector,
        FundingPersistenceDetector,
        DirectLiquidityStressDetector,
        LiquidityStressDetector,
        ProxyLiquidityStressDetector,
        FlowExhaustionDetector,
        PostDeleveragingReboundDetector,
        LiquidationCascadeDetector,
        SessionOpenDetector,
        SessionCloseDetector,
    ]

    for detector_cls in detector_classes:
        assert audit._has_hardcoded_parameters(detector_cls) is False


def test_has_hardcoded_parameters_does_not_swallow_unexpected_runtime_errors(monkeypatch) -> None:
    def _boom(_obj):
        raise RuntimeError("unexpected inspect failure")

    monkeypatch.setattr(inspect, "getsource", _boom)
    try:
        audit._has_hardcoded_parameters(FundingExtremeOnsetDetector)
    except RuntimeError as exc:
        assert "unexpected inspect failure" in str(exc)
    else:
        raise AssertionError("expected RuntimeError to propagate")


def test_run_audit_reports_evidence_tier_counts() -> None:
    report = audit.run_audit()

    assert "evidence_tier_counts" in report["summary"]
    assert report["summary"]["evidence_tier_counts"].get("proxy", 0) >= 0
    assert report["summary"]["evidence_tier_counts"].get("hybrid", 0) >= 1
    assert all("evidence_tier" in row for row in report["detectors"])
