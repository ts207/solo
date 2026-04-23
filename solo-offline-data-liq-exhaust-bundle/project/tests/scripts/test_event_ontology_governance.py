from __future__ import annotations

from project.scripts.build_event_ontology_artifacts import build_outputs
from project.scripts.event_ontology_audit import run_audit


def test_event_ontology_audit_passes():
    report = run_audit()
    assert report["summary"]["status"] == "passed", report["issues"]


def test_event_ontology_artifacts_cover_expected_outputs():
    outputs = build_outputs()
    names = {path.name for path in outputs}
    assert "event_ontology_mapping.md" in names
    assert "canonical_to_raw_event_map.md" in names
    assert "composite_event_catalog.md" in names
    assert "context_tag_catalog.md" in names
    assert "strategy_construct_catalog.md" in names
