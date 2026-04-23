from __future__ import annotations

import sys
from pathlib import Path

from project.scripts import ontology_consistency_audit as audit


def test_ontology_audit_fails_closed_on_unresolved_state_source_mapping(monkeypatch):
    monkeypatch.setattr(
        audit,
        "normalize_state_registry_records",
        lambda _state_registry: [
            {
                "state_id": "FAKE_STATE",
                "source_event_type": "MISSING_EVENT",
            }
        ],
    )
    monkeypatch.setattr(audit, "materialized_state_ids", lambda: [])
    monkeypatch.setattr(
        audit,
        "validate_state_registry_source_events",
        lambda **_kwargs: [
            "state_id=FAKE_STATE has source_event_type=MISSING_EVENT not present in canonical registry"
        ],
    )

    report = audit.run_audit(Path("."))

    assert report["states"]["states_with_missing_source_event"] == ["FAKE_STATE"]
    assert any(issue.startswith("state_source_event_issues=") for issue in report["failures"])


def test_ontology_audit_main_exits_nonzero_on_failures(monkeypatch):
    monkeypatch.setattr(
        audit,
        "run_audit",
        lambda _repo_root: {
            "failures": ["state_source_event_issues=FAKE_STATE"],
            "implemented_contract": {},
            "counts": {},
            "states": {},
        },
    )
    monkeypatch.setattr(sys, "argv", ["ontology_consistency_audit.py", "--repo-root", "."])

    assert audit.main() == 1
