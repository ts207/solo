from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from project.events.registry import (
    build_detector_eligibility_matrix_rows,
    build_detector_migration_ledger_rows,
)


DOCS_ROOT = Path("docs/generated")


def _sorted_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return sorted(rows, key=lambda row: str(row["event_name"]))


def test_checked_in_detector_eligibility_matrix_matches_builder_output() -> None:
    path = DOCS_ROOT / "detector_eligibility_matrix.json"
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert _sorted_rows(payload) == _sorted_rows(build_detector_eligibility_matrix_rows())


def test_checked_in_detector_migration_ledger_matches_builder_output() -> None:
    path = DOCS_ROOT / "detector_migration_ledger.json"
    payload = json.loads(path.read_text(encoding="utf-8"))

    expected = _sorted_rows(build_detector_migration_ledger_rows())
    assert _sorted_rows(payload) == expected


def test_checked_in_detector_governance_summary_matches_builder_counts() -> None:
    path = DOCS_ROOT / "detector_governance_summary.json"
    payload = json.loads(path.read_text(encoding="utf-8"))

    migration_rows = build_detector_migration_ledger_rows()
    assert payload["governed_detectors"] == len(migration_rows)
    assert payload["migration_bucket_counts"] == dict(
        Counter(str(row["migration_bucket"]) for row in migration_rows)
    )
    assert payload["migration_target_counts"] == dict(
        Counter(str(row["target_state"]) for row in migration_rows)
    )
    assert payload["migration_owner_counts"] == dict(
        Counter(str(row["owner"]) for row in migration_rows)
    )
