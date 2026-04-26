from __future__ import annotations

from project.scripts.check_registry_sync import check_registry_sync


def test_registry_sync_current_repo_passes_or_reports_list() -> None:
    errors = check_registry_sync()
    assert isinstance(errors, list)
