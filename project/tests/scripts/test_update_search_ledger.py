from __future__ import annotations

from project.scripts import update_search_ledger


def test_update_search_ledger_script_exposes_main() -> None:
    assert callable(update_search_ledger.main)
