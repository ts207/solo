from __future__ import annotations

from project.scripts import update_results_index


def test_update_results_index_script_exposes_main() -> None:
    assert callable(update_results_index.main)
