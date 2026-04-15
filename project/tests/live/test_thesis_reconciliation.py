from __future__ import annotations

import pytest

from project.core.exceptions import DataIntegrityError
from project.live.thesis_reconciliation import reconcile_thesis_batch
from project.live.thesis_store import ThesisStore


def test_reconcile_thesis_batch_raises_on_malformed_previous_batch_metadata(tmp_path):
    persist_dir = tmp_path / "persist"
    persist_dir.mkdir(parents=True, exist_ok=True)
    (persist_dir / "thesis_batch_metadata.json").write_text("{not valid json", encoding="utf-8")

    current_store = ThesisStore([], run_id="RUN_CURRENT")

    with pytest.raises(DataIntegrityError, match="Failed to read thesis batch metadata"):
        reconcile_thesis_batch(current_store, persist_dir, thesis_manager_state={})
