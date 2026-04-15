from __future__ import annotations

from pathlib import Path

import pytest

from project.core.exceptions import DataIntegrityError
from project.research.search_intelligence import _safe_read_legacy_ledger


def test_safe_read_legacy_ledger_raises_on_corrupted_existing_file(tmp_path: Path) -> None:
    ledger_path = tmp_path / "tested_ledger.parquet"
    ledger_path.write_bytes(b"NOTPARQUET")

    with pytest.raises(DataIntegrityError):
        _safe_read_legacy_ledger(ledger_path)
