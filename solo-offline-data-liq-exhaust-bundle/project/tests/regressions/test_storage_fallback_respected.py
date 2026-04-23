from __future__ import annotations

from pathlib import Path

import pandas as pd

from project.io.utils import read_parquet, write_parquet
from project.reliability.regression_checks import assert_storage_fallback_respected


def test_storage_fallback_respected(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("BACKTEST_FORCE_CSV_FALLBACK", "1")
    df = pd.DataFrame({"a": [1, 2]})
    actual, storage = write_parquet(df, tmp_path / "artifact.parquet")
    assert storage == "parquet"
    assert actual == tmp_path / "artifact.parquet"
    assert_storage_fallback_respected(actual)
    restored = read_parquet(actual)
    assert restored.to_dict(orient="list") == {"a": [1, 2]}
