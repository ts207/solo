from __future__ import annotations

import argparse
from pathlib import Path

from project.pipelines.pipeline_planning import _discover_local_cleaned_coverage


def test_local_cleaned_preflight_flags_unreadable_native_parquet(tmp_path, monkeypatch):
    external_root = tmp_path / "offline-data" / "cleaned_bars" / "perp" / "BTCUSDT" / "bars_5m" / "year=2024" / "month=01"
    external_root.mkdir(parents=True, exist_ok=True)
    (external_root / "part-000.parquet").write_bytes(b"PAR1native")

    args = argparse.Namespace(
        timeframes="5m",
        offline_mode=1,
        offline_cleaned_root=str(tmp_path / "offline-data" / "cleaned_bars"),
        start="2024-01-01",
        end="2024-01-02",
    )

    result = _discover_local_cleaned_coverage(
        args=args,
        data_root=tmp_path / "data",
        parsed_symbols=["BTCUSDT"],
    )

    assert result["covered_perp_timeframes"] == []
    joined = "\n".join(result["coverage_gaps"])
    assert "unreadable in the current runtime" in joined
