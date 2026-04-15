from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

from project.pipelines.ingest import build_universe_snapshots


def _write_bars(root: Path, timeframe: str) -> None:
    out_dir = (
        root
        / "lake"
        / "cleaned"
        / "perp"
        / "BTCUSDT"
        / f"bars_{timeframe}"
        / "year=2026"
        / "month=01"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range(
                "2026-01-01", periods=3, freq="1min" if timeframe == "1m" else "5min", tz="UTC"
            ),
            "open": [1.0, 1.0, 1.0],
            "high": [1.0, 1.0, 1.0],
            "low": [1.0, 1.0, 1.0],
            "close": [1.0, 1.0, 1.0],
            "volume": [1.0, 1.0, 1.0],
        }
    )
    df.to_csv(out_dir / "bars.csv", index=False)


def test_universe_snapshot_reads_requested_timeframe(monkeypatch, tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    _write_bars(data_root, "1m")
    _write_bars(data_root, "5m")

    monkeypatch.setattr(build_universe_snapshots, "get_data_root", lambda: data_root)
    monkeypatch.setattr(build_universe_snapshots, "start_manifest", lambda *args, **kwargs: {})
    monkeypatch.setattr(build_universe_snapshots, "finalize_manifest", lambda *args, **kwargs: None)

    def fake_list_parquet_files(path: Path):
        return sorted(path.rglob("*.csv")) if path and path.exists() else []

    def fake_read_parquet(files):
        return pd.read_csv(files[0])

    monkeypatch.setattr(build_universe_snapshots, "list_parquet_files", fake_list_parquet_files)
    monkeypatch.setattr(build_universe_snapshots, "read_parquet", fake_read_parquet)

    sys.argv = [
        "build_universe_snapshots.py",
        "--run_id",
        "r1",
        "--symbols",
        "BTCUSDT",
        "--market",
        "perp",
        "--timeframe",
        "1m",
    ]
    assert build_universe_snapshots.main() == 0

    summary = json.loads(
        (data_root / "reports" / "universe" / "r1" / "1m" / "universe_membership.json").read_text(
            encoding="utf-8"
        )
    )
    assert summary["timeframe"] == "1m"
    assert summary["dataset"] == "bars_1m"
