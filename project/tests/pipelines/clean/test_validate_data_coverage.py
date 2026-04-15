from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from project.pipelines.clean import validate_data_coverage


def test_validate_data_coverage_fails_on_missing_ratio(monkeypatch, tmp_path):
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=20, freq="5min", tz="UTC"),
            "open": [100.0] * 19 + [None],
            "high": [101.0] * 20,
            "low": [99.0] * 20,
            "close": [100.5] * 20,
            "volume": [10.0] * 20,
            "quote_volume": [1000.0] * 20,
            "taker_base_volume": [5.0] * 20,
            "is_gap": [False] * 20,
        }
    )

    monkeypatch.setattr(validate_data_coverage, "get_data_root", lambda: tmp_path)
    monkeypatch.setattr(
        validate_data_coverage, "choose_partition_dir", lambda candidates: Path("/tmp/fake")
    )
    monkeypatch.setattr(
        validate_data_coverage, "list_parquet_files", lambda path: [path / "data.parquet"]
    )
    monkeypatch.setattr(validate_data_coverage, "read_parquet", lambda files: frame)
    monkeypatch.setattr(validate_data_coverage, "start_manifest", lambda *args, **kwargs: {})
    monkeypatch.setattr(validate_data_coverage, "finalize_manifest", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        validate_data_coverage.sys,
        "argv",
        [
            "validate_data_coverage.py",
            "--run_id",
            "test_run",
            "--symbols",
            "BTCUSDT",
            "--max_gap_pct",
            "0.10",
            "--max_missing_ratio",
            "0.001",
        ],
    )

    assert validate_data_coverage.main() == 1


def test_validate_data_coverage_warns_without_failing(monkeypatch, tmp_path):
    frame = pd.DataFrame(
        {
            "timestamp": list(pd.date_range("2026-01-01", periods=19, freq="5min", tz="UTC"))
            + [pd.Timestamp("2026-01-01 01:30:00+00:00")],
            "open": [100.0] * 20,
            "high": [101.0] * 20,
            "low": [99.0] * 20,
            "close": [100.5] * 20,
            "volume": [10.0] * 20,
            "quote_volume": [1000.0] * 20,
            "taker_base_volume": [5.0] * 20,
            "is_gap": [False] * 20,
        }
    )
    captured: dict[str, object] = {}

    monkeypatch.setattr(validate_data_coverage, "get_data_root", lambda: tmp_path)
    monkeypatch.setattr(
        validate_data_coverage, "choose_partition_dir", lambda candidates: Path("/tmp/fake")
    )
    monkeypatch.setattr(
        validate_data_coverage, "list_parquet_files", lambda path: [path / "data.parquet"]
    )
    monkeypatch.setattr(validate_data_coverage, "read_parquet", lambda files: frame)
    monkeypatch.setattr(validate_data_coverage, "start_manifest", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        validate_data_coverage,
        "finalize_manifest",
        lambda *args, **kwargs: captured.setdefault("stats", kwargs.get("stats")),
    )
    monkeypatch.setattr(
        validate_data_coverage.sys,
        "argv",
        [
            "validate_data_coverage.py",
            "--run_id",
            "warn_run",
            "--symbols",
            "BTCUSDT",
            "--max_gap_pct",
            "0.10",
            "--max_duplicate_timestamps",
            "2",
            "--warn_duplicate_timestamps",
            "0",
        ],
    )

    assert validate_data_coverage.main() == 0

    stats = captured["stats"]
    assert stats["failure_count"] == 0
    assert stats["warning_count"] == 1
    assert stats["symbols"]["BTCUSDT"]["status"] == "warn"
    assert stats["symbols"]["BTCUSDT"]["warnings"]
    report_path = Path(stats["report_path"])
    assert report_path.exists()
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["warning_count"] == 1
    assert payload["symbols"]["BTCUSDT"]["status"] == "warn"
