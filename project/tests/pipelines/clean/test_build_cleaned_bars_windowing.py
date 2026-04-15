from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

import project.pipelines.clean.build_cleaned_bars as build_cleaned_bars


def _raw_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:05:00Z",
                    "2026-01-01T00:10:00Z",
                ],
                utc=True,
            ),
            "symbol": ["BTCUSDT", "BTCUSDT", "BTCUSDT"],
            "open": [100.0, 101.0, 102.0],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.5, 101.5, 102.5],
            "volume": [10.0, 11.0, 12.0],
        }
    )


def _funding_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
            "funding_rate": [0.0001],
            "source": ["unknown"],
        }
    )


def test_build_cleaned_respects_requested_start_end_window(monkeypatch, tmp_path):
    read_calls = {"i": 0}
    writes: list[pd.DataFrame] = []

    def fake_list_parquet_files(_path):
        return [Path("dummy.parquet")]

    def fake_read_parquet(_files):
        read_calls["i"] += 1
        return _raw_frame() if read_calls["i"] == 1 else _funding_frame()

    def fake_start_manifest(stage_name, run_id, params, inputs, outputs):
        return {
            "stage": stage_name,
            "run_id": run_id,
            "params": params,
            "inputs": inputs,
            "outputs": outputs,
        }

    def fake_finalize_manifest(manifest, status, error=None, stats=None):
        return manifest

    def fake_write_parquet(df, path):
        writes.append(df.copy())
        return Path(path), "parquet"

    monkeypatch.setattr(build_cleaned_bars, "get_data_root", lambda: tmp_path / "data")
    monkeypatch.setattr(build_cleaned_bars, "list_parquet_files", fake_list_parquet_files)
    monkeypatch.setattr(build_cleaned_bars, "read_parquet", fake_read_parquet)
    monkeypatch.setattr(build_cleaned_bars, "start_manifest", fake_start_manifest)
    monkeypatch.setattr(build_cleaned_bars, "finalize_manifest", fake_finalize_manifest)
    monkeypatch.setattr(build_cleaned_bars, "write_parquet", fake_write_parquet)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_cleaned_bars.py",
            "--run_id",
            "r_window",
            "--symbols",
            "BTCUSDT",
            "--market",
            "perp",
            "--start",
            "2026-01-01T00:05:00Z",
            "--end",
            "2026-01-01T00:15:00Z",
            "--funding_scale",
            "bps",
        ],
    )

    rc = build_cleaned_bars.main()

    assert rc == 0
    assert writes
    out = writes[0]
    assert out["timestamp"].min() >= pd.Timestamp("2026-01-01T00:05:00Z")
    assert out["timestamp"].max() < pd.Timestamp("2026-01-01T00:15:00Z")
    assert len(out) == 2


def test_build_cleaned_warns_when_funding_window_does_not_overlap(monkeypatch, tmp_path, caplog):
    read_calls = {"i": 0}
    writes: list[pd.DataFrame] = []

    def fake_list_parquet_files(_path):
        return [Path("dummy.parquet")]

    def fake_read_parquet(_files):
        read_calls["i"] += 1
        if read_calls["i"] == 1:
            return _raw_frame()
        return pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2026-02-01T00:00:00Z"], utc=True),
                "funding_rate": [0.0001],
                "source": ["unknown"],
            }
        )

    def fake_start_manifest(stage_name, run_id, params, inputs, outputs):
        return {
            "stage": stage_name,
            "run_id": run_id,
            "params": params,
            "inputs": inputs,
            "outputs": outputs,
        }

    def fake_finalize_manifest(manifest, status, error=None, stats=None):
        return manifest

    def fake_write_parquet(df, path):
        writes.append(df.copy())
        return Path(path), "parquet"

    monkeypatch.setattr(build_cleaned_bars, "get_data_root", lambda: tmp_path / "data")
    monkeypatch.setattr(build_cleaned_bars, "list_parquet_files", fake_list_parquet_files)
    monkeypatch.setattr(build_cleaned_bars, "read_parquet", fake_read_parquet)
    monkeypatch.setattr(build_cleaned_bars, "start_manifest", fake_start_manifest)
    monkeypatch.setattr(build_cleaned_bars, "finalize_manifest", fake_finalize_manifest)
    monkeypatch.setattr(build_cleaned_bars, "write_parquet", fake_write_parquet)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_cleaned_bars.py",
            "--run_id",
            "r_funding_gap",
            "--symbols",
            "BTCUSDT",
            "--market",
            "perp",
            "--start",
            "2026-01-01T00:00:00Z",
            "--end",
            "2026-01-01T00:15:00Z",
            "--funding_scale",
            "decimal",
        ],
    )

    with caplog.at_level("WARNING"):
        rc = build_cleaned_bars.main()

    assert rc == 0
    assert writes
    assert "Funding data for BTCUSDT does not overlap requested window" in caplog.text


def test_build_cleaned_writes_data_quality_report(monkeypatch, tmp_path):
    read_calls = {"i": 0}
    writes: list[pd.DataFrame] = []
    finalized: dict[str, object] = {}

    def fake_list_parquet_files(_path):
        return [Path("dummy.parquet")]

    def fake_read_parquet(_files):
        read_calls["i"] += 1
        return _raw_frame() if read_calls["i"] == 1 else _funding_frame()

    def fake_start_manifest(stage_name, run_id, params, inputs, outputs):
        return {
            "stage": stage_name,
            "run_id": run_id,
            "params": params,
            "inputs": inputs,
            "outputs": outputs,
        }

    def fake_finalize_manifest(manifest, status, error=None, stats=None):
        finalized["status"] = status
        finalized["stats"] = stats
        return manifest

    def fake_write_parquet(df, path):
        writes.append(df.copy())
        return Path(path), "parquet"

    monkeypatch.setattr(build_cleaned_bars, "get_data_root", lambda: tmp_path / "data")
    monkeypatch.setattr(build_cleaned_bars, "list_parquet_files", fake_list_parquet_files)
    monkeypatch.setattr(build_cleaned_bars, "read_parquet", fake_read_parquet)
    monkeypatch.setattr(build_cleaned_bars, "start_manifest", fake_start_manifest)
    monkeypatch.setattr(build_cleaned_bars, "finalize_manifest", fake_finalize_manifest)
    monkeypatch.setattr(build_cleaned_bars, "write_parquet", fake_write_parquet)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_cleaned_bars.py",
            "--run_id",
            "r_quality",
            "--symbols",
            "BTCUSDT",
            "--market",
            "perp",
            "--start",
            "2026-01-01T00:00:00Z",
            "--end",
            "2026-01-01T00:15:00Z",
            "--funding_scale",
            "bps",
        ],
    )

    rc = build_cleaned_bars.main()

    assert rc == 0
    assert writes
    stats = finalized["stats"]["symbols"]["BTCUSDT"]
    report_path = Path(stats["data_quality_report_path"])
    assert report_path.exists()
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "data_quality_report_v1"
    assert payload["symbol"] == "BTCUSDT"
    assert payload["timeframe"] == "5m"
    assert payload["overall"]["rows"] == 3
    assert "2026-01" in payload["by_month"]


def test_build_cleaned_prefers_run_scoped_raw_and_funding(monkeypatch, tmp_path):
    run_id = "r_scoped"
    data_root = tmp_path / "data"

    run_raw_dir = (
        data_root / "lake" / "runs" / run_id / "raw" / "binance" / "perp" / "BTCUSDT" / "ohlcv_5m"
    )
    global_raw_dir = data_root / "lake" / "raw" / "binance" / "perp" / "BTCUSDT" / "ohlcv_5m"
    run_funding_dir = (
        data_root / "lake" / "runs" / run_id / "raw" / "binance" / "perp" / "BTCUSDT" / "funding"
    )
    global_funding_dir = data_root / "lake" / "raw" / "binance" / "perp" / "BTCUSDT" / "funding"

    for directory in (run_raw_dir, global_raw_dir, run_funding_dir, global_funding_dir):
        directory.mkdir(parents=True, exist_ok=True)

    run_raw_file = run_raw_dir / "run.parquet"
    global_raw_file = global_raw_dir / "global.parquet"
    run_funding_file = run_funding_dir / "run.parquet"
    global_funding_file = global_funding_dir / "global.parquet"

    for file_path in (run_raw_file, global_raw_file, run_funding_file, global_funding_file):
        file_path.write_text("", encoding="utf-8")

    reads: list[str] = []

    def fake_read_parquet(files):
        first = str(list(files)[0])
        reads.append(first)
        if first == str(run_raw_file):
            return _raw_frame()
        if first == str(run_funding_file):
            return _funding_frame()
        raise AssertionError(f"unexpected fallback read: {first}")

    def fake_start_manifest(stage_name, run_id, params, inputs, outputs):
        return {
            "stage": stage_name,
            "run_id": run_id,
            "params": params,
            "inputs": inputs,
            "outputs": outputs,
        }

    def fake_finalize_manifest(manifest, status, error=None, stats=None):
        return manifest

    def fake_write_parquet(df, path):
        return Path(path), "parquet"

    monkeypatch.setattr(build_cleaned_bars, "get_data_root", lambda: data_root)
    monkeypatch.setattr(build_cleaned_bars, "read_parquet", fake_read_parquet)
    monkeypatch.setattr(build_cleaned_bars, "start_manifest", fake_start_manifest)
    monkeypatch.setattr(build_cleaned_bars, "finalize_manifest", fake_finalize_manifest)
    monkeypatch.setattr(build_cleaned_bars, "write_parquet", fake_write_parquet)
    monkeypatch.setattr(build_cleaned_bars, "validate_input_provenance", lambda inputs: None)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_cleaned_bars.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT",
            "--market",
            "perp",
            "--funding_scale",
            "bps",
        ],
    )

    rc = build_cleaned_bars.main()

    assert rc == 0
    assert reads == [str(run_raw_file), str(run_funding_file)]
