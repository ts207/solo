from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

from project.core.feature_schema import feature_dataset_dir_name
from project.pipelines.features import build_features


def test_load_spot_close_reference_uses_requested_timeframe(monkeypatch):
    captured_paths: list[str] = []

    def fake_choose_partition_dir(paths):
        captured_paths.extend(str(path) for path in paths)
        return None

    monkeypatch.setattr(build_features, "choose_partition_dir", fake_choose_partition_dir)

    out = build_features._load_spot_close_reference(
        symbol="BTCUSDT",
        run_id="r1",
        data_root=Path("/tmp"),
        timeframe="1m",
    )

    assert out.empty
    assert any("bars_1m" in path for path in captured_paths)
    assert not any("bars_5m" in path for path in captured_paths)


def test_prune_partition_files_by_window_keeps_only_relevant_months():
    files = [
        Path("/tmp/features/year=2025/month=12/part.parquet"),
        Path("/tmp/features/year=2026/month=01/part.parquet"),
        Path("/tmp/features/year=2026/month=02/part.parquet"),
    ]

    out = build_features._prune_partition_files_by_window(
        files,
        start="2026-01-15T00:00:00Z",
        end="2026-01-31T23:59:59Z",
    )

    assert out == [Path("/tmp/features/year=2026/month=01/part.parquet")]


def test_main_writes_output_to_requested_timeframe_path(monkeypatch, tmp_path):
    bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [10.0],
        }
    )
    captured_writes: list[Path] = []

    monkeypatch.setattr(build_features, "get_data_root", lambda: tmp_path / "data")
    monkeypatch.setattr(build_features, "start_manifest", lambda *args, **kwargs: {})
    monkeypatch.setattr(build_features, "finalize_manifest", lambda *args, **kwargs: None)

    def fake_choose_partition_dir(paths):
        for path in paths:
            if "bars_1m" in str(path):
                return path
        return None

    monkeypatch.setattr(build_features, "choose_partition_dir", fake_choose_partition_dir)
    monkeypatch.setattr(build_features, "list_parquet_files", lambda _path: [Path("dummy.parquet")])
    monkeypatch.setattr(build_features, "read_parquet", lambda _files: bars.copy())
    monkeypatch.setattr(
        build_features,
        "build_features",
        lambda bars, funding, symbol, run_id, data_root, timeframe, market="perp": bars.copy(),
    )
    monkeypatch.setattr(
        build_features, "write_parquet", lambda _df, path: captured_writes.append(path)
    )

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_features.py",
            "--run_id",
            "r1",
            "--symbols",
            "BTCUSDT",
            "--timeframe",
            "1m",
        ],
    )

    rc = build_features.main()

    assert rc == 0
    assert captured_writes
    expected_dir = f"/features/perp/BTCUSDT/1m/{feature_dataset_dir_name('v2')}/"
    assert any(expected_dir in str(path) for path in captured_writes)


def test_main_writes_feature_quality_report(monkeypatch, tmp_path):
    bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [10.0],
            "basis_zscore": [0.2],
        }
    )
    finalized: dict[str, object] = {}

    monkeypatch.setattr(build_features, "get_data_root", lambda: tmp_path / "data")
    monkeypatch.setattr(build_features, "start_manifest", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        build_features,
        "finalize_manifest",
        lambda *args, **kwargs: finalized.setdefault("stats", kwargs.get("stats")),
    )

    def fake_choose_partition_dir(paths):
        for path in paths:
            if "bars_5m" in str(path):
                return path
        return None

    monkeypatch.setattr(build_features, "choose_partition_dir", fake_choose_partition_dir)
    monkeypatch.setattr(build_features, "list_parquet_files", lambda _path: [Path("dummy.parquet")])
    monkeypatch.setattr(build_features, "read_parquet", lambda _files: bars.copy())
    monkeypatch.setattr(
        build_features,
        "build_features",
        lambda bars, funding, symbol, run_id, data_root, timeframe, market="perp": bars.copy(),
    )
    monkeypatch.setattr(build_features, "write_parquet", lambda _df, path: path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_features.py",
            "--run_id",
            "r_quality",
            "--symbols",
            "BTCUSDT",
            "--timeframe",
            "5m",
        ],
    )

    rc = build_features.main()

    assert rc == 0
    report_path = Path(finalized["stats"]["symbols"]["BTCUSDT"]["feature_quality_report_path"])
    assert report_path.exists()
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "feature_quality_report_v2"
    assert payload["symbol"] == "BTCUSDT"
    assert payload["baseline_run_id"] is None
    assert payload["quality"]["feature_count"] >= 1


def test_main_writes_feature_quality_report_with_baseline(monkeypatch, tmp_path):
    bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z"], utc=True),
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.5, 101.5],
            "volume": [10.0, 12.0],
            "basis_zscore": [0.2, 0.3],
        }
    )
    baseline = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2025-12-01T00:00:00Z", "2025-12-01T00:05:00Z"], utc=True),
            "basis_zscore": [1.2, 1.3],
            "close": [95.0, 95.5],
            "volume": [9.0, 9.5],
        }
    )
    finalized: dict[str, object] = {}

    monkeypatch.setattr(build_features, "get_data_root", lambda: tmp_path / "data")
    monkeypatch.setattr(build_features, "start_manifest", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        build_features,
        "finalize_manifest",
        lambda *args, **kwargs: finalized.setdefault("stats", kwargs.get("stats")),
    )
    monkeypatch.setattr(build_features, "choose_partition_dir", lambda paths: paths[0])
    monkeypatch.setattr(build_features, "list_parquet_files", lambda _path: [Path("dummy.parquet")])
    monkeypatch.setattr(build_features, "read_parquet", lambda _files: bars.copy())
    monkeypatch.setattr(
        build_features,
        "_load_baseline_features",
        lambda **kwargs: baseline.copy(),
    )
    monkeypatch.setattr(
        build_features,
        "build_features",
        lambda bars, funding, symbol, run_id, data_root, timeframe, market="perp": bars.copy(),
    )
    monkeypatch.setattr(build_features, "write_parquet", lambda _df, path: path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_features.py",
            "--run_id",
            "r_quality_baseline",
            "--symbols",
            "BTCUSDT",
            "--timeframe",
            "5m",
            "--baseline_run_id",
            "baseline_run",
        ],
    )

    rc = build_features.main()

    assert rc == 0
    report_path = Path(finalized["stats"]["symbols"]["BTCUSDT"]["feature_quality_report_path"])
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["baseline_run_id"] == "baseline_run"
    assert payload["quality"]["baseline"]["label"] == "baseline_run"


def test_main_fails_when_no_feature_artifacts_are_produced(monkeypatch, tmp_path):
    finalized: dict[str, object] = {}

    monkeypatch.setattr(
        "project.core.config.get_data_root", lambda: tmp_path / "data"
    )
    monkeypatch.setattr(build_features, "start_manifest", lambda *args, **kwargs: {})

    def fake_finalize_manifest(manifest, status, **kwargs):
        finalized["status"] = status
        finalized["error"] = kwargs.get("error")
        finalized["stats"] = kwargs.get("stats")

    monkeypatch.setattr(build_features, "finalize_manifest", fake_finalize_manifest)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_features.py",
            "--run_id",
            "r_empty_outputs",
            "--symbols",
            "",
            "--timeframe",
            "5m",
        ],
    )

    rc = build_features.main()

    assert rc == 1
    assert finalized["status"] == "failed"
    assert "no feature artifacts" in str(finalized["error"])


def test_filter_time_window_respects_start_and_end() -> None:
    bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01T00:00:00Z",
                    "2026-01-15T00:00:00Z",
                    "2026-02-01T00:00:00Z",
                ],
                utc=True,
            ),
            "close": [100.0, 101.0, 102.0],
        }
    )

    out = build_features._filter_time_window(
        bars,
        start="2026-01-10",
        end="2026-01-31 23:59:59",
    )

    assert len(out) == 1
    assert out["timestamp"].iloc[0] == pd.Timestamp("2026-01-15T00:00:00Z")
