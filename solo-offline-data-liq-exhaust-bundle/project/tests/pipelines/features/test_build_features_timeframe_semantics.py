from __future__ import annotations

from pathlib import Path

import pandas as pd

from project.pipelines.features import build_features


def test_duration_to_bars_scales_with_timeframe() -> None:
    assert build_features._duration_to_bars(minutes=30, timeframe="1m") == 30
    assert build_features._duration_to_bars(minutes=30, timeframe="5m") == 6
    assert build_features._duration_to_bars(minutes=30, timeframe="15m") == 2


def test_liquidation_assignment_uses_active_bar_width(tmp_path: Path, monkeypatch) -> None:
    bars_1m = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01 00:00:00", periods=5, freq="1min", tz="UTC"),
            "open": [1, 1, 1, 1, 1],
            "high": [1, 1, 1, 1, 1],
            "low": [1, 1, 1, 1, 1],
            "close": [1, 1, 1, 1, 1],
            "volume": [1, 1, 1, 1, 1],
        }
    )
    bars_5m = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01 00:00:00", periods=2, freq="5min", tz="UTC"),
            "open": [1, 1],
            "high": [1, 1],
            "low": [1, 1],
            "close": [1, 1],
            "volume": [1, 1],
        }
    )
    liq = pd.DataFrame(
        {
            "timestamp": [pd.Timestamp("2026-01-01 00:04:00", tz="UTC")],
            "notional_usd": [50.0],
        }
    )

    def fake_choose_partition_dir(_paths):
        return Path("/tmp/fake")

    def fake_list_parquet_files(_path):
        return [Path("dummy.parquet")]

    def fake_read_parquet(_files):
        return liq.copy()

    monkeypatch.setattr(build_features, "choose_partition_dir", fake_choose_partition_dir)
    monkeypatch.setattr(build_features, "list_parquet_files", fake_list_parquet_files)
    monkeypatch.setattr(build_features, "read_parquet", fake_read_parquet)

    out_1m = build_features._merge_optional_oi_liquidation(
        bars_1m, symbol="BTCUSDT", market="perp", run_id="r1", data_root=tmp_path, timeframe="1m"
    )
    out_5m = build_features._merge_optional_oi_liquidation(
        bars_5m, symbol="BTCUSDT", market="perp", run_id="r1", data_root=tmp_path, timeframe="5m"
    )

    assert (
        out_1m.loc[
            out_1m["timestamp"] == pd.Timestamp("2026-01-01 00:04:00", tz="UTC"),
            "liquidation_count",
        ].iat[0]
        == 1.0
    )
    assert (
        out_5m.loc[
            out_5m["timestamp"] == pd.Timestamp("2026-01-01 00:00:00", tz="UTC"),
            "liquidation_count",
        ].iat[0]
        == 1.0
    )
