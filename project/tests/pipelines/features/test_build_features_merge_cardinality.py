from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from project.pipelines.features import build_features


def _bars() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z", "2026-01-01T00:10:00Z"],
                utc=True,
                format="ISO8601",
            ),
            "open": [1.0, 1.0, 1.0],
            "high": [1.0, 1.0, 1.0],
            "low": [1.0, 1.0, 1.0],
            "close": [1.0, 1.0, 1.0],
        }
    )


def test_merge_optional_oi_deduplicates_timestamps(monkeypatch):
    oi = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"], utc=True, format="ISO8601"
            ),
            "open_interest": [10.0, 11.0],
        }
    )

    def fake_list_parquet_files(_path):
        return [Path("dummy.parquet")]

    def fake_read_parquet(_files):
        return oi.copy()

    def fake_choose_partition_dir(paths):
        for p in paths:
            if "open_interest" in str(p):
                return p
        return None

    monkeypatch.setattr(build_features, "list_parquet_files", fake_list_parquet_files)
    monkeypatch.setattr(build_features, "read_parquet", fake_read_parquet)
    monkeypatch.setattr(build_features, "choose_partition_dir", fake_choose_partition_dir)

    out = build_features._merge_optional_oi_liquidation(
        _bars(), symbol="BTCUSDT", market="perp", run_id="r1", data_root=Path("/tmp")
    )
    # Should keep the last one (11.0)
    assert out.loc[0, "oi_notional"] == 11.0


def test_merge_optional_liquidation_aggregates_events_using_active_timeframe(monkeypatch):
    liq = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01T00:00:10Z",
                    "2026-01-01T00:04:59Z",
                    "2026-01-01T00:05:01Z",
                ],
                utc=True,
                format="ISO8601",
            ),
            "notional_usd": [100.0, 120.0, 30.0],
        }
    )

    def fake_list_parquet_files(_path):
        return [Path("dummy.parquet")]

    def fake_read_parquet(_files):
        return liq.copy()

    def fake_choose_partition_dir(paths):
        for p in paths:
            if "liquidations" in str(p):
                return p
        return None

    monkeypatch.setattr(build_features, "list_parquet_files", fake_list_parquet_files)
    monkeypatch.setattr(build_features, "read_parquet", fake_read_parquet)
    monkeypatch.setattr(build_features, "choose_partition_dir", fake_choose_partition_dir)

    out = build_features._merge_optional_oi_liquidation(
        _bars(),
        symbol="BTCUSDT",
        market="perp",
        run_id="r1",
        data_root=Path("/tmp"),
        timeframe="5m",
    )
    assert out.loc[0, "liquidation_notional"] == pytest.approx(220.0)
    assert out.loc[0, "liquidation_count"] == pytest.approx(2.0)
    assert out.loc[1, "liquidation_notional"] == pytest.approx(30.0)
    assert out.loc[1, "liquidation_count"] == pytest.approx(1.0)
    assert out.loc[2, "liquidation_notional"] == pytest.approx(0.0)
    assert out.loc[2, "liquidation_count"] == pytest.approx(0.0)

    out_1m = build_features._merge_optional_oi_liquidation(
        _bars(),
        symbol="BTCUSDT",
        market="perp",
        run_id="r1",
        data_root=Path("/tmp"),
        timeframe="1m",
    )
    assert out_1m.loc[0, "liquidation_notional"] == pytest.approx(100.0)
    assert out_1m.loc[0, "liquidation_count"] == pytest.approx(1.0)
    assert out_1m.loc[1, "liquidation_notional"] == pytest.approx(30.0)
    assert out_1m.loc[1, "liquidation_count"] == pytest.approx(1.0)
    assert out_1m.loc[2, "liquidation_notional"] == pytest.approx(0.0)
    assert out_1m.loc[2, "liquidation_count"] == pytest.approx(0.0)


def test_merge_optional_oi_respects_staleness_tolerance(monkeypatch):
    oi = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True, format="ISO8601"),
            "open_interest": [10.0],
        }
    )

    def fake_list_parquet_files(_path):
        return [Path("dummy.parquet")]

    def fake_read_parquet(_files):
        return oi.copy()

    def fake_choose_partition_dir(paths):
        for p in paths:
            if "open_interest" in str(p):
                return p
        return None

    bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2026-01-01T00:00:00Z", "2026-01-01T05:00:00Z"], utc=True, format="ISO8601"
            ),
            "open": [1.0, 1.0],
            "high": [1.0, 1.0],
            "low": [1.0, 1.0],
            "close": [1.0, 1.0],
        }
    )

    monkeypatch.setattr(build_features, "list_parquet_files", fake_list_parquet_files)
    monkeypatch.setattr(build_features, "read_parquet", fake_read_parquet)
    monkeypatch.setattr(build_features, "choose_partition_dir", fake_choose_partition_dir)

    out = build_features._merge_optional_oi_liquidation(
        bars,
        symbol="BTCUSDT",
        market="perp",
        run_id="r1",
        data_root=Path("/tmp"),
        timeframe="1m",
    )

    assert out.loc[0, "oi_notional"] == 10.0
    assert pd.isna(out.loc[1, "oi_notional"])
