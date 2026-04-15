from __future__ import annotations

from pathlib import Path

import pandas as pd

from project.research import phase2_event_analyzer


def test_attach_event_market_features_reads_market_context_from_feature_store(monkeypatch, tmp_path):
    events = pd.DataFrame(
        {
            "symbol": ["BTCUSDT"],
            "enter_ts": [pd.Timestamp("2024-01-01T00:05:00Z")],
        }
    )
    features = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2024-01-01T00:00:00Z", "2024-01-01T00:05:00Z"], utc=True
            ),
            "close": [100.0, 101.0],
        }
    )
    market_context = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2024-01-01T00:00:00Z", "2024-01-01T00:05:00Z"], utc=True
            ),
            "vol_regime": ["low", "high"],
            "vol_regime_code": [0.0, 2.0],
            "carry_state": ["funding_neg", "funding_pos"],
            "carry_state_code": [-1.0, 1.0],
            "ms_trend_state": [0.0, 1.0],
            "ms_spread_state": [0.0, 1.0],
        }
    )

    monkeypatch.setattr(phase2_event_analyzer, "get_data_root", lambda: tmp_path / "data")
    monkeypatch.setattr(phase2_event_analyzer, "choose_partition_dir", lambda paths: paths[0])

    def fake_list_parquet_files(path: Path):
        return [Path(path) / "slice.parquet"]

    def fake_read_parquet(files):
        path = str(next(iter(files)))
        if "market_context" in path:
            return market_context.copy()
        if "features_feature_schema_v2" in path:
            return features.copy()
        return pd.DataFrame()

    monkeypatch.setattr(phase2_event_analyzer, "list_parquet_files", fake_list_parquet_files)
    monkeypatch.setattr(phase2_event_analyzer, "read_parquet", fake_read_parquet)

    out = phase2_event_analyzer.attach_event_market_features(
        events,
        run_id="r1",
        symbols=["BTCUSDT"],
        timeframe="5m",
    )

    assert out.loc[0, "vol_regime"] == "high"
    assert out.loc[0, "carry_state"] == "funding_pos"
    assert out.loc[0, "ms_trend_state"] == 1.0
    assert out.loc[0, "ms_spread_state"] == 1.0
