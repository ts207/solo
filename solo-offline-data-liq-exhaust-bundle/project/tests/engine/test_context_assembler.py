from __future__ import annotations

import pandas as pd

from project.engine.context_assembler import load_context_data


def test_load_context_data_reads_legacy_market_state_timeframe_path(tmp_path) -> None:
    data_root = tmp_path / "data"
    legacy_dir = data_root / "lake" / "context" / "market_state" / "BTCUSDT" / "5m"
    legacy_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "timestamp": "2024-01-01T00:00:00Z",
                "vol_regime": "high",
                "vol_regime_code": 2.0,
                "carry_state": "funding_pos",
                "carry_state_code": 1.0,
                "ms_trend_state": 1.0,
                "ms_spread_state": 0.0,
            }
        ]
    ).to_parquet(legacy_dir / "part-000.parquet", index=False)

    loaded = load_context_data(data_root, symbol="BTCUSDT", run_id="run_1", timeframe="5m")

    assert len(loaded.index) == 1
    assert loaded.loc[0, "vol_regime"] == "high"
    assert loaded.loc[0, "carry_state"] == "funding_pos"
    assert loaded.loc[0, "ms_trend_state"] == 1.0
    assert loaded.loc[0, "ms_spread_state"] == 0.0
