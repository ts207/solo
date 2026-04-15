from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from project.pipelines.features import build_microstructure_rollup as micro_rollup


def _synthetic_tob_1s() -> pd.DataFrame:
    ts = pd.date_range("2025-01-01T00:00:00Z", periods=24 * 300, freq="1s")
    base_spread = np.ones(len(ts), dtype=float) * 1.0
    # Final 5m bucket is stressed.
    base_spread[-300:] = 12.0
    mid = np.ones(len(ts), dtype=float) * 100.0
    ask = mid + (base_spread / 2.0) / 10_000.0 * mid
    bid = mid - (base_spread / 2.0) / 10_000.0 * mid
    bid_qty = np.ones(len(ts), dtype=float) * 5.0
    ask_qty = np.ones(len(ts), dtype=float) * 5.0
    bid_qty[-300:] = 1.0
    ask_qty[-300:] = 9.0
    return pd.DataFrame(
        {
            "timestamp": ts,
            "bid_price": bid,
            "ask_price": ask,
            "bid_qty": bid_qty,
            "ask_qty": ask_qty,
        }
    )


def test_build_microstructure_rollup_emits_expected_columns_and_no_lookahead():
    tob = _synthetic_tob_1s()
    out = micro_rollup._build_rollup("BTCUSDT", tob)

    required = {
        "timestamp",
        "symbol",
        "micro_spread_stress",
        "micro_depth_depletion",
        "micro_sweep_pressure",
        "micro_imbalance",
        "micro_feature_coverage",
    }
    assert required.issubset(set(out.columns))
    assert not out.empty
    assert out["timestamp"].is_monotonic_increasing
    assert (out["symbol"] == "BTCUSDT").all()

    # No lookahead check: early bucket metrics should not anticipate final stress bucket.
    early_mean = float(out["micro_spread_stress"].iloc[:10].mean())
    late_value = float(out["micro_spread_stress"].iloc[-1])
    assert late_value > early_mean


def test_main_applies_requested_time_window(monkeypatch, tmp_path):
    tob = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2025-01-01T23:59:59Z",
                    "2025-01-02T00:00:00Z",
                    "2025-01-02T12:00:00Z",
                    "2025-01-03T00:00:00Z",
                ],
                utc=True,
            ),
            "bid_price": [100.0, 100.0, 100.0, 100.0],
            "ask_price": [100.1, 100.1, 100.1, 100.1],
            "bid_qty": [1.0, 1.0, 1.0, 1.0],
            "ask_qty": [1.0, 1.0, 1.0, 1.0],
        }
    )
    captured: dict[str, pd.DataFrame] = {}

    monkeypatch.setattr(micro_rollup, "get_data_root", lambda: tmp_path / "data")
    monkeypatch.setattr(micro_rollup, "_load_tob_1s", lambda run_id, symbol: tob.copy())

    def fake_build_rollup(symbol, filtered):
        captured["filtered"] = filtered.copy()
        return pd.DataFrame({"timestamp": [pd.Timestamp("2025-01-02T00:00:00Z")]})

    monkeypatch.setattr(micro_rollup, "_build_rollup", fake_build_rollup)
    monkeypatch.setattr(micro_rollup, "start_manifest", lambda *args, **kwargs: {})
    monkeypatch.setattr(micro_rollup, "finalize_manifest", lambda *args, **kwargs: {})
    monkeypatch.setattr(micro_rollup, "write_parquet", lambda df, path: (path, "csv"))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_microstructure_rollup.py",
            "--run_id",
            "r1",
            "--symbols",
            "BTCUSDT",
            "--timeframe",
            "5m",
            "--force",
            "0",
            "--start",
            "2025-01-02",
            "--end",
            "2025-01-02",
        ],
    )

    rc = micro_rollup.main()
    assert rc == 0
    filtered = captured["filtered"]
    assert filtered["timestamp"].min() == pd.Timestamp("2025-01-02T00:00:00Z")
    assert filtered["timestamp"].max() == pd.Timestamp("2025-01-02T12:00:00Z")
