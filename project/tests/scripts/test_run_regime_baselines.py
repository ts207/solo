from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from project.scripts.run_regime_baselines import main


def _write_market_context(data_root: Path, *, run_id: str, symbol: str) -> None:
    out_dir = (
        data_root
        / "lake"
        / "runs"
        / run_id
        / "features"
        / "perp"
        / symbol
        / "5m"
        / "market_context"
        / "year=2022"
        / "month=01"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    n = 140
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2022-01-01", periods=n, freq="5min", tz="UTC"),
            "close": [100.0 + i for i in range(n)],
            "symbol": [symbol] * n,
            "vol_regime": ["high"] * n,
            "carry_state": ["funding_neg"] * n,
            "ms_trend_state": [1.0] * n,
            "spread_bps": [1.0] * n,
        }
    )
    frame.to_parquet(out_dir / f"market_context_{symbol}_2022-01.parquet", index=False)


def test_run_regime_baselines_writes_required_outputs(tmp_path):
    _write_market_context(tmp_path, run_id="source_run", symbol="BTCUSDT")

    rc = main(
        [
            "--run-id",
            "regime_baselines_test",
            "--matrix-id",
            "core_v1",
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--horizons",
            "12,24,48",
            "--data-root",
            str(tmp_path),
            "--source-run-id",
            "source_run",
        ]
    )

    assert rc == 0
    out_dir = tmp_path / "reports" / "regime_baselines" / "regime_baselines_test"
    for name in [
        "regime_baselines.json",
        "regime_baselines.parquet",
        "regime_baselines.md",
        "regime_search_burden.json",
    ]:
        assert (out_dir / name).exists()

    payload = json.loads((out_dir / "regime_baselines.json").read_text(encoding="utf-8"))
    burden = json.loads((out_dir / "regime_search_burden.json").read_text(encoding="utf-8"))
    assert payload["schema_version"] == "regime_baseline_v1"
    assert payload["row_count"] == 108
    assert payload["source_run_id"] == "source_run"
    assert burden["schema_version"] == "regime_search_burden_v1"
    assert burden["num_tests"] == 108

    df = pd.read_parquet(out_dir / "regime_baselines.parquet")
    assert len(df) == 108
    assert set(df["schema_version"]) == {"regime_baseline_v1"}


def test_run_regime_baselines_rejects_unknown_matrix(tmp_path):
    rc = main(
        [
            "--run-id",
            "bad_matrix",
            "--matrix-id",
            "unknown",
            "--symbols",
            "BTCUSDT",
            "--horizons",
            "12",
            "--data-root",
            str(tmp_path),
        ]
    )

    assert rc == 1
