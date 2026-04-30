from __future__ import annotations

import json

import pandas as pd

from project.scripts.run_funding_data_triage import main


def _write_market_context(data_root, *, run_id: str, symbol: str) -> None:
    out = (
        data_root
        / "lake"
        / "runs"
        / run_id
        / "features"
        / "perp"
        / symbol
        / "5m"
        / "market_context"
        / "year=2023"
        / "month=01"
    )
    out.mkdir(parents=True, exist_ok=True)
    timestamps = pd.date_range(
        pd.Timestamp("2023-01-01", tz="UTC"),
        pd.Timestamp("2023-01-10", tz="UTC"),
        freq="5min",
    )
    update_index = (pd.Series(range(len(timestamps))) // 96).astype(float)
    rate = ((update_index % 5) - 2) / 10_000.0
    pd.DataFrame(
        {
            "timestamp": timestamps,
            "funding_rate_scaled": rate,
            "funding_abs_pct": rate.abs(),
            "funding_rate": rate,
        }
    ).to_parquet(out / f"market_context_{symbol}_2023-01.parquet", index=False)


def _write_raw_funding(data_root, *, run_id: str, symbol: str) -> None:
    out = data_root / "lake" / "runs" / run_id / "raw" / "perp" / symbol / "funding"
    out.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2023-01-01", periods=4, freq="8h", tz="UTC"),
            "funding_rate": [0.0001, -0.0001, 0.0002, -0.0002],
            "funding_rate_scaled": [0.0001, -0.0001, 0.0002, -0.0002],
        }
    ).to_parquet(out / f"{symbol}_funding.parquet", index=False)


def test_run_funding_data_triage_writes_outputs(tmp_path):
    _write_market_context(tmp_path, run_id="source_run", symbol="BTCUSDT")
    _write_raw_funding(tmp_path, run_id="source_run", symbol="BTCUSDT")

    rc = main(
        [
            "--run-id",
            "funding_triage_test",
            "--symbols",
            "BTCUSDT",
            "--data-root",
            str(tmp_path),
            "--source-run-id",
            "source_run",
        ]
    )

    assert rc == 0
    out_dir = tmp_path / "reports" / "funding_data_triage" / "funding_triage_test"
    for name in [
        "funding_data_triage.json",
        "funding_data_triage.parquet",
        "funding_data_triage.md",
    ]:
        assert (out_dir / name).exists()

    payload = json.loads((out_dir / "funding_data_triage.json").read_text(encoding="utf-8"))
    assert payload["schema_version"] == "funding_data_triage_v1"
    assert payload["row_count"] == 2
    assert payload["source_run_id"] == "source_run"
    assert payload["rows"][0]["classification"] == "valid_stepwise"
    assert "funding_rate" in payload["rows"][0]["source_funding_fields"]
    assert pd.read_parquet(out_dir / "funding_data_triage.parquet").shape[0] == 2
