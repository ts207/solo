from __future__ import annotations

import json

import pandas as pd

from project.scripts.run_data_quality_audit import main


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
    n = 1001
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range(
                pd.Timestamp("2023-01-01", tz="UTC"),
                pd.Timestamp("2023-01-01", tz="UTC") + pd.Timedelta(days=181),
                periods=n,
            ),
            "funding_rate_scaled": [float(i + 1) for i in range(n)],
            "funding_abs_pct": [float(i + 2) for i in range(n)],
            "oi_notional": [float(i + 3) for i in range(n)],
            "oi_delta_1h": [0.0 if i % 2 else float(i) for i in range(n)],
            "rv_96": [float(i + 4) for i in range(n)],
            "rv_percentile_24h": [float((i % 100) + 1) for i in range(n)],
            "spread_bps": [float(i + 5) for i in range(n)],
            "slippage_bps": [0.0 if i % 2 else float(i + 1) for i in range(n)],
            "market_depth": [float(i + 6) for i in range(n)],
            "basis_zscore": [0.0 if i % 2 else float(i + 1) for i in range(n)],
            "liquidation_notional": [0.0] * (n - 1) + [10.0],
            "volume": [float(i + 7) for i in range(n)],
        }
    )
    frame.to_parquet(out / f"market_context_{symbol}_2023-01.parquet", index=False)


def test_run_data_quality_audit_writes_outputs(tmp_path):
    _write_market_context(tmp_path, run_id="source_run", symbol="BTCUSDT")

    rc = main(
        [
            "--run-id",
            "data_quality_audit_test",
            "--symbols",
            "BTCUSDT",
            "--data-root",
            str(tmp_path),
            "--source-run-id",
            "source_run",
        ]
    )

    assert rc == 0
    out_dir = tmp_path / "reports" / "data_quality_audit" / "data_quality_audit_test"
    for name in [
        "data_quality_audit.json",
        "data_quality_audit.parquet",
        "data_quality_audit.md",
        "mechanism_data_quality.json",
    ]:
        assert (out_dir / name).exists()

    payload = json.loads((out_dir / "data_quality_audit.json").read_text(encoding="utf-8"))
    mechanism = json.loads((out_dir / "mechanism_data_quality.json").read_text(encoding="utf-8"))
    assert payload["schema_version"] == "data_quality_audit_v1"
    assert payload["row_count"] == 13
    assert payload["source_run_id"] == "source_run"
    assert mechanism["schema_version"] == "mechanism_data_quality_v1"
    assert pd.read_parquet(out_dir / "data_quality_audit.parquet").shape[0] == 13
