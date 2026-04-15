from __future__ import annotations

import pandas as pd
import pytest

from project.io.utils import write_parquet
from project.pipelines.eval import build_market_efficiency_report as report_mod


def test_build_market_efficiency_report_frame_emits_one_row_per_symbol() -> None:
    bars = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=12, freq="1min", tz="UTC").tolist()
            + pd.date_range("2026-01-02", periods=12, freq="1min", tz="UTC").tolist(),
            "symbol": ["BTCUSDT"] * 12 + ["ETHUSDT"] * 12,
            "close": [100 + idx for idx in range(12)] + [200 + idx * 2 for idx in range(12)],
        }
    )

    report = report_mod.build_market_efficiency_report_frame(bars, timeframe="1m")

    assert list(report["symbol"]) == ["BTCUSDT", "ETHUSDT"]
    assert list(report["timeframe"]) == ["1m", "1m"]
    assert set(report.columns) == {
        "symbol",
        "timeframe",
        "observations",
        "variance_ratio",
        "hurst_exponent",
        "return_autocorr",
        "data_start",
        "data_end",
    }


def test_run_market_efficiency_report_writes_promised_artifact(
    tmp_path, monkeypatch
) -> None:
    bars = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=32, freq="1min", tz="UTC"),
            "symbol": ["BTCUSDT"] * 32,
            "close": [100.0 + idx for idx in range(32)],
        }
    )
    bars_path = tmp_path / "bars.parquet"
    out_path = tmp_path / "lake" / "reports" / "market_health" / "efficiency_v1.parquet"
    write_parquet(bars, bars_path)

    finalized: list[tuple[str, dict[str, object] | None]] = []
    monkeypatch.setattr(report_mod, "get_data_root", lambda: tmp_path)
    monkeypatch.setattr(report_mod, "start_manifest", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        report_mod,
        "finalize_manifest",
        lambda _manifest, status, stats=None: finalized.append((status, stats)),
    )

    written = report_mod.run_market_efficiency_report(
        run_id="efficiency_test",
        symbols=["BTCUSDT"],
        bars_path=str(bars_path),
        out_path=str(out_path),
    )

    persisted = pd.read_parquet(written)
    assert written == out_path
    assert persisted.loc[0, "symbol"] == "BTCUSDT"
    assert int(persisted.loc[0, "observations"]) == 31
    assert finalized == [("success", {"rows": 1, "symbols": 1})]


def test_run_market_efficiency_report_rejects_multisymbol_override_without_symbol_column(
    tmp_path, monkeypatch
) -> None:
    bars = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=16, freq="1min", tz="UTC"),
            "close": [100.0 + idx for idx in range(16)],
        }
    )
    bars_path = tmp_path / "bars_no_symbol.parquet"
    write_parquet(bars, bars_path)
    monkeypatch.setattr(report_mod, "get_data_root", lambda: tmp_path)

    with pytest.raises(ValueError, match="without a 'symbol' column"):
        report_mod.run_market_efficiency_report(
            run_id="efficiency_test",
            symbols=["BTCUSDT", "ETHUSDT"],
            bars_path=str(bars_path),
        )
