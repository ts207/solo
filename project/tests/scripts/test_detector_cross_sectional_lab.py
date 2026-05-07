from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from project.scripts import detector_cross_sectional_lab as lab


def test_strategy_backlog_records_new_families_and_freezes_short_build() -> None:
    path = Path("spec/research/strategy_backlog.yaml")
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))

    assert payload["active_research_families"] == [
        "CROSS_SECTIONAL_PERP_MOMENTUM",
        "VOL_COMPRESSION_BREAKOUT",
        "FUNDING_DIVERGENCE",
        "LIQUIDITY_SHOCK_RECOVERY_FORWARD",
        "SESSION_BREAKOUT_REVERSAL",
    ]
    assert payload["frozen_families"] == ["SHORT_BUILD_CONTINUATION"]
    assert payload["do_not_trade"] == ["all_new_families_until_validated"]


def test_cross_sectional_lab_keeps_outputs_research_only(monkeypatch, tmp_path: Path) -> None:
    symbols = ["AAAUSDT", "BBBUSDT", "CCCUSDT", "DDDUSDT", "EEEUSDT", "FFFUSDT"]
    rows = []
    drifts = {
        "AAAUSDT": 0.004,
        "BBBUSDT": 0.003,
        "CCCUSDT": 0.0002,
        "DDDUSDT": -0.0002,
        "EEEUSDT": -0.003,
        "FFFUSDT": -0.004,
    }
    starts = {symbol: 100.0 for symbol in symbols}
    for year in (2022, 2023, 2024):
        timestamps = pd.date_range(f"{year}-01-01", periods=120, freq="h", tz="UTC")
        for symbol in symbols:
            drift = drifts[symbol]
            close = starts[symbol] * np.cumprod(np.full(len(timestamps), 1.0 + drift))
            starts[symbol] = float(close[-1])
            for idx, ts in enumerate(timestamps):
                rows.append(
                    {
                        "timestamp": ts,
                        "symbol": symbol,
                        "open": close[idx],
                        "high": close[idx] * 1.001,
                        "low": close[idx] * 0.999,
                        "close": close[idx],
                        "volume": 1000.0 + idx,
                        "oi_notional": 1_000_000.0 + idx,
                        "funding_rate_scaled": 0.0,
                        "shadow_year": str(year),
                        "shadow_month": ts.strftime("%Y-%m"),
                    }
                )
    frame = lab._add_cross_sectional_features(pd.DataFrame(rows))

    def fake_load_frames(repo_root: Path, requested_symbols: list[str], years: list[int]):
        return frame, {}, {symbol: {"rows": 360} for symbol in requested_symbols}

    monkeypatch.setattr(lab, "_load_frames", fake_load_frames)
    report, csv = lab.build_cross_sectional_report(
        repo_root=tmp_path,
        symbols=symbols,
        years=[2022, 2023, 2024],
        lookbacks=[12],
        horizons=[12],
        basket_size=2,
        min_cross_section=6,
        rebalance_minutes=60,
        extra_slippage_bps=0.0,
        cost_overrides={symbol: 0.0 for symbol in symbols},
        json_output=tmp_path / "report.json",
        csv_output=tmp_path / "report.csv",
    )

    assert report["candidate_count"] == 16
    assert report["paper_approved_events"] == []
    assert report["live_approved_events"] == []
    assert report["top_variants"][0]["paper_approved"] is False
    assert report["top_variants"][0]["live_approved"] is False
    assert "fresh_validation_candidate" in report["status_counts"]
    assert not csv.empty
    assert (tmp_path / "report.json").exists()
    assert (tmp_path / "report.csv").exists()
