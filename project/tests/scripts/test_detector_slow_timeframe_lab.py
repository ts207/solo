from __future__ import annotations

from pathlib import Path

import pandas as pd

from project.scripts import detector_slow_timeframe_lab as lab


def _synthetic_slow_frame() -> pd.DataFrame:
    rows = []
    symbols = [f"SYM{idx}USDT" for idx in range(8)]
    timestamps = pd.date_range("2022-01-01", periods=96 * 35, freq="5min", tz="UTC")
    for symbol_idx, symbol in enumerate(symbols):
        close = 100.0 + symbol_idx
        drift = (symbol_idx - 3.5) * 0.000002
        for ts in timestamps:
            close *= 1.0 + drift
            rows.append(
                {
                    "timestamp": ts,
                    "symbol": symbol,
                    "open": close,
                    "high": close * 1.001,
                    "low": close * 0.999,
                    "close": close,
                    "volume": 1000.0,
                    "oi_notional": 1_000_000.0 + symbol_idx,
                    "funding_rate_scaled": 0.001,
                    "shadow_year": "2022",
                    "shadow_month": ts.strftime("%Y-%m"),
                }
            )
    return pd.DataFrame(rows).sort_values(["symbol", "timestamp"]).reset_index(drop=True)


def test_slow_timeframe_lab_writes_research_only_outputs(monkeypatch, tmp_path: Path) -> None:
    frame = _synthetic_slow_frame()
    symbols = sorted(frame["symbol"].unique().tolist())

    def fake_prepare(repo_root: Path, symbol: str, years: list[int]) -> pd.DataFrame:
        del repo_root, years
        return frame[frame["symbol"] == symbol].drop(columns=["symbol"]).copy()

    monkeypatch.setattr(lab, "_prepare_symbol_frame", fake_prepare)
    report, csv = lab.build_slow_timeframe_report(
        repo_root=tmp_path,
        symbols=symbols,
        years=[2022],
        extra_slippage_bps=10.0,
        cost_overrides={symbol: 0.0 for symbol in symbols},
        json_output=tmp_path / "slow_timeframe_lab_report.json",
        csv_output=tmp_path / "top_slow_timeframe_variants.csv",
    )

    assert report["scope"]["family"] == "SLOW_RELATIVE_STRENGTH_ROTATION"
    assert report["scope"]["bar_interval"] == "4h"
    assert report["scope"]["signal_count"] == 1
    assert report["candidate_count"] == 1
    assert set(csv["variant_id"]) == {
        "SLOW_RELATIVE_STRENGTH_ROTATION__4H__TOP2_BOTTOM2__MOM_24H_72H_VOL_ADJ__NO_EXTREME_FUNDING"
    }
    assert int(csv.iloc[0]["event_count"]) > 0
    assert report["paper_approved_events"] == []
    assert report["live_approved_events"] == []
    assert report["top_variants"][0]["paper_approved"] is False
    assert report["top_variants"][0]["live_approved"] is False
    assert (tmp_path / "slow_timeframe_lab_report.json").exists()
    assert (tmp_path / "top_slow_timeframe_variants.csv").exists()


def test_slow_timeframe_lab_does_not_reference_deferred_market_microstructure_data() -> None:
    source = Path("project/scripts/detector_slow_timeframe_lab.py").read_text(encoding="utf-8")
    forbidden_terms = [
        "_".join(parts)
        for parts in (
            ("book", "ticker"),
            ("spread", "bps"),
            ("depth", "usd"),
            ("best", "bid"),
            ("best", "ask"),
        )
    ]
    for forbidden in forbidden_terms:
        assert forbidden not in source
