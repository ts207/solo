from __future__ import annotations

from pathlib import Path

import pandas as pd

from project.scripts import detector_daily_long_trend_lab as lab


def _synthetic_daily_source() -> pd.DataFrame:
    rows = []
    symbols = [
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
        "BNBUSDT",
        "XRPUSDT",
        "ADAUSDT",
        "DOGEUSDT",
        "LTCUSDT",
    ]
    timestamps = pd.date_range("2022-01-01", periods=96 * 360, freq="5min", tz="UTC")
    for symbol_idx, symbol in enumerate(symbols):
        close = 100.0 + symbol_idx
        drift = (symbol_idx - 3.0) * 0.000001
        funding = (2.0 - symbol_idx) * 0.0000005
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
                    "oi_notional": 1_000_000.0 + symbol_idx + len(rows) * 0.01,
                    "funding_rate_scaled": funding,
                    "shadow_year": "2022",
                    "shadow_month": ts.strftime("%Y-%m"),
                }
            )
    return pd.DataFrame(rows).sort_values(["symbol", "timestamp"]).reset_index(drop=True)


def test_daily_long_trend_lab_writes_research_only_outputs(monkeypatch, tmp_path: Path) -> None:
    frame = _synthetic_daily_source()
    symbols = sorted(frame["symbol"].unique().tolist())

    def fake_prepare(repo_root: Path, symbol: str, years: list[int]) -> pd.DataFrame:
        del repo_root, years
        return frame[frame["symbol"] == symbol].drop(columns=["symbol"]).copy()

    monkeypatch.setattr(lab.daily_lab, "_prepare_symbol_frame", fake_prepare)
    report, csv = lab.build_daily_long_trend_report(
        repo_root=tmp_path,
        symbols=symbols,
        years=[2022],
        extra_slippage_bps=10.0,
        cost_overrides={symbol: 0.0 for symbol in symbols},
        json_output=tmp_path / "daily_long_trend_lab_report.json",
        csv_output=tmp_path / "top_daily_long_trend_variants.csv",
    )

    assert report["scope"]["family"] == "DAILY_LONG_TREND"
    assert report["scope"]["portfolio"] == "long_only_daily_rebalance"
    assert report["candidate_count"] == 696
    assert report["signal_candidate_count"] == 288
    assert set(csv["control_type"]) == set(lab.CONTROL_TYPES)
    assert set(csv[csv["control_type"] == "top_momentum_long"]["rank_signal"]) == set(
        lab.RANK_SIGNALS
    )
    assert set(csv["hold_days"]) == {5, 10, 20}
    assert set(csv[csv["control_type"] == "top_momentum_long"]["basket_size"]) == {1, 2, 3}
    assert set(csv["funding_filter"]) == {"off", "not_extreme"}
    assert set(csv["btc_regime"]) == {"any", "btc_above_30d_ma"}
    assert set(csv["vol_regime"]) == {"any", "not_crash_vol"}
    for column in (
        "annualized_sharpe",
        "max_drawdown",
        "exposure_days",
        "turnover",
        "by_symbol",
        "by_year",
        "by_month",
        "funding_pnl_share",
        "price_pnl_share",
        "top_symbol_pnl_share",
        "top_month_pnl_share",
    ):
        assert column in csv.columns
    assert report["paper_approved_events"] == []
    assert report["live_approved_events"] == []
    assert (tmp_path / "daily_long_trend_lab_report.json").exists()
    assert (tmp_path / "top_daily_long_trend_variants.csv").exists()


def test_daily_long_trend_lab_does_not_reference_deferred_market_microstructure_data() -> None:
    source = Path("project/scripts/detector_daily_long_trend_lab.py").read_text(encoding="utf-8")
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
