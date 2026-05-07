from __future__ import annotations

from pathlib import Path

import pandas as pd

from project.scripts import detector_daily_carry_trend_diagnose as diagnose


def _synthetic_daily_source() -> pd.DataFrame:
    rows = []
    symbols = [f"SYM{idx}USDT" for idx in range(8)]
    timestamps = pd.date_range("2022-01-01", periods=96 * 320, freq="5min", tz="UTC")
    for symbol_idx, symbol in enumerate(symbols):
        close = 100.0 + symbol_idx
        drift = (symbol_idx - 3.5) * 0.000001
        funding = (3.5 - symbol_idx) * 0.000001
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


def test_daily_carry_trend_diagnosis_writes_bounded_outputs(monkeypatch, tmp_path: Path) -> None:
    frame = _synthetic_daily_source()
    symbols = sorted(frame["symbol"].unique().tolist())

    def fake_prepare(repo_root: Path, symbol: str, years: list[int]) -> pd.DataFrame:
        del repo_root, years
        return frame[frame["symbol"] == symbol].drop(columns=["symbol"]).copy()

    monkeypatch.setattr(diagnose.daily_lab, "_prepare_symbol_frame", fake_prepare)
    report, csv = diagnose.build_daily_carry_trend_diagnosis(
        repo_root=tmp_path,
        symbols=symbols,
        years=[2022],
        extra_slippage_bps=10.0,
        cost_overrides={symbol: 0.0 for symbol in symbols},
        json_output=tmp_path / "daily_carry_trend_diagnosis.json",
        csv_output=tmp_path / "daily_carry_trend_diagnosis.csv",
    )

    assert report["scope"]["family"] == "DAILY_CARRY_TREND_DIAGNOSIS"
    assert report["scope"]["base_family"] == "DAILY_CARRY_TREND"
    assert report["candidate_count"] == 72
    assert set(csv["rebalance_days"]) == {1, 7}
    assert set(csv["hold_days"]) == {5, 10, 20}
    assert set(csv["basket_size_per_side"]) == {1, 2, 3}
    assert set(csv["direction_mode"]) == {"trend_follow"}
    assert set(csv["rank_signal"]) == {"ret_14d", "ret_30d"}
    assert set(csv["funding_filter"]) == {"on", "off"}
    for column in (
        "gross_pnl",
        "net_pnl",
        "cost_paid_bps",
        "turnover",
        "annualized_turnover",
        "long_leg_pnl",
        "short_leg_pnl",
        "price_pnl",
        "funding_pnl",
        "by_year",
        "by_symbol",
        "max_drawdown",
        "sharpe",
    ):
        assert column in csv.columns
    assert report["paper_approved_events"] == []
    assert report["live_approved_events"] == []
    assert (tmp_path / "daily_carry_trend_diagnosis.json").exists()
    assert (tmp_path / "daily_carry_trend_diagnosis.csv").exists()


def test_daily_carry_trend_diagnosis_does_not_reference_deferred_market_microstructure_data() -> (
    None
):
    source = Path("project/scripts/detector_daily_carry_trend_diagnose.py").read_text(
        encoding="utf-8"
    )
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
