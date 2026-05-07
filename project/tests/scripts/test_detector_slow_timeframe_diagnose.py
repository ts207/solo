from __future__ import annotations

from pathlib import Path

import pandas as pd

from project.scripts import detector_slow_timeframe_diagnose as diagnose


def _synthetic_slow_frame() -> pd.DataFrame:
    rows = []
    symbols = [f"SYM{idx}USDT" for idx in range(8)]
    timestamps = pd.date_range("2022-01-01", periods=96 * 45, freq="5min", tz="UTC")
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


def test_slow_timeframe_diagnosis_writes_grid_and_ranking_sanity(
    monkeypatch, tmp_path: Path
) -> None:
    frame = _synthetic_slow_frame()
    symbols = sorted(frame["symbol"].unique().tolist())

    def fake_prepare(repo_root: Path, symbol: str, years: list[int]) -> pd.DataFrame:
        del repo_root, years
        return frame[frame["symbol"] == symbol].drop(columns=["symbol"]).copy()

    monkeypatch.setattr(diagnose.slow_lab, "_prepare_symbol_frame", fake_prepare)
    report, csv = diagnose.build_slow_timeframe_diagnosis(
        repo_root=tmp_path,
        symbols=symbols,
        years=[2022],
        extra_slippage_bps=10.0,
        cost_overrides={symbol: 0.0 for symbol in symbols},
        json_output=tmp_path / "slow_timeframe_diagnosis.json",
        csv_output=tmp_path / "slow_relative_strength_diagnosis.csv",
    )

    assert report["candidate_count"] == 36
    assert set(csv["direction_mode"]) == {"momentum", "reversal"}
    assert set(csv["rebalance_hours"]) == {4, 12, 24}
    assert set(csv["hold_hours"]) == {4, 12, 24}
    assert set(csv["basket_size"]) == {2, 3}
    for column in (
        "long_leg_gross",
        "long_leg_net",
        "short_leg_gross",
        "short_leg_net",
        "basket_gross",
        "basket_net",
        "no_cost_gross",
        "with_cost_net",
        "plus_10_bps_net",
    ):
        assert column in csv.columns
    assert report["ranking_sanity_sample"]
    assert report["paper_approved_events"] == []
    assert report["live_approved_events"] == []
    assert (tmp_path / "slow_timeframe_diagnosis.json").exists()
    assert (tmp_path / "slow_relative_strength_diagnosis.csv").exists()
    assert (tmp_path / "slow_relative_strength_ranking_sanity.csv").exists()


def test_slow_timeframe_diagnosis_does_not_reference_deferred_market_microstructure_data() -> None:
    source = Path("project/scripts/detector_slow_timeframe_diagnose.py").read_text(encoding="utf-8")
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
