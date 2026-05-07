from __future__ import annotations

from pathlib import Path

import pandas as pd

from project.scripts import detector_funding_divergence_lab as lab


def _synthetic_funding_frame() -> pd.DataFrame:
    rows = []
    symbols = ["AAAUSDT", "BBBUSDT", "CCCUSDT"]
    for symbol in symbols:
        timestamps = pd.date_range("2022-01-01", periods=180, freq="5min", tz="UTC")
        close = [100.0] * len(timestamps)
        funding = [0.001] * len(timestamps)
        for idx in range(48, len(timestamps)):
            close[idx] = close[idx - 1] * 1.001
            funding[idx] = funding[idx - 1] - 0.00001
        for idx in (80, 120, 160):
            funding[idx] = -abs(funding[idx - 1])
        for idx, ts in enumerate(timestamps):
            rows.append(
                {
                    "timestamp": ts,
                    "symbol": symbol,
                    "close": close[idx],
                    "price_ret_12": 0.012 if idx >= 48 else 0.0,
                    "price_ret_48": 0.048 if idx >= 48 else 0.0,
                    "funding_level": funding[idx],
                    "funding_slope_3": -0.00003,
                    "funding_slope_6": -0.00006,
                    "funding_abs_percentile": 95.0,
                    "funding_sign": "positive" if funding[idx] > 0.0 else "negative",
                    "funding_sign_flip": idx in {80, 120, 160},
                    "oi_change_12": 0.01,
                    "volume_z": 2.0,
                    "failed_breakout_rejection_24": idx in {90, 130},
                    "failed_breakdown_reclaim_24": False,
                    "trend_regime": "uptrend",
                    "vol_regime": "mid_vol",
                    "shadow_year": "2022",
                    "shadow_month": ts.strftime("%Y-%m"),
                }
            )
    return pd.DataFrame(rows).sort_values(["symbol", "timestamp"]).reset_index(drop=True)


def test_funding_divergence_lab_writes_research_only_outputs(monkeypatch, tmp_path: Path) -> None:
    frame = _synthetic_funding_frame()
    symbols = ["AAAUSDT", "BBBUSDT", "CCCUSDT"]

    def fake_load_frames(repo_root: Path, requested_symbols: list[str], years: list[int]):
        return frame, {}, {symbol: {"rows": 180} for symbol in requested_symbols}

    monkeypatch.setattr(lab, "_load_frames", fake_load_frames)
    report, csv = lab.build_funding_divergence_report(
        repo_root=tmp_path,
        symbols=symbols,
        years=[2022],
        lookbacks=[12],
        funding_pcts=[90.0],
        exit_policies=["time_stop12_max48"],
        cooldown_bars=12,
        extra_slippage_bps=10.0,
        cost_overrides={symbol: 0.0 for symbol in symbols},
        json_output=tmp_path / "funding_divergence_lab_report.json",
        csv_output=tmp_path / "top_funding_divergence_variants.csv",
    )

    assert report["candidate_count"] == 324
    assert set(csv["base_variant"]) == {
        "PRICE_UP_FUNDING_DOWN_CONTINUATION_LONG",
        "PRICE_UP_FUNDING_HIGH_FAILED_BREAKOUT_SHORT",
        "PRICE_DOWN_FUNDING_UP_CONTINUATION_SHORT",
        "PRICE_DOWN_FUNDING_NEG_FAILED_BREAKDOWN_LONG",
        "FUNDING_SIGN_FLIP_CONTINUATION",
        "FUNDING_SIGN_FLIP_REVERSAL",
    }
    assert report["paper_approved_events"] == []
    assert report["live_approved_events"] == []
    assert report["top_variants"][0]["paper_approved"] is False
    assert report["top_variants"][0]["live_approved"] is False
    assert (tmp_path / "funding_divergence_lab_report.json").exists()
    assert (tmp_path / "top_funding_divergence_variants.csv").exists()


def test_funding_divergence_regime_artifact_guard() -> None:
    row = {
        "event_count": 150,
        "top_symbol_month_share": 0.10,
        "top_month_event_share": 0.40,
        "best_exit": {
            "net_bps": 25.0,
            "t_stat": 3.0,
            "cost_survival": 0.9,
            "slippage_plus_10_bps": {"survives": True},
        },
        "positive_symbols": ["AAAUSDT", "BBBUSDT", "CCCUSDT"],
        "walk_forward": {"pass": True},
    }

    assert lab._status(row) == "regime_artifact_research_only"
