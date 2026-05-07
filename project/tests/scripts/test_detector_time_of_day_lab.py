from __future__ import annotations

from pathlib import Path

import pandas as pd

from project.scripts import detector_time_of_day_lab as lab


def _synthetic_timing_frame() -> pd.DataFrame:
    rows = []
    symbols = ["AAAUSDT", "BBBUSDT", "CCCUSDT"]
    timestamps = pd.date_range("2022-01-01", periods=720, freq="5min", tz="UTC")
    for symbol in symbols:
        close = 100.0
        for ts in timestamps:
            if ts.minute == 0 and ts.hour == 16:
                close *= 1.002
            elif ts.minute == 0 and ts.hour == 17:
                close *= 1.001
            else:
                close *= 1.00002
            session = (
                "asia"
                if ts.hour <= 7
                else "europe"
                if ts.hour <= 13
                else "us"
                if ts.hour <= 21
                else "late_us"
            )
            rows.append(
                {
                    "timestamp": ts,
                    "symbol": symbol,
                    "open": close,
                    "close": close,
                    "high": close * 1.001,
                    "low": close * 0.999,
                    "volume": 1000.0,
                    "oi_notional": 1_000_000.0 + len(rows),
                    "funding_rate_scaled": 0.001,
                    "volume_z": 2.0,
                    "oi_change_12": 0.01,
                    "funding_sign": "positive",
                    "trend_regime": "uptrend",
                    "vol_regime": "mid_vol",
                    "session": session,
                    "shadow_year": "2022",
                    "shadow_month": ts.strftime("%Y-%m"),
                }
            )
    return pd.DataFrame(rows).sort_values(["symbol", "timestamp"]).reset_index(drop=True)


def test_time_of_day_lab_writes_research_only_outputs(monkeypatch, tmp_path: Path) -> None:
    frame = _synthetic_timing_frame()
    symbols = ["AAAUSDT", "BBBUSDT", "CCCUSDT"]

    def fake_load_frames(repo_root: Path, requested_symbols: list[str], years: list[int]):
        return (
            lab._add_timing_features(frame),
            {},
            {symbol: {"rows": 720} for symbol in requested_symbols},
        )

    monkeypatch.setattr(lab, "_load_frames", fake_load_frames)
    report, csv = lab.build_time_of_day_report(
        repo_root=tmp_path,
        symbols=symbols,
        years=[2022],
        hours=[16],
        exit_policies=["time_stop12_max48"],
        extra_slippage_bps=10.0,
        cost_overrides={symbol: 0.0 for symbol in symbols},
        json_output=tmp_path / "time_of_day_lab_report.json",
        csv_output=tmp_path / "top_time_of_day_variants.csv",
    )

    assert report["scope"]["family"] == "TIME_OF_DAY_DRIFT"
    assert report["candidate_count"] == 128
    assert report["paper_approved_events"] == []
    assert report["live_approved_events"] == []
    assert set(report["top_variants"][0]["controls"]) == {
        "same_hour_next_day",
        "same_hour_previous_day",
        "neighbor_hour_minus_1",
        "neighbor_hour_plus_1",
        "inverted_direction",
        "randomized_funding_sign",
        "non_funding_hours",
    }
    assert set(csv["direction_mode"]) == {
        "funding_fade",
        "trend_follow",
        "funding_sign",
        "previous_12bar_momentum",
    }
    assert (tmp_path / "time_of_day_lab_report.json").exists()
    assert (tmp_path / "top_time_of_day_variants.csv").exists()


def test_time_of_day_lab_does_not_reference_deferred_market_microstructure_data() -> None:
    source = Path("project/scripts/detector_time_of_day_lab.py").read_text(encoding="utf-8")
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
