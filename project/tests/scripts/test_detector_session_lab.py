from __future__ import annotations

from pathlib import Path

import pandas as pd

from project.scripts import detector_session_lab as lab


def _synthetic_session_frame() -> pd.DataFrame:
    rows = []
    symbols = ["AAAUSDT", "BBBUSDT", "CCCUSDT"]
    timestamps = pd.date_range("2022-01-01", periods=288, freq="5min", tz="UTC")
    for symbol in symbols:
        for idx, ts in enumerate(timestamps):
            hour = ts.hour
            session = (
                "asia"
                if hour <= 7
                else "europe"
                if hour <= 13
                else "us"
                if hour <= 21
                else "late_us"
            )
            close = 100.0
            high = 100.2
            low = 99.8
            if idx in {96, 108, 120}:
                close = 101.0
                high = 101.2
            if idx in {168, 180, 192}:
                close = 99.0
                low = 98.8
            if idx in {264, 276}:
                close = 102.0
                high = 102.2
            rows.append(
                {
                    "timestamp": ts,
                    "symbol": symbol,
                    "close": close,
                    "high": high,
                    "low": low,
                    "asia_range_high": 100.5,
                    "asia_range_low": 99.5,
                    "europe_range_high": 100.5,
                    "europe_range_low": 99.5,
                    "volume_z": 2.0,
                    "oi_change_12": 0.01,
                    "funding_sign": "negative",
                    "trend_regime": "uptrend",
                    "vol_regime": "mid_vol",
                    "session": session,
                    "day_of_week": ts.dayofweek,
                    "weekend_flag": ts.dayofweek >= 5,
                    "shadow_year": "2022",
                    "shadow_month": ts.strftime("%Y-%m"),
                }
            )
    return pd.DataFrame(rows).sort_values(["symbol", "timestamp"]).reset_index(drop=True)


def test_session_lab_writes_research_only_outputs(monkeypatch, tmp_path: Path) -> None:
    frame = _synthetic_session_frame()
    symbols = ["AAAUSDT", "BBBUSDT", "CCCUSDT"]

    def fake_load_frames(repo_root: Path, requested_symbols: list[str], years: list[int]):
        return frame, {}, {symbol: {"rows": 288} for symbol in requested_symbols}

    monkeypatch.setattr(lab, "_load_frames", fake_load_frames)
    report, csv = lab.build_session_report(
        repo_root=tmp_path,
        symbols=symbols,
        years=[2022],
        breakout_buffer_bps=[0.0],
        exit_policies=["time_stop12_max48"],
        cooldown_bars=12,
        extra_slippage_bps=10.0,
        cost_overrides={symbol: 0.0 for symbol in symbols},
        json_output=tmp_path / "session_lab_report.json",
        csv_output=tmp_path / "top_session_variants.csv",
    )

    assert report["candidate_count"] == 384
    assert set(csv["base_variant"]) == {
        "ASIA_RANGE_EUROPE_BREAKOUT_CONTINUATION",
        "ASIA_RANGE_EUROPE_FAKEOUT_REVERSAL",
        "EUROPE_RANGE_US_BREAKOUT_CONTINUATION",
        "EUROPE_RANGE_US_FAKEOUT_REVERSAL",
        "US_TREND_LATE_US_CONTINUATION",
        "WEEKEND_RANGE_BREAKOUT",
        "WEEKEND_FAKEOUT_REVERSAL",
        "FUNDING_WINDOW_DRIFT",
    }
    assert report["paper_approved_events"] == []
    assert report["live_approved_events"] == []
    assert report["top_variants"][0]["paper_approved"] is False
    assert report["top_variants"][0]["live_approved"] is False
    assert report["scope"]["data_scope"] == ["OHLCV", "open_interest", "funding"]
    assert (tmp_path / "session_lab_report.json").exists()
    assert (tmp_path / "top_session_variants.csv").exists()


def test_session_lab_does_not_reference_book_data() -> None:
    source = Path("project/scripts/detector_session_lab.py").read_text(encoding="utf-8")

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
