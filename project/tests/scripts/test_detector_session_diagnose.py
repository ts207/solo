from __future__ import annotations

from pathlib import Path

import pandas as pd

from project.scripts import detector_session_diagnose as diagnose


def _synthetic_session_frame() -> pd.DataFrame:
    rows = []
    symbols = ["AAAUSDT", "BBBUSDT", "CCCUSDT"]
    timestamps = pd.date_range("2022-01-01", periods=360, freq="5min", tz="UTC")
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
            close = 100.0 + (idx * 0.01)
            rows.append(
                {
                    "timestamp": ts,
                    "symbol": symbol,
                    "close": close,
                    "high": close + 0.2,
                    "low": close - 0.2,
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


def test_session_funding_window_diagnosis_writes_deduped_outputs(
    monkeypatch, tmp_path: Path
) -> None:
    frame = _synthetic_session_frame()
    symbols = ["AAAUSDT", "BBBUSDT", "CCCUSDT"]

    def fake_load_frames(repo_root: Path, requested_symbols: list[str], years: list[int]):
        return frame, {}, {symbol: {"rows": 360} for symbol in requested_symbols}

    monkeypatch.setattr(diagnose.session_lab, "_load_frames", fake_load_frames)
    report, events = diagnose.build_diagnosis(
        repo_root=tmp_path,
        symbols=symbols,
        years=[2022],
        variant="FUNDING_WINDOW_DRIFT",
        breakout_buffer_bps=[0.0, 5.0],
        exit_policies=["time_stop12_max48"],
        extra_slippage_bps=10.0,
        cost_overrides={symbol: 0.0 for symbol in symbols},
        json_output=tmp_path / "session_funding_window_diagnosis.json",
        events_output=tmp_path / "session_funding_window_events.csv",
    )

    assert report["decision"] == "research_only_do_not_promote_do_not_trade"
    assert report["scope"]["event_fingerprint"] == [
        "symbol",
        "event_ts",
        "direction",
        "exit_policy",
    ]
    assert report["dedupe"]["deduped_group_count"] < report["dedupe"]["variant_count"]
    assert report["dedupe"]["selected_group"]["same_event_set"] is True
    assert set(events["funding_timestamp_utc"]).issubset({"08:00", "16:00"})
    assert report["paper_approved_events"] == []
    assert report["live_approved_events"] == []
    assert set(report["placebos"]) == {
        "same_sessions_random_funding_signs",
        "same_funding_signs_shifted_plus_1_day",
        "same_timestamps_inverted_direction",
        "non_funding_hours_only",
    }
    assert (tmp_path / "session_funding_window_diagnosis.json").exists()
    assert (tmp_path / "session_funding_window_events.csv").exists()


def test_session_diagnosis_does_not_reference_deferred_market_microstructure_data() -> None:
    source = Path("project/scripts/detector_session_diagnose.py").read_text(encoding="utf-8")
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
