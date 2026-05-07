from __future__ import annotations

from pathlib import Path

import pandas as pd

from project.scripts import detector_vol_compression_lab as lab


def _synthetic_vol_frame() -> pd.DataFrame:
    rows = []
    symbols = ["AAAUSDT", "BBBUSDT", "CCCUSDT"]
    for symbol in symbols:
        timestamps = pd.date_range("2022-01-01", periods=180, freq="5min", tz="UTC")
        close = [100.0] * len(timestamps)
        high = [100.2] * len(timestamps)
        low = [99.8] * len(timestamps)
        for idx in (60, 100, 140):
            close[idx] = 101.0
            high[idx] = 101.2
            close[idx + 12] = 102.0
        for idx in (70, 110, 150):
            close[idx] = 99.0
            low[idx] = 98.8
            close[idx + 12] = 98.0
        for idx in (80, 120):
            close[idx] = 99.8
            high[idx] = 101.2
            close[idx + 12] = 98.8
        for idx in (90, 130):
            close[idx] = 100.2
            low[idx] = 98.8
            close[idx + 12] = 101.2
        for idx, ts in enumerate(timestamps):
            rows.append(
                {
                    "timestamp": ts,
                    "symbol": symbol,
                    "close": close[idx],
                    "high": high[idx],
                    "low": low[idx],
                    "rv_percentile_96": 5.0,
                    "range_percentile_96": 5.0,
                    "atr_percentile_96": 5.0,
                    "donchian_high_96": 100.5,
                    "donchian_low_96": 99.5,
                    "volume_z": 2.0,
                    "oi_change_12": 0.01,
                    "funding_sign": "zero",
                    "trend_regime": "uptrend",
                    "shadow_year": "2022",
                    "shadow_month": ts.strftime("%Y-%m"),
                }
            )
    return pd.DataFrame(rows).sort_values(["symbol", "timestamp"]).reset_index(drop=True)


def test_vol_compression_lab_writes_research_only_outputs(monkeypatch, tmp_path: Path) -> None:
    frame = _synthetic_vol_frame()
    symbols = ["AAAUSDT", "BBBUSDT", "CCCUSDT"]

    def fake_load_frames(repo_root: Path, requested_symbols: list[str], years: list[int]):
        return frame, {}, {symbol: {"rows": 180} for symbol in requested_symbols}

    monkeypatch.setattr(lab, "_load_frames", fake_load_frames)
    report, csv = lab.build_vol_compression_report(
        repo_root=tmp_path,
        symbols=symbols,
        years=[2022],
        compression_pcts=[10.0],
        range_pcts=[10.0],
        breakout_buffer_bps=[0.0],
        exit_policies=["time_stop12_max48"],
        cooldown_bars=12,
        extra_slippage_bps=10.0,
        cost_overrides={symbol: 0.0 for symbol in symbols},
        json_output=tmp_path / "vol_compression_lab_report.json",
        csv_output=tmp_path / "top_vol_compression_variants.csv",
    )

    assert report["candidate_count"] == 32
    assert set(csv["base_variant"]) == {
        "COMPRESSION_UP_BREAKOUT_CONTINUATION",
        "COMPRESSION_DOWN_BREAKOUT_CONTINUATION",
        "COMPRESSION_UP_FAKEOUT_REVERSAL",
        "COMPRESSION_DOWN_FAKEOUT_REVERSAL",
    }
    assert report["paper_approved_events"] == []
    assert report["live_approved_events"] == []
    assert report["top_variants"][0]["paper_approved"] is False
    assert report["top_variants"][0]["live_approved"] is False
    assert (tmp_path / "vol_compression_lab_report.json").exists()
    assert (tmp_path / "top_vol_compression_variants.csv").exists()
