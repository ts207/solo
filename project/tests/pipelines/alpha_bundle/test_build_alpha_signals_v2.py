from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

import project.pipelines.alpha_bundle.build_alpha_signals_v2 as build_alpha_signals_v2


def _bars_frame() -> pd.DataFrame:
    ts = pd.date_range("2026-01-01T00:00:00Z", periods=80, freq="15min", tz="UTC")
    close = pd.Series(range(100, 180), dtype=float)
    return pd.DataFrame(
        {
            "timestamp": ts,
            "close": close,
            "volume": 10.0,
        }
    )


def _funding_frame(*, scaled: bool) -> pd.DataFrame:
    data = {
        "timestamp": pd.date_range("2026-01-01T00:00:00Z", periods=80, freq="15min", tz="UTC"),
    }
    if scaled:
        data["funding_rate_scaled"] = [0.0001] * 80
    else:
        data["funding_rate"] = [0.0001] * 80
    return pd.DataFrame(data)


def test_alpha_signals_require_canonical_funding_rate_scaled(monkeypatch, tmp_path):
    bars_path = tmp_path / "bars.parquet"
    funding_path = tmp_path / "funding.parquet"
    bars_path.touch()
    funding_path.touch()

    calls = {"i": 0}

    def fake_read_parquet(_files):
        calls["i"] += 1
        return _bars_frame() if calls["i"] == 1 else _funding_frame(scaled=False)

    monkeypatch.setattr(build_alpha_signals_v2, "read_parquet", fake_read_parquet)
    monkeypatch.setattr(
        build_alpha_signals_v2, "write_parquet", lambda df, path: (Path(path), "parquet")
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_alpha_signals_v2.py",
            "--run_id",
            "alpha_test",
            "--symbol",
            "BTCUSDT",
            "--bars_path",
            str(bars_path),
            "--funding_path",
            str(funding_path),
            "--out_dir",
            str(tmp_path / "out"),
        ],
    )

    with pytest.raises(ValueError, match="funding_rate_scaled"):
        build_alpha_signals_v2.main()


def test_alpha_signals_emit_canonical_funding_rate_scaled(monkeypatch, tmp_path):
    bars_path = tmp_path / "bars.parquet"
    funding_path = tmp_path / "funding.parquet"
    bars_path.touch()
    funding_path.touch()

    calls = {"i": 0}
    captured: list[pd.DataFrame] = []

    def fake_read_parquet(_files):
        calls["i"] += 1
        return _bars_frame() if calls["i"] == 1 else _funding_frame(scaled=True)

    def fake_write_parquet(df, path):
        captured.append(df.copy())
        return Path(path), "parquet"

    monkeypatch.setattr(build_alpha_signals_v2, "read_parquet", fake_read_parquet)
    monkeypatch.setattr(build_alpha_signals_v2, "write_parquet", fake_write_parquet)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_alpha_signals_v2.py",
            "--run_id",
            "alpha_test",
            "--symbol",
            "BTCUSDT",
            "--bars_path",
            str(bars_path),
            "--funding_path",
            str(funding_path),
            "--out_dir",
            str(tmp_path / "out"),
        ],
    )

    rc = build_alpha_signals_v2.main()

    assert rc == 0
    assert captured
    assert "funding_rate_scaled" in captured[0].columns
    assert "funding_rate" in captured[0].columns
    assert captured[0]["funding_rate_scaled"].dropna().eq(0.0001).all()
