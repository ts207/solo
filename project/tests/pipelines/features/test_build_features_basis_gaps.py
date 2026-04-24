from __future__ import annotations

from pathlib import Path

import pandas as pd

from project.pipelines.features import build_features


def test_basis_features_keep_nan_when_spot_is_missing(monkeypatch):
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2024-01-01T00:00:00Z", "2024-01-01T00:06:00Z", "2024-01-01T00:12:00Z"],
                utc=True,
                format="ISO8601",
            ),
            "close": [100.0, 101.0, 102.0],
        }
    )

    spot = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z"], utc=True, format="ISO8601"),
            "spot_close": [100.0],
        }
    )

    monkeypatch.setattr(
        build_features,
        "_load_spot_close_reference",
        lambda symbol, run_id, data_root, timeframe="5m": spot,
    )
    out = build_features._add_basis_features(
        frame, symbol="BTCUSDT", run_id="r1", market="perp", data_root=Path("/tmp")
    )

    # Shift(1) means index 0 is NaN, index 1 has value from bar 0
    assert pd.isna(out.loc[0, "basis_bps"])
    assert out.loc[1, "basis_bps"] == 0.0
    assert pd.isna(out.loc[2, "basis_bps"])
    assert pd.isna(out.loc[1, "basis_zscore"])
    assert pd.isna(out.loc[2, "basis_zscore"])
    assert out.loc[0, "basis_spot_coverage"] == 1.0 / 3.0


def test_spread_zscore_uses_spread_bps_not_basis(monkeypatch):
    n = 120
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "close": [100.0] * n,
            "spread_bps": list(range(n)),
        }
    )
    spot = pd.DataFrame(
        {
            "timestamp": frame["timestamp"],
            "spot_close": [100.0] * n,
        }
    )
    monkeypatch.setattr(
        build_features,
        "_load_spot_close_reference",
        lambda symbol, run_id, data_root, timeframe="5m": spot,
    )
    out = build_features._add_basis_features(
        frame, symbol="BTCUSDT", run_id="r1", market="perp", data_root=Path("/tmp")
    )

    assert out["basis_bps"].fillna(0.0).abs().max() == 0.0
    assert out["basis_zscore"].fillna(0.0).abs().max() == 0.0
    assert out["spread_zscore"].fillna(0.0).abs().max() > 0.0


def test_basis_spot_coverage_counts_exact_spot_bars_only(monkeypatch):
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2024-01-01T00:00:00Z", "2024-01-01T00:05:00Z", "2024-01-01T00:10:00Z"],
                utc=True,
                format="ISO8601",
            ),
            "close": [100.0, 101.0, 102.0],
        }
    )
    # Middle spot bar is missing; backward asof still provides spot_close at 00:05,
    # but coverage should only count exact timestamp matches.
    spot = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2024-01-01T00:00:00Z", "2024-01-01T00:10:00Z"], utc=True, format="ISO8601"
            ),
            "spot_close": [100.0, 102.0],
        }
    )

    monkeypatch.setattr(
        build_features,
        "_load_spot_close_reference",
        lambda symbol, run_id, data_root, timeframe="5m": spot,
    )
    out = build_features._add_basis_features(
        frame, symbol="BTCUSDT", run_id="r1", market="perp", data_root=Path("/tmp")
    )
    assert out.loc[0, "basis_spot_coverage"] == 2.0 / 3.0


def test_basis_values_require_exact_spot_match(monkeypatch):
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01T00:05:00Z"], utc=True, format="ISO8601"),
            "close": [101.0],
        }
    )
    # Backward asof would otherwise map this to 00:00 spot (within 5m),
    # but basis should be masked when exact spot timestamp is missing.
    spot = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z"], utc=True, format="ISO8601"),
            "spot_close": [100.0],
        }
    )
    monkeypatch.setattr(
        build_features,
        "_load_spot_close_reference",
        lambda symbol, run_id, data_root, timeframe="5m": spot,
    )
    out = build_features._add_basis_features(
        frame, symbol="BTCUSDT", run_id="r1", market="perp", data_root=Path("/tmp")
    )
    assert pd.isna(out.loc[0, "basis_bps"])
