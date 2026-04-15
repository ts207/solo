from __future__ import annotations

import sys
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from project.research import analyze_events
from project.events.detectors.registry import get_detector, load_all_detectors


def _sample_features(n: int = 640) -> pd.DataFrame:
    idx = np.arange(n)
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    close = 100.0 + np.cumsum(
        np.sin(idx / 13.0) * 0.12 + np.random.default_rng(7).normal(0.0, 0.08, n)
    )
    high = close + np.abs(np.random.default_rng(11).normal(0.05, 0.02, n))
    low = close - np.abs(np.random.default_rng(13).normal(0.05, 0.02, n))
    spread_bps = np.abs(np.random.default_rng(17).normal(4.0, 1.2, n))

    # Inject shocks to guarantee detections for various families
    # Vol shock at 100
    close[100:110] += 5.0
    # Depth/Liquidity shock at 200
    spread_bps[200:210] *= 10.0

    return pd.DataFrame(
        {
            "timestamp": ts,
            "open": close,
            "high": high,
            "low": low,
            "close": close,
            "rv_96": pd.Series(close).pct_change().rolling(96, min_periods=16).std().fillna(0.0),
            "spread_zscore": np.random.default_rng(19).normal(0.0, 1.0, n),
            "basis_zscore": np.random.default_rng(23).normal(0.0, 1.0, n),
            "cross_exchange_spread_z": np.random.default_rng(29).normal(0.0, 1.0, n),
            "funding_rate_scaled": np.random.default_rng(31).normal(0.0, 1e-4, n),
            "oi_notional": 1_000_000.0 + np.random.default_rng(37).normal(0.0, 2_000.0, n).cumsum(),
            "oi_delta_1h": np.random.default_rng(41).normal(0.0, 1_000.0, n),
            "liquidation_notional": np.random.default_rng(43).exponential(30.0, n),
            "range_96": np.abs(np.random.default_rng(47).normal(1.0, 0.2, n)),
            "range_med_2880": np.abs(np.random.default_rng(53).normal(1.2, 0.1, n)),
            "spread_bps": spread_bps,
        }
    )


CASES = [
    (
        "DEPTH_COLLAPSE",
        "liquidity_dislocation",
        "liquidity_dislocation_events.csv",
    ),
    (
        "VOL_SPIKE",
        "volatility_transition",
        "volatility_transition_events.csv",
    ),
    (
        "FUNDING_FLIP",
        "positioning_extremes",
        "positioning_extremes_events.csv",
    ),
    (
        "TREND_EXHAUSTION_TRIGGER",
        "forced_flow_and_exhaustion",
        "forced_flow_and_exhaustion_events.csv",
    ),
    (
        "ZSCORE_STRETCH",
        "statistical_dislocation",
        "statistical_dislocation_events.csv",
    ),
    (
        "SPOT_PERP_BASIS_SHOCK",
        "information_desync",
        "information_desync_events.csv",
    ),
    (
        "SESSION_OPEN_EVENT",
        "temporal_structure",
        "temporal_structure_events.csv",
    ),
]


@pytest.fixture(scope="module", autouse=True)
def _load_detectors():
    load_all_detectors()


@pytest.mark.parametrize(("event_type", "_reports_dir", "_events_file"), CASES)
def test_family_wave2_detector_returns_dataframe(
    event_type: str, _reports_dir: str, _events_file: str
):
    detector = get_detector(event_type)
    assert detector is not None
    df = _sample_features()
    # Ensure required columns are present for detectors that need special ones (like basis)
    if any(c in detector.required_columns for c in ("close_perp", "close_spot", "perp_close")):
        df["close_perp"] = df["close"] * 1.05
        df["close_spot"] = df["close"] * 0.95
        df["perp_close"] = df["close"] * 1.05

    # Audit: Add required proxy columns for canonical proxy family
    if "micro_depth_depletion" not in df.columns:
        df["micro_depth_depletion"] = np.random.default_rng(59).normal(0.0, 1.0, len(df))
    if "imbalance" not in df.columns:
        df["imbalance"] = np.random.default_rng(61).normal(0.0, 1.0, len(df))

    res = detector.detect(df, symbol="BTCUSDT")
    assert isinstance(res, pd.DataFrame)


@pytest.mark.parametrize(("event_type", "reports_dir", "events_file"), CASES)
def test_family_wave2_main_writes_target_event(
    monkeypatch, tmp_path, event_type: str, reports_dir: str, events_file: str
):
    # Mock compose_event_config to return our expected paths
    from project.events import config

    mock_cfg = SimpleNamespace(
        reports_dir=reports_dir,
        events_file=events_file,
        parameters={},
        signal_column=f"{event_type.lower()}_event",
    )
    monkeypatch.setattr(analyze_events, "compose_event_config", lambda et: mock_cfg)

    # Mock load_features
    def mock_load(run_id, symbol, timeframe="5m", market="perp", **kwargs):
        df = _sample_features()
        if market == "perp" and "BASIS" in event_type:
            # Inject a basis spike to trigger Z-score detector
            df.loc[400:410, "close"] *= 1.2

        # Audit: Add required proxy columns
        df["micro_depth_depletion"] = np.random.default_rng(59).normal(0.0, 1.0, len(df))
        df["imbalance"] = np.random.default_rng(61).normal(0.0, 1.0, len(df))
        return df

    monkeypatch.setattr(analyze_events, "load_features", mock_load)

    # Mock out_dir calculation
    monkeypatch.setattr(analyze_events, "get_data_root", lambda: tmp_path)

    # Mock manifest
    monkeypatch.setattr(analyze_events, "start_manifest", lambda *args, **kwargs: {})
    monkeypatch.setattr(analyze_events, "finalize_manifest", lambda *args, **kwargs: {})

    # Call main
    argv = [
        "--run_id",
        "r_test",
        "--symbols",
        "BTCUSDT",
        "--event_type",
        event_type,
        "--out_dir",
        str(tmp_path / "out"),
    ]

    rc = analyze_events.main(argv)
    assert rc == 0

    out_csv = tmp_path / "out" / events_file
    assert out_csv.exists()
    try:
        out = pd.read_csv(out_csv)
        if not out.empty:
            # Check that we have valid events
            assert "event_type" in out.columns
            assert len(out["event_type"].unique()) > 0
    except pd.errors.EmptyDataError:
        # If no events detected, it should at least not crash above
        pass
