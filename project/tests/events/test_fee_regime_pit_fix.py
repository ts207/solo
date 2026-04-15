"""Regression test: FeeRegimeChangeDetector must not use future data."""

from __future__ import annotations
import pandas as pd
import numpy as np
from project.events.families.temporal import FeeRegimeChangeDetector


def _make_fee_df(fee_values: list[float]) -> pd.DataFrame:
    n = len(fee_values)
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": ts,
            "fee_bps": fee_values,
            "close": np.ones(n),
        }
    )


def test_fee_regime_detector_no_future_lookahead():
    """Detector must not fire on the bar BEFORE a fee change is confirmed.

    Bars 0-9: fee=1.0 (stable baseline)
    Bar 10:   fee=2.0 (first bar at new level — NOT confirmed yet, must NOT fire)
    Bar 11+:  fee=2.0 (second+ bar — confirmed, detector MAY fire starting here)
    """
    fees = [1.0] * 10 + [2.0] * 5
    df = _make_fee_df(fees)
    det = FeeRegimeChangeDetector()
    # Use small windows so we have valid quantiles in a short series
    params = {"lookback_window": 10, "min_periods": 2}
    events = det.detect(df, symbol="BTC", **params)

    # Bar index 10 timestamp = 2024-01-01 00:50:00 UTC
    # First permissible fire is bar 11 = 2024-01-01 00:55:00 UTC
    forbidden_ts = pd.Timestamp("2024-01-01 00:50:00", tz="UTC")
    fire_times = [pd.to_datetime(row["timestamp"]) for row in events.to_dict(orient="records")]
    assert forbidden_ts not in fire_times, (
        f"Detector fired at {forbidden_ts} — the first bar of a fee change before "
        "it was confirmed. This indicates future lookahead (LT-003)."
    )


def test_fee_regime_fires_only_after_confirmation():
    """Detector must fire at the confirmed bar (second bar at new level), not before.

    Bar 0-19: fee=1.0 (stable)
    Bar 20:   fee=3.0 (unconfirmed — must NOT fire)
    Bar 21:   fee=3.0 (confirmed — detector should fire here)
    Bar 22+:  fee=3.0 (stable at new level)
    """
    fees = [1.0] * 20 + [3.0] * 5
    df = _make_fee_df(fees)
    det = FeeRegimeChangeDetector()
    params = {"lookback_window": 10, "min_periods": 2}
    events = det.detect(df, symbol="BTC", **params)

    fire_times = [pd.to_datetime(row["timestamp"]) for row in events.to_dict(orient="records")]
    # Bar 20 (index 20): 2024-01-01 01:40:00 UTC — must NOT fire
    # Bar 21 (index 21): 2024-01-01 01:45:00 UTC — may fire
    forbidden = pd.Timestamp("2024-01-01 01:40:00", tz="UTC")
    assert forbidden not in fire_times, (
        "Detector fired at bar 20 (first bar of change) — future lookahead present."
    )
    # At least one event should exist (at bar 21 or later)
    assert len(events) > 0, "Detector produced no events at all for a clear regime change."
