from __future__ import annotations

import pandas as pd

from project.research.gating import build_event_return_frame


def test_build_event_return_frame_falls_back_to_feature_side_regime_labels():
    features = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=8, freq="5min", tz="UTC"),
            "close": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0],
            "vol_regime": ["low", "mid", "high", "mid", "low", "high", "mid", "low"],
            "liquidity_state": ["thin", "normal", "thick", "normal", "thin", "thick", "normal", "thin"],
        }
    )
    events = pd.DataFrame(
        {
            "timestamp": [features["timestamp"].iloc[1], features["timestamp"].iloc[3]],
            "enter_ts": [features["timestamp"].iloc[1], features["timestamp"].iloc[3]],
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "event_type": ["VOL_SHOCK", "VOL_SHOCK"],
        }
    )

    out = build_event_return_frame(
        events,
        features,
        rule="continuation",
        horizon="5m",
        canonical_family="VOL_SHOCK",
        horizon_bars_override=1,
        entry_lag_bars=1,
    )

    assert not out.empty
    assert list(out["vol_regime"]) == ["mid", "mid"]
    assert list(out["liquidity_state"]) == ["normal", "normal"]
