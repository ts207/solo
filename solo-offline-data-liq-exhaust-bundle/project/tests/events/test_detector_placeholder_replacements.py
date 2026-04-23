from __future__ import annotations

import numpy as np
import pandas as pd

from project.events.detectors.registry import get_detector, load_all_detectors


def _base_frame(n: int = 480) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    close = 100.0 + np.linspace(0.0, 4.0, n)
    high = close + 0.2
    low = close - 0.2
    volume = np.full(n, 1000.0)
    rv_96 = np.full(n, 1.2)
    spread_zscore = np.full(n, 0.5)
    spread_bps = np.full(n, 4.0)
    slippage_bps = np.full(n, 1.0)
    fee_bps = np.full(n, 1.0)
    pairs_zscore = np.full(n, 0.2)
    return pd.DataFrame(
        {
            "timestamp": ts,
            "close": close,
            "high": high,
            "low": low,
            "volume": volume,
            "rv_96": rv_96,
            "spread_zscore": spread_zscore,
            "spread_bps": spread_bps,
            "slippage_bps": slippage_bps,
            "fee_bps": fee_bps,
            "pairs_zscore": pairs_zscore,
        }
    )


def test_replacement_detectors_emit_events() -> None:
    load_all_detectors()
    cases: dict[str, tuple[pd.DataFrame, dict[str, float | int]]] = {}

    climax_df = _base_frame()
    climax_df.loc[470, "volume"] = 12000.0
    climax_df.loc[470, "close"] = climax_df.loc[469, "close"] - 1.8
    climax_df.loc[470, "high"] = climax_df.loc[469, "close"] + 2.5
    climax_df.loc[470, "low"] = climax_df.loc[470, "close"] - 2.5
    cases["CLIMAX_VOLUME_BAR"] = (
        climax_df,
        {
            "volume_quantile": 0.95,
            "return_quantile": 0.75,
            "range_quantile": 0.90,
            "wick_ratio_min": 0.20,
        },
    )

    failed_df = _base_frame()
    failed_df.loc[:468, "close"] = 100.0 + np.linspace(0.0, 14.0, 469)
    failed_df.loc[469, "close"] = failed_df.loc[468, "close"] + 1.2
    failed_df.loc[470, "close"] = failed_df.loc[468, "close"] - 0.8
    failed_df.loc[470, "high"] = failed_df.loc[469, "close"] + 0.4
    failed_df.loc[470, "low"] = failed_df.loc[470, "close"] - 0.4
    cases["FAILED_CONTINUATION"] = (failed_df, {"breakout_window": 24, "reversal_window": 3})

    rebound_df = _base_frame()
    rebound_df["oi_delta_1h"] = 0.0
    rebound_df["liquidation_notional"] = 0.0
    rebound_df.loc[450:454, "close"] = np.array([111.0, 109.5, 107.2, 105.4, 104.0])
    rebound_df.loc[450:454, "high"] = rebound_df.loc[450:454, "close"] + 0.4
    rebound_df.loc[450:454, "low"] = rebound_df.loc[450:454, "close"] - 0.8
    rebound_df.loc[450:454, "oi_delta_1h"] = np.array([-18.0, -22.0, -28.0, -24.0, -16.0])
    rebound_df.loc[450:454, "liquidation_notional"] = np.array([160.0, 220.0, 260.0, 210.0, 150.0])
    rebound_df.loc[450:454, "rv_96"] = np.array([3.2, 4.0, 4.8, 4.2, 3.6])
    rebound_df.loc[455:461, "close"] = np.array([104.8, 105.7, 106.9, 108.0, 108.8, 109.4, 110.0])
    rebound_df.loc[455:461, "high"] = rebound_df.loc[455:461, "close"] + np.array(
        [0.9, 1.0, 1.1, 1.0, 0.9, 0.8, 0.8]
    )
    rebound_df.loc[455:461, "low"] = rebound_df.loc[455:461, "close"] - np.array(
        [0.1, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2]
    )
    rebound_df.loc[455:461, "oi_delta_1h"] = np.array([-6.0, -3.0, -1.0, 0.0, 1.0, 1.0, 1.0])
    rebound_df.loc[455:461, "liquidation_notional"] = np.array(
        [40.0, 12.0, 0.0, 0.0, 0.0, 0.0, 0.0]
    )
    rebound_df.loc[455:461, "rv_96"] = np.array([2.9, 2.2, 1.7, 1.5, 1.35, 1.3, 1.25])
    rebound_df.loc[450:461, "spread_bps"] = np.array(
        [12.0, 14.0, 16.0, 13.0, 10.0, 8.0, 6.0, 4.5, 3.8, 3.3, 3.0, 3.0]
    )
    cases["POST_DELEVERAGING_REBOUND"] = (
        rebound_df,
        {
            "cluster_window": 6,
            "rebound_window_bars": 6,
            "rebound_window": 3,
            "cooldown_bars": 6,
            "post_cluster_lookback": 24,
        },
    )



    slip_df = _base_frame()
    slip_df.loc[470, "slippage_bps"] = 18.0
    slip_df.loc[470, "spread_bps"] = 3.0
    cases["SLIPPAGE_SPIKE_EVENT"] = (slip_df, {})

    fee_df = _base_frame()
    fee_df.loc[470:, "fee_bps"] = 4.0
    cases["FEE_REGIME_CHANGE_EVENT"] = (fee_df, {})

    copula_df = _base_frame()
    copula_df.loc[465:469, "pairs_zscore"] = np.array([0.8, 1.2, 1.8, 2.6, 2.1])
    copula_df.loc[470, "pairs_zscore"] = 1.4
    copula_df.loc[465:470, "spread_zscore"] = np.array([1.0, 1.2, 1.5, 1.8, 2.0, 2.4])
    cases["COPULA_PAIRS_TRADING"] = (copula_df, {})

    for event_type, (frame, params) in cases.items():
        detector = get_detector(event_type)
        assert detector is not None
        events = detector.detect(frame, symbol="BTCUSDT", **params)
        assert not events.empty, event_type
        assert event_type in set(events["event_type"].astype(str))
