"""
Event detection verification suite.
"""

from __future__ import annotations

import pandas as pd
import numpy as np
from typing import Dict, Any, List
from project.events.families.volatility import VolSpikeDetector
from project.events.families.liquidity import LiquidityStressDetector


class DetectionVerificationSuite:
    def __init__(self):
        self.results: List[Dict[str, Any]] = []

    def verify_vol_spike(self, df: pd.DataFrame, shock_idx: int):
        """Verify VolSpikeDetector triggers around shock_idx."""
        detector = VolSpikeDetector()
        events = detector.detect(df, symbol="TEST")

        shock_ts = df.loc[shock_idx, "timestamp"]
        # Allow +/- 5 bars
        shock_events = events[
            (events["eval_bar_ts"] >= shock_ts - pd.Timedelta(minutes=25))
            & (events["eval_bar_ts"] <= shock_ts + pd.Timedelta(minutes=25))
        ]

        is_pass = len(shock_events) > 0
        self.results.append(
            {
                "pass": is_pass,
                "detector": "VolSpikeDetector",
                "event_count": len(shock_events),
                "reason": "Found event around shock" if is_pass else "No event around shock",
            }
        )
        return is_pass

    def verify_liquidity_shock(self, df: pd.DataFrame, shock_idx: int):
        """Verify LiquidityStressDetector triggers at shock_idx."""
        detector = LiquidityStressDetector()
        events = detector.detect(df, symbol="TEST")

        shock_ts = df.loc[shock_idx, "timestamp"]
        shock_events = events[events["eval_bar_ts"] == shock_ts]

        is_pass = len(shock_events) > 0
        self.results.append(
            {
                "pass": is_pass,
                "detector": "LiquidityStressDetector",
                "event_count": len(shock_events),
                "reason": "Found event at shock" if is_pass else "No event at shock",
            }
        )
        return is_pass

    def verify_edge_cases(self):
        """Verify detectors handle edge cases (zero volume, constant price)."""
        n = 100
        df = pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
                "close": 100.0,
                "high": 100.0,
                "low": 100.0,
                "volume": 0.0,
                "rv_96": 0.0,
                "range_96": 0.0,
                "range_med_2880": 0.0,
                "spread_bps": 0.0,
                "depth_usd": 1000000.0,
            }
        )

        try:
            det = VolSpikeDetector()
            evs = det.detect(df, symbol="TEST")
            is_pass = evs.empty
            self.results.append(
                {
                    "pass": is_pass,
                    "detector": "VolSpikeDetector",
                    "event_count": len(evs),
                    "reason": "Correctly ignored constant price"
                    if is_pass
                    else "Triggered on constant price",
                }
            )
        except Exception as e:
            self.results.append(
                {
                    "pass": False,
                    "detector": "VolSpikeDetector",
                    "reason": f"Crashed on edge case: {e}",
                }
            )
            is_pass = False

        return is_pass

    def get_report(self) -> pd.DataFrame:
        return pd.DataFrame(self.results)


def run_detection_verification() -> pd.DataFrame:
    """Run standard detection verification."""
    suite = DetectionVerificationSuite()

    # 1. Vol Spike test data
    n = 4000
    rng = np.random.default_rng(42)
    prices = np.exp(rng.normal(0.0, 0.001, n).cumsum()) * 100.0
    df_vol = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "close": prices,
            "high": prices * 1.001,
            "low": prices * 0.999,
            "rv_96": pd.Series(np.clip(rng.normal(0.01, 0.002, n), 0.001, None)),
            "range_96": pd.Series(np.full(n, 0.02)),
            "range_med_2880": pd.Series(np.full(n, 1.0)),
            "ms_vol_state": pd.Series(np.zeros(n)),
            "ms_vol_confidence": pd.Series(np.full(n, 0.95)),
            "ms_vol_entropy": pd.Series(np.full(n, 0.10)),
        }
    )
    shock_idx = 3500
    df_vol.loc[shock_idx - 1 :, "ms_vol_state"] = 2.0
    df_vol.loc[shock_idx, "close"] *= 1.10
    df_vol.loc[shock_idx, "rv_96"] = 0.50

    suite.verify_vol_spike(df_vol, shock_idx)

    # 2. Liquidity Shock test data
    df_liq = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "close": 100.0,
            "high": 100.1,
            "low": 99.9,
            "spread_bps": 2.0,
            "depth_usd": 1000000.0,
            "ms_spread_state": pd.Series(np.zeros(n)),
            "ms_spread_confidence": pd.Series(np.full(n, 0.95)),
            "ms_spread_entropy": pd.Series(np.full(n, 0.10)),
        }
    )
    liq_shock_idx = 500
    df_liq.loc[liq_shock_idx - 1 :, "ms_spread_state"] = 1.0
    df_liq.loc[liq_shock_idx, "spread_bps"] = 20.0
    df_liq.loc[liq_shock_idx, "depth_usd"] = 100000.0

    suite.verify_liquidity_shock(df_liq, liq_shock_idx)
    suite.verify_edge_cases()

    return suite.get_report()
