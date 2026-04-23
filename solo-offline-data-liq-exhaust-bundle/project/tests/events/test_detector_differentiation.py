from __future__ import annotations

import numpy as np
import pandas as pd

from project.core.copula_pairs import copula_pair_universe, load_copula_pairs
from project.events.families.canonical_proxy import (
    AbsorptionProxyDetector,
    DepthCollapseDetector,
    DepthStressProxyDetector,
    OrderflowImbalanceShockDetector,
    PriceVolImbalanceProxyDetector,
    SweepStopRunDetector,
    WickReversalProxyDetector,
)
from project.events.families.temporal import CopulaPairsTradingDetector
from project.events.detectors.exhaustion import FlowExhaustionDetector
from project.events.detectors.liquidity import ProxyLiquidityStressDetector
from project.tests.synthetic_truth.scenarios.factory import ScenarioFactory


def _proxy_frame(n: int = 640) -> pd.DataFrame:
    rng = np.random.default_rng(7)
    close = pd.Series(100.0 * np.exp(np.cumsum(rng.normal(0.0, 0.0015, n))))
    high = close * 1.002
    low = close * 0.998
    volume = pd.Series(rng.uniform(800.0, 1200.0, n))
    rv_96 = pd.Series(rng.uniform(0.001, 0.003, n))
    spread_zscore = pd.Series(rng.normal(0.4, 0.2, n)).abs()
    micro_depth_depletion = pd.Series(rng.uniform(0.05, 0.20, n))
    imbalance = pd.Series(rng.normal(0.0, 0.15, n))
    close.iloc[420:430] *= np.array([1.0, 1.012, 1.018, 1.024, 1.020, 1.014, 1.008, 1.004, 1.002, 1.001])
    high.iloc[420:430] = close.iloc[420:430] * np.array([1.003, 1.010, 1.020, 1.030, 1.026, 1.018, 1.012, 1.008, 1.006, 1.005])
    low.iloc[420:430] = close.iloc[420:430] * np.array([0.998, 0.997, 0.996, 0.995, 0.996, 0.997, 0.998, 0.999, 0.999, 0.999])
    volume.iloc[420:430] = np.linspace(2400.0, 4200.0, 10)
    rv_96.iloc[420:430] = np.linspace(0.006, 0.014, 10)
    spread_zscore.iloc[420:430] = np.linspace(1.2, 3.8, 10)
    micro_depth_depletion.iloc[420:430] = np.linspace(0.40, 0.85, 10)
    imbalance.iloc[420:430] = np.linspace(0.02, -0.02, 10)

    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "close": close,
            "high": high,
            "low": low,
            "volume": volume,
            "rv_96": rv_96,
            "spread_zscore": spread_zscore,
            "micro_depth_depletion": micro_depth_depletion,
            "imbalance": imbalance,
        }
    )


def test_proxy_detectors_use_distinct_feature_surfaces() -> None:
    frame = _proxy_frame()
    price = PriceVolImbalanceProxyDetector().prepare_features(frame)
    wick = WickReversalProxyDetector().prepare_features(frame)
    absorb = AbsorptionProxyDetector().prepare_features(frame)
    depth = DepthStressProxyDetector().prepare_features(frame)

    assert {"flow_pressure", "volume_z", "ret_q"}.issubset(price.keys())
    assert {"wick_dominance", "reclaim", "range_q"}.issubset(wick.keys())
    assert {"absorption_score", "imbalance_low"}.issubset(absorb.keys())
    assert {"stress_score", "depth_q"}.issubset(depth.keys())



def test_sweep_stoprun_is_not_the_same_as_generic_wick_reversal() -> None:
    n = 600
    close = pd.Series(np.full(n, 100.0))
    high = pd.Series(np.full(n, 100.3))
    low = pd.Series(np.full(n, 99.7))
    volume = pd.Series(np.full(n, 1000.0))
    rv_96 = pd.Series(np.full(n, 0.0025))
    close.iloc[320:330] = [100.0, 100.7, 101.8, 103.0, 102.4, 101.2, 100.5, 100.1, 99.95, 100.0]
    high.iloc[320:330] = [100.2, 101.8, 103.5, 104.4, 103.0, 101.6, 100.9, 100.3, 100.1, 100.2]
    low.iloc[320:330] = [99.8, 99.7, 99.6, 99.5, 99.8, 100.0, 100.1, 100.0, 99.95, 99.96]
    volume.iloc[320:330] = np.linspace(1800.0, 3400.0, 10)
    rv_96.iloc[320:330] = np.linspace(0.004, 0.012, 10)

    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "close": close,
            "high": high,
            "low": low,
            "volume": volume,
            "rv_96": rv_96,
        }
    )

    generic = WickReversalProxyDetector().detect(frame, symbol="BTCUSDT")
    sweep = SweepStopRunDetector().detect(frame, symbol="BTCUSDT")

    assert len(sweep) >= 1
    assert len(sweep) >= len(generic)


def test_orderflow_shock_requires_directional_flow_confirmation() -> None:
    n = 640
    close = pd.Series(np.full(n, 100.0))
    high = pd.Series(np.full(n, 100.2))
    low = pd.Series(np.full(n, 99.8))
    volume = pd.Series(np.full(n, 900.0))
    rv_96 = pd.Series(np.full(n, 0.0020))
    close.iloc[410:420] = np.array([100.0, 100.7, 101.8, 103.4, 104.8, 103.9, 102.1, 100.8, 100.1, 100.3])
    high.iloc[410:420] = close.iloc[410:420] * np.array([1.001, 1.012, 1.018, 1.025, 1.030, 1.020, 1.012, 1.008, 1.005, 1.004])
    low.iloc[410:420] = close.iloc[410:420] * np.array([0.999, 0.998, 0.997, 0.996, 0.995, 0.996, 0.997, 0.998, 0.999, 0.999])
    volume.iloc[410:420] = np.linspace(2500.0, 4800.0, 10)
    rv_96.iloc[410:420] = np.linspace(0.005, 0.016, 10)

    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "close": close,
            "high": high,
            "low": low,
            "volume": volume,
            "rv_96": rv_96,
        }
    )

    detector = OrderflowImbalanceShockDetector()
    features = detector.prepare_features(frame)

    assert "directional_flow" in features
    assert "directional_flow_q" in features
    mask = detector.compute_raw_mask(frame, features=features)
    assert mask.sum() >= 1

def test_depth_collapse_adds_suddenness_gate() -> None:
    frame = _proxy_frame()
    detector = DepthCollapseDetector()
    features = detector.prepare_features(frame)
    assert {"collapse_impulse", "collapse_q"}.issubset(features.keys())
    collapse_mask = detector.compute_raw_mask(frame, features=features)
    stress_mask = DepthStressProxyDetector().compute_raw_mask(frame, features=features)
    assert collapse_mask.sum() <= stress_mask.sum()


def test_copula_pairs_universe_expanded_and_detector_uses_pair_close() -> None:
    pairs = load_copula_pairs()
    universe = copula_pair_universe()
    assert len(pairs) >= 3
    assert {"BTCUSDT", "ETHUSDT", "SOLUSDT"}.issubset(universe)

    n = 500
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    close = np.full(n, 100.0)
    pair_close = np.full(n, 100.0)
    close[430:440] = np.array([100.0, 101.1, 102.8, 104.0, 103.2, 101.8, 100.6, 99.8, 99.7, 100.0])
    pair_close[430:440] = np.array([100.0, 99.2, 97.8, 96.4, 96.9, 98.2, 99.1, 99.8, 100.1, 100.0])
    frame = pd.DataFrame(
        {
            "timestamp": ts,
            "close": close,
            "pair_close": pair_close,
            "spread_zscore": np.concatenate([np.full(430, 0.25), np.linspace(0.8, 3.5, 70)]),
            "symbol": "BTCUSDT",
        }
    )
    out = CopulaPairsTradingDetector().detect(frame, symbol="BTCUSDT")
    assert not out.empty
    assert "COPULA_PAIRS_TRADING" in set(out["event_type"].astype(str))


def test_promoted_compatibility_detectors_emit_hybrid_evidence_metadata() -> None:
    frame = _proxy_frame()
    price = PriceVolImbalanceProxyDetector()
    absorption = AbsorptionProxyDetector()
    depth = DepthStressProxyDetector()
    wick = WickReversalProxyDetector()
    price_meta = price.compute_metadata(0, price.prepare_features(frame))
    absorption_meta = absorption.compute_metadata(0, absorption.prepare_features(frame))
    depth_meta = depth.compute_metadata(0, depth.prepare_features(frame))
    wick_meta = wick.compute_metadata(0, wick.prepare_features(frame))

    flow_frame = frame.copy()
    flow_frame["oi_delta_1h"] = np.concatenate([
        np.zeros(len(flow_frame) - 10),
        np.linspace(-12.0, -30.0, 10),
    ])
    flow_frame["liquidation_notional"] = np.concatenate([
        np.full(len(flow_frame) - 10, 1.0),
        np.linspace(40.0, 120.0, 10),
    ])
    flow = FlowExhaustionDetector()
    flow_meta = flow.compute_metadata(0, flow.prepare_features(flow_frame))

    liquidity_frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=len(frame), freq="5min", tz="UTC"),
            "close": frame["close"],
            "high": frame["high"],
            "low": frame["low"],
            "depth_usd": np.linspace(150000.0, 120000.0, len(frame)),
            "spread_bps": np.linspace(8.0, 12.0, len(frame)),
            "ms_imbalance_24": np.linspace(-0.15, 0.15, len(frame)),
            "ms_spread_state": np.full(len(frame), 2.0),
        }
    )
    liquidity = ProxyLiquidityStressDetector()
    liquidity_meta = liquidity.compute_metadata(0, liquidity.prepare_features(liquidity_frame))

    assert price_meta["evidence_tier"] == "hybrid"
    assert absorption_meta["evidence_tier"] == "hybrid"
    assert depth_meta["evidence_tier"] == "hybrid"
    assert wick_meta["evidence_tier"] == "hybrid"
    assert flow_meta["evidence_tier"] == "hybrid"
    assert liquidity_meta["evidence_tier"] == "hybrid"


def test_hybridized_remaining_compatibility_events_fire_on_positive_synthetic_scenarios() -> None:
    for event_type in ("LIQUIDITY_STRESS_PROXY", "WICK_REVERSAL_PROXY"):
        frame, _ = ScenarioFactory.for_event(event_type, "positive").create()
        detector = ProxyLiquidityStressDetector() if event_type == "LIQUIDITY_STRESS_PROXY" else WickReversalProxyDetector()
        out = detector.detect(frame, symbol="BTCUSDT")
        assert not out.empty
        assert event_type in set(out["event_type"].astype(str))


def test_hybridized_remaining_compatibility_events_stay_quiet_on_negative_synthetic_scenarios() -> None:
    for event_type in ("LIQUIDITY_STRESS_PROXY", "WICK_REVERSAL_PROXY"):
        frame, _ = ScenarioFactory.for_event(event_type, "negative").create()
        detector = ProxyLiquidityStressDetector() if event_type == "LIQUIDITY_STRESS_PROXY" else WickReversalProxyDetector()
        out = detector.detect(frame, symbol="BTCUSDT")
        assert out.empty
