from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pytest

from project.events.detectors.dislocation_base import (
    BasisDislocationDetectorV2,
    FndDislocDetectorV2,
    SpotPerpBasisShockDetectorV2,
)
from project.events.detectors.desync_base import (
    BetaSpikeDetectorV2,
    CorrelationBreakdownDetectorV2,
    CrossAssetDesyncDetectorV2,
    CrossVenueDesyncDetectorV2,
    IndexComponentDivergenceDetectorV2,
    LeadLagBreakDetectorV2,
)
from project.events.detectors.liquidation_base import (
    LiquidationCascadeDetectorV2,
    LiquidationCascadeProxyDetectorV2,
)
from project.events.detectors.liquidity_base import (
    DepthCollapseDetectorV2,
    DirectLiquidityStressDetectorV2,
    LiquidityGapDetectorV2,
    LiquidityShockDetectorV2,
    ProxyLiquidityStressDetectorV2,
    LiquidityVacuumDetectorV2,
)
from project.events.detectors.positioning_base import (
    FundingExtremeOnsetDetectorV2,
    FundingFlipDetectorV2,
    FundingNormalizationDetectorV2,
    FundingPersistenceDetectorV2,
    OIFlushDetectorV2,
    OISpikeNegativeDetectorV2,
    OISpikePositiveDetectorV2,
)
from project.events.detectors.volatility_base import (
    BreakoutTriggerDetectorV2,
    RangeCompressionDetectorV2,
    VolClusterShiftDetectorV2,
    VolRegimeShiftDetectorV2,
    VolRelaxationStartDetectorV2,
    VolShockDetectorV2,
    VolSpikeDetectorV2,
)
from project.events.event_output_schema import (
    REQUIRED_EVENT_OUTPUT_COLUMNS,
    DetectedEvent,
    normalize_event_output_frame,
    validate_event_output_frame,
)
from project.events.registry import list_v2_detectors


def test_detected_event_bounds_and_serialization() -> None:
    event = DetectedEvent(
        event_name='VOL_SPIKE',
        event_version='v2',
        detector_class='VolSpikeDetectorV2',
        symbol='BTCUSDT',
        timeframe='5m',
        ts_start=datetime.now(timezone.utc),
        ts_end=datetime.now(timezone.utc),
        canonical_family='VOLATILITY_TRANSITION',
        subtype='vol_spike',
        phase='shock',
        evidence_mode='direct',
        role='trigger',
        confidence=1.2,
        severity=-0.3,
        trigger_value=3.4,
        threshold_snapshot={'version': '2.0'},
        source_features={'rv_z': 2.4},
        detector_metadata={'cluster_id': 'vol_regime'},
        required_context_present=True,
        data_quality_flag='ok',
        merge_key='BTCUSDT:vol_regime',
        cooldown_until=None,
    )
    payload = event.as_dict()
    assert payload['confidence'] == 1.0
    assert payload['severity'] == 0.0
    assert isinstance(payload['ts_start'], str)
    assert payload['family'] == payload['canonical_family']


def test_detected_event_rejects_invalid_quality_flag() -> None:
    with pytest.raises(ValueError):
        DetectedEvent(
            event_name='VOL_SPIKE',
            event_version='v2',
            detector_class='VolSpikeDetectorV2',
            symbol='BTCUSDT',
            timeframe='5m',
            ts_start=datetime.now(timezone.utc),
            ts_end=datetime.now(timezone.utc),
            canonical_family='VOLATILITY_TRANSITION',
            subtype='vol_spike',
            phase='shock',
            evidence_mode='direct',
            role='trigger',
            confidence=0.5,
            severity=0.5,
            trigger_value=1.0,
            threshold_snapshot={},
            source_features={},
            detector_metadata={},
            required_context_present=True,
            data_quality_flag='bad',
            merge_key=None,
            cooldown_until=None,
        )


def test_output_validator_rejects_missing_required_columns_after_normalization() -> None:
    frame = normalize_event_output_frame(pd.DataFrame([{"event_name": "VOL_SPIKE"}]))
    with pytest.raises(ValueError, match="missing required columns"):
        validate_event_output_frame(frame, require_rows=True)


def _liquidity_core_df(n: int = 120) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": ts,
            "close": np.full(n, 100.0),
            "high": np.full(n, 101.0),
            "low": np.full(n, 99.0),
            "depth_usd": [100_000.0] * (n - 10) + [5_000.0] * 10,
            "spread_bps": [2.0] * (n - 10) + [25.0] * 10,
        }
    )


def _liquidity_proxy_df(n: int = 120) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": ts,
            "close": [100.0] * (n - 10) + [100.0, 95.0, 90.0, 85.0, 80.0, 75.0, 70.0, 65.0, 60.0, 55.0],
            "high": [100.5] * (n - 10) + [101.0, 105.0, 110.0, 115.0, 120.0, 125.0, 130.0, 135.0, 140.0, 145.0],
            "low": [99.5] * (n - 10) + [99.0, 85.0, 80.0, 75.0, 70.0, 65.0, 60.0, 55.0, 50.0, 45.0],
            "volume": [1000.0] * (n - 10) + [100.0] * 10,
        }
    )


def _liquidity_misc_df(n: int = 800) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    close = np.full(n, 100.0)
    high = close + 0.5
    low = close - 0.5
    df = pd.DataFrame({"timestamp": ts, "close": close, "high": high, "low": low})
    df["depth_usd"] = 100_000.0
    df["spread_bps"] = 1.0
    df["volume"] = 1_000.0
    df.loc[df.index[-5:], "depth_usd"] = 10_000.0
    df.loc[df.index[-5:], "spread_bps"] = 3.0
    df.loc[df.index[-3], "close"] = 108.0
    df.loc[df.index[-3], "high"] = 109.0
    df.loc[df.index[-3], "low"] = 104.0
    df.loc[df.index[-2:], "depth_usd"] = 8_000.0
    df.loc[df.index[-2:], "spread_bps"] = 5.0
    return df


def _liquidation_core_df(n: int = 1000) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": ts,
            "close": [100.0] * (n - 10) + [95, 90, 85, 80, 75, 70, 65, 60, 55, 50],
            "high": [101.0] * (n - 10) + [96, 91, 86, 81, 76, 71, 66, 61, 56, 51],
            "low": [99.0] * (n - 10) + [94, 89, 84, 79, 74, 69, 64, 59, 54, 49],
            "liquidation_notional": [100.0] * (n - 10)
            + [10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000],
            "oi_delta_1h": [0.0] * (n - 10) + [-100, -200, -300, -400, -500, -600, -700, -800, -900, -1000],
            "oi_notional": [10000.0] * n,
        }
    )


def _liquidation_proxy_df(n: int = 1000) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    close = np.full(n, 100.0)
    low = np.full(n, 99.0)
    df = pd.DataFrame(
        {
            "timestamp": ts,
            "close": close,
            "high": np.full(n, 101.0),
            "low": low,
            "volume": np.full(n, 1000.0),
            "oi_notional": np.full(n, 10000.0),
            "oi_delta_1h": np.zeros(n),
        }
    )
    df.loc[df.index[-10:], "close"] = np.linspace(100, 85, 10)
    df.loc[df.index[-10:], "low"] = np.linspace(99, 80, 10)
    df.loc[df.index[-10:], "volume"] = np.linspace(1000, 10000, 10)
    df.loc[df.index[-10:], "oi_delta_1h"] = np.linspace(-50, -600, 10)
    return df


def _vol_spike_df(n: int = 3000) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    rv = np.full(n, 0.01)
    rv[-50:] = [0.2 + i * 0.01 for i in range(50)]
    return pd.DataFrame(
        {
            "timestamp": ts,
            "close": np.full(n, 100.0),
            "rv_96": rv,
            "range_96": np.full(n, 0.02),
            "range_med_2880": np.full(n, 0.02),
            "ms_vol_state": np.full(n, 2.0),
            "ms_vol_confidence": np.full(n, 1.0),
            "ms_vol_entropy": np.zeros(n),
        }
    )


def _vol_shock_df(n: int = 3200) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    rv = np.full(n, 0.01)
    rv[-20:-10] = np.linspace(0.01, 0.3, 10)
    rv[-10:] = np.linspace(0.3, -0.2, 10)
    close = np.full(n, 100.0)
    close[-20:-10] = np.linspace(100, 110, 10)
    close[-10:] = np.linspace(110, 102, 10)
    return pd.DataFrame(
        {
            "timestamp": ts,
            "close": close,
            "rv_96": rv,
            "range_96": np.full(n, 0.02),
            "range_med_2880": np.full(n, 0.02),
            "ms_vol_state": np.full(n, 2.0),
            "ms_vol_confidence": np.full(n, 1.0),
            "ms_vol_entropy": np.zeros(n),
        }
    )


def _volatility_transition_df(n: int = 3200) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    close = np.full(n, 100.0)
    high = np.full(n, 100.2)
    low = np.full(n, 99.8)
    rv = np.full(n, 0.01)
    range_96 = np.full(n, 0.02)
    range_med_2880 = np.full(n, 0.02)

    rv[-60:-30] = 0.002
    rv[-30:] = 0.05
    rv[-20:] = [0.01] * 10 + [0.25, 0.26, 0.27, 0.28, 0.29, 0.30, 0.31, 0.32, 0.33, 0.34]

    range_96[-20:-3] = 0.008
    range_96[-3:] = [0.008, 0.021, 0.023]
    close[-3:] = [100.0, 105.0, 108.0]
    high[-3:] = [100.2, 105.5, 108.5]
    low[-3:] = [99.8, 99.9, 104.0]

    return pd.DataFrame(
        {
            "timestamp": ts,
            "close": close,
            "high": high,
            "low": low,
            "rv_96": rv,
            "range_96": range_96,
            "range_med_2880": range_med_2880,
            "ms_vol_state": np.full(n, 2.0),
            "ms_vol_confidence": np.full(n, 1.0),
            "ms_vol_entropy": np.zeros(n),
        }
    )


def _basis_core_df(n: int = 3200) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    close_spot = np.full(n, 100.0)
    close_perp = np.full(n, 100.1)
    funding = np.full(n, 0.00002)
    close_perp[-6:] = [100.0, 100.4, 100.9, 102.0, 104.0, 106.0]
    funding[-6:] = [0.00002, 0.00004, 0.00007, 0.00020, 0.00035, 0.00045]
    return pd.DataFrame(
        {
            "timestamp": ts,
            "close_spot": close_spot,
            "close_perp": close_perp,
            "funding_rate_scaled": funding,
            "ms_funding_state": np.full(n, 2.5),
            "ms_funding_confidence": np.full(n, 1.0),
            "ms_funding_entropy": np.zeros(n),
        }
    )


def _funding_df(n: int = 3200) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    funding_abs_pct = np.full(n, 10.0)
    funding_abs = np.full(n, 0.0001)
    funding_scaled = np.full(n, 0.0001)
    funding_abs_pct[-20:-8] = np.linspace(20.0, 99.0, 12)
    funding_abs[-20:-8] = np.linspace(0.0001, 0.0011, 12)
    funding_scaled[-20:-8] = np.linspace(0.0001, 0.0011, 12)
    funding_abs_pct[-8:-3] = [98.0, 96.0, 92.0, 60.0, 40.0]
    funding_abs[-8:-3] = [0.0010, 0.0009, 0.0008, 0.0005, 0.0003]
    funding_scaled[-8:-3] = [0.0010, 0.0009, 0.0008, 0.0005, 0.0003]
    funding_abs_pct[-3:] = [70.0, 80.0, 85.0]
    funding_abs[-3:] = [0.0005, 0.0006, 0.0007]
    funding_scaled[-3:] = [-0.0005, -0.0006, -0.0007]
    df = pd.DataFrame(
        {
            "timestamp": ts,
            "funding_abs_pct": funding_abs_pct,
            "funding_abs": funding_abs,
            "funding_rate_scaled": funding_scaled,
        }
    )
    df["fp_active"] = 0.0
    df["fp_age_bars"] = 0.0
    df["fp_severity"] = 0.0
    df.loc[df.index[-16:-8], "fp_active"] = 1.0
    df.loc[df.index[-16:-8], "fp_age_bars"] = np.arange(0, 8)
    df.loc[df.index[-16:-8], "fp_severity"] = np.linspace(0.6, 1.4, 8)
    return df


def _oi_df(n: int = 400) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    close = np.full(n, 100.0)
    oi = np.linspace(10000.0, 10200.0, n)
    close[96:120] = np.linspace(100.0, 108.0, 24)
    oi[96:120] = np.linspace(10200.0, 18000.0, 24)
    close[180:204] = np.linspace(108.0, 95.0, 24)
    oi[180:204] = np.concatenate([np.linspace(18000.0, 52000.0, 4), np.linspace(52000.0, 70000.0, 20)])
    close[320:] = np.linspace(95.0, 85.0, n - 320)
    oi[320:] = np.linspace(42000.0, 15000.0, n - 320)
    return pd.DataFrame(
        {
            "timestamp": ts,
            "close": close,
            "oi_notional": oi,
            "ms_oi_state": np.full(n, 2.5),
            "ms_oi_confidence": np.full(n, 1.0),
            "ms_oi_entropy": np.zeros(n),
        }
    )


def _cross_venue_df(n: int = 220) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    close_spot = np.full(n, 100.0)
    close_perp = np.full(n, 100.0)
    close_perp[20:60] = 100.02
    close_perp[60:100] = 99.98
    close_perp[-6:] = [100.0, 100.0, 104.0, 104.0, 104.0, 104.0]
    return pd.DataFrame({"timestamp": ts, "close_spot": close_spot, "close_perp": close_perp})


def _pair_df(n: int = 400) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    close = np.full(n, 100.0)
    pair = np.full(n, 100.0)
    for i in range(1, n):
        step = np.sin(i / 20) / 500
        close[i] = close[i - 1] * (1 + step)
        pair[i] = pair[i - 1] * (1 + step * 0.98)
    close[-8:] = close[-9] * np.array([1.0, 1.01, 1.03, 1.06, 1.10, 1.15, 1.20, 1.25])
    pair[-8:] = pair[-9] * np.array([1.0, 1.002, 1.004, 1.006, 1.008, 1.010, 1.012, 1.014])
    return pd.DataFrame(
        {
            "timestamp": ts,
            "close": close,
            "pair_close": pair,
            "rv_96": np.concatenate([np.full(n - 8, 0.001), np.linspace(0.002, 0.02, 8)]),
        }
    )


def _regime_pair_df(n: int = 500) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    base = np.sin(np.arange(n) / 15) / 500
    close = np.empty(n)
    pair = np.empty(n)
    close[0] = 100.0
    pair[0] = 100.0
    for i in range(1, n):
        close[i] = close[i - 1] * (1 + base[i])
        pair[i] = pair[i - 1] * (1 + base[i] * 0.98)
    for i in range(n - 80, n):
        step = np.sin(i / 6) / 80
        close[i] = close[i - 1] * (1 + step)
        pair[i] = pair[i - 1] * (1 - step)
    rv_96 = np.concatenate([np.full(n - 80, 0.0015), np.linspace(0.002, 0.03, 80)])
    return pd.DataFrame({"timestamp": ts, "close": close, "pair_close": pair, "rv_96": rv_96})


def _flush_df() -> pd.DataFrame:
    df = _oi_df()
    df["ms_oi_state"] = -1.0
    return df


DEPLOYABLE_CORE_CASES = [
    (BasisDislocationDetectorV2(), _basis_core_df(), {"symbol": "BTCUSDT", "timeframe": "5m"}),
    (FndDislocDetectorV2(), _basis_core_df(), {"symbol": "BTCUSDT", "timeframe": "5m"}),
    (SpotPerpBasisShockDetectorV2(), _basis_core_df(), {"symbol": "BTCUSDT", "timeframe": "5m"}),
    (DirectLiquidityStressDetectorV2(), _liquidity_core_df(), {"symbol": "BTCUSDT", "timeframe": "5m"}),
    (LiquidityShockDetectorV2(), _liquidity_core_df(), {"symbol": "BTCUSDT", "timeframe": "5m"}),
    (LiquidityVacuumDetectorV2(), _liquidity_core_df(), {"symbol": "BTCUSDT", "timeframe": "5m"}),
    (LiquidationCascadeDetectorV2(), _liquidation_core_df(), {"symbol": "BTCUSDT", "timeframe": "5m", "liq_median_window": 20}),
    (VolSpikeDetectorV2(), _vol_spike_df(), {"symbol": "BTCUSDT", "timeframe": "5m"}),
    (VolShockDetectorV2(), _vol_shock_df(), {"symbol": "BTCUSDT", "timeframe": "5m"}),
]


ALL_V2_SCHEMA_CASES = [
    *DEPLOYABLE_CORE_CASES,
    (ProxyLiquidityStressDetectorV2(), _liquidity_proxy_df(), {"symbol": "BTCUSDT", "timeframe": "5m"}),
    (DepthCollapseDetectorV2(), _liquidity_misc_df(), {"symbol": "BTCUSDT", "timeframe": "5m"}),
    (LiquidityGapDetectorV2(), _liquidity_misc_df(), {"symbol": "BTCUSDT", "timeframe": "5m"}),
    (LiquidationCascadeProxyDetectorV2(), _liquidation_proxy_df(), {"symbol": "BTCUSDT", "timeframe": "5m"}),
    (VolClusterShiftDetectorV2(), _volatility_transition_df(), {"symbol": "BTCUSDT", "timeframe": "5m", "shift_quantile": 0.95}),
    (RangeCompressionDetectorV2(), _volatility_transition_df(), {"symbol": "BTCUSDT", "timeframe": "5m", "compression_ratio_max": 0.8, "compression_ratio_min": 0.95}),
    (BreakoutTriggerDetectorV2(), _volatility_transition_df(), {"symbol": "BTCUSDT", "timeframe": "5m", "compression_ratio_max": 0.8, "min_breakout_distance": 0.0015, "expansion_quantile": 0.8, "vol_lookback_window": 96, "threshold_window": 2880}),
    (VolRegimeShiftDetectorV2(), _volatility_transition_df(), {"symbol": "BTCUSDT", "timeframe": "5m", "regime_window": 120, "rv_low_quantile": 0.33, "rv_high_quantile": 0.66}),
    (VolRelaxationStartDetectorV2(), _vol_shock_df(), {"symbol": "BTCUSDT", "timeframe": "5m"}),
    (FundingExtremeOnsetDetectorV2(), _funding_df(), {"symbol": "BTCUSDT", "timeframe": "5m"}),
    (FundingPersistenceDetectorV2(), _funding_df(), {"symbol": "BTCUSDT", "timeframe": "5m"}),
    (FundingNormalizationDetectorV2(), _funding_df(), {"symbol": "BTCUSDT", "timeframe": "5m"}),
    (FundingFlipDetectorV2(), _funding_df(), {"symbol": "BTCUSDT", "timeframe": "5m"}),
    (OISpikePositiveDetectorV2(), _oi_df().iloc[:170].copy(), {"symbol": "BTCUSDT", "timeframe": "5m"}),
    (OISpikeNegativeDetectorV2(), _oi_df().iloc[:260].copy(), {"symbol": "BTCUSDT", "timeframe": "5m"}),
    (OIFlushDetectorV2(), _flush_df(), {"symbol": "BTCUSDT", "timeframe": "5m"}),
    (CrossVenueDesyncDetectorV2(), _cross_venue_df(), {"symbol": "BTCUSDT", "timeframe": "5m", "lookback_window": 40, "threshold": 2.0, "persistence_bars": 2, "min_basis_bps": 5}),
    (CrossAssetDesyncDetectorV2(), _pair_df(), {"symbol": "BTCUSDT", "timeframe": "5m", "lookback_window": 120, "threshold_z": 2.0, "threshold_quantile": 0.9}),
    (IndexComponentDivergenceDetectorV2(), _pair_df(), {"symbol": "BTCUSDT", "timeframe": "5m", "lookback_window": 120, "threshold_z": 2.0, "threshold_quantile": 0.9}),
    (LeadLagBreakDetectorV2(), _pair_df(), {"symbol": "BTCUSDT", "timeframe": "5m", "lookback_window": 120, "threshold_z": 2.0, "threshold_quantile": 0.9}),
    (CorrelationBreakdownDetectorV2(), _regime_pair_df(), {"symbol": "BTCUSDT", "timeframe": "5m", "regime_window": 60, "transition_z_threshold": 1.5, "corr_floor": 0.4, "min_prior_corr": 0.7}),
    (BetaSpikeDetectorV2(), _regime_pair_df(), {"symbol": "BTCUSDT", "timeframe": "5m", "regime_window": 60, "transition_z_threshold": 1.5, "rv_quantile": 0.6}),
]


@pytest.mark.parametrize(
    ("detector", "frame", "params"),
    DEPLOYABLE_CORE_CASES,
)
def test_deployable_core_detectors_emit_full_v2_output_schema(detector, frame: pd.DataFrame, params: dict) -> None:
    events = detector.detect_events(frame, params)
    validate_event_output_frame(events, require_rows=True)
    assert set(REQUIRED_EVENT_OUTPUT_COLUMNS).issubset(events.columns)
    assert set(events["event_name"]) == {detector.event_name}
    assert set(events["event_version"]) == {"v2"}
    assert set(events["data_quality_flag"]) <= {"ok", "degraded", "invalid"}
    assert events["family"].equals(events["canonical_family"])


def test_v2_schema_cases_cover_registered_v2_detectors() -> None:
    expected = {contract.event_name for contract in list_v2_detectors()}
    actual = {detector.event_name for detector, _, _ in ALL_V2_SCHEMA_CASES}
    assert actual == expected


@pytest.mark.parametrize(("detector", "frame", "params"), ALL_V2_SCHEMA_CASES)
def test_all_v2_detectors_emit_full_v2_output_schema(detector, frame: pd.DataFrame, params: dict) -> None:
    events = detector.detect_events(frame, params)
    validate_event_output_frame(events, require_rows=True)
    assert set(REQUIRED_EVENT_OUTPUT_COLUMNS).issubset(events.columns)
    assert set(events["event_name"]) == {detector.event_name}
    assert set(events["event_version"]) == {"v2"}
    assert set(events["data_quality_flag"]) <= {"ok", "degraded", "invalid"}
    assert events["family"].equals(events["canonical_family"])
