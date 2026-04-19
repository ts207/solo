from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from project.events.detectors.base_v2 import BaseDetectorV2
from project.events.detectors.registry import get_detector, get_detector_class
from project.events.policy import DEPLOYABLE_CORE_EVENT_TYPES
from project.events.registries.basis import BASIS_DETECTORS
from project.events.registries.liquidation import LIQUIDATION_DETECTORS


def _basis_df(n: int = 3200) -> pd.DataFrame:
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


def _liquidity_df(n: int = 120) -> pd.DataFrame:
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


def _liquidation_df(n: int = 1000) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": ts,
            "close": [100.0] * (n - 10) + [95, 90, 85, 80, 75, 70, 65, 60, 55, 50],
            "high": [101.0] * (n - 10) + [96, 91, 86, 81, 76, 71, 66, 61, 56, 51],
            "low": [99.0] * (n - 10) + [94, 89, 84, 79, 74, 69, 64, 59, 54, 49],
            "liquidation_notional": [100.0] * (n - 10)
            + [10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000],
            "oi_delta_1h": [0.0] * (n - 10)
            + [-100, -200, -300, -400, -500, -600, -700, -800, -900, -1000],
            "oi_notional": [10000.0] * n,
        }
    )


def _oi_negative_df(n: int = 260) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    close = np.full(n, 100.0)
    oi = np.linspace(10000.0, 10200.0, n)
    close[180:204] = np.linspace(108.0, 95.0, 24)
    oi[180:204] = np.concatenate(
        [np.linspace(18000.0, 52000.0, 4), np.linspace(52000.0, 70000.0, 20)]
    )
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


def _runtime_case(event_type: str) -> tuple[pd.DataFrame, dict[str, object]]:
    if event_type in {"BASIS_DISLOC", "FND_DISLOC", "SPOT_PERP_BASIS_SHOCK"}:
        return _basis_df(), {"symbol": "BTCUSDT", "timeframe": "5m"}
    if event_type in {"LIQUIDITY_SHOCK", "LIQUIDITY_STRESS_DIRECT", "LIQUIDITY_VACUUM"}:
        return _liquidity_df(), {"symbol": "BTCUSDT", "timeframe": "5m"}
    if event_type == "LIQUIDATION_CASCADE":
        return _liquidation_df(), {"symbol": "BTCUSDT", "timeframe": "5m", "liq_median_window": 20}
    if event_type == "VOL_SPIKE":
        return _vol_spike_df(), {"symbol": "BTCUSDT", "timeframe": "5m"}
    if event_type == "VOL_SHOCK":
        return _vol_shock_df(), {"symbol": "BTCUSDT", "timeframe": "5m"}
    if event_type == "OI_SPIKE_NEGATIVE":
        return _oi_negative_df(), {"symbol": "BTCUSDT", "timeframe": "5m"}
    raise KeyError(event_type)


def test_basis_and_liquidation_runtime_families_use_dedicated_registry_modules() -> None:
    assert {"BASIS_DISLOC", "FND_DISLOC", "SPOT_PERP_BASIS_SHOCK"} <= set(BASIS_DETECTORS)
    assert {"LIQUIDATION_CASCADE", "LIQUIDATION_CASCADE_PROXY"} <= set(LIQUIDATION_DETECTORS)


@pytest.mark.parametrize("event_type", sorted(DEPLOYABLE_CORE_EVENT_TYPES))
def test_runtime_core_detector_dispatch_resolves_to_v2_and_emits_rows(event_type: str) -> None:
    detector_cls = get_detector_class(event_type)
    detector = get_detector(event_type)

    assert detector_cls is not None
    assert detector is not None
    assert issubclass(detector_cls, BaseDetectorV2)

    frame, params = _runtime_case(event_type)
    events = detector.detect_events(frame, params)

    assert not events.empty, event_type
    assert set(events["event_name"]) == {event_type}
    assert set(events["event_version"]) == {"v2"}
    assert events["confidence"].notna().all()
    assert events["severity"].notna().all()
    assert events["data_quality_flag"].notna().all()
    assert events["merge_key"].notna().all()
