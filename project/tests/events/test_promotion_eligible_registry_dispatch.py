from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from project.events.detectors.base_v2 import BaseDetectorV2
from project.events.detectors.registry import get_detector, get_detector_class
from project.events.registries.desync import DESYNC_DETECTORS
from project.events.registries.oi import OI_DETECTORS
from project.events.registries.regime import REGIME_DETECTORS


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


def _oi_df(n: int = 400) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    close = np.full(n, 100.0)
    oi = np.linspace(10000.0, 10200.0, n)
    close[96:120] = np.linspace(100.0, 108.0, 24)
    oi[96:120] = np.linspace(10200.0, 18000.0, 24)
    close[180:204] = np.linspace(108.0, 95.0, 24)
    oi[180:204] = np.concatenate(
        [np.linspace(18000.0, 52000.0, 4), np.linspace(52000.0, 70000.0, 20)]
    )
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


def _flush_df() -> pd.DataFrame:
    df = _oi_df()
    df["ms_oi_state"] = -1.0
    return df


def _promotion_case(event_type: str) -> tuple[pd.DataFrame, dict[str, object]]:
    if event_type == "CROSS_VENUE_DESYNC":
        return _cross_venue_df(), {
            "symbol": "BTCUSDT",
            "timeframe": "5m",
            "lookback_window": 40,
            "threshold": 2.0,
            "persistence_bars": 2,
            "min_basis_bps": 5,
        }
    if event_type in {"INDEX_COMPONENT_DIVERGENCE", "LEAD_LAG_BREAK", "CROSS_ASSET_DESYNC_EVENT"}:
        return _pair_df(), {
            "symbol": "BTCUSDT",
            "timeframe": "5m",
            "lookback_window": 120,
            "threshold_z": 2.0,
            "threshold_quantile": 0.9,
        }
    if event_type in {"CORRELATION_BREAKDOWN_EVENT", "BETA_SPIKE_EVENT"}:
        return _regime_pair_df(), {
            "symbol": "BTCUSDT",
            "timeframe": "5m",
            "regime_window": 60,
            "transition_z_threshold": 1.5,
            "corr_floor": 0.4,
            "min_prior_corr": 0.7,
            "rv_quantile": 0.6,
        }
    if event_type in {"OI_SPIKE_POSITIVE", "OI_SPIKE_NEGATIVE"}:
        return _oi_df(), {"symbol": "BTCUSDT", "timeframe": "5m"}
    if event_type == "OI_FLUSH":
        return _flush_df(), {"symbol": "BTCUSDT", "timeframe": "5m"}
    raise KeyError(event_type)


def test_promotion_eligible_desync_and_oi_families_use_registry_modules() -> None:
    assert {
        "CROSS_VENUE_DESYNC",
        "INDEX_COMPONENT_DIVERGENCE",
        "LEAD_LAG_BREAK",
        "CORRELATION_BREAKDOWN_EVENT",
        "BETA_SPIKE_EVENT",
    } <= set(DESYNC_DETECTORS) | set(REGIME_DETECTORS)
    assert {"OI_SPIKE_POSITIVE", "OI_SPIKE_NEGATIVE", "OI_FLUSH"} <= set(OI_DETECTORS)


@pytest.mark.parametrize(
    "event_type",
    [
        "BETA_SPIKE_EVENT",
        "CORRELATION_BREAKDOWN_EVENT",
        "CROSS_VENUE_DESYNC",
        "INDEX_COMPONENT_DIVERGENCE",
        "LEAD_LAG_BREAK",
        "OI_FLUSH",
        "OI_SPIKE_NEGATIVE",
        "OI_SPIKE_POSITIVE",
    ],
)
def test_promotion_eligible_detector_dispatch_resolves_to_v2_and_emits_rows(
    event_type: str,
) -> None:
    detector_cls = get_detector_class(event_type)
    detector = get_detector(event_type)

    assert detector_cls is not None
    assert detector is not None
    assert issubclass(detector_cls, BaseDetectorV2)

    frame, params = _promotion_case(event_type)
    events = detector.detect_events(frame, params)

    assert not events.empty, event_type
    assert set(events["event_name"]) == {event_type}
    assert set(events["event_version"]) == {"v2"}
    assert events["confidence"].notna().all()
    assert events["severity"].notna().all()
    assert events["data_quality_flag"].notna().all()
