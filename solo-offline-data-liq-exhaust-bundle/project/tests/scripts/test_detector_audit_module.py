"""Tests for detector_audit_module shared measurement logic."""

import math
import pandas as pd
import pytest


def _make_df(n: int = 5000) -> pd.DataFrame:
    """Build a minimal rich DataFrame for testing."""
    import numpy as np

    ts = pd.date_range("2023-01-01", periods=n, freq="5min", tz="UTC")
    close = pd.Series(30000.0 + np.cumsum(np.random.randn(n) * 50), name="close")
    df = pd.DataFrame(
        {
            "timestamp": ts,
            "open": close,
            "high": close * 1.001,
            "low": close * 0.999,
            "close": close,
            "close_perp": close,
            "close_spot": close * 0.9998,
            "volume": 1000.0,
            "quote_volume": close * 1000.0,
            "trade_count": 500,
            "taker_buy_volume": 500.0,
            "taker_buy_quote_volume": close * 500.0,
            "spread_bps": 2.5,
            "depth_usd": 5_000_000.0,
            "funding_rate_scaled": 0.0001,
            "symbol": "BTCUSDT",
        }
    )
    log_ret = np.log(close / close.shift(1))
    df["rv_96"] = log_ret.rolling(96, min_periods=12).std()
    return df


def test_measure_detector_returns_metrics_dict():
    from project.events.detectors.registry import load_all_detectors, get_detector
    from project.scripts.detector_audit_module import measure_detector

    load_all_detectors()
    detector = get_detector("VOL_SPIKE")
    assert detector is not None, "VOL_SPIKE detector must be registered"

    df = _make_df()
    segments = []  # no truth windows → uncovered

    metrics = measure_detector(detector, df, "BTCUSDT", segments, "test_run")

    assert metrics["event_type"] == "VOL_SPIKE"
    assert metrics["symbol"] == "BTCUSDT"
    assert metrics["classification"] == "uncovered"
    assert metrics["error"] is None
    assert isinstance(metrics["total_events"], int)
    assert isinstance(metrics["precision"], float)


def test_measure_detector_handles_missing_required_column():
    from project.events.detectors.registry import load_all_detectors, get_detector
    from project.scripts.detector_audit_module import measure_detector

    load_all_detectors()
    detector = get_detector("BASIS_DISLOC")
    assert detector is not None

    df = _make_df()
    df = df.drop(columns=["close_spot"])  # BASIS_DISLOC requires close_spot
    segments = []

    metrics = measure_detector(detector, df, "BTCUSDT", segments, "test_run")
    assert metrics["classification"] == "error"
    assert metrics["error"] is not None


def test_classify_noisy():
    from project.scripts.detector_audit_module import _classify

    assert _classify(precision=0.30, recall=0.60, expected_windows=5) == "noisy"


def test_classify_silent():
    from project.scripts.detector_audit_module import _classify

    assert _classify(precision=0.70, recall=0.10, expected_windows=5) == "silent"


def test_classify_broken():
    from project.scripts.detector_audit_module import _classify

    assert _classify(precision=0.20, recall=0.10, expected_windows=5) == "broken"


def test_classify_stable():
    from project.scripts.detector_audit_module import _classify

    assert _classify(precision=0.60, recall=0.50, expected_windows=5) == "stable"


def test_classify_uncovered():
    from project.scripts.detector_audit_module import _classify

    assert _classify(precision=0.0, recall=0.0, expected_windows=0) == "uncovered"


def test_count_hits_basic():
    from project.scripts.detector_audit_module import _count_hits

    base = pd.Timestamp("2023-01-01 12:00:00", tz="UTC")
    event_times = pd.to_datetime(
        pd.Series(
            [
                base,  # inside window 1
                base + pd.Timedelta("1h"),  # inside window 1
                base + pd.Timedelta("10h"),  # outside both windows
            ]
        ),
        utc=True,
    )
    windows = [
        (base - pd.Timedelta("30min"), base + pd.Timedelta("2h")),  # window 1
        (base + pd.Timedelta("20h"), base + pd.Timedelta("22h")),  # window 2 — no hits
    ]
    in_window, windows_hit = _count_hits(event_times, windows)
    assert in_window == 2  # 2 events inside window 1
    assert windows_hit == 1  # only window 1 was hit


def test_build_truth_windows_filters():
    from project.scripts.detector_audit_module import _build_truth_windows

    segments = [
        {
            "symbol": "BTCUSDT",
            "start_ts": "2023-01-01T00:00:00+00:00",
            "end_ts": "2023-01-01T01:00:00+00:00",
            "expected_event_types": ["VOL_SPIKE"],
        },
        {
            "symbol": "ETHUSDT",  # wrong symbol
            "start_ts": "2023-01-02T00:00:00+00:00",
            "end_ts": "2023-01-02T01:00:00+00:00",
            "expected_event_types": ["VOL_SPIKE"],
        },
        {
            "symbol": "BTCUSDT",
            "start_ts": "2023-01-03T00:00:00+00:00",
            "end_ts": "2023-01-03T01:00:00+00:00",
            "expected_event_types": ["FUNDING_FLIP"],  # wrong event type
        },
    ]
    tolerance = pd.Timedelta("30min")
    windows = _build_truth_windows(segments, "VOL_SPIKE", "BTCUSDT", tolerance)
    assert len(windows) == 1  # only first segment matches
    start, end = windows[0]
    assert start == pd.Timestamp("2023-01-01T00:00:00+00:00") - tolerance
    assert end == pd.Timestamp("2023-01-01T01:00:00+00:00") + tolerance


def test_enrich_df_computes_range_columns():
    from project.scripts.detector_audit_module import _enrich_df
    import numpy as np

    n = 200
    ts = pd.date_range("2023-01-01", periods=n, freq="5min", tz="UTC")
    close = pd.Series(30000.0 + np.arange(n, dtype=float), name="close")
    df = pd.DataFrame(
        {
            "timestamp": ts,
            "open": close,
            "high": close * 1.001,
            "low": close * 0.999,
            "close": close,
        }
    )
    result = _enrich_df(df)
    assert "rv_96" in result.columns
    assert "spread_zscore" not in result.columns
    assert "range_96" in result.columns
    assert "range_med_2880" in result.columns
    # must not mutate original
    assert "rv_96" not in df.columns


def test_enrich_df_computes_spread_zscore_and_imbalance_when_available():
    from project.scripts.detector_audit_module import _enrich_df
    import numpy as np

    n = 300
    ts = pd.date_range("2023-01-01", periods=n, freq="5min", tz="UTC")
    close = pd.Series(30000.0 + np.arange(n, dtype=float), name="close")
    df = pd.DataFrame(
        {
            "timestamp": ts,
            "open": close,
            "high": close * 1.001,
            "low": close * 0.999,
            "close": close,
            "spread_bps": np.linspace(2.0, 5.0, n),
            "bid_depth_usd": np.linspace(1_200_000.0, 1_400_000.0, n),
            "ask_depth_usd": np.linspace(800_000.0, 600_000.0, n),
        }
    )
    result = _enrich_df(df)
    assert "spread_zscore" in result.columns
    assert "imbalance" in result.columns
    assert result["spread_zscore"].notna().sum() > 0
    assert result["imbalance"].abs().max() <= 1.0


def test_enrich_df_does_not_overwrite_existing():
    from project.scripts.detector_audit_module import _enrich_df
    import numpy as np

    n = 200
    ts = pd.date_range("2023-01-01", periods=n, freq="5min", tz="UTC")
    close = pd.Series(30000.0 + np.arange(n, dtype=float), name="close")
    df = pd.DataFrame(
        {
            "timestamp": ts,
            "close": close,
            "open": close,
            "high": close * 1.001,
            "low": close * 0.999,
            "rv_96": 99.0,  # sentinel value
        }
    )
    result = _enrich_df(df)
    assert (result["rv_96"] == 99.0).all()  # must not overwrite


def test_audit_script_is_importable():
    """Verify the audit CLI script can be imported without errors."""
    import importlib

    mod = importlib.import_module("project.scripts.audit_detector_precision_recall")
    assert hasattr(mod, "main")


@pytest.mark.parametrize("event_type", ["ABSORPTION_PROXY", "DEPTH_STRESS_PROXY"])
def test_measure_detector_proxy_runs_on_spread_bps_only(event_type: str):
    from project.events.detectors.registry import load_all_detectors, get_detector
    from project.scripts.detector_audit_module import measure_detector

    load_all_detectors()
    detector = get_detector(event_type)
    assert detector is not None

    df = _make_df()
    # The audit path should derive spread_zscore from spread_bps instead of erroring on missing column.
    df = df.drop(columns=["depth_usd"]).assign(
        bid_depth_usd=3_000_000.0,
        ask_depth_usd=2_000_000.0,
    )
    segments = []

    metrics = measure_detector(detector, df, "BTCUSDT", segments, "test_run")
    assert metrics["classification"] == "uncovered"
    assert metrics["error"] is None
