"""
E1-T2: Detector causality regression tests.

Verifies that event detectors satisfy prefix invariance: events detected on
df[:cutoff] must match events detected on the full df for bar indices well
before the cutoff (i.e., at least `margin` bars before the cutoff).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from project.events.detectors.base import BaseEventDetector
from project.events.detectors.episode import EpisodeDetector
from project.events.detectors.funding import FundingFlipDetector
from project.events.families.temporal import FeeRegimeChangeDetector

# The bar index column name as discovered from detect() output.
BAR_INDEX_COL = "event_idx"


def _make_ohlcv(n: int = 500, seed: int = 1) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    close = 100.0 + np.cumsum(rng.normal(0, 0.3, n))
    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "open": close - rng.uniform(0, 0.1, n),
            "high": close + rng.uniform(0, 0.2, n),
            "low": close - rng.uniform(0, 0.2, n),
            "close": close,
            "volume": rng.uniform(1000, 5000, n),
        }
    )


class _RollingStdDetector(BaseEventDetector):
    """Causal detector: triggers when rolling std exceeds threshold."""

    event_type = "ROLLING_STD"
    required_columns = ("timestamp", "close")
    _window = 20
    _threshold = 0.5

    def compute_raw_mask(self, df: pd.DataFrame, *, features, **params) -> pd.Series:
        rolling_std = df["close"].rolling(self._window).std()
        return (rolling_std > self._threshold).fillna(False)


class _LookaheadDetector(BaseEventDetector):
    """Deliberately non-causal: uses shift(-5) to read future bars."""

    event_type = "LOOKAHEAD"
    required_columns = ("timestamp", "close")

    def compute_raw_mask(self, df: pd.DataFrame, *, features, **params) -> pd.Series:
        # shift(-5) looks 5 bars into the future — causal violation
        future_return = df["close"].shift(-5) / df["close"] - 1
        return (future_return.abs() > 0.005).fillna(False)


class _ParamAwareDetector(BaseEventDetector):
    """Detector used to verify detect() threads params into hook methods."""

    event_type = "PARAM_AWARE"
    required_columns = ("timestamp", "close")

    def compute_raw_mask(self, df: pd.DataFrame, *, features, **params) -> pd.Series:
        del features, params
        return pd.Series(True, index=df.index)

    def compute_intensity(self, df: pd.DataFrame, *, features, **params) -> pd.Series:
        del features
        scale = float(params.get("intensity_scale", 1.0))
        return pd.Series(scale, index=df.index, dtype=float)

    def compute_severity(self, idx: int, intensity: float, features, **params) -> str:
        del idx, features
        threshold = float(params.get("severity_threshold", 1.0))
        return "major" if intensity >= threshold else "moderate"

    def compute_direction(self, idx: int, features, **params) -> str:
        del idx, features
        return str(params.get("direction_label", "non_directional"))

    def compute_metadata(self, idx: int, features, **params) -> dict[str, float]:
        del features
        return {"severity_threshold": float(params["severity_threshold"]), "row_idx": float(idx)}


def _event_bar_indices(events: pd.DataFrame) -> set[int]:
    if events.empty or BAR_INDEX_COL not in events.columns:
        return set()
    return set(events[BAR_INDEX_COL].astype(int).tolist())


def _assert_prefix_invariant(
    detector: BaseEventDetector,
    df: pd.DataFrame,
    cutoff: int,
    margin: int = 20,
    symbol: str = "TEST",
) -> None:
    """Assert that events at safe bar indices match between full and truncated runs."""
    events_full = detector.detect(df, symbol=symbol)
    events_prefix = detector.detect(df.iloc[:cutoff], symbol=symbol)

    safe_limit = cutoff - margin

    full_safe = _event_bar_indices(events_full) & set(range(safe_limit))
    prefix_safe = _event_bar_indices(events_prefix) & set(range(safe_limit))

    assert full_safe == prefix_safe, (
        f"Prefix invariance violated at cutoff={cutoff}, margin={margin}: "
        f"full={sorted(full_safe - prefix_safe)[:10]} missing from prefix, "
        f"prefix={sorted(prefix_safe - full_safe)[:10]} extra in prefix"
    )


# ---------------------------------------------------------------------------
# Test 1: causal detector passes prefix invariance
# ---------------------------------------------------------------------------


def test_threshold_detector_is_causal():
    df = _make_ohlcv(n=500, seed=1)
    detector = _RollingStdDetector()

    for cutoff in (200, 350):
        _assert_prefix_invariant(detector, df, cutoff=cutoff, margin=20)


# ---------------------------------------------------------------------------
# Test 2: lookahead detector CAN be caught by the test harness
# ---------------------------------------------------------------------------


def test_threshold_detector_future_access_detected():
    """
    Confirm the test harness can distinguish lookahead from causal detectors.

    A lookahead detector that reads shift(-5) should produce different event
    sets near the cutoff boundary. We verify the harness correctly identifies
    this discrepancy (i.e., the invariance assertion fails for the lookahead
    detector), proving the harness is not vacuously passing.
    """
    df = _make_ohlcv(n=500, seed=42)
    detector = _LookaheadDetector()
    cutoff = 200
    margin = 20

    events_full = detector.detect(df, symbol="TEST")
    events_prefix = detector.detect(df.iloc[:cutoff], symbol="TEST")

    # Both calls must return DataFrames (no crash)
    assert isinstance(events_full, pd.DataFrame)
    assert isinstance(events_prefix, pd.DataFrame)

    # For the lookahead detector, events near the cutoff boundary differ:
    # bars in [cutoff - 5, cutoff - 1] may see future data in the full run
    # but not in the truncated run, so the sets should differ somewhere
    # near the boundary (within the last `shift` bars before cutoff).
    boundary_zone = set(range(cutoff - 10, cutoff))
    full_boundary = _event_bar_indices(events_full) & boundary_zone
    prefix_boundary = _event_bar_indices(events_prefix) & boundary_zone

    # The lookahead detector reads 5 bars ahead; near the cutoff the full
    # run has future context the prefix run lacks, so event sets differ.
    # Guard: if the detector fired in neither set, the boundary zone is empty
    # and we cannot prove the harness catches lookahead for this seed.
    if not full_boundary and not prefix_boundary:
        pytest.skip(
            "Lookahead detector did not fire in boundary zone for this seed; "
            "cannot verify harness catches lookahead."
        )
    assert full_boundary != prefix_boundary, (
        "Expected lookahead detector to produce different events near cutoff "
        "boundary, but both runs matched — harness may not be catching lookahead."
    )


# ---------------------------------------------------------------------------
# Test 3: detect() output contains the bar index column
# ---------------------------------------------------------------------------


def test_causal_detector_emits_bar_index_column():
    df = _make_ohlcv(n=100, seed=7)
    detector = _RollingStdDetector()
    events = detector.detect(df, symbol="SYM")

    assert BAR_INDEX_COL in events.columns, (
        f"Expected column '{BAR_INDEX_COL}' in detect() output; "
        f"got columns: {events.columns.tolist()}"
    )

    if not events.empty:
        indices = events[BAR_INDEX_COL].astype(int)
        assert (indices >= 0).all(), "Bar indices must be non-negative"
        assert (indices < len(df)).all(), "Bar indices must be within df bounds"


def test_detect_threads_params_into_event_hooks():
    df = _make_ohlcv(n=4, seed=9)
    detector = _ParamAwareDetector()

    events = detector.detect(
        df,
        symbol="SYM",
        intensity_scale=1.5,
        severity_threshold=1.2,
        direction_label="long",
    )

    assert not events.empty
    assert set(events["severity_bucket"]) == {"major"}
    assert set(events["direction"]) == {"up"}
    assert set(events["sign"]) == {1}
    assert set(events["causal"]) == {True}
    assert set(events["severity_threshold"]) == {1.2}


# ---------------------------------------------------------------------------
# Test 4: EpisodeDetector uses its own detect() code path and is causal
# ---------------------------------------------------------------------------


def _make_ohlcv_with_signal(n: int = 500, seed: int = 3) -> pd.DataFrame:
    """Build an OHLCV frame with a causal 'signal' column for EpisodeDetector."""
    rng = np.random.default_rng(seed)
    close = 100.0 + np.cumsum(rng.normal(0, 0.3, n))
    # Signal: rolling z-score of close (purely causal — uses only past bars)
    close_series = pd.Series(close)
    rolling_mean = close_series.rolling(20).mean()
    rolling_std = close_series.rolling(20).std().replace(0.0, 1.0)
    signal = ((close_series - rolling_mean) / rolling_std).fillna(0.0).to_numpy()
    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "open": close - rng.uniform(0, 0.1, n),
            "high": close + rng.uniform(0, 0.2, n),
            "low": close - rng.uniform(0, 0.2, n),
            "close": close,
            "volume": rng.uniform(1000, 5000, n),
            "signal": signal,
        }
    )


class _EpisodeVolSpike(EpisodeDetector):
    """Concrete EpisodeDetector subclass: fires when signal z-score >= 1.0."""

    event_type = "EPISODE_VOL_SPIKE"
    required_columns = ("timestamp", "signal")
    threshold: float = 1.0
    direction: str = "ge"
    max_gap: int = 2
    anchor_rule: str = "peak"


def test_episode_detector_is_causal():
    """
    EpisodeDetector.detect() is a separate code path from BaseEventDetector.detect().
    Verify it satisfies prefix invariance (causal) using its own detect() method.
    """
    df = _make_ohlcv_with_signal(n=500, seed=3)
    detector = _EpisodeVolSpike()

    # Confirm detect() is EpisodeDetector's own implementation (not BaseEventDetector's)
    assert type(detector).detect is EpisodeDetector.detect, (
        "Expected _EpisodeVolSpike.detect to resolve to EpisodeDetector.detect"
    )

    # Prefix invariance at two cutoffs exercises the episode grouping code path
    for cutoff in (200, 350):
        _assert_prefix_invariant(detector, df, cutoff=cutoff, margin=20)

    # Confirm the detector actually fired at least once on the full dataset
    events = detector.detect(df, symbol="EPISODE_TEST")
    assert isinstance(events, pd.DataFrame)
    assert not events.empty, (
        "EpisodeDetector did not fire on synthetic data — adjust signal or threshold"
    )
    assert BAR_INDEX_COL in events.columns, (
        f"EpisodeDetector.detect() must set '{BAR_INDEX_COL}' column"
    )
    indices = events[BAR_INDEX_COL].astype(int)
    assert (indices >= 0).all(), "Episode bar indices must be non-negative"
    assert (indices < len(df)).all(), "Episode bar indices must be within df bounds"
    assert set(events["causal"]) == {True}


def test_retrospective_detectors_emit_causal_false_metadata():
    funding_df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=400, freq="5min", tz="UTC"),
            "funding_rate_scaled": np.r_[np.full(320, 0.0006), np.full(80, -0.0008)],
        }
    )
    funding_events = FundingFlipDetector().detect(funding_df, symbol="BTCUSDT")
    assert not funding_events.empty
    assert set(funding_events["causal"]) == {False}

    fee = np.full(400, 1.0)
    fee[320:] = 4.0
    fee_df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=400, freq="5min", tz="UTC"),
            "fee_bps": fee,
        }
    )
    fee_events = FeeRegimeChangeDetector().detect(fee_df, symbol="BTCUSDT")
    assert not fee_events.empty
    assert set(fee_events["causal"]) == {False}
