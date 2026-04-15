"""
Shared measurement logic for detector precision/recall auditing.

Imported by both audit_detector_precision_recall.py (CLI) and
project/tests/events/test_detector_precision_recall.py (regression tests).

Placement in project/scripts/ (not project/events/) avoids circular import risk:
some family modules import from project.research at module level.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd

from project.events.detectors.base import BaseEventDetector


# ---------------------------------------------------------------------------
# Classification constants
# ---------------------------------------------------------------------------

MIN_PRECISION: float = 0.50
MIN_RECALL: float = 0.30

AUDIT_RUN_IDS: Dict[str, str] = {
    "2021_bull": "synthetic_2021_bull",
    "default": "synthetic_2025_full_year",
    "stress_crash": "synthetic_2025_stress_crash",
    "golden": "golden_synthetic_discovery",
}
KNOWN_RUN_IDS = set(AUDIT_RUN_IDS.values())
SYNTHETIC_LIVE_ONLY_EVENT_TYPES = {
    "ABSORPTION_PROXY",
    "DEPTH_STRESS_PROXY",
}


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class DetectorMetrics:
    event_type: str
    symbol: str
    run_id: str
    total_events: int
    event_rate_per_1k: float
    in_window_events: int
    off_regime_events: int
    expected_windows: int
    windows_hit: int
    precision: float
    recall: float  # float("nan") when expected_windows == 0
    classification: str  # stable | noisy | silent | broken | uncovered | error
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d = {k: v for k, v in self.__dict__.items()}
        if math.isnan(d.get("recall", 0)):
            d["recall"] = None  # JSON-serializable
        return d

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def _classify(precision: float, recall: float, expected_windows: int) -> str:
    """Classify a detector result into one of five classes."""
    if expected_windows == 0:
        return "uncovered"
    p_ok = precision >= MIN_PRECISION
    r_ok = recall >= MIN_RECALL
    if p_ok and r_ok:
        return "stable"
    if not p_ok and r_ok:
        return "noisy"
    if p_ok and not r_ok:
        return "silent"
    return "broken"


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_manifest(data_root: Path, run_id: str) -> Dict[str, Any]:
    """Load synthetic_generation_manifest.json for a run_id."""
    path = data_root / "synthetic" / run_id / "synthetic_generation_manifest.json"
    return json.loads(path.read_text(encoding="utf-8"))


def load_truth_segments(data_root: Path, run_id: str) -> List[Dict[str, Any]]:
    """Load synthetic_regime_segments.json for a run_id."""
    path = data_root / "synthetic" / run_id / "synthetic_regime_segments.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "segments" in payload:
        return list(payload["segments"])
    return list(payload)


def build_symbol_df(symbol_entry: Dict[str, Any]) -> pd.DataFrame:
    """
    Build a rich merged DataFrame for one symbol using manifest paths.

    Columns produced (beyond raw OHLCV):
      - close_perp: alias of perp close
      - close_spot: spot close, merged by timestamp
      - funding_rate_scaled: forward-filled from funding parquet
      - rv_96: rolling 96-bar realized vol (computed if absent)
    """
    paths = symbol_entry["paths"]

    # --- perp bars ---
    perp_frames = [pd.read_parquet(p) for p in paths["cleaned_perp"]]
    perp = pd.concat(perp_frames, ignore_index=True)
    perp["timestamp"] = pd.to_datetime(perp["timestamp"], utc=True, errors="coerce")
    perp = perp.sort_values("timestamp").reset_index(drop=True)
    perp["close_perp"] = perp["close"]  # basis detectors need close_perp

    # --- spot bars ---
    spot_paths = paths.get("cleaned_spot", [])
    if spot_paths:
        spot_frames = [pd.read_parquet(p) for p in spot_paths]
        spot = pd.concat(spot_frames, ignore_index=True)
        spot["timestamp"] = pd.to_datetime(spot["timestamp"], utc=True, errors="coerce")
        spot = (
            spot[["timestamp", "close"]]
            .rename(columns={"close": "close_spot"})
            .sort_values("timestamp")
        )
        perp = pd.merge_asof(
            perp.sort_values("timestamp"),
            spot,
            on="timestamp",
            direction="nearest",
            tolerance=pd.Timedelta("5min"),
        ).reset_index(drop=True)

    # --- funding: forward-fill to bar frequency ---
    # Skip if already present in the cleaned perp parquet (avoids merge_asof column suffix collision)
    funding_path = paths.get("funding")
    if "funding_rate_scaled" not in perp.columns and funding_path and Path(funding_path).exists():
        funding = pd.read_parquet(funding_path)
        funding["timestamp"] = pd.to_datetime(funding["timestamp"], utc=True, errors="coerce")
        if "funding_rate_scaled" in funding.columns:
            funding = funding[["timestamp", "funding_rate_scaled"]].sort_values("timestamp")
            perp = pd.merge_asof(
                perp.sort_values("timestamp"),
                funding,
                on="timestamp",
                direction="backward",
            ).reset_index(drop=True)

    perp["symbol"] = symbol_entry["symbol"]
    return perp.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Measurement
# ---------------------------------------------------------------------------


def _get_tolerance_td(
    event_type: str, tolerance_minutes: Union[int, Dict[str, int]]
) -> pd.Timedelta:
    if isinstance(tolerance_minutes, dict):
        minutes = tolerance_minutes.get(event_type, 30)
    else:
        minutes = int(tolerance_minutes)
    return pd.Timedelta(minutes=minutes)


def _build_truth_windows(
    segments: List[Dict[str, Any]],
    event_type: str,
    symbol: str,
    tolerance: pd.Timedelta,
) -> List[tuple]:
    windows = []
    for seg in segments:
        if seg.get("symbol", "").upper() != symbol.upper():
            continue
        if event_type.upper() not in [et.upper() for et in seg.get("expected_event_types", [])]:
            continue
        start = pd.Timestamp(seg["start_ts"], tz="UTC") - tolerance
        end = pd.Timestamp(seg["end_ts"], tz="UTC") + tolerance
        windows.append((start, end))
    return windows


def _count_hits(
    event_times: pd.Series,
    windows: List[tuple],
) -> tuple:
    """Returns (in_window_count, windows_hit_count)."""
    if event_times.empty or not windows:
        return 0, 0
    in_window = pd.Series(False, index=event_times.index)
    windows_hit = 0
    for start_ts, end_ts in windows:
        mask = event_times.between(start_ts, end_ts, inclusive="both")
        if bool(mask.any()):
            windows_hit += 1
        in_window = in_window | mask
    return int(in_window.sum()), windows_hit


def _enrich_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pre-compute derived columns that some detectors require but that may be
    absent from a minimal test/audit DataFrame (e.g. range_96, range_med_2880).

    This mirrors the logic in project/pipelines/features/build_features.py so
    that measure_detector works on DataFrames that have raw OHLCV but not yet
    the full feature set.
    """
    df = df.copy()

    # rv_96: log-return realized vol over 96 bars
    if "rv_96" not in df.columns and "close" in df.columns:
        log_ret = np.log(df["close"] / df["close"].shift(1))
        df["rv_96"] = log_ret.rolling(96, min_periods=12).std()

    # spread_zscore: mimic the canonical feature builder when spread_bps is available
    if "spread_zscore" not in df.columns and "spread_bps" in df.columns:
        spread = pd.to_numeric(df["spread_bps"], errors="coerce")
        roll_mean = spread.rolling(96, min_periods=12).mean()
        roll_std = spread.rolling(96, min_periods=12).std()
        df["spread_zscore"] = (spread - roll_mean) / roll_std.replace(0.0, np.nan)

    # Depth imbalance is a common synthetic microstructure proxy. Preserve or derive it when possible.
    if "imbalance" not in df.columns and {"bid_depth_usd", "ask_depth_usd"}.issubset(df.columns):
        bid_depth = pd.to_numeric(df["bid_depth_usd"], errors="coerce")
        ask_depth = pd.to_numeric(df["ask_depth_usd"], errors="coerce")
        total_depth = (bid_depth + ask_depth).replace(0.0, np.nan)
        df["imbalance"] = ((bid_depth - ask_depth) / total_depth).fillna(0.0)

    if "micro_depth_depletion" not in df.columns:
        if "depth_usd" in df.columns:
            depth = pd.to_numeric(df["depth_usd"], errors="coerce")
        elif {"bid_depth_usd", "ask_depth_usd"}.issubset(df.columns):
            depth = pd.to_numeric(df["bid_depth_usd"], errors="coerce") + pd.to_numeric(
                df["ask_depth_usd"], errors="coerce"
            )
        else:
            depth = None
        if depth is not None:
            baseline = depth.rolling(24, min_periods=1).mean().shift(1).fillna(depth)
            df["micro_depth_depletion"] = (1.0 - (depth / baseline.replace(0.0, np.nan))).fillna(
                0.0
            )

    # range_96: high/low ratio over 96 bars (lagged 1)
    if "range_96" not in df.columns and "high" in df.columns and "low" in df.columns:
        high_96 = df["high"].rolling(96, min_periods=1).max().shift(1)
        low_96 = df["low"].rolling(96, min_periods=1).min().shift(1)
        df["range_96"] = (high_96 / low_96.replace(0.0, np.nan) - 1.0).fillna(0.0)

    # range_med_2880: rolling median of range_96 over 2880 bars (lagged 1)
    if "range_med_2880" not in df.columns and "range_96" in df.columns:
        df["range_med_2880"] = (
            df["range_96"].rolling(2880, min_periods=1).median().shift(1).fillna(0.0)
        )

    return df


def measure_detector(
    detector: BaseEventDetector,
    df: pd.DataFrame,
    symbol: str,
    segments: List[Dict[str, Any]],
    run_id: str,
    tolerance_minutes: Union[int, Dict[str, int]] = 30,
) -> DetectorMetrics:
    """
    Run a detector against a prepared DataFrame and compute precision/recall.

    Precision = in_window_events / total_events  (0.0 if no events fired)
    Recall    = windows_hit / expected_windows   (NaN if no truth windows)

    Returns a DetectorMetrics dataclass. On detection errors (e.g. missing
    required columns), classification is set to "error" and error field is set.
    """
    event_type = detector.event_type
    tolerance = _get_tolerance_td(event_type, tolerance_minutes)
    truth_windows = _build_truth_windows(segments, event_type, symbol, tolerance)

    # Enrich the DataFrame with derived columns before passing to detector
    df = _enrich_df(df)

    try:
        events = detector.detect(df, symbol=symbol)
    except Exception as exc:
        return DetectorMetrics(
            event_type=event_type,
            symbol=symbol,
            run_id=run_id,
            total_events=0,
            event_rate_per_1k=0.0,
            in_window_events=0,
            off_regime_events=0,
            expected_windows=len(truth_windows),
            windows_hit=0,
            precision=0.0,
            recall=float("nan"),
            classification="error",
            error=str(exc),
        )

    total_bars = len(df)

    ts_col = next(
        (c for c in ("signal_ts", "timestamp", "eval_bar_ts") if c in events.columns),
        None,
    )
    if ts_col and not events.empty:
        event_times = pd.to_datetime(events[ts_col], utc=True, errors="coerce").dropna()
    else:
        event_times = pd.Series(dtype="datetime64[ns, UTC]")

    total_events = len(event_times)
    event_rate = (total_events / max(1, total_bars)) * 1000.0
    in_window, windows_hit = _count_hits(event_times, truth_windows)
    off_regime = max(0, total_events - in_window)
    expected_windows = len(truth_windows)

    precision = float(in_window / total_events) if total_events > 0 else 0.0
    recall = float(windows_hit / expected_windows) if expected_windows > 0 else float("nan")

    recall_for_classify = 0.0 if math.isnan(recall) else recall
    classification = _classify(precision, recall_for_classify, expected_windows)

    return DetectorMetrics(
        event_type=event_type,
        symbol=symbol,
        run_id=run_id,
        total_events=total_events,
        event_rate_per_1k=round(event_rate, 2),
        in_window_events=in_window,
        off_regime_events=off_regime,
        expected_windows=expected_windows,
        windows_hit=windows_hit,
        precision=round(precision, 4),
        recall=round(recall, 4) if not math.isnan(recall) else float("nan"),
        classification=classification,
        error=None,
    )
