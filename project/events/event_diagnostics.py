from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

from project.events.event_specs import (
    EVENT_REGISTRY_SPECS,
)


def generate_event_coverage_report(
    data_root: Path,
    run_id: str,
    symbols: List[str],
    fail_fast: bool = True,
) -> pd.DataFrame:
    from project.events.event_repository import load_registry_events

    events = load_registry_events(data_root=data_root, run_id=run_id, symbols=symbols)

    if events.empty:
        return pd.DataFrame()

    report_rows = []
    event_types = events["event_type"].unique()

    for evt_type in event_types:
        evt_events = events[events["event_type"] == evt_type]

        total_hits = len(evt_events)
        if total_hits == 0:
            continue

        hits_per_symbol = evt_events.groupby("symbol").size()
        hits_min = int(hits_per_symbol.min()) if len(hits_per_symbol) > 0 else 0
        hits_median = float(hits_per_symbol.median()) if len(hits_per_symbol) > 0 else 0.0
        hits_max = int(hits_per_symbol.max()) if len(hits_per_symbol) > 0 else 0

        timestamp_col = "timestamp"
        if timestamp_col in evt_events.columns:
            ts = pd.to_datetime(evt_events[timestamp_col], utc=True)
            first_hit = ts.min().isoformat() if not ts.empty else None
            last_hit = ts.max().isoformat() if not ts.empty else None
        else:
            first_hit = None
            last_hit = None

        key_columns = ["signal_column", "direction", "sign"]
        pct_nan = {}
        for col in key_columns:
            if col in evt_events.columns:
                pct_nan[col] = float(evt_events[col].isna().mean() * 100)
            else:
                pct_nan[col] = 100.0

        row = {
            "event_type": evt_type,
            "synthetic_coverage_status": getattr(
                EVENT_REGISTRY_SPECS.get(evt_type), "synthetic_coverage", "uncovered"
            ),
            "total_hits": total_hits,
            "hits_per_symbol_min": hits_min,
            "hits_per_symbol_median": hits_median,
            "hits_per_symbol_max": hits_max,
            "pct_nan_signal_column": pct_nan.get("signal_column", 0.0),
            "pct_nan_direction": pct_nan.get("direction", 0.0),
            "pct_nan_sign": pct_nan.get("sign", 0.0),
            "first_hit": first_hit,
            "last_hit": last_hit,
        }
        report_rows.append(row)

        if fail_fast:
            if total_hits > 0:
                for col in ["signal_column"]:
                    if col in evt_events.columns:
                        all_nan = evt_events[col].isna().all()
                        if all_nan:
                            raise ValueError(
                                f"Event type '{evt_type}' has all-NaN '{col}' - "
                                f"detection failure. Check prerequisites and thresholds."
                            )

    return pd.DataFrame(report_rows)


def calibrate_event_thresholds(
    events: pd.DataFrame,
    event_type: str,
    target_percentile: float = 99.5,
    min_hits_per_year: int = 20,
    max_hits_per_year: int = 200,
) -> Dict[str, object]:
    if events.empty:
        return {
            "recommended_threshold": None,
            "estimated_hits_per_year": 0,
            "calibration_status": "no_data",
        }

    evt_events = events[events["event_type"] == event_type]
    if evt_events.empty:
        return {
            "recommended_threshold": None,
            "estimated_hits_per_year": 0,
            "calibration_status": "no_data",
        }

    score_columns = [
        "event_score",
        "severity",
        "stress_score",
        "shock_return",
        "adverse_proxy",
    ]

    scores = None
    for col in score_columns:
        if col in evt_events.columns:
            scores = pd.to_numeric(evt_events[col], errors="coerce").dropna()
            if not scores.empty:
                break

    if scores is None or scores.empty:
        return {
            "recommended_threshold": None,
            "estimated_hits_per_year": 0,
            "calibration_status": "no_scoring_column",
        }

    threshold = np.percentile(scores, target_percentile)

    timestamps = pd.to_datetime(evt_events["timestamp"], utc=True)
    if len(timestamps) > 1:
        time_span_years = (timestamps.max() - timestamps.min()).days / 365.25
        if time_span_years > 0:
            estimated_hits_per_year = len(evt_events) / time_span_years
        else:
            estimated_hits_per_year = len(evt_events)
    else:
        estimated_hits_per_year = 0

    if estimated_hits_per_year < min_hits_per_year:
        status = "too_low"
    elif estimated_hits_per_year > max_hits_per_year:
        status = "too_high"
    else:
        status = "ok"

    return {
        "recommended_threshold": float(threshold),
        "estimated_hits_per_year": float(estimated_hits_per_year),
        "calibration_status": status,
        "percentile_used": target_percentile,
        "score_column": col if scores is not None else None,
    }


def verify_index_alignment(
    events: pd.DataFrame,
    bar_timestamps: pd.DatetimeIndex,
    symbol: str,
) -> Dict[str, object]:
    if events.empty:
        return {
            "aligned_count": 0,
            "misaligned_count": 0,
            "mismatch_rate": 0.0,
            "misaligned_timestamps": [],
        }

    evt_symbol = events[events["symbol"] == symbol]
    if evt_symbol.empty:
        return {
            "aligned_count": 0,
            "misaligned_count": 0,
            "mismatch_rate": 0.0,
            "misaligned_timestamps": [],
        }

    event_ts = pd.to_datetime(evt_symbol["timestamp"], utc=True)

    bar_set = set(bar_timestamps)
    aligned_mask = event_ts.isin(bar_set)
    aligned_count = int(aligned_mask.sum())
    misaligned_count = len(event_ts) - aligned_count
    mismatch_rate = float(misaligned_count / len(event_ts)) * 100 if len(event_ts) > 0 else 0.0

    misaligned_ts = event_ts[~aligned_mask].tolist()

    return {
        "aligned_count": aligned_count,
        "misaligned_count": misaligned_count,
        "mismatch_rate": mismatch_rate,
        "misaligned_timestamps": [ts.isoformat() for ts in misaligned_ts],
    }


def registry_contract_check(events: pd.DataFrame, flags: pd.DataFrame, symbol: str) -> None:
    from project.core.validation import ts_ns_utc

    if events.empty:
        return

    ev = events[events["symbol"] == symbol]
    if not ev.empty:
        invalid = ev[ev["signal_ts"] < ev["enter_ts"]]
        if not invalid.empty:
            raise ValueError(f"Events for {symbol} have signal_ts < enter_ts")

    if not flags.empty:
        f_sym = flags[flags["symbol"] == symbol]
        if not f_sym.empty:
            ts = ts_ns_utc(f_sym["timestamp"])
            if len(ts) > 1:
                if not ts.is_monotonic_increasing:
                    raise ValueError(f"Event flags for {symbol} are not monotonic")
                if ts.duplicated().any():
                    raise ValueError(f"Event flags for {symbol} have duplicates")


def build_event_feature_frame(
    data_root: Path,
    run_id: str,
    symbol: str,
) -> pd.DataFrame:
    from project.events.event_repository import load_registry_events

    events = load_registry_events(data_root=data_root, run_id=run_id, symbols=[symbol])
    if events.empty:
        return pd.DataFrame()

    rows = []
    for _, row in events.iterrows():
        try:
            payload = json.loads(row["features_at_event"])
        except (ValueError, TypeError):
            payload = {}

        prefix = str(row["event_type"]).lower()
        flattened = {"timestamp": row["timestamp"]}
        for k, v in payload.items():
            flattened[f"{prefix}_{k}"] = v
        rows.append(flattened)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.sort_values("timestamp")

    def _last_non_null(col):
        non_null = col.dropna()
        return non_null.iloc[-1] if not non_null.empty else None

    df = df.groupby("timestamp", sort=True).agg(_last_non_null).reset_index()
    return df
