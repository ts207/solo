from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from project import PROJECT_ROOT
from project.core.config import get_data_root
from project.core.constants import DEFAULT_EVENT_HORIZON_BARS
from project.core.feature_schema import feature_dataset_dir_name
from project.specs.gates import load_gates_spec as _shared_load_gates_spec


def _load_gates_spec() -> dict[str, Any]:
    return _shared_load_gates_spec(PROJECT_ROOT.parent)


from project.io.utils import (
    choose_partition_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
)


def _default_horizons_bars_csv() -> str:
    return ",".join(str(int(x)) for x in DEFAULT_EVENT_HORIZON_BARS)


def _load_bars(run_id: str, symbol: str, timeframe: str = "5m") -> pd.DataFrame:
    DATA_ROOT = get_data_root()
    candidates = [
        run_scoped_lake_path(DATA_ROOT, run_id, "cleaned", "perp", symbol, f"bars_{timeframe}"),
        DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / f"bars_{timeframe}",
    ]
    bars_dir = choose_partition_dir(candidates)
    if not bars_dir:
        return pd.DataFrame()
    files = list_parquet_files(bars_dir)
    if not files:
        return pd.DataFrame()
    return read_parquet(files)


def _load_features(run_id: str, symbol: str) -> pd.DataFrame:
    """Load PIT features table for join-rate computation."""
    DATA_ROOT = get_data_root()
    feature_dataset = feature_dataset_dir_name()
    candidates = [
        run_scoped_lake_path(DATA_ROOT, run_id, "features", "perp", symbol, "5m", feature_dataset),
        DATA_ROOT / "lake" / "features" / "perp" / symbol / "5m" / feature_dataset,
    ]
    features_dir = choose_partition_dir(candidates)
    if not features_dir:
        return pd.DataFrame()
    files = list_parquet_files(features_dir)
    if not files:
        return pd.DataFrame()
    df = read_parquet(files)
    if df.empty or "timestamp" not in df.columns:
        return pd.DataFrame()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    return df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)


def _compute_join_rate(
    events_df: pd.DataFrame,
    features_df: pd.DataFrame,
    horizons_bars: list[int],
    *,
    max_feature_staleness: pd.Timedelta | None = pd.Timedelta("1h"),
) -> dict[str, float]:
    """
    Join-rate computation using merge_asof(direction="backward").

    Feature join rate
    ----------------
    Each event is matched to the most-recent feature bar whose timestamp is
    <= the event timestamp.  This handles the common case where events carry
    sub-bar timestamps (e.g. 10:03:17) that will never match a bar boundary
    (10:00:00) under exact equality.

    Label join rate (per horizon h)
    --------------------------------
    Starting from the matched bar's *integer position* in features_df, we
    check that bar at position (matched_pos + h) exists and has a finite
    close price.  We derive matched_pos from the merge_asof result rather
    than re-searching by timestamp, which avoids the off-by-one that
    searchsorted(side="left") introduces for unaligned timestamps.
    """
    null_result: dict[str, float] = {"features": 0.0, **{f"label_{h}b": 0.0 for h in horizons_bars}}

    if events_df.empty or features_df.empty:
        return null_result

    ts_col = "timestamp" if "timestamp" in events_df.columns else "enter_ts"
    if ts_col not in events_df.columns:
        return null_result

    # Build a single-column events frame sorted by timestamp.
    evt_ts = pd.to_datetime(events_df[ts_col], utc=True, errors="coerce")
    evt_frame = (
        pd.DataFrame({"timestamp": evt_ts}).dropna().sort_values("timestamp").reset_index(drop=True)
    )
    n_events = len(evt_frame)
    if n_events == 0:
        return null_result

    # Stamp each feature row with its integer position so we can recover it
    # after the merge without a second searchsorted call.
    feat = features_df.copy().sort_values("timestamp").reset_index(drop=True)
    feat["_feat_pos"] = feat.index  # 0-based integer position in feat

    # merge_asof(direction="backward"): for each event find the latest bar
    # whose timestamp <= event timestamp.
    merged = pd.merge_asof(
        evt_frame,
        feat[
            ["timestamp", "_feat_pos", "close"]
            if "close" in feat.columns
            else ["timestamp", "_feat_pos"]
        ],
        on="timestamp",
        direction="backward",
        tolerance=max_feature_staleness,
    )

    # Feature join rate: rows where merge found a match (_feat_pos is not NaN).
    feat_joined = merged["_feat_pos"].notna().sum()
    feature_join_rate = float(feat_joined / n_events)

    # Label join rate per horizon.
    has_close = "close" in feat.columns
    close_arr = feat["close"].to_numpy(dtype=float) if has_close else None
    n_feat = len(feat)

    label_rates: dict[str, float] = {}
    for h in horizons_bars:
        if not has_close or close_arr is None:
            label_rates[f"label_{h}b"] = 0.0
            continue
        valid = 0
        for row in merged.to_dict("records"):
            pos = row["_feat_pos"]
            if pd.isna(pos):
                continue
            future_pos = int(pos) + h
            if future_pos < n_feat and np.isfinite(close_arr[future_pos]):
                valid += 1
        label_rates[f"label_{h}b"] = float(valid / n_events)

    return {"features": feature_join_rate, **label_rates}


def _compute_sensitivity(
    events_df: pd.DataFrame,
    severity_cols: list[str],
    pct_delta: float = 0.10,
) -> dict[str, float]:
    """
    Real sensitivity sweep: vary the effective event threshold by ±pct_delta
    and measure how prevalence changes.  Uses severity/magnitude columns in
    the events_df if present; falls back to a rank-based approximation.

    Returns prevalence_elasticity = |%Δ events / %Δ threshold|.
    """
    if events_df.empty:
        return {
            "threshold_delta_pct": [-pct_delta * 100, pct_delta * 100],
            "prevalence_stability_index": float("nan"),
            "prevalence_elasticity": float("nan"),
            "sensitivity_method": "no_events",
        }

    n_base = len(events_df)

    # Find a numeric severity column to use as a proxy for the event threshold
    severity_col: str | None = None
    for col in severity_cols:
        if col in events_df.columns:
            arr = pd.to_numeric(events_df[col], errors="coerce").dropna()
            if len(arr) > 0:
                severity_col = col
                break

    if severity_col is None:
        return {
            "threshold_delta_pct": [-pct_delta * 100, pct_delta * 100],
            "prevalence_stability_index": float("nan"),
            "prevalence_elasticity": float("nan"),
            "sensitivity_method": "unavailable_no_severity",
        }

    sev = pd.to_numeric(events_df[severity_col], errors="coerce").dropna()
    threshold_base = float(sev.median())
    if threshold_base == 0.0:
        threshold_base = float(sev.mean()) or 1.0

    threshold_tight = threshold_base * (1.0 + pct_delta)
    threshold_loose = threshold_base * (1.0 - pct_delta)

    n_tight = int((sev >= threshold_tight).sum())
    n_loose = int((sev >= threshold_loose).sum())

    if n_base > 0:
        elasticity_tight = abs((n_tight - n_base) / n_base / pct_delta) if pct_delta != 0 else 0.0
        elasticity_loose = abs((n_loose - n_base) / n_base / pct_delta) if pct_delta != 0 else 0.0
        elasticity = float(max(elasticity_tight, elasticity_loose))
        stability_index = float(n_tight / n_base)
    else:
        elasticity = float("nan")
        stability_index = float("nan")

    return {
        "threshold_delta_pct": [-pct_delta * 100, pct_delta * 100],
        "prevalence_stability_index": stability_index,
        "prevalence_elasticity": elasticity,
        "sensitivity_method": f"severity_col:{severity_col}",
    }


def _event_identity_columns(events_df: pd.DataFrame) -> pd.Series:
    if events_df.empty:
        return pd.Series(dtype=str)
    for cols in (["symbol", "enter_ts"], ["symbol", "timestamp"], ["enter_ts"], ["timestamp"]):
        if all(col in events_df.columns for col in cols):
            frame = events_df.loc[:, cols].copy()
            for c in cols:
                if "ts" in c or "time" in c:
                    frame[c] = pd.to_datetime(frame[c], utc=True, errors="coerce").astype(str)
                else:
                    frame[c] = frame[c].astype(str)
            return frame.astype(str).agg("|".join, axis=1)
    return pd.Series(events_df.index.astype(str), index=events_df.index, dtype=str)


def _event_sign_series(events_df: pd.DataFrame) -> pd.Series:
    mapping = {
        "long": 1.0,
        "up": 1.0,
        "buy": 1.0,
        "bull": 1.0,
        "short": -1.0,
        "down": -1.0,
        "sell": -1.0,
        "bear": -1.0,
    }
    for col in ["sign", "event_direction", "signal_direction", "direction_score", "direction"]:
        if col not in events_df.columns:
            continue
        raw = events_df[col]
        if pd.api.types.is_numeric_dtype(raw):
            out = np.sign(pd.to_numeric(raw, errors="coerce")).replace(0.0, np.nan)
        else:
            out = raw.astype(str).str.lower().map(mapping)
        if out.notna().any():
            return out
    return pd.Series(np.nan, index=events_df.index, dtype=float)


def _compute_rerun_proxy_metrics(
    events_df: pd.DataFrame, severity_cols: list[str], pct_delta: float = 0.10
) -> dict[str, Any]:
    base_count = len(events_df)
    if base_count == 0:
        return {
            "base_prevalence": 0,
            "prevalence_elasticity": float("nan"),
            "candidate_identity_stability": float("nan"),
            "sign_stability": float("nan"),
            "proxy_method": "no_events",
        }

    severity_col = None
    sev = None
    for col in severity_cols:
        if col in events_df.columns:
            candidate = pd.to_numeric(events_df[col], errors="coerce")
            if candidate.notna().any():
                severity_col = col
                sev = candidate
                break
    if sev is None:
        return {
            "base_prevalence": base_count,
            "prevalence_elasticity": float("nan"),
            "candidate_identity_stability": float("nan"),
            "sign_stability": float("nan"),
            "proxy_method": "unavailable_no_severity",
        }

    working = events_df.copy()
    working["_sev"] = sev
    working = working.dropna(subset=["_sev"]).sort_values("_sev", ascending=False)
    if working.empty:
        return {
            "base_prevalence": base_count,
            "prevalence_elasticity": float("nan"),
            "candidate_identity_stability": float("nan"),
            "sign_stability": float("nan"),
            "proxy_method": f"severity_col:{severity_col}:no_valid_rows",
        }

    n = len(working)
    tight_n = max(1, int(np.floor(n * (1.0 - pct_delta))))
    loose_n = min(n, max(tight_n, int(np.ceil(n * (1.0 - pct_delta / 2.0)))))
    base_ids = set(_event_identity_columns(working).tolist())
    tight = working.head(tight_n)
    loose = working.head(loose_n)
    tight_ids = set(_event_identity_columns(tight).tolist())
    loose_ids = set(_event_identity_columns(loose).tolist())

    overlap_tight = len(base_ids & tight_ids) / max(1, len(base_ids | tight_ids))
    overlap_loose = len(base_ids & loose_ids) / max(1, len(base_ids | loose_ids))
    identity_stability = float(min(overlap_tight, overlap_loose))

    base_sign = _event_sign_series(working)
    tight_sign = _event_sign_series(tight)
    aligned = pd.concat([base_sign.rename("base"), tight_sign.rename("tight")], axis=1).dropna()
    if aligned.empty:
        sign_stability = float("nan")
    else:
        sign_stability = float((np.sign(aligned["base"]) == np.sign(aligned["tight"])).mean())

    prevalence_elasticity = abs((tight_n - n) / max(1, n) / pct_delta) if pct_delta else 0.0
    return {
        "base_prevalence": base_count,
        "prevalence_elasticity": float(prevalence_elasticity),
        "candidate_identity_stability": identity_stability,
        "sign_stability": sign_stability,
        "proxy_method": f"severity_col:{severity_col}",
    }


def _compare_event_runs(
    base_events: pd.DataFrame, rerun_events: pd.DataFrame, severity_cols: Sequence[str]
) -> dict[str, Any]:
    base_ids = (
        set(_event_identity_columns(base_events).tolist()) if not base_events.empty else set()
    )
    rerun_ids = (
        set(_event_identity_columns(rerun_events).tolist()) if not rerun_events.empty else set()
    )
    base_n = len(base_ids)
    rerun_n = len(rerun_ids)
    union_n = len(base_ids | rerun_ids)
    inter_n = len(base_ids & rerun_ids)
    identity_stability = float(inter_n / union_n) if union_n > 0 else float("nan")

    def _signed_id_frame(df: pd.DataFrame) -> pd.DataFrame:
        ids = _event_identity_columns(df).rename("event_id")
        signs = _event_sign_series(df).rename("sign")
        out = (
            pd.concat([ids, signs], axis=1)
            .dropna(subset=["event_id"])
            .drop_duplicates(subset=["event_id"], keep="last")
        )
        return out

    base_signed = _signed_id_frame(base_events)
    rerun_signed = _signed_id_frame(rerun_events)
    aligned = base_signed.merge(
        rerun_signed, on="event_id", how="inner", suffixes=("_base", "_rerun")
    ).dropna()
    if aligned.empty:
        sign_stability = float("nan")
    else:
        sign_stability = float(
            (np.sign(aligned["sign_base"]) == np.sign(aligned["sign_rerun"])).mean()
        )

    prevalence_elasticity = abs((rerun_n - base_n) / max(1, base_n))
    metrics: dict[str, Any] = {
        "base_prevalence": int(base_n),
        "rerun_prevalence": int(rerun_n),
        "prevalence_elasticity": float(prevalence_elasticity),
        "candidate_identity_stability": identity_stability,
        "sign_stability": sign_stability,
    }

    severity_col = next(
        (
            col
            for col in severity_cols
            if col in base_events.columns and col in rerun_events.columns
        ),
        None,
    )
    if severity_col is not None:
        base_sev = pd.to_numeric(base_events[severity_col], errors="coerce")
        rerun_sev = pd.to_numeric(rerun_events[severity_col], errors="coerce")
        if base_sev.notna().any() and rerun_sev.notna().any():
            metrics["base_severity_median"] = float(base_sev.median())
            metrics["rerun_severity_median"] = float(rerun_sev.median())
    return metrics


def _load_rerun_events(out_dir: Path, event_type: str, symbol: str) -> pd.DataFrame:
    candidates: list[Path] = []
    preferred = [
        out_dir / str(event_type).strip().upper() / "events.parquet",
        out_dir / str(event_type).strip().upper() / "events.csv",
        out_dir
        / str(event_type).strip().upper()
        / f"{str(event_type).strip().lower()}_events.parquet",
        out_dir / str(event_type).strip().upper() / f"{str(event_type).strip().lower()}_events.csv",
    ]
    for path in preferred:
        if path.exists():
            candidates.append(path)
    if not candidates:
        candidates.extend(sorted(out_dir.rglob("*.parquet")))
        candidates.extend(sorted(out_dir.rglob("*.csv")))
    frames: list[pd.DataFrame] = []
    for path in candidates:
        try:
            df = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path)
        except Exception as exc:
            logging.debug(f"Failed to load rerun events from {path}: {exc}")
            continue

        if df.empty:
            continue
        if "event_type" in df.columns:
            mask = df["event_type"].astype(str).str.upper().eq(str(event_type).strip().upper())
            if mask.any():
                df = df.loc[mask].copy()
        if "symbol" in df.columns:
            mask = df["symbol"].astype(str).str.upper().eq(str(symbol).strip().upper())
            if mask.any():
                df = df.loc[mask].copy()
        if any(col in df.columns for col in ["enter_ts", "timestamp", "event_ts", "signal_ts"]):
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    dedup_cols = [
        col
        for col in ["event_type", "symbol", "enter_ts", "timestamp", "event_ts", "signal_ts"]
        if col in out.columns
    ]
    if dedup_cols:
        out = out.drop_duplicates(subset=dedup_cols)
    return out.reset_index(drop=True)


# Known severity columns per event type (in priority order)
_SEVERITY_COLS: dict[str, list[str]] = {
    "VOL_SHOCK": ["rv_shock_magnitude", "shock_severity", "range_pct"],
    "LIQUIDITY_VACUUM": ["depth_drop_pct", "stress_score", "spread_bps"],
    "FORCED_FLOW_EXHAUSTION": ["exhaustion_score", "range_pct"],
    "CROSS_VENUE_DESYNC": ["basis_bps", "desync_magnitude"],
    "FUNDING_EXTREME_ONSET": ["episode_magnitude", "funding_rate_abs"],
    "FUNDING_PERSISTENCE_TRIGGER": ["episode_magnitude", "funding_rate_abs"],
    "FUNDING_NORMALIZATION_TRIGGER": ["episode_magnitude", "funding_rate_abs"],
    "OI_SPIKE_POSITIVE": ["oi_z", "oi_pct_change"],
    "OI_SPIKE_NEGATIVE": ["oi_z", "oi_pct_change"],
    "OI_FLUSH": ["oi_z", "oi_pct_change"],
    "LIQUIDATION_CASCADE": ["liquidation_notional", "liquidation_count"],
}


def _compute_rerun_sensitivity(
    run_id: str,
    event_type: str,
    symbol: str,
    base_events: pd.DataFrame,
    timeframe: str = "5m",
) -> dict[str, Any]:
    """Best-effort true rerun of the mapped detector and comparison to the base event set."""
    import subprocess
    import tempfile

    from project.research.export_edge_candidates import PHASE2_EVENT_CHAIN

    detector_script = None
    detector_args: list[str] = []
    for etype, script, args in PHASE2_EVENT_CHAIN:
        if etype == event_type:
            detector_script = script
            detector_args = list(args or [])
            break

    if not detector_script:
        return {"status": "skipped", "reason": "no_detector_mapping"}

    script_path = PROJECT_ROOT / "research" / detector_script
    if not script_path.exists():
        return {"status": "skipped", "reason": f"missing_detector_script:{detector_script}"}

    severity_cols = _SEVERITY_COLS.get(event_type, [])
    try:
        with tempfile.TemporaryDirectory(prefix=f"rerun_{str(event_type).lower()}_") as tmpdir:
            out_dir = Path(tmpdir) / "out"
            cmd = [
                sys.executable,
                str(script_path),
                "--run_id",
                str(run_id),
                "--symbols",
                str(symbol),
                "--timeframe",
                str(timeframe),
                *detector_args,
                "--out_dir",
                str(out_dir),
            ]
            proc = subprocess.run(
                cmd,
                cwd=str(PROJECT_ROOT.parent),
                capture_output=True,
                text=True,
                timeout=300,
                check=False,
            )
            if proc.returncode != 0:
                return {
                    "status": "rerun_failed",
                    "returncode": int(proc.returncode),
                    "stderr_tail": "\n".join(proc.stderr.splitlines()[-10:]),
                    "stdout_tail": "\n".join(proc.stdout.splitlines()[-10:]),
                }
            rerun_events = _load_rerun_events(out_dir, event_type=event_type, symbol=symbol)
            if rerun_events.empty:
                return {"status": "rerun_failed", "reason": "no_rerun_events_loaded"}
            metrics = _compare_event_runs(base_events, rerun_events, severity_cols=severity_cols)
            return {
                "status": "actual_rerun",
                "detector_script": detector_script,
                **metrics,
            }
    except Exception as exc:
        return {"status": "rerun_failed", "reason": f"{type(exc).__name__}:{exc}"}


def validate_event_quality(
    events_df: pd.DataFrame,
    bars_df: pd.DataFrame,
    event_type: str,
    symbol: str,
    run_id: str,
    timeframe: str = "5m",
    horizons_bars: list[int] | None = None,
    max_join_staleness_minutes: int | None = None,
    run_rerun_sensitivity: bool = False,
) -> dict[str, Any]:
    if horizons_bars is None:
        horizons_bars = list(DEFAULT_EVENT_HORIZON_BARS)

    if events_df.empty:
        return {"pass": False, "reason": "No events detected"}

    # 1. Prevalence
    total_bars = len(bars_df)
    total_events = len(events_df)
    events_per_10k = (total_events / total_bars) * 10000 if total_bars > 0 else 0

    bars_per_day = (
        1440
        if timeframe == "1m"
        else (288 if timeframe == "5m" else (96 if timeframe == "15m" else 24))
    )
    days = total_bars / bars_per_day if bars_per_day > 0 else 1
    events_per_day = total_events / days if days > 0 else 0

    # 2. Clustering (dedup efficacy)
    events_df = events_df.sort_values("enter_idx")
    diffs = events_df["enter_idx"].diff().dropna()
    clustering_5 = float((diffs <= 5).mean()) if not diffs.empty else 0.0

    # 3. Join Rate — real join against features + labels
    features_df = _load_features(run_id, symbol)
    gates = _load_gates_spec().get("gate_e1", {})
    resolved_join_staleness_minutes = (
        int(gates.get("max_join_staleness_minutes", 60))
        if max_join_staleness_minutes is None
        else int(max_join_staleness_minutes)
    )
    resolved_join_staleness_minutes = max(0, int(resolved_join_staleness_minutes))
    join_metrics = _compute_join_rate(
        events_df,
        features_df,
        horizons_bars,
        max_feature_staleness=pd.Timedelta(minutes=resolved_join_staleness_minutes),
    )
    # Primary join rate = feature join rate
    join_rate = join_metrics.get("features", 0.0)

    # 4. Sensitivity Sweep
    severity_cols = _SEVERITY_COLS.get(event_type, ["severity", "magnitude", "score"])
    proxy_sensitivity = _compute_sensitivity(events_df, severity_cols)

    rerun_sensitivity = {}
    if run_rerun_sensitivity:
        rerun_sensitivity = _compute_rerun_sensitivity(
            run_id=run_id,
            event_type=event_type,
            symbol=symbol,
            base_events=events_df,
            timeframe=timeframe,
        )

    # 5. Hard Fail Rules (Gate E-1)
    min_prev = gates.get("min_prevalence_10k", 1.0)
    max_prev = gates.get("max_prevalence_10k", 500.0)
    min_join = gates.get("min_join_rate", 0.99)
    max_clust = gates.get("max_clustering_5b", 0.20)
    max_elasticity = gates.get("max_prevalence_elasticity", 2.0)

    fail_reasons = []
    if not (min_prev <= events_per_10k <= max_prev):
        fail_reasons.append(
            f"PREVALENCE_OUT_OF_BOUNDS ({events_per_10k:.2f} not in [{min_prev}, {max_prev}])"
        )
    if join_rate < min_join:
        fail_reasons.append(f"LOW_JOIN_RATE ({join_rate:.4f} < {min_join})")
    if clustering_5 > max_clust:
        fail_reasons.append(f"EXCESSIVE_CLUSTERING ({clustering_5:.4f} > {max_clust})")

    elasticity = rerun_sensitivity.get(
        "prevalence_elasticity", proxy_sensitivity.get("prevalence_elasticity", float("nan"))
    )
    if not pd.isna(elasticity) and elasticity > max_elasticity:
        fail_reasons.append(f"HIGH_ELASTICITY ({elasticity:.2f} > {max_elasticity})")

    report = {
        "event_type": event_type,
        "symbol": symbol,
        "metrics": {
            "total_events": total_events,
            "events_per_day": float(events_per_day),
            "events_per_10k_bars": float(events_per_10k),
            "clustering_ratio_5b": float(clustering_5),
            "join_rate_features": float(join_metrics.get("features", 0.0)),
            "join_rate": float(join_rate),
            **{k: float(v) for k, v in join_metrics.items() if k != "features"},
        },
        "thresholds": {
            "min_prevalence_10k": min_prev,
            "max_prevalence_10k": max_prev,
            "min_join_rate": min_join,
            "max_join_staleness_minutes": int(resolved_join_staleness_minutes),
            "max_clustering_5b": max_clust,
            "max_prevalence_elasticity": max_elasticity,
        },
        "proxy_sensitivity": proxy_sensitivity,
        "rerun_sensitivity": rerun_sensitivity,
        "gate_e1_pass": len(fail_reasons) == 0,
        "fail_reasons": fail_reasons,
    }
    return report


def main():
    DATA_ROOT = get_data_root()
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--event_type", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument(
        "--max_join_staleness_minutes",
        type=int,
        default=None,
        help="Maximum backward staleness (minutes) allowed for feature join-rate asof matching.",
    )
    parser.add_argument(
        "--horizons_bars",
        default=_default_horizons_bars_csv(),
        help="Forward horizons in bars for label join-rate check (default: derived from timeframe constants)",
    )
    parser.add_argument("--run_rerun_sensitivity", type=int, default=0)
    args = parser.parse_args()

    horizons_bars = [int(x.strip()) for x in args.horizons_bars.split(",") if x.strip()]
    symbols = [s.strip() for s in args.symbols.split(",")]

    reports_root = DATA_ROOT / "reports" / args.event_type / args.run_id
    events_path = reports_root / f"{args.event_type}_events.csv"

    if not events_path.exists():
        logging.error(f"Events file not found: {events_path}")
        sys.exit(1)

    events_df = pd.read_csv(events_path)
    reports = []
    overall_pass = True

    for symbol in symbols:
        bars = _load_bars(args.run_id, symbol, args.timeframe)
        sym_events = (
            events_df[events_df["symbol"] == symbol] if "symbol" in events_df.columns else events_df
        )
        report = validate_event_quality(
            sym_events,
            bars,
            args.event_type,
            symbol,
            args.run_id,
            timeframe=args.timeframe,
            horizons_bars=horizons_bars,
            max_join_staleness_minutes=args.max_join_staleness_minutes,
            run_rerun_sensitivity=bool(args.run_rerun_sensitivity),
        )
        reports.append(report)
        if not report.get("gate_e1_pass", False):
            overall_pass = False

    quality_report_path = reports_root / "event_quality_report.json"
    with open(quality_report_path, "w") as f:
        json.dump(reports, f, indent=2)

    logging.info(f"Event Quality Report written to {quality_report_path}")
    if not overall_pass:
        logging.error("GATE E-1 FAILED for one or more symbols.")
        sys.exit(1)


if __name__ == "__main__":
    main()
