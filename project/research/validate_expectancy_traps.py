from __future__ import annotations
from project.core.config import get_data_root

import argparse
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd

from project.core.feature_schema import feature_dataset_dir_name
from project.specs.manifest import finalize_manifest, start_manifest
from project.io.utils import (
    choose_partition_dir,
    ensure_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
)
from project.core.stats import (
    newey_west_t_stat_for_mean,
    bh_adjust,
)
from project.research.stats.expectancy import (
    distribution_stats,
    circular_block_bootstrap_pvalue,
    oos_diagnostics,
    apply_robust_survivor_gates,
    tail_report,
    capacity_diagnostics,
)
from project.research.gating import one_sided_p_from_t
from project.research.expectancy_traps_support import (
    load_expectancy_payload,
    parse_horizons,
    pick_window_column,
    rolling_percentile,
    stable_row_seed,
    write_empty_robustness_payload,
)
from project.eval import build_walk_forward_split_labels


@dataclass
class CompressionEvent:
    symbol: str
    start_idx: int
    end_idx: int
    end_reason: str
    trend_state: int
    funding_bucket: str
    year: int
    vol_q: str
    bull_bear: str
    enter_ts: pd.Timestamp


EVENT_ROW_COLUMNS = [
    "symbol",
    "event_start_idx",
    "enter_ts",
    "split_label",
    "year",
    "vol_q",
    "bull_bear",
    "funding_bucket",
    "horizon",
    "end_reason",
    "trend_state",
    "breakout_dir",
    "breakout_aligns_htf",
    "time_to_expansion_bars",
    "mfe_post_end",
    "event_return",
    "event_directional_return",
]

ROBUST_GATE_PROFILES: Dict[str, Dict[str, float | int]] = {
    "discovery": {
        "min_samples": 60,
        "tstat_threshold": 1.64,
        "robust_hac_t_threshold": 1.64,
        "robust_bootstrap_alpha": 0.20,
        "robust_fdr_q": 0.20,
        "robust_hac_max_lag": 8,
        "robust_bootstrap_iters": 2000,
        "robust_bootstrap_block_size": 8,
        "robust_bootstrap_seed": 7,
        "oos_min_samples": 20,
        "require_oos_positive": 1,
        "require_oos_sign_consistency": 0,
    },
    "synthetic": {
        "min_samples": 8,
        "tstat_threshold": 0.5,
        "robust_hac_t_threshold": 0.5,
        "robust_bootstrap_alpha": 0.40,
        "robust_fdr_q": 0.40,
        "robust_hac_max_lag": 4,
        "robust_bootstrap_iters": 500,
        "robust_bootstrap_block_size": 4,
        "robust_bootstrap_seed": 7,
        "oos_min_samples": 4,
        "require_oos_positive": 0,
        "require_oos_sign_consistency": 0,
    },
    "promotion": {
        "min_samples": 100,
        "tstat_threshold": 2.0,
        "robust_hac_t_threshold": 1.96,
        "robust_bootstrap_alpha": 0.10,
        "robust_fdr_q": 0.10,
        "robust_hac_max_lag": 12,
        "robust_bootstrap_iters": 2000,
        "robust_bootstrap_block_size": 8,
        "robust_bootstrap_seed": 7,
        "oos_min_samples": 40,
        "require_oos_positive": 1,
        "require_oos_sign_consistency": 1,
    },
}


def _apply_gate_profile_defaults(args: argparse.Namespace) -> argparse.Namespace:
    profile = str(getattr(args, "gate_profile", "custom")).strip().lower()
    if profile == "custom":
        return args
    overrides = ROBUST_GATE_PROFILES.get(profile)
    if not overrides:
        raise ValueError(f"Unknown gate profile: {profile}")
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


def _newey_west_t_stat(series: pd.Series, max_lag: int) -> Tuple[float, float, int]:
    """Compute HAC t-stat and one-sided p-value for a directional series.

    E-MISC-002: the previous formula used a two-sided normal approximation
    (2*(1 - Phi(|t|))) which inflated p-values for directional hypotheses.
    All callers here test a directional edge (compression return > 0), so the
    correct form is the one-sided right-tail p-value consistent with the rest
    of the gate chain.  See also _robust_row_fields() which already uses
    one_sided_p_from_t for the primary robust path.
    """
    result = newey_west_t_stat_for_mean(series.to_numpy(), max_lag=max_lag)
    t_stat = float(result.t_stat)
    if not np.isfinite(t_stat):
        return 0.0, 1.0, int(result.lag)
    p_value = one_sided_p_from_t(t_stat, df=max(int(result.n) - 1, 1))
    return t_stat, p_value, int(result.lag)



def _circular_block_bootstrap_pvalue(
    series: pd.Series,
    *,
    block_size: int,
    n_boot: int,
    seed: int,
) -> float:
    return float(
        circular_block_bootstrap_pvalue(
            series,
            block_size=int(block_size),
            n_boot=int(n_boot),
            seed=int(seed),
        )
    )


def _apply_robust_survivor_gates(
    df: pd.DataFrame,
    **kwargs,
) -> pd.DataFrame:
    return apply_robust_survivor_gates(df, **kwargs)


def _robust_row_fields(
    *,
    event_frame: pd.DataFrame,
    ret_col: str,
    condition: str,
    horizon: int,
    hac_max_lag: int,
    bootstrap_block_size: int,
    bootstrap_iters: int,
    bootstrap_seed: int,
    oos_min_samples: int,
    require_oos_positive: int,
    require_oos_sign_consistency: int,
) -> Dict[str, object]:
    series = (
        pd.to_numeric(event_frame[ret_col], errors="coerce")
        if ret_col in event_frame.columns
        else pd.Series(dtype=float)
    )
    hac_res = newey_west_t_stat_for_mean(series.to_numpy(), max_lag=hac_max_lag)

    hac_p = one_sided_p_from_t(hac_res.t_stat, df=max(hac_res.n - 1, 1))

    boot_seed = stable_row_seed(condition=condition, horizon=horizon, base_seed=bootstrap_seed)
    bootstrap_p = circular_block_bootstrap_pvalue(
        series,
        block_size=int(bootstrap_block_size),
        n_boot=int(bootstrap_iters),
        seed=boot_seed,
    )
    oos = oos_diagnostics(
        event_frame,
        ret_col=ret_col,
        oos_min_samples=int(oos_min_samples),
        require_oos_positive=int(require_oos_positive),
        require_oos_sign_consistency=int(require_oos_sign_consistency),
    )
    return {
        "hac_t": float(hac_res.t_stat),
        "hac_p": float(hac_p),
        "hac_used_lag": int(hac_res.lag),
        "bootstrap_p": float(bootstrap_p),
        **oos,
    }


def _load_symbol_features(symbol: str, run_id: str) -> pd.DataFrame:
    DATA_ROOT = get_data_root()
    feature_dataset = feature_dataset_dir_name()
    candidates = [
        run_scoped_lake_path(DATA_ROOT, run_id, "features", "perp", symbol, "5m", feature_dataset),
        DATA_ROOT / "lake" / "features" / "perp" / symbol / "5m" / feature_dataset,
    ]
    features_dir = choose_partition_dir(candidates)
    files = list_parquet_files(features_dir) if features_dir else []
    if not files:
        raise ValueError(f"No features found for {symbol}: {candidates[0]}")
    df = read_parquet(files)
    if df.empty:
        raise ValueError(f"Empty features for {symbol}")
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df.sort_values("timestamp").reset_index(drop=True)


def _build_features(
    df: pd.DataFrame, htf_window: int, htf_lookback: int, funding_pct_window: int
) -> pd.DataFrame:
    rv_pct_col = pick_window_column(df.columns, "rv_pct_")
    range_med_col = pick_window_column(df.columns, "range_med_")

    close = df["close"].astype(float)
    htf_ma = close.rolling(window=htf_window, min_periods=htf_window).mean()
    htf_delta = htf_ma - htf_ma.shift(htf_lookback)
    trend_state = pd.Series(
        np.where(htf_delta > 0, 1, np.where(htf_delta < 0, -1, 0)), index=df.index
    )

    funding_pct = rolling_percentile(df["funding_rate_scaled"].astype(float), funding_pct_window)
    funding_bucket = pd.Series(
        np.select(
            [funding_pct <= 20, funding_pct >= 80],
            ["low", "high"],
            default="mid",
        ),
        index=df.index,
    ).where(funding_pct.notna())

    compression = ((df[rv_pct_col] <= 10.0) & (df["range_96"] <= 0.8 * df[range_med_col])).fillna(
        False
    )

    out = df.copy()
    out["trend_state"] = trend_state
    out["funding_bucket"] = funding_bucket
    out["compression"] = compression
    out["prior_high_96"] = out["high_96"].shift(1)
    out["prior_low_96"] = out["low_96"].shift(1)
    out["breakout_up"] = out["close"] > out["prior_high_96"]
    out["breakout_down"] = out["close"] < out["prior_low_96"]
    out["breakout_any"] = out["breakout_up"] | out["breakout_down"]
    out["vol_q"] = pd.qcut(out["rv_96"], q=4, labels=["Q1", "Q2", "Q3", "Q4"], duplicates="drop")
    out["bull_bear"] = np.where(close / close.shift(96) - 1.0 >= 0, "bull", "bear")
    return out


def _leakage_check(df: pd.DataFrame, htf_window: int, htf_lookback: int) -> Dict[str, object]:
    close = df["close"].astype(float)
    full_ma = close.rolling(window=htf_window, min_periods=htf_window).mean()
    full_delta = full_ma - full_ma.shift(htf_lookback)
    full_trend = pd.Series(
        np.where(full_delta > 0, 1, np.where(full_delta < 0, -1, 0)), index=df.index
    )

    rng = np.random.default_rng(7)
    candidates = np.arange(htf_window + htf_lookback, len(df))
    if len(candidates) == 0:
        return {"pass": False, "checked": 0, "mismatches": 0}
    sample = rng.choice(candidates, size=min(500, len(candidates)), replace=False)

    mismatches = 0
    for i in sample:
        partial = close.iloc[: i + 1]
        ma = partial.rolling(window=htf_window, min_periods=htf_window).mean()
        delta = ma - ma.shift(htf_lookback)
        trend_i = int(np.sign(delta.iloc[-1])) if pd.notna(delta.iloc[-1]) else 0
        if trend_i != int(full_trend.iloc[i]):
            mismatches += 1
    return {"pass": mismatches == 0, "checked": int(len(sample)), "mismatches": int(mismatches)}


def _extract_compression_events(
    df: pd.DataFrame, symbol: str, max_duration: int
) -> List[CompressionEvent]:
    events: List[CompressionEvent] = []
    n = len(df)
    i = 1
    while i < n:
        if not bool(df.at[i, "compression"]) or bool(df.at[i - 1, "compression"]):
            i += 1
            continue

        start = i
        max_end = min(n - 1, start + max_duration - 1)
        end = start
        end_reason = "max_duration"

        j = start
        while j <= max_end:
            if bool(df.at[j, "breakout_any"]):
                end = j
                end_reason = "breakout"
                break
            if not bool(df.at[j, "compression"]):
                end = j
                end_reason = "compression_off"
                break
            end = j
            j += 1

        ts = df.at[start, "timestamp"]
        vol_q = df.at[start, "vol_q"]
        events.append(
            CompressionEvent(
                symbol=symbol,
                start_idx=start,
                end_idx=end,
                end_reason=end_reason,
                trend_state=int(df.at[start, "trend_state"])
                if pd.notna(df.at[start, "trend_state"])
                else 0,
                funding_bucket=str(df.at[start, "funding_bucket"])
                if pd.notna(df.at[start, "funding_bucket"])
                else "na",
                year=int(ts.year),
                vol_q=str(vol_q) if pd.notna(vol_q) else "na",
                bull_bear=str(df.at[start, "bull_bear"]),
                enter_ts=pd.to_datetime(ts, utc=True),
            )
        )
        i = end + 1
    return events


def _first_expansion_after(df: pd.DataFrame, idx: int, lookahead: int) -> Tuple[int | None, int]:
    n = len(df)
    end = min(n - 1, idx + lookahead)
    for j in range(idx + 1, end + 1):
        if bool(df.at[j, "breakout_up"]):
            return j, 1
        if bool(df.at[j, "breakout_down"]):
            return j, -1
    return None, 0


def _event_rows(
    df: pd.DataFrame,
    events: List[CompressionEvent],
    horizons: List[int],
    expansion_lookahead: int,
    mfe_horizon: int,
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    close = df["close"].to_numpy(dtype=float)
    high = df["high"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)
    n = len(df)

    for ev in events:
        expansion_idx = ev.end_idx if bool(df.at[ev.end_idx, "breakout_any"]) else None
        breakout_dir = (
            1
            if bool(df.at[ev.end_idx, "breakout_up"])
            else -1
            if bool(df.at[ev.end_idx, "breakout_down"])
            else 0
        )
        if expansion_idx is None:
            expansion_idx, breakout_dir = _first_expansion_after(
                df, ev.end_idx, expansion_lookahead
            )

        time_to_expansion = (expansion_idx - ev.start_idx) if expansion_idx is not None else np.nan
        aligns = (
            bool(breakout_dir == ev.trend_state)
            if breakout_dir != 0 and ev.trend_state != 0
            else np.nan
        )

        mfe = np.nan
        mfe_end = min(n - 1, ev.end_idx + mfe_horizon)
        if breakout_dir != 0 and ev.end_idx + 1 <= mfe_end:
            entry = close[ev.end_idx]
            if breakout_dir > 0:
                mfe = float(np.nanmax(high[ev.end_idx + 1 : mfe_end + 1]) / entry - 1.0)
            else:
                mfe = float(entry / np.nanmin(low[ev.end_idx + 1 : mfe_end + 1]) - 1.0)

        for h in horizons:
            if ev.end_idx + h >= n:
                continue
            ret = float(close[ev.end_idx + h] / close[ev.end_idx] - 1.0)
            directional_ret = float(ret * ev.trend_state) if ev.trend_state != 0 else np.nan
            rows.append(
                {
                    "symbol": ev.symbol,
                    "event_start_idx": ev.start_idx,
                    "enter_ts": ev.enter_ts,
                    "split_label": "",
                    "year": ev.year,
                    "vol_q": ev.vol_q,
                    "bull_bear": ev.bull_bear,
                    "funding_bucket": ev.funding_bucket,
                    "horizon": h,
                    "end_reason": ev.end_reason,
                    "trend_state": ev.trend_state,
                    "breakout_dir": breakout_dir,
                    "breakout_aligns_htf": aligns,
                    "time_to_expansion_bars": time_to_expansion,
                    "mfe_post_end": mfe,
                    "event_return": ret,
                    "event_directional_return": directional_ret,
                }
            )
    return rows


def _split_sign_report(events: pd.DataFrame, col: str, ret_col: str) -> Dict[str, object]:
    if events.empty:
        return {"stable_sign": False, "groups": {}}
    grouped = events.groupby(col, dropna=False)[ret_col].mean().dropna()
    groups = {str(k): float(v) for k, v in grouped.items()}
    if grouped.empty:
        return {"stable_sign": False, "groups": groups}
    positive = grouped > 0
    stable_sign = bool(positive.all() or (~positive).all())
    return {"stable_sign": stable_sign, "groups": groups}


def _bar_condition_stats(df: pd.DataFrame, condition: str, horizon: int) -> Dict[str, float]:
    close = df["close"].astype(float)
    fwd = close.shift(-horizon) / close - 1.0

    if condition == "compression":
        mask = df["compression"]
        ret = fwd.where(mask)
    elif condition == "compression_plus_htf_trend":
        mask = df["compression"] & (df["trend_state"] != 0)
        ret = (fwd * df["trend_state"]).where(mask)
    elif condition == "compression_plus_funding_low":
        mask = df["compression"] & (df["funding_bucket"] == "low")
        ret = fwd.where(mask)
    else:
        raise ValueError(f"Unknown condition: {condition}")

    return distribution_stats(ret)


def _event_condition_frame(
    events_df: pd.DataFrame, condition: str, horizon: int
) -> Tuple[pd.DataFrame, str]:
    ret_col = (
        "event_directional_return" if condition == "compression_plus_htf_trend" else "event_return"
    )

    if events_df.empty or "horizon" not in events_df.columns:
        return pd.DataFrame(columns=EVENT_ROW_COLUMNS), ret_col

    frame = events_df[events_df["horizon"] == horizon].copy()
    if condition == "compression":
        pass
    elif condition == "compression_plus_htf_trend":
        frame = frame[frame["trend_state"] != 0]
    elif condition == "compression_plus_funding_low":
        frame = frame[frame["funding_bucket"] == "low"]
    else:
        raise ValueError(f"Unknown condition: {condition}")

    return frame, ret_col


def _split_overlap_diagnostics(events_df: pd.DataFrame, embargo_bars: int) -> Dict[str, object]:
    if events_df.empty:
        return {"pass": False, "embargo_bars": int(embargo_bars), "details": []}

    unique_events = events_df.drop_duplicates(subset=["symbol", "event_start_idx"]).copy()
    details: List[Dict[str, object]] = []
    global_pass = True

    for symbol, group in unique_events.groupby("symbol", dropna=False):
        g = group.sort_values("event_start_idx").reset_index(drop=True)
        boundary_gaps: Dict[str, int] = {}
        for left, right in [("train", "validation"), ("validation", "test")]:
            left_idx = g.index[g["split_label"] == left]
            right_idx = g.index[g["split_label"] == right]
            if len(left_idx) == 0 or len(right_idx) == 0:
                boundary_gaps[f"{left}_to_{right}"] = -1
                global_pass = False
                continue
            gap = int(right_idx.min() - left_idx.max() - 1)
            boundary_gaps[f"{left}_to_{right}"] = gap
            if gap < int(embargo_bars):
                global_pass = False

        details.append({"symbol": str(symbol), "boundary_gaps": boundary_gaps})

    return {"pass": bool(global_pass), "embargo_bars": int(embargo_bars), "details": details}


def _parameter_stability_diagnostics(
    trap_df: pd.DataFrame,
    *,
    base_min_samples: int,
    base_tstat_threshold: float,
    sample_delta: int,
    tstat_delta: float,
) -> Dict[str, object]:
    if trap_df.empty:
        return {
            "pass": False,
            "rank_consistency": 0.0,
            "performance_decay": 1.0,
            "neighborhood_supported": False,
            "scenarios": [],
        }

    scenarios = [
        {
            "name": "base",
            "min_samples": int(base_min_samples),
            "tstat": float(base_tstat_threshold),
        },
        {
            "name": "tight",
            "min_samples": int(base_min_samples + sample_delta),
            "tstat": float(base_tstat_threshold + tstat_delta),
        },
        {
            "name": "loose",
            "min_samples": max(1, int(base_min_samples - sample_delta)),
            "tstat": max(0.0, float(base_tstat_threshold - tstat_delta)),
        },
    ]

    def _survivor_frame(min_samples: int, tstat: float) -> pd.DataFrame:
        sub = trap_df[
            (trap_df["event_samples"] >= min_samples)
            & (trap_df["event_mean"] > 0)
            & (trap_df["event_t"] >= tstat)
        ]
        return sub.copy()

    def _survivor_set(sub: pd.DataFrame) -> set[str]:
        return {f"{r.condition}|{int(r.horizon)}" for r in sub.itertuples(index=False)}

    base_sub = _survivor_frame(int(base_min_samples), float(base_tstat_threshold))
    base_set = _survivor_set(base_sub)
    rows = []
    overlap_scores = []
    scenario_perf: Dict[str, float] = {}
    for sc in scenarios:
        sub = _survivor_frame(int(sc["min_samples"]), float(sc["tstat"]))
        sset = _survivor_set(sub)
        denom = max(1, len(base_set | sset))
        jaccard = float(len(base_set & sset) / denom)
        overlap_scores.append(jaccard)
        mean_perf = float(sub["event_mean"].mean()) if not sub.empty else np.nan
        scenario_perf[str(sc["name"])] = mean_perf
        rows.append(
            {
                **sc,
                "survivors": len(sset),
                "jaccard_to_base": jaccard,
                "mean_event_return": (None if np.isnan(mean_perf) else mean_perf),
            }
        )

    rank_consistency = float(np.mean(overlap_scores)) if overlap_scores else 0.0
    base_perf = scenario_perf.get("base", np.nan)
    valid_perf = [v for v in scenario_perf.values() if np.isfinite(v)]
    if np.isfinite(base_perf) and base_perf > 0.0 and valid_perf:
        worst_perf = float(min(valid_perf))
        performance_decay = float(max(0.0, (base_perf - worst_perf) / max(abs(base_perf), 1e-9)))
    else:
        performance_decay = 1.0

    neighborhood_supported = any(
        (row.get("name") != "base") and (int(row.get("survivors", 0)) > 0) for row in rows
    )
    passed = bool(
        len(base_set) > 0
        and neighborhood_supported
        and rank_consistency >= 0.3
        and performance_decay <= 1.0
    )
    return {
        "pass": passed,
        "rank_consistency": rank_consistency,
        "performance_decay": performance_decay,
        "neighborhood_supported": bool(neighborhood_supported),
        "scenarios": rows,
    }


def main(argv: List[str] | None = None) -> int:
    DATA_ROOT = get_data_root()
    parser = argparse.ArgumentParser(description="Validate conditional expectancy.")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--horizons", default="4,16,96")
    parser.add_argument("--htf_window", type=int, default=384)
    parser.add_argument("--htf_lookback", type=int, default=96)
    parser.add_argument("--funding_pct_window", type=int, default=2880)
    parser.add_argument("--max_event_duration", type=int, default=96)
    parser.add_argument("--expansion_lookahead", type=int, default=192)
    parser.add_argument("--mfe_horizon", type=int, default=96)
    parser.add_argument(
        "--gate_profile",
        choices=["discovery", "promotion", "custom", "synthetic"],
        default="discovery",
    )
    parser.add_argument("--retail_profile", default="capital_constrained")
    parser.add_argument("--tstat_threshold", type=float, default=2.0)
    parser.add_argument("--min_samples", type=int, default=100)
    parser.add_argument("--robust_hac_t_threshold", type=float, default=1.96)
    parser.add_argument("--robust_bootstrap_alpha", type=float, default=0.10)
    parser.add_argument("--robust_fdr_q", type=float, default=0.10)
    parser.add_argument("--robust_hac_max_lag", type=int, default=12)
    parser.add_argument("--robust_bootstrap_iters", type=int, default=800)
    parser.add_argument("--robust_bootstrap_block_size", type=int, default=8)
    parser.add_argument("--robust_bootstrap_seed", type=int, default=7)
    parser.add_argument("--oos_min_samples", type=int, default=40)
    parser.add_argument("--require_oos_positive", type=int, default=1)
    parser.add_argument("--require_oos_sign_consistency", type=int, default=1)
    parser.add_argument("--embargo_bars", type=int, default=0)
    parser.add_argument("--stability_sample_delta", type=int, default=20)
    parser.add_argument("--stability_tstat_delta", type=float, default=0.5)
    parser.add_argument("--capacity_min_events_per_day", type=float, default=0.5)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args(argv)
    args = _apply_gate_profile_defaults(args)

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    horizons = parse_horizons(args.horizons)

    out_dir = (
        Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "expectancy" / args.run_id
    )
    ensure_dir(out_dir)
    outputs = [{"path": str(out_dir / "conditional_expectancy_robustness.json")}]
    if args.log_path:
        outputs.append({"path": str(args.log_path)})
    manifest = start_manifest("validate_expectancy_traps", args.run_id, vars(args), [], outputs)

    try:
        expectancy_payload = load_expectancy_payload(out_dir / "conditional_expectancy.json")
        if expectancy_payload and not bool(expectancy_payload.get("expectancy_exists", False)):
            rc = write_empty_robustness_payload(
                out_dir=out_dir,
                run_id=args.run_id,
                symbols=symbols,
                horizons=horizons,
                skip_reason="expectancy_analysis_reported_no_evidence",
            )
            finalize_manifest(
                manifest,
                "success",
                stats={
                    "skipped": True,
                    "skip_reason": "expectancy_analysis_reported_no_evidence",
                    "survivor_count": 0,
                },
            )
            return rc

        leakage = {}
        all_bar_df = []
        all_event_rows: List[Dict[str, object]] = []
        event_summary_rows: List[Dict[str, object]] = []

        for symbol in symbols:
            df = _load_symbol_features(symbol, run_id=args.run_id)
            df = _build_features(df, args.htf_window, args.htf_lookback, args.funding_pct_window)
            leakage[symbol] = _leakage_check(df, args.htf_window, args.htf_lookback)
            events = _extract_compression_events(
                df, symbol=symbol, max_duration=args.max_event_duration
            )
            rows = _event_rows(
                df,
                events,
                horizons=horizons,
                expansion_lookahead=args.expansion_lookahead,
                mfe_horizon=args.mfe_horizon,
            )
            all_event_rows.extend(rows)
            all_bar_df.append(df)

            breakout_count = sum(1 for e in events if e.end_reason == "breakout")
            event_summary_rows.append(
                {
                    "symbol": symbol,
                    "event_count": len(events),
                    "breakout_end_count": breakout_count,
                    "breakout_end_rate": float(breakout_count / len(events)) if events else 0.0,
                }
            )

        master_bars = pd.concat(all_bar_df, ignore_index=True)
        events_df = pd.DataFrame(all_event_rows, columns=EVENT_ROW_COLUMNS)
        if not events_df.empty:
            events_df["enter_ts"] = pd.to_datetime(events_df["enter_ts"], utc=True, errors="coerce")
            events_df["split_label"] = build_walk_forward_split_labels(events_df, time_col="enter_ts")

        split_overlap = _split_overlap_diagnostics(events_df, embargo_bars=args.embargo_bars)

        conditions = ["compression", "compression_plus_htf_trend", "compression_plus_funding_low"]
        trap_rows = []
        split_rows = []
        tail_rows = []
        symmetry_rows = []
        expansion_rows = []

        rng = np.random.default_rng(11)

        for condition in conditions:
            for horizon in horizons:
                bar_stats = _bar_condition_stats(master_bars, condition, horizon)
                event_frame, ret_col = _event_condition_frame(events_df, condition, horizon)
                event_series = (
                    event_frame[ret_col] if ret_col in event_frame else pd.Series(dtype=float)
                )
                event_stats = distribution_stats(event_series)
                robust_fields = _robust_row_fields(
                    event_frame=event_frame,
                    ret_col=ret_col,
                    condition=condition,
                    horizon=int(horizon),
                    hac_max_lag=int(args.robust_hac_max_lag),
                    bootstrap_block_size=int(args.robust_bootstrap_block_size),
                    bootstrap_iters=int(args.robust_bootstrap_iters),
                    bootstrap_seed=int(args.robust_bootstrap_seed),
                    oos_min_samples=int(args.oos_min_samples),
                    require_oos_positive=int(args.require_oos_positive),
                    require_oos_sign_consistency=int(args.require_oos_sign_consistency),
                )

                trap_rows.append(
                    {
                        "condition": condition,
                        "horizon": horizon,
                        "bar_samples": bar_stats["samples"],
                        "bar_mean": bar_stats["mean_return"],
                        "bar_t": bar_stats["t_stat"],
                        "event_samples": event_stats["samples"],
                        "event_mean": event_stats["mean_return"],
                        "event_t": event_stats["t_stat"],
                        **robust_fields,
                    }
                )

                year_split = _split_sign_report(event_frame, "year", ret_col)
                vol_split = _split_sign_report(event_frame, "vol_q", ret_col)
                bull_split = _split_sign_report(event_frame, "bull_bear", ret_col)

                split_rows.append(
                    {
                        "condition": condition,
                        "horizon": horizon,
                        "year_stable_sign": year_split["stable_sign"],
                        "vol_q_stable_sign": vol_split["stable_sign"],
                        "bull_bear_stable_sign": bull_split["stable_sign"],
                        "year_means": year_split["groups"],
                        "vol_q_means": vol_split["groups"],
                        "bull_bear_means": bull_split["groups"],
                    }
                )

                tail = tail_report(
                    event_frame[ret_col] if ret_col in event_frame else pd.Series(dtype=float)
                )
                tail_rows.append(
                    {
                        "condition": condition,
                        "horizon": horizon,
                        "mean": event_stats["mean_return"],
                        "median": tail["median"],
                        "p25": tail["p25"],
                        "p75": tail["p75"],
                        "top_1pct_contribution": tail["top_1pct_contribution"],
                        "top_5pct_contribution": tail["top_5pct_contribution"],
                    }
                )

                if condition == "compression_plus_htf_trend":
                    base = event_frame[ret_col].dropna()
                    opp = -base
                    rand_sign = pd.Series(rng.choice([-1.0, 1.0], size=len(base)), index=base.index)
                    rnd = base.abs() * rand_sign
                    symmetry_rows.append(
                        {
                            "condition": condition,
                            "horizon": horizon,
                            "base_mean": float(base.mean()) if len(base) else 0.0,
                            "base_t": distribution_stats(base)["t_stat"],
                            "opposite_mean": float(opp.mean()) if len(opp) else 0.0,
                            "opposite_t": distribution_stats(opp)["t_stat"],
                            "random_mean": float(rnd.mean()) if len(rnd) else 0.0,
                            "random_t": distribution_stats(rnd)["t_stat"],
                        }
                    )

            cond_all, _ = _event_condition_frame(events_df, condition, horizons[0])
            cond_all = (
                cond_all.drop_duplicates(
                    subset=[
                        "symbol",
                        "year",
                        "vol_q",
                        "bull_bear",
                        "time_to_expansion_bars",
                        "mfe_post_end",
                        "trend_state",
                        "funding_bucket",
                        "end_reason",
                        "breakout_dir",
                    ]
                )
                if not cond_all.empty
                else cond_all
            )
            expansion_rows.append(
                {
                    "condition": condition,
                    "events": int(len(cond_all)),
                    "time_to_expansion_median": float(cond_all["time_to_expansion_bars"].median())
                    if not cond_all.empty
                    else np.nan,
                    "time_to_expansion_p25": float(cond_all["time_to_expansion_bars"].quantile(0.25))
                    if not cond_all.empty
                    else np.nan,
                    "time_to_expansion_p75": float(cond_all["time_to_expansion_bars"].quantile(0.75))
                    if not cond_all.empty
                    else np.nan,
                    "mfe_median": float(cond_all["mfe_post_end"].median())
                    if not cond_all.empty
                    else np.nan,
                    "mfe_mean": float(cond_all["mfe_post_end"].mean())
                    if not cond_all.empty
                    else np.nan,
                    "breakout_align_rate": float(cond_all["breakout_aligns_htf"].dropna().mean())
                    if not cond_all.empty
                    else np.nan,
                }
            )

        trap_df = pd.DataFrame(trap_rows)
        trap_df = apply_robust_survivor_gates(
            trap_df,
            min_samples=int(args.min_samples),
            legacy_tstat_threshold=float(args.tstat_threshold),
            robust_hac_t_threshold=float(args.robust_hac_t_threshold),
            bootstrap_alpha=float(args.robust_bootstrap_alpha),
            fdr_q=float(args.robust_fdr_q),
            oos_min_samples=int(args.oos_min_samples),
            require_oos_positive=int(args.require_oos_positive),
            require_oos_sign_consistency=int(args.require_oos_sign_consistency),
        )

        stability = _parameter_stability_diagnostics(
            trap_df,
            base_min_samples=args.min_samples,
            base_tstat_threshold=args.tstat_threshold,
            sample_delta=args.stability_sample_delta,
            tstat_delta=args.stability_tstat_delta,
        )
        capacity = capacity_diagnostics(
            events_df, symbols=symbols, min_events_per_day=args.capacity_min_events_per_day
        )

        payload = {
            "run_id": args.run_id,
            "symbols": symbols,
            "horizons": horizons,
            "stability_diagnostics": stability,
            "capacity_diagnostics": capacity,
            "survivors": trap_df[trap_df["gate_robust_survivor"]].to_dict(orient="records"),
        }

        json_path = out_dir / "conditional_expectancy_robustness.json"
        json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

        print(f"Wrote {json_path}")
        finalize_manifest(
            manifest,
            "success",
            stats={
                "skipped": False,
                "split_overlap_rows": int(len(split_overlap)),
                "event_rows": int(len(events_df)),
                "trap_rows": int(len(trap_df)),
                "survivor_count": int(len(payload["survivors"])),
            },
        )
        return 0
    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        raise


if __name__ == "__main__":
    raise SystemExit(main())
