from __future__ import annotations

import numpy as np
import pandas as pd

from project.core.stats import (
    bh_adjust,
    newey_west_t_stat_for_mean,
)


def distribution_stats(returns: pd.Series) -> dict[str, float]:
    clean = returns.dropna().astype(float)
    n = len(clean)
    if n == 0:
        return {
            "samples": 0,
            "mean_return": 0.0,
            "median_return": 0.0,
            "std_return": 0.0,
            "win_rate": 0.0,
            "p25": 0.0,
            "p75": 0.0,
            "t_stat": 0.0,
        }
    mean_val = float(clean.mean())
    median_val = float(clean.median())
    std_val = float(clean.std())
    # Use Newey-West t-stat to account for autocorrelation
    nw_res = newey_west_t_stat_for_mean(clean)
    t_stat = float(nw_res.t_stat) if np.isfinite(nw_res.t_stat) else 0.0
    return {
        "samples": n,
        "mean_return": mean_val,
        "median_return": median_val,
        "std_return": std_val,
        "win_rate": float((clean > 0).mean()),
        "p25": float(clean.quantile(0.25)),
        "p75": float(clean.quantile(0.75)),
        "t_stat": t_stat,
    }


def circular_block_bootstrap_pvalue(
    values: pd.Series, *, block_size: int, n_boot: int, seed: int
) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna().astype(float).to_numpy()
    n = len(clean)
    if n < 2 or n_boot <= 0:
        return 1.0
    observed = float(clean.mean())
    if not np.isfinite(observed):
        return 1.0
    centered = clean - observed
    block = int(max(1, min(block_size, n)))
    blocks_per_draw = int(np.ceil(n / block))
    rng = np.random.default_rng(int(seed))
    exceed = 0
    for _ in range(int(n_boot)):
        starts = rng.integers(0, n, size=blocks_per_draw)
        sample = np.empty(blocks_per_draw * block, dtype=float)
        pos = 0
        for s in starts:
            idx = (int(s) + np.arange(block)) % n
            sample[pos : pos + block] = centered[idx]
            pos += block
        draw_mean = float(sample[:n].mean())
        if abs(draw_mean) >= abs(observed):
            exceed += 1
    return float((exceed + 1) / (int(n_boot) + 1))


def oos_diagnostics(
    event_frame: pd.DataFrame,
    *,
    ret_col: str,
    oos_min_samples: int,
    require_oos_positive: int,
    require_oos_sign_consistency: int,
) -> dict[str, object]:
    if event_frame.empty or ret_col not in event_frame.columns:
        return {
            "oos_samples": 0,
            "train_mean": np.nan,
            "validation_mean": np.nan,
            "test_mean": np.nan,
            "oos_mean": np.nan,
            "oos_positive": False,
            "oos_sign_consistent": False,
            "oos_pass": False,
        }
    values = pd.to_numeric(event_frame[ret_col], errors="coerce")
    split = (
        event_frame["split_label"].astype(str)
        if "split_label" in event_frame.columns
        else pd.Series("", index=event_frame.index, dtype="object")
    )
    train_vals = values[split == "train"].dropna()
    val_vals = values[split == "validation"].dropna()
    test_vals = values[split == "test"].dropna()
    oos_vals = pd.concat([val_vals, test_vals], ignore_index=True)
    train_mean = float(train_vals.mean()) if not train_vals.empty else np.nan
    val_mean = float(val_vals.mean()) if not val_vals.empty else np.nan
    test_mean = float(test_vals.mean()) if not test_vals.empty else np.nan
    oos_mean = float(oos_vals.mean()) if not oos_vals.empty else np.nan
    oos_positive = bool(np.isfinite(oos_mean) and oos_mean > 0.0)
    oos_sign_consistent = bool(
        np.isfinite(train_mean)
        and np.isfinite(oos_mean)
        and train_mean != 0.0
        and oos_mean != 0.0
        and np.sign(train_mean) == np.sign(oos_mean)
    )
    oos_pass = bool(
        len(oos_vals) >= int(oos_min_samples)
        and (not int(require_oos_positive) or oos_positive)
        and (not int(require_oos_sign_consistency) or oos_sign_consistent)
    )
    return {
        "oos_samples": len(oos_vals),
        "train_mean": train_mean,
        "validation_mean": val_mean,
        "test_mean": test_mean,
        "oos_mean": oos_mean,
        "oos_positive": oos_positive,
        "oos_sign_consistent": oos_sign_consistent,
        "oos_pass": oos_pass,
    }


def apply_robust_survivor_gates(
    trap_df: pd.DataFrame,
    *,
    min_samples: int,
    legacy_tstat_threshold: float,
    robust_hac_t_threshold: float,
    bootstrap_alpha: float,
    fdr_q: float,
    oos_min_samples: int,
    require_oos_positive: int,
    require_oos_sign_consistency: int,
) -> pd.DataFrame:
    out = trap_df.copy()
    if out.empty:
        return out
    for col, default in (
        ("event_samples", 0.0),
        ("event_mean", 0.0),
        ("event_t", 0.0),
        ("hac_t", 0.0),
        ("hac_p", 1.0),
        ("bootstrap_p", 1.0),
        ("oos_samples", 0.0),
        ("oos_mean", np.nan),
    ):
        if col not in out.columns:
            out[col] = default
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(default)

    if "oos_sign_consistent" not in out.columns:
        out["oos_sign_consistent"] = False
    out["oos_sign_consistent"] = out["oos_sign_consistent"].astype(bool)
    if "oos_positive" not in out.columns:
        out["oos_positive"] = out["oos_mean"] > 0.0
    out["oos_positive"] = out["oos_positive"].astype(bool)
    if "oos_pass" not in out.columns:
        out["oos_pass"] = False
    out["oos_pass"] = out["oos_pass"].astype(bool)

    # HAC p-value is the primary p-value for FDR control.
    # Bootstrap remains a separate required gate because the two tests share the same
    # return series and do not satisfy Fisher-style independence assumptions.
    out["composite_p_value"] = out["hac_p"]

    out["composite_p_value"] = out["composite_p_value"].clip(lower=0.0, upper=1.0)
    out["fdr_q_value"] = bh_adjust(out["composite_p_value"]).astype(float)

    out["gate_legacy_survivor"] = (
        (out["event_samples"] >= int(min_samples))
        & (out["event_mean"] > 0.0)
        & (out["event_t"] >= float(legacy_tstat_threshold))
    )

    out["gate_robust_survivor"] = (
        (out["event_samples"] >= int(min_samples))
        & (out["event_mean"] > 0.0)
        & (out["hac_t"] >= float(robust_hac_t_threshold))
        & (out["bootstrap_p"] <= float(bootstrap_alpha))
        & (out["fdr_q_value"] <= float(fdr_q))
        & (out["oos_samples"] >= int(oos_min_samples))
        & ((not int(require_oos_positive)) | out["oos_positive"])
        & ((not int(require_oos_sign_consistency)) | out["oos_sign_consistent"])
    )
    out["gate_oos"] = (
        (out["oos_samples"] >= int(oos_min_samples))
        & ((not int(require_oos_positive)) | out["oos_positive"])
        & ((not int(require_oos_sign_consistency)) | out["oos_sign_consistent"])
    )
    return out


def tail_report(returns: pd.Series) -> dict[str, float]:
    clean = returns.dropna().astype(float)
    if clean.empty:
        return {
            "median": 0.0,
            "p25": 0.0,
            "p75": 0.0,
            "top_1pct_contribution": 0.0,
            "top_5pct_contribution": 0.0,
        }
    total = float(clean.sum())
    sorted_desc = clean.sort_values(ascending=False)
    n = len(sorted_desc)
    n1 = max(1, int(np.ceil(n * 0.01)))
    n5 = max(1, int(np.ceil(n * 0.05)))
    top1 = float(sorted_desc.iloc[:n1].sum())
    top5 = float(sorted_desc.iloc[:n5].sum())
    denom = total if total != 0.0 else np.nan
    return {
        "median": float(clean.median()),
        "p25": float(clean.quantile(0.25)),
        "p75": float(clean.quantile(0.75)),
        "top_1pct_contribution": float(top1 / denom) if np.isfinite(denom) else 0.0,
        "top_5pct_contribution": float(top5 / denom) if np.isfinite(denom) else 0.0,
    }


def capacity_diagnostics(
    events_df: pd.DataFrame, symbols: list[str], min_events_per_day: float
) -> dict[str, object]:
    if events_df.empty:
        return {"pass": False, "estimated_events_per_day": 0.0, "symbol_details": []}
    frame = events_df.copy()
    frame["date"] = pd.to_datetime(frame["enter_ts"], utc=True, errors="coerce").dt.floor("D")
    per_day = frame.groupby(["symbol", "date"], dropna=True).size().reset_index(name="event_count")
    details = []
    for sym in symbols:
        sym_rows = per_day[per_day["symbol"] == sym]
        avg_events = float(sym_rows["event_count"].mean()) if not sym_rows.empty else 0.0
        details.append({"symbol": sym, "avg_events_per_day": avg_events})
    est = float(np.mean([d["avg_events_per_day"] for d in details])) if details else 0.0
    return {
        "pass": bool(est >= float(min_events_per_day)),
        "estimated_events_per_day": est,
        "threshold_events_per_day": float(min_events_per_day),
        "symbol_details": details,
    }
