"""
Numerical reasoning and core shrinkage math kernels.
"""

from __future__ import annotations
import logging
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


def _time_decay_weights(
    event_ts: pd.Series,
    *,
    ref_ts: pd.Timestamp,
    tau_seconds: float,
    floor_weight: float,
) -> pd.Series:
    if event_ts.empty:
        return pd.Series(dtype=float)
    if float(tau_seconds) <= 0.0:
        return pd.Series(1.0, index=event_ts.index, dtype=float)
    delta = (
        (ref_ts - pd.to_datetime(event_ts, utc=True, errors="coerce"))
        .dt.total_seconds()
        .fillna(0.0)
        .clip(lower=0.0)
    )
    w = np.exp(-delta / float(tau_seconds))
    floor = max(0.0, min(1.0, float(floor_weight)))
    return pd.Series(np.maximum(w, floor), index=event_ts.index, dtype=float)


def _effective_sample_size(weights: pd.Series) -> float:
    w = pd.to_numeric(weights, errors="coerce").fillna(0.0).clip(lower=0.0)
    s1 = float(w.sum())
    s2 = float((w * w).sum())
    if s1 <= 0.0 or s2 <= 0.0:
        return 0.0
    return float((s1 * s1) / s2)


def _aggregate_effect_units(
    df: pd.DataFrame,
    *,
    unit_cols: List[str],
    n_col: str,
    mean_col: str,
    var_col: str,
    prefix: str,
) -> pd.DataFrame:
    cols = unit_cols + [n_col, mean_col, var_col]
    if df.empty:
        return pd.DataFrame(columns=unit_cols + [f"n_{prefix}", f"mean_{prefix}", f"var_{prefix}"])

    work = df[cols].copy()
    work["_n"] = pd.to_numeric(work[n_col], errors="coerce").fillna(0.0).clip(lower=0.0)
    work["_mean"] = pd.to_numeric(work[mean_col], errors="coerce").fillna(0.0)
    work["_var"] = pd.to_numeric(work[var_col], errors="coerce").fillna(0.0).clip(lower=0.0)

    rows: List[Dict[str, Any]] = []
    for keys, g in work.groupby(unit_cols, dropna=False):
        g = g.copy()
        total_n = float(g["_n"].sum())
        if total_n <= 0.0:
            mean_u = 0.0
            var_u = 0.0
        else:
            mean_u = float((g["_n"] * g["_mean"]).sum() / total_n)
            within = float(((g["_n"] - 1.0).clip(lower=0.0) * g["_var"]).sum())
            between = float((g["_n"] * (g["_mean"] - mean_u) ** 2).sum())
            denom = max(total_n - 1.0, 1.0)
            var_u = float(max(0.0, (within + between) / denom))

        if not isinstance(keys, tuple):
            keys = (keys,)
        row = {col: val for col, val in zip(unit_cols, keys)}
        row[f"n_{prefix}"] = total_n
        row[f"mean_{prefix}"] = mean_u
        row[f"var_{prefix}"] = var_u
        rows.append(row)
    return pd.DataFrame(rows)


def _estimate_adaptive_lambda(
    units_df: pd.DataFrame,
    *,
    parent_cols: List[str],
    child_col: str,
    n_col: str,
    mean_col: str,
    var_col: str,
    lambda_name: str,
    fixed_lambda: float,
    adaptive: bool,
    lambda_min: float,
    lambda_max: float,
    eps: float,
    min_total_samples: int,
    min_samples_for_adaptive_lambda: int = 30,
    previous_lambda_by_parent: Optional[Dict[Tuple[Any, ...], float]] = None,
    lambda_smoothing_alpha: float = 0.1,
    lambda_shock_cap_pct: float = 0.5,
    lambda_decay_factor: float = 0.0,
) -> pd.DataFrame:
    if units_df.empty:
        return pd.DataFrame(
            columns=parent_cols
            + [
                lambda_name,
                f"{lambda_name}_status",
                f"{lambda_name}_sigma_within",
                f"{lambda_name}_sigma_between",
                f"{lambda_name}_total_n",
                f"{lambda_name}_child_count",
                f"{lambda_name}_raw",
                f"{lambda_name}_prev",
            ]
        )

    if not adaptive:
        base = units_df[parent_cols].drop_duplicates().copy()
        base[lambda_name] = float(fixed_lambda)
        base[f"{lambda_name}_status"] = "fixed"
        base[f"{lambda_name}_sigma_within"] = np.nan
        base[f"{lambda_name}_sigma_between"] = np.nan
        base[f"{lambda_name}_total_n"] = np.nan
        base[f"{lambda_name}_child_count"] = np.nan
        base[f"{lambda_name}_raw"] = np.nan
        base[f"{lambda_name}_prev"] = np.nan
        return base

    rows: List[Dict[str, Any]] = []
    for keys, g in units_df.groupby(parent_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        parent_payload = {col: val for col, val in zip(parent_cols, keys)}

        n = pd.to_numeric(g[n_col], errors="coerce").fillna(0.0).clip(lower=0.0)
        mean = pd.to_numeric(g[mean_col], errors="coerce").fillna(0.0)
        var = pd.to_numeric(g[var_col], errors="coerce").fillna(0.0).clip(lower=0.0)
        child_count = int((n > 0.0).sum())
        total_n = float(n.sum())

        status = "adaptive"
        sigma_within = np.nan
        sigma_between = np.nan

        if total_n < float(max(0, int(min_total_samples))):
            # Explicit skip: no pooling when support is too low.
            lam = 0.0
            status = "insufficient_data"
        elif child_count <= 1:
            lam = float(lambda_max)
            status = "single_child"
            sigma_within = float(
                ((n - 1.0).clip(lower=0.0) * var).sum()
                / max(float((n - 1.0).clip(lower=0.0).sum()), 1.0)
            )
            sigma_between = 0.0
        elif total_n < float(min_samples_for_adaptive_lambda):
            # Rare event: insufficient data for reliable adaptive lambda estimation.
            # Use conservative fixed lambda instead of adaptive estimate.
            lam = float(fixed_lambda)
            status = "rare_event_fixed"
            sigma_within = np.nan
            sigma_between = np.nan
        else:
            mean_global = float((n * mean).sum() / max(total_n, 1.0))
            denom_within = float((n - 1.0).clip(lower=0.0).sum())
            sigma_within = float(((n - 1.0).clip(lower=0.0) * var).sum() / max(denom_within, 1.0))
            sigma_between = float((n * (mean - mean_global) ** 2).sum() / max(total_n, 1.0))
            lam_raw = float(sigma_within / max(sigma_between, float(eps)))
            lam = float(np.clip(lam_raw, float(lambda_min), float(lambda_max)))

        lam_raw = float(lam)
        lam_prev = np.nan
        if status == "adaptive" and previous_lambda_by_parent:
            key_tuple = tuple(parent_payload[c] for c in parent_cols)
            prev_val = previous_lambda_by_parent.get(key_tuple)
            if prev_val is not None and float(prev_val) > 0.0:
                # Apply cross-session decay to the previous estimate (Item 10)
                decay = max(0.0, min(1.0, float(lambda_decay_factor)))
                lam_prev = float(prev_val) * (1.0 - decay)

                alpha = max(0.0, min(1.0, float(lambda_smoothing_alpha)))
                smoothed = ((1.0 - alpha) * lam_prev) + (alpha * lam_raw)
                cap = max(0.0, float(lambda_shock_cap_pct))
                lo = lam_prev * (1.0 - cap)
                hi = lam_prev * (1.0 + cap)
                lam = float(np.clip(smoothed, lo, hi))
                status = "adaptive_smoothed"

        row = dict(parent_payload)
        row[lambda_name] = float(lam)
        row[f"{lambda_name}_raw"] = float(lam_raw)
        row[f"{lambda_name}_prev"] = float(lam_prev) if np.isfinite(lam_prev) else np.nan
        row[f"{lambda_name}_status"] = status
        row[f"{lambda_name}_sigma_within"] = (
            float(sigma_within) if np.isfinite(sigma_within) else np.nan
        )
        row[f"{lambda_name}_sigma_between"] = (
            float(sigma_between) if np.isfinite(sigma_between) else np.nan
        )
        row[f"{lambda_name}_total_n"] = total_n
        row[f"{lambda_name}_child_count"] = child_count
        rows.append(row)
    return pd.DataFrame(rows)


def _compute_loso_stability(
    df: pd.DataFrame,
    *,
    group_cols: List[str],
    symbol_col: str = "_symbol",
    effect_col: str = "effect_raw",
    n_col: str = "_n",
) -> pd.Series:
    """
    S2: Leave-one-symbol-out shrinkage stability.
    Returns a boolean series indicating if the candidate remains stable when its symbol is excluded from pooling.
    """
    if df.empty:
        return pd.Series(dtype=bool)

    out_stable = pd.Series(True, index=df.index)

    # Iterate over pooling groups
    for keys, group in df.groupby(group_cols, dropna=False):
        if len(group[symbol_col].unique()) <= 1:
            # LOSO cannot validate single-symbol groups — there is no out-of-sample
            # symbol to leave out. Mark as unstable so these are restricted to
            # shadow-only promotion until cross-symbol evidence is available.
            out_stable.loc[group.index] = False
            continue

        total_n = group[n_col].sum()
        group_mean = (group[n_col] * group[effect_col]).sum() / max(total_n, 1.0)

        for sym in group[symbol_col].unique():
            sym_mask = group[symbol_col] == sym
            sym_n = group.loc[sym_mask, n_col].sum()
            sym_mean = (group.loc[sym_mask, n_col] * group.loc[sym_mask, effect_col]).sum() / max(
                sym_n, 1.0
            )

            # Compute group mean excluding this symbol
            loso_n = total_n - sym_n
            if loso_n <= 0:
                loso_mean = 0.0
            else:
                loso_mean = (total_n * group_mean - sym_n * sym_mean) / loso_n

            # Stability check:
            # 1. Sign consistency: Shrunk effect should have same sign as raw if pooling is fair.
            # Here we check if the pooling target (loso_mean) has the same sign.
            if abs(sym_mean) > 1e-9 and abs(loso_mean) > 1e-9:
                if np.sign(sym_mean) != np.sign(loso_mean):
                    # Sign flip when pooling! This candidate's effect might be borrowed.
                    # We flag it if the borrowed component is dominant.
                    # For simplicity, mark as unstable if signs oppose.
                    out_stable.loc[group[sym_mask].index] = False

    return out_stable


def _apply_hierarchical_shrinkage(
    raw_df: pd.DataFrame,
    *,
    lambda_state: float = 100.0,
    lambda_event: float = 300.0,
    lambda_family: float = 1000.0,
    adaptive_lambda: bool = True,
    adaptive_lambda_min: float = 5.0,
    adaptive_lambda_max: float = 5000.0,
    adaptive_lambda_eps: float = 1e-8,
    adaptive_lambda_min_total_samples: int = 200,
    previous_lambda_maps: Optional[Dict[str, Dict[Tuple[Any, ...], float]]] = None,
    lambda_smoothing_alpha: float = 0.1,
    lambda_shock_cap_pct: float = 0.5,
    lambda_decay_factor: float = 0.05,
    elapsed_days: Optional[float] = None,
    lambda_decay_halflife_days: float = 90.0,
    train_only_lambda: bool = False,
    split_col: Optional[str] = None,
    run_mode: str = "exploratory",  # Added run_mode
) -> pd.DataFrame:
    """Empirical-Bayes partial pooling across family -> event -> state."""

    # A7: When elapsed_days is provided, derive lambda_decay_factor from elapsed
    # time rather than using a fixed per-session rate.  This prevents cross-session
    # continuity from collapsing when sessions are infrequent.
    # Decay model: factor = 1 - exp(-elapsed_days / halflife), so after one
    # halflife the previous lambda contributes ~63% of the blended estimate.
    if elapsed_days is not None and float(elapsed_days) >= 0.0:
        import math as _math
        _halflife = max(1.0, float(lambda_decay_halflife_days))
        lambda_decay_factor = float(1.0 - _math.exp(-float(elapsed_days) / _halflife))

    # S1: Enforce train-only estimation if split is available
    is_confirmatory = str(run_mode).lower() in {
        "confirmatory",
        "production",
        "certification",
        "promotion",
        "deploy",
    }
    effective_train_only = train_only_lambda
    aggregate_train_count_col: Optional[str] = None

    if split_col and split_col in raw_df.columns:
        # Respect the explicit caller contract. Train-only estimation is enabled
        # only when requested or when confirmatory mode requires it.
        if train_only_lambda or is_confirmatory:
            effective_train_only = True
        else:
            effective_train_only = False
    elif "train_n_obs" in raw_df.columns:
        if train_only_lambda or is_confirmatory:
            effective_train_only = True
            aggregate_train_count_col = "train_n_obs"
        else:
            effective_train_only = False
    elif is_confirmatory:
        log.warning("Confirmatory mode requested but split_col is missing. No pooling possible.")
        effective_train_only = False

    if raw_df.empty:
        out = raw_df.copy()
        for col in (
            "effect_raw",
            "effect_shrunk_family",
            "effect_shrunk_event",
            "effect_shrunk_state",
            "shrinkage_weight_family",
            "shrinkage_weight_event",
            "shrinkage_weight_state",
            "p_value_raw",
            "p_value_shrunk",
            "p_value_for_fdr",
            "shrinkage_scope",
            "shrinkage_loso_stable",
        ):
            out[col] = pd.Series(dtype=float)
        return out

    out = raw_df.copy().reset_index(drop=True)
    out["effect_raw"] = pd.to_numeric(
        out.get("expectancy", pd.Series(0.0, index=out.index)), errors="coerce"
    ).fillna(0.0)
    out["p_value_raw"] = (
        pd.to_numeric(out.get("p_value", pd.Series(1.0, index=out.index)), errors="coerce")
        .fillna(1.0)
        .clip(0.0, 1.0)
    )
    out["_n"] = (
        pd.to_numeric(
            out.get(
                "effective_sample_size",
                out.get("n_events", out.get("sample_size", pd.Series(0, index=out.index))),
            ),
            errors="coerce",
        )
        .fillna(0.0)
        .clip(lower=0.0)
    )
    if "std_return" not in out.columns:
        out["std_return"] = np.nan
    out["std_return"] = pd.to_numeric(out["std_return"], errors="coerce")

    family_col = (
        out["research_family"]
        if "research_family" in out.columns
        else out["canonical_family"]
        if "canonical_family" in out.columns
        else out.get("event_type", pd.Series("", index=out.index))
    )
    event_col = (
        out["canonical_event_type"]
        if "canonical_event_type" in out.columns
        else out.get("event_type", pd.Series("", index=out.index))
    )
    verb_col = (
        out["template_verb"]
        if "template_verb" in out.columns
        else out.get("rule_template", pd.Series("", index=out.index))
    )
    horizon_col = out["horizon"] if "horizon" in out.columns else pd.Series("", index=out.index)
    symbol_col = out["symbol"] if "symbol" in out.columns else pd.Series("", index=out.index)
    state_col = out["state_id"] if "state_id" in out.columns else pd.Series("", index=out.index)

    out["_family"] = family_col.astype(str).str.strip().str.upper()
    out["_event"] = event_col.astype(str).str.strip().str.upper()
    out["_verb"] = verb_col.astype(str).str.strip()
    out["_horizon"] = horizon_col.astype(str).str.strip()
    out["_symbol"] = symbol_col.astype(str).str.strip().str.upper()
    out["_state"] = state_col.fillna("").astype(str).str.strip().str.upper()

    # Extract Regime to condition the global mean
    def _extract_regime(x):
        if "VOL_REGIME_HIGH" in x or ("HIGH" in x and "VOL" in x):
            return "HIGH"
        if "VOL_REGIME_LOW" in x or ("LOW" in x and "VOL" in x):
            return "LOW"
        if "VOL_REGIME_SHOCK" in x or ("SHOCK" in x and "VOL" in x):
            return "SHOCK"
        if "VOL_REGIME_MID" in x or ("MID" in x and "VOL" in x):
            return "MID"
        return "MIXED"

    out["_regime"] = out["_state"].apply(_extract_regime)

    out["_var"] = (out["std_return"] ** 2).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    # Preserve condition-level candidates while shrinking along ontology levels.
    # Adaptive lambdas are estimated per (family,event,verb,horizon), not per symbol.
    # We partition the global grand mean by regime to prevent unjust penalization across regimes.
    global_cols = ["_regime", "_verb", "_horizon"]
    family_cols = global_cols + ["_family"]
    event_cols = family_cols + ["_event"]
    state_cols = event_cols + ["_state"]

    # When effective_train_only is True, compute group means/variances for lambda
    # estimation from train rows only, preventing holdout data leakage.
    if effective_train_only and split_col and split_col in out.columns:
        _lambda_src = out[out[split_col].astype(str).str.strip().str.lower() == "train"].copy()
        if _lambda_src.empty:
            # E-SHRK-002: train-only mode requested but no rows carry split_label='train'.
            # Falling back to all data to avoid a completely empty shrinkage estimate, but
            # this means lambda estimation uses holdout data — flag it explicitly.
            log.warning(
                "_apply_hierarchical_shrinkage: train_only_lambda=True but no rows with "
                "split_label='train' found. Falling back to all-data lambda estimation. "
                "Set 'shrinkage_scope' will be suffixed with '_FALLBACK' in the output."
            )
            _lambda_src = out
            # Mark the fallback in the scope column so it is visible in audit outputs.
            out["shrinkage_scope"] = "train_only_FALLBACK_all_data"
    elif (
        effective_train_only
        and aggregate_train_count_col
        and aggregate_train_count_col in out.columns
    ):
        _lambda_src = out.copy()
        _lambda_src["_n"] = (
            pd.to_numeric(_lambda_src[aggregate_train_count_col], errors="coerce")
            .fillna(0.0)
            .clip(lower=0.0)
        )
        if not bool((_lambda_src["_n"] > 0.0).any()):
            _lambda_src = out
    else:
        _lambda_src = out

    if effective_train_only and aggregate_train_count_col:
        out["shrinkage_scope"] = "train_only_aggregate_counts"
    else:
        out["shrinkage_scope"] = "train_only" if effective_train_only else "all_data"
    out["shrinkage_hierarchy_levels"] = "regime|verb|horizon|family|event|state|symbol"

    global_stats = _aggregate_effect_units(
        _lambda_src,
        unit_cols=global_cols,
        n_col="_n",
        mean_col="effect_raw",
        var_col="_var",
        prefix="global",
    )
    out = out.merge(
        global_stats[global_cols + ["mean_global", "n_global", "var_global"]],
        on=global_cols,
        how="left",
    )

    family_stats = _aggregate_effect_units(
        _lambda_src,
        unit_cols=family_cols,
        n_col="_n",
        mean_col="effect_raw",
        var_col="_var",
        prefix="family",
    )
    out = out.merge(
        family_stats[family_cols + ["mean_family", "n_family", "var_family"]],
        on=family_cols,
        how="left",
    )

    lambda_family_df = _estimate_adaptive_lambda(
        family_stats,
        parent_cols=global_cols,
        child_col="_family",
        n_col="n_family",
        mean_col="mean_family",
        var_col="var_family",
        lambda_name="lambda_family",
        fixed_lambda=float(lambda_family),
        adaptive=bool(adaptive_lambda),
        lambda_min=float(adaptive_lambda_min),
        lambda_max=float(adaptive_lambda_max),
        eps=float(adaptive_lambda_eps),
        min_total_samples=int(adaptive_lambda_min_total_samples),
        min_samples_for_adaptive_lambda=30,
        previous_lambda_by_parent=(previous_lambda_maps or {}).get("family"),
        lambda_smoothing_alpha=float(lambda_smoothing_alpha),
        lambda_shock_cap_pct=float(lambda_shock_cap_pct),
        lambda_decay_factor=float(lambda_decay_factor),
    )
    out = out.merge(
        lambda_family_df[
            global_cols
            + [
                "lambda_family",
                "lambda_family_status",
                "lambda_family_sigma_within",
                "lambda_family_sigma_between",
                "lambda_family_total_n",
                "lambda_family_child_count",
                "lambda_family_raw",
                "lambda_family_prev",
            ]
        ],
        on=global_cols,
        how="left",
    )

    out["shrinkage_weight_family"] = np.where(
        out["lambda_family_status"] == "insufficient_data",
        0.0,
        out["n_family"]
        / (
            out["n_family"]
            + pd.to_numeric(out["lambda_family"], errors="coerce").fillna(float(lambda_family))
        ),
    )
    out["shrinkage_weight_family"] = (
        out["shrinkage_weight_family"].replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(0.0, 1.0)
    )
    out["effect_shrunk_family"] = (
        out["shrinkage_weight_family"] * out["mean_family"]
        + (1.0 - out["shrinkage_weight_family"]) * out["mean_global"]
    )

    event_stats = _aggregate_effect_units(
        _lambda_src,
        unit_cols=event_cols,
        n_col="_n",
        mean_col="effect_raw",
        var_col="_var",
        prefix="event",
    )
    out = out.merge(
        event_stats[event_cols + ["mean_event", "n_event", "var_event"]],
        on=event_cols,
        how="left",
    )

    lambda_event_df = _estimate_adaptive_lambda(
        event_stats,
        parent_cols=family_cols,
        child_col="_event",
        n_col="n_event",
        mean_col="mean_event",
        var_col="var_event",
        lambda_name="lambda_event",
        fixed_lambda=float(lambda_event),
        adaptive=bool(adaptive_lambda),
        lambda_min=float(adaptive_lambda_min),
        lambda_max=float(adaptive_lambda_max),
        eps=float(adaptive_lambda_eps),
        min_total_samples=int(adaptive_lambda_min_total_samples),
        min_samples_for_adaptive_lambda=30,
        previous_lambda_by_parent=(previous_lambda_maps or {}).get("event"),
        lambda_smoothing_alpha=float(lambda_smoothing_alpha),
        lambda_shock_cap_pct=float(lambda_shock_cap_pct),
        lambda_decay_factor=float(lambda_decay_factor),
    )
    out = out.merge(
        lambda_event_df[
            family_cols
            + [
                "lambda_event",
                "lambda_event_status",
                "lambda_event_sigma_within",
                "lambda_event_sigma_between",
                "lambda_event_total_n",
                "lambda_event_child_count",
                "lambda_event_raw",
                "lambda_event_prev",
            ]
        ],
        on=family_cols,
        how="left",
    )

    out["shrinkage_weight_event"] = np.where(
        out["lambda_event_status"] == "insufficient_data",
        0.0,
        out["n_event"]
        / (
            out["n_event"]
            + pd.to_numeric(out["lambda_event"], errors="coerce").fillna(float(lambda_event))
        ),
    )
    out["shrinkage_weight_event"] = (
        out["shrinkage_weight_event"].replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(0.0, 1.0)
    )
    out["effect_shrunk_event"] = (
        out["shrinkage_weight_event"] * out["mean_event"]
        + (1.0 - out["shrinkage_weight_event"]) * out["effect_shrunk_family"]
    )

    state_mask_lambda = _lambda_src["_state"] != ""
    state_stats = _aggregate_effect_units(
        _lambda_src[state_mask_lambda],
        unit_cols=state_cols,
        n_col="_n",
        mean_col="effect_raw",
        var_col="_var",
        prefix="state",
    )
    out = out.merge(
        state_stats[state_cols + ["mean_state", "n_state", "var_state"]],
        on=state_cols,
        how="left",
    )

    lambda_state_df = _estimate_adaptive_lambda(
        state_stats,
        parent_cols=event_cols,
        child_col="_state",
        n_col="n_state",
        mean_col="mean_state",
        var_col="var_state",
        lambda_name="lambda_state",
        fixed_lambda=float(lambda_state),
        adaptive=bool(adaptive_lambda),
        lambda_min=float(adaptive_lambda_min),
        lambda_max=float(adaptive_lambda_max),
        eps=float(adaptive_lambda_eps),
        min_total_samples=int(adaptive_lambda_min_total_samples),
        min_samples_for_adaptive_lambda=30,
        previous_lambda_by_parent=(previous_lambda_maps or {}).get("state"),
        lambda_smoothing_alpha=float(lambda_smoothing_alpha),
        lambda_shock_cap_pct=float(lambda_shock_cap_pct),
        lambda_decay_factor=float(lambda_decay_factor),
    )
    out = out.merge(
        lambda_state_df[
            event_cols
            + [
                "lambda_state",
                "lambda_state_status",
                "lambda_state_sigma_within",
                "lambda_state_sigma_between",
                "lambda_state_total_n",
                "lambda_state_child_count",
                "lambda_state_raw",
                "lambda_state_prev",
            ]
        ],
        on=event_cols,
        how="left",
    )

    # 3. State layer (cross-symbol)
    out["shrinkage_weight_state_group"] = np.where(
        out["lambda_state_status"] == "insufficient_data",
        1.0,
        out["n_state"]
        / (
            out["n_state"]
            + pd.to_numeric(out["lambda_state"], errors="coerce").fillna(float(lambda_state))
        ),
    )
    out["shrinkage_weight_state_group"] = (
        out["shrinkage_weight_state_group"]
        .replace([np.inf, -np.inf], np.nan)
        .fillna(1.0)
        .clip(0.0, 1.0)
    )

    state_mask = out["_state"] != ""
    out["effect_shrunk_state_group"] = np.where(
        state_mask,
        out["shrinkage_weight_state_group"] * out["mean_state"]
        + (1.0 - out["shrinkage_weight_state_group"]) * out["effect_shrunk_event"],
        out["effect_shrunk_event"],
    )

    # 4. Symbol layer (the candidate)
    symbol_cols = state_cols + ["_symbol"]
    symbol_stats = _lambda_src[symbol_cols + ["_n", "effect_raw", "_var"]].copy()
    symbol_stats = symbol_stats.rename(
        columns={"_n": "n_symbol", "effect_raw": "mean_symbol", "_var": "var_symbol"}
    )

    lambda_symbol_df = _estimate_adaptive_lambda(
        symbol_stats,
        parent_cols=state_cols,
        child_col="_symbol",
        n_col="n_symbol",
        mean_col="mean_symbol",
        var_col="var_symbol",
        lambda_name="lambda_symbol",
        fixed_lambda=float(lambda_state),
        adaptive=bool(adaptive_lambda),
        lambda_min=float(adaptive_lambda_min),
        lambda_max=float(adaptive_lambda_max),
        eps=float(adaptive_lambda_eps),
        min_total_samples=int(adaptive_lambda_min_total_samples),
        min_samples_for_adaptive_lambda=30,
        previous_lambda_by_parent=None,
        lambda_smoothing_alpha=float(lambda_smoothing_alpha),
        lambda_shock_cap_pct=float(lambda_shock_cap_pct),
        lambda_decay_factor=float(lambda_decay_factor),
    )

    out = out.merge(
        lambda_symbol_df[
            state_cols + ["lambda_symbol", "lambda_symbol_status", "lambda_symbol_child_count"]
        ],
        on=state_cols,
        how="left",
    )

    # Store pooling group size
    out["shrinkage_pooling_group_size"] = out["lambda_symbol_child_count"]

    # If a state has only one symbol (or insufficient data to adaptively estimate symbol-level variance),
    # fall back to the fixed user-provided lambda_state to ensure small-N heavily pools toward the state.
    out["lambda_symbol_eff"] = np.where(
        out["lambda_symbol_status"].isin(["insufficient_data", "single_child"]),
        float(lambda_state),
        pd.to_numeric(out["lambda_symbol"], errors="coerce").fillna(float(lambda_state)),
    )

    out["shrinkage_weight_state"] = out["_n"] / (out["_n"] + out["lambda_symbol_eff"])
    out["shrinkage_weight_state"] = (
        out["shrinkage_weight_state"].replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(0.0, 1.0)
    )

    # The candidate is shrunk towards the cross-symbol group mean!
    out["effect_shrunk_state"] = (
        out["shrinkage_weight_state"] * out["effect_raw"]
        + (1.0 - out["shrinkage_weight_state"]) * out["effect_shrunk_state_group"]
    )

    out["shrinkage_factor"] = 1.0 - out["shrinkage_weight_state"]

    # Rare event flag: indicate when sample size is too small for reliable adaptive shrinkage
    # This helps distinguish "truly no edge" from "edge obscured by shrinkage"
    RARE_EVENT_THRESHOLD = 50
    out["rare_event_flag"] = (out["_n"] < RARE_EVENT_THRESHOLD).astype(bool)

    # S2: LOSO Stability and Diagnostics
    out["shrinkage_loso_stable"] = _compute_loso_stability(
        out, group_cols=state_cols, symbol_col="_symbol", effect_col="effect_raw", n_col="_n"
    )

    # Posterior residual dispersion: (raw - shrunk) / stderr
    # This identifies symbols that are "outliers" from their pooling group.
    se = out["std_return"] / np.sqrt(np.maximum(out["_n"], 1.0))
    out["shrinkage_posterior_residual_z"] = (
        out["effect_raw"] - out["effect_shrunk_state"]
    ) / se.replace(0, np.nan)

    # Shrinkage contribution breakdown
    # Total Shrinkage = Raw - Shrunk
    out["shrinkage_delta"] = out["effect_raw"] - out["effect_shrunk_state"]
    # We can also track contributions from each layer if we kept them

    # Flag candidates whose effect exists ONLY due to cross-symbol borrowing
    # i.e., raw effect sign != shrunk effect sign OR raw effect magnitude is very small compared to borrowed component
    out["shrinkage_borrowing_dominant"] = np.where(
        (np.sign(out["effect_raw"]) != np.sign(out["effect_shrunk_state"]))
        & (out["effect_raw"] != 0),
        True,
        False,
    )

    # Build shrunken p-values from state-shrunk effect and raw standard error.
    valid_se = np.isfinite(se) & (se > 0.0) & (out["_n"] > 1.0)
    p_shrunk = out["p_value_raw"].astype(float).copy()
    t_shrunk = pd.Series(0.0, index=out.index, dtype=float)

    val = out.loc[valid_se, "effect_shrunk_state"] / se.loc[valid_se]
    t_shrunk.loc[valid_se] = val.astype(float)
    # Vectorized: use scipy when present; otherwise use compatibility fallback.
    try:
        from scipy.stats import t as _scipy_t
    except ModuleNotFoundError:  # pragma: no cover - environment-specific fallback
        from project.core.stats import stats as _stats_compat

        _scipy_t = _stats_compat.t

    _df_vals = (out.loc[valid_se, "_n"].astype(float) - 1.0).clip(lower=1.0)
    # E-SHRK-001: use one-sided right-tail sf on the SIGNED t-stat.
    # Previous code took abs(t) then multiplied by 2 — that is a two-sided p-value.
    # For directional hypotheses a negative shrunk t-stat (sign flip post-shrinkage)
    # should receive a high p-value (close to 1.0), not a low one.
    _t_signed = t_shrunk.loc[valid_se].astype(float)
    if not _t_signed.empty:
        p_shrunk.loc[valid_se] = _scipy_t.sf(_t_signed, df=_df_vals).clip(0.0, 1.0)
    out["p_value_shrunk"] = (
        pd.to_numeric(p_shrunk, errors="coerce").fillna(out["p_value_raw"]).clip(0.0, 1.0)
    )
    out["p_value_for_fdr"] = out["p_value_shrunk"]

    drop_cols = [
        "_n",
        "_family",
        "_event",
        "_verb",
        "_horizon",
        "_regime",
        "_symbol",
        "_state",
        "mean_global",
        "n_global",
        "mean_family",
        "n_family",
        "mean_event",
        "n_event",
        "var_event",
        "mean_state",
        "n_state",
        "var_state",
        "mean_global",
        "n_global",
        "var_global",
        "mean_family",
        "n_family",
        "var_family",
        "lambda_symbol_child_count",
        "lambda_symbol_eff",
    ]
    return out.drop(columns=[c for c in drop_cols if c in out.columns], errors="ignore")
