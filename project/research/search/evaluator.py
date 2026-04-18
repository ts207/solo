"""
Batch hypothesis evaluator (Rich Version).

Evaluates a list of HypothesisSpec against a wide feature table and returns
a metrics DataFrame. Reuses the existing project.research infrastructure
(forward returns, cost model, sparsification) rather than reimplementing it.

The evaluator is trigger-type-agnostic: event, state, transition, and
feature_predicate triggers all resolve to a boolean mask over the feature table.
"""

from __future__ import annotations

import math
import logging
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from project.core.timeframes import bars_per_year, normalize_timeframe, timeframe_spec
from project.domain.hypotheses import HypothesisSpec, TriggerType
from project.research.helpers.shrinkage import _time_decay_weights, _effective_sample_size
from project.core.column_registry import ColumnRegistry

# Robustness framework imports
from project.research.robustness.regime_evaluator import evaluate_by_regime
from project.research.robustness.robustness_scorer import compute_robustness_score
from project.research.robustness.stress_test import (
    evaluate_stress_scenarios,
    STRESS_SCENARIOS,
    _apply_stress_mask,
)
from project.research.robustness.kill_switch import detect_kill_switches
from project.research.robustness.regime_labeler import label_regimes

# Shared utilities
from project.research.search.feasibility import FeasibilityResult, check_hypothesis_feasibility
from project.research.search.stage_models import (
    CandidateHypothesis,
    EvaluatedHypothesis,
    FeasibilityCheckedHypothesis,
)
from project.research.search.evaluator_utils import (
    horizon_bars as _horizon_bars_func,
    forward_log_returns as _forward_log_returns,
    excursion_stats as _excursion_stats,
    trigger_mask as _trigger_mask,
    context_mask as _context_mask,
    trigger_key as _trigger_key,
    signed_returns_for_spec as _signed_returns_for_spec,
)

log = logging.getLogger(__name__)


METRICS_COLUMNS = [
    "hypothesis_id",
    "trigger_type",
    "trigger_key",
    "direction",
    "horizon",
    "template_id",
    "entry_lag",
    "entry_lag_bars",
    "n",
    "train_n_obs",
    "validation_n_obs",
    "test_n_obs",
    "validation_samples",
    "test_samples",
    "mean_return_bps",
    "t_stat",
    "sharpe",
    "hit_rate",
    "cost_adjusted_return_bps",
    "p_value",
    "p_value_raw",
    "p_value_for_fdr",
    "mae_mean_bps",
    "mfe_mean_bps",
    "robustness_score",
    "stress_score",
    "kill_switch_count",
    "capacity_proxy",
    "placebo_shift_effect",
    "placebo_random_entry_effect",
    "direction_reversal_effect",
    "regime_support_ratio",
    "null_strength_ratio",
    "after_cost_expectancy_bps",
    "cost_survival_ratio",
    "coverage_ratio",
    "turnover_proxy",
    "microstructure_stress_proxy",
    "event_family_key",
    "template_family_key",
    "direction_key",
    "horizon_bucket",
    "context_signature",
    "symbol_timeframe_key",
    "valid",
    "invalid_reason",
]

_EVALUATION_SPLIT_LABELS = {"train", "validation", "test"}


def _normal_p_value(stat: float) -> float:
    # E-EVAL-001: one-sided right-tail p-value for directional hypotheses.
    # erfc(t/√2)/2 == P(Z > t) for Z ~ N(0,1).
    # The previous formula erfc(|t|/√2) was two-sided and inflated p-values by 2×.
    if not np.isfinite(stat):
        return 1.0
    return float(math.erfc(float(stat) / math.sqrt(2.0)) / 2.0)



def evaluated_records_from_metrics(metrics_df: pd.DataFrame) -> pd.DataFrame:
    if metrics_df.empty:
        return pd.DataFrame(columns=METRICS_COLUMNS)
    out = metrics_df.copy()
    out["status"] = "evaluated"
    return out


def _null_row(spec: HypothesisSpec, n: int, reason: str = "unknown") -> Dict[str, Any]:
    candidate = CandidateHypothesis(spec=spec, search_spec_name="evaluation")
    checked = FeasibilityCheckedHypothesis(
        candidate=candidate,
        feasibility=FeasibilityResult(valid=False, reasons=(reason,), details={}),
    )
    evaluated = EvaluatedHypothesis(
        checked=checked,
        valid=False,
        invalid_reason=reason,
        metrics={
            "n": n,
            "train_n_obs": 0,
            "validation_n_obs": 0,
            "test_n_obs": 0,
            "validation_samples": 0,
            "test_samples": 0,
            "mean_return_bps": 0.0,
            "t_stat": 0.0,
            "sharpe": 0.0,
            "hit_rate": 0.0,
            "cost_adjusted_return_bps": 0.0,
            "p_value": 1.0,
            "p_value_raw": 1.0,
            # Raw evaluator output has no shrinkage-specific multiplicity input yet,
            # so the FDR input defaults to the raw p-value contract.
            "p_value_for_fdr": 1.0,
            "mae_mean_bps": 0.0,
            "mfe_mean_bps": 0.0,
            "robustness_score": 0.0,
            "stress_score": 0.0,
            "kill_switch_count": 0,
            "capacity_proxy": 0.0,
            "placebo_shift_effect": np.nan,
            "placebo_random_entry_effect": np.nan,
            "direction_reversal_effect": np.nan,
            "regime_support_ratio": np.nan,
            "null_strength_ratio": np.nan,
            "after_cost_expectancy_bps": np.nan,
            "cost_survival_ratio": np.nan,
            "coverage_ratio": np.nan,
            "turnover_proxy": np.nan,
            "microstructure_stress_proxy": np.nan,
            "event_family_key": "UNKNOWN",
            "template_family_key": "UNKNOWN",
            "direction_key": "UNKNOWN",
            "horizon_bucket": "UNKNOWN",
            "context_signature": "UNKNOWN",
            "symbol_timeframe_key": "UNKNOWN",
        },
    )
    row = evaluated.to_record()
    return {column: row.get(column) for column in METRICS_COLUMNS}


def _is_supported_profile(spec: HypothesisSpec) -> tuple[bool, str]:
    if str(spec.cost_profile).strip().lower() != "standard":
        return False, "unsupported_cost_profile"
    if str(spec.objective_profile).strip().lower() != "mean_return":
        return False, "unsupported_objective_profile"
    return True, ""


def _resolved_split_label_for_window(
    *,
    features: pd.DataFrame,
    event_index: Any,
    entry_lag_bars: int,
    horizon_bars: int,
    position_lookup: pd.Series,
) -> str | None:
    if "split_label" not in features.columns:
        return None
    if event_index not in position_lookup.index:
        return None
    pos = position_lookup.get(event_index)
    if pos is None or pd.isna(pos):
        return None
    entry_pos = int(pos) + int(entry_lag_bars)
    future_pos = entry_pos + int(horizon_bars)
    if entry_pos < 0 or future_pos >= len(features):
        return None
    window = features.iloc[entry_pos : future_pos + 1]["split_label"].astype(str)
    labels = [str(label).strip().lower() for label in window.tolist()]
    labels = [label for label in labels if label and label not in {"nan", "none", "null"}]
    if not labels:
        return None
    unique = set(labels)
    if len(unique) != 1:
        return None
    label = next(iter(unique))
    return label if label in _EVALUATION_SPLIT_LABELS else None


def evaluate_hypothesis_batch(
    hypotheses: List[HypothesisSpec],
    features: pd.DataFrame,
    *,
    cost_bps: float = 2.0,
    min_sample_size: int = 20,
    annualisation_factor: Optional[float] = None,
    time_decay_tau_days: Optional[float] = 60.0,
    use_context_quality: bool = True,
    folds: list[Any] | None = None,
) -> pd.DataFrame:
    """
    Evaluate a batch of HypothesisSpec with rich metrics.
    """
    if "close" not in features.columns:
        raise ValueError("features DataFrame must contain a 'close' column")
    if not hypotheses:
        return pd.DataFrame(columns=METRICS_COLUMNS)
    if features.empty:
        rows = [_null_row(spec, 0) for spec in hypotheses]
        return pd.DataFrame(rows)

    if annualisation_factor is None:
        # Infer timeframe from features index frequency if possible
        # Default to 5m if unknown
        try:
            from project.core.timeframes import normalize_timeframe

            # Assuming the index has freq or we can infer it
            pandas_freq = features.index.inferred_freq
            if pandas_freq:
                # Map pandas freq to our timeframe
                # Simplified mapping for common ones
                mapping = {
                    "5min": "5m",
                    "1min": "1m",
                    "15min": "15m",
                    "1H": "1h",
                    "4H": "4h",
                    "1D": "1d",
                }
                tf = mapping.get(pandas_freq, "5m")
                ann = float(bars_per_year(tf))
            else:
                ann = float(bars_per_year("5m"))
        except Exception:
            ann = float(bars_per_year("5m"))
    else:
        ann = annualisation_factor

    # Compute population volatility across full forward distribution to avoid selection bias
    # Use 15m default if hbars not yet resolved, but better to calculate inside loop per horizon.
    # However, to avoid redundant computation, we can cache fwd series.
    fwd_cache: Dict[int, pd.Series] = {}

    # Pre-calculate time decay weights if timestamp is available
    weights = pd.Series(1.0, index=features.index)
    if "timestamp" in features.columns and time_decay_tau_days:
        ref_ts = pd.to_datetime(features["timestamp"].max(), utc=True)
        weights = _time_decay_weights(
            features["timestamp"],
            ref_ts=ref_ts,
            tau_seconds=time_decay_tau_days * 86400.0,
            floor_weight=0.05,
        )

    # Pre-calculate shared masks for robustness evaluation
    regime_labels = label_regimes(features)
    stress_masks = {s["name"]: _apply_stress_mask(s, features) for s in STRESS_SCENARIOS}
    position_lookup = pd.Series(np.arange(len(features), dtype=int), index=features.index)

    rows: List[Dict[str, Any]] = []
    regime_rows: List[Dict[str, Any]] = []  # Phase 4.2 — per-hypothesis regime breakdown
    fold_detail_rows: List[Dict[str, Any]] = []

    for spec in hypotheses:
        profiles_supported, profile_reason = _is_supported_profile(spec)
        if not profiles_supported:
            rows.append(_null_row(spec, 0, profile_reason))
            continue

        feasibility = check_hypothesis_feasibility(spec, features=features)
        if not feasibility.valid:
            rows.append(_null_row(spec, 0, feasibility.primary_reason or "infeasible"))
            continue

        hbars = _horizon_bars_func(spec.horizon)
        direction_sign = (
            1.0 if spec.direction == "long" else -1.0 if spec.direction == "short" else 1.0
        )

        # Resolve trigger mask on the trigger bar.
        mask_raw = _trigger_mask(spec, features)

        # Feature conditions are defined over trigger rows, not shifted entry rows.
        if spec.feature_condition is not None:
            fc_spec = HypothesisSpec(
                trigger=spec.feature_condition,
                direction=spec.direction,
                horizon=spec.horizon,
                template_id=spec.template_id,
            )
            fc_mask = _trigger_mask(fc_spec, features)
            mask_raw = mask_raw & fc_mask

        # Apply entry lag
        if spec.entry_lag < 1:
            rows.append(_null_row(spec, 0, "entry_lag_guardrail"))
            continue
        mask = mask_raw.astype("boolean").shift(spec.entry_lag, fill_value=False).astype(bool)

        # Apply context filter (regime conditioning)
        # If context is specified but cannot be resolved to feature columns, skip this hypothesis.
        if spec.context:
            ctx_mask = _context_mask(
                spec.context,
                features,
                use_context_quality=use_context_quality,
            )
            if ctx_mask is None:
                rows.append(_null_row(spec, 0, "context_unresolvable"))
                continue
            mask = mask & ctx_mask

        if not mask.any():
            rows.append(_null_row(spec, 0, "no_trigger_hits"))
            continue

        # Compute forward returns and extracts
        if hbars not in fwd_cache:
            fwd_cache[hbars] = _forward_log_returns(features["close"], hbars)

        fwd = fwd_cache[hbars]
        event_returns = fwd[mask].dropna()
        if "split_label" in features.columns and not event_returns.empty:
            resolved_split_labels: Dict[Any, str] = {}
            kept_indices: List[Any] = []
            for idx in event_returns.index:
                split_label = _resolved_split_label_for_window(
                    features=features,
                    event_index=idx,
                    entry_lag_bars=int(spec.entry_lag),
                    horizon_bars=int(hbars),
                    position_lookup=position_lookup,
                )
                if split_label is None:
                    continue
                resolved_split_labels[idx] = split_label
                kept_indices.append(idx)
            event_returns = event_returns.loc[kept_indices]
            if event_returns.empty:
                rows.append(_null_row(spec, 0, "no_split_compatible_events"))
                continue
        n = len(event_returns)
        split_counts = {
            "train_n_obs": 0,
            "validation_n_obs": 0,
            "test_n_obs": 0,
            "validation_samples": 0,
            "test_samples": 0,
        }
        if "split_label" in features.columns and not event_returns.empty:
            split_labels = pd.Series(
                [resolved_split_labels.get(idx, "") for idx in event_returns.index],
                index=event_returns.index,
                dtype=object,
            )
            split_counts["train_n_obs"] = int((split_labels == "train").sum())
            split_counts["validation_n_obs"] = int((split_labels == "validation").sum())
            split_counts["test_n_obs"] = int((split_labels == "test").sum())
            split_counts["validation_samples"] = split_counts["validation_n_obs"]
            split_counts["test_samples"] = split_counts["test_n_obs"]

        if n < min_sample_size:
            rows.append(_null_row(spec, n, "min_sample_size"))
            continue

        event_weights = weights[mask].loc[event_returns.index]

        # ── Refined Statistical Estimators ──
        # Effective Sample Size from time-decay weights
        # n_eff_w = (sum w)^2 / (sum w^2)
        n_eff_w = float(_effective_sample_size(event_weights))
        # NOTE: Overlap correction is handled entirely by the Newey-West
        # variance estimator below — no separate n_eff deflation is needed.

        signed, sign_reason = _signed_returns_for_spec(spec, features, event_returns)
        if signed is None:
            rows.append(_null_row(spec, n, sign_reason or "direction_resolution_failed"))
            continue

        # 2. Weighted Mean
        w_sum = event_weights.sum()
        weighted_mean = float((signed * event_weights).sum() / w_sum)

        # SF-003: Newey-West robust variance (handling overlap serial correlation).
        # We manually calculate an approximated AR(hbars) overlapping variance for t-stats,
        # integrating the reliability weights.
        v1 = w_sum
        v2 = (event_weights**2).sum()
        denom = v1 - (v2 / v1)

        if denom > 0:
            # Base sample weighted variance
            weighted_var = ((event_weights * (signed - weighted_mean) ** 2).sum()) / denom

            # Newey-West overlap correction
            # Lags up to (hbars - 1)
            nw_var = weighted_var
            n_samples = len(signed)

            if hbars > 1 and n_samples > hbars:
                signed_demeaned = (signed - weighted_mean).values
                w_arr = event_weights.values

                # Approximate sum of autocorrelations out to hbars - 1 lag
                cov_sum = 0.0
                for lag in range(1, hbars):
                    # Bartlett kernel weight: 1 - lag / hbars
                    kernel = 1.0 - (lag / hbars)

                    # Weighted auto-covariance at this lag
                    w_lag = w_arr[lag:] * w_arr[:-lag]
                    x_lag = signed_demeaned[lag:] * signed_demeaned[:-lag]
                    cov_lag = (w_lag * x_lag).sum() / denom

                    cov_sum += 2.0 * kernel * cov_lag

                nw_var += cov_sum

            weighted_std = np.sqrt(max(0.0, float(nw_var)))
        else:
            weighted_std = 0.0

        # Check for zero variance or too small sample early
        if weighted_std < 1e-10:
            rows.append(_null_row(spec, n, "low_variance"))
            continue

        # ── Enhanced Robustness Framework ──
        # 1. Per-Regime Evaluation
        regime_evals = evaluate_by_regime(
            spec, features, horizon_bars=hbars, min_n_per_regime=5, regime_labels=regime_labels
        )

        # 2. Composite Robustness Score
        robustness = compute_robustness_score(regime_evals, overall_direction=direction_sign)

        # 3. Stress Test Score
        stress_evals = evaluate_stress_scenarios(
            spec, features, horizon_bars=hbars, min_n=5, stress_masks=stress_masks
        )
        if not stress_evals.empty and stress_evals["valid"].any():
            valid_stress = stress_evals[stress_evals["valid"]]
            # Stress score is fraction of survived scenarios (t_stat > 1.0, a meaningful threshold)
            stress_survived = (valid_stress["t_stat"] > 1.0).sum()
            stress_score = float(stress_survived / len(valid_stress))
        else:
            stress_score = 0.0

        # 4. Kill-Switch Detection
        ks_df = detect_kill_switches(spec, features, horizon_bars=hbars, min_n=10)
        ks_count = len(ks_df) if not ks_df.empty else 0

        # Excursions
        maes, mfes = _excursion_stats(features["close"], mask, hbars, direction_sign)
        mae_mean = float(maes.mean())
        mfe_mean = float(mfes.mean())

        # Capacity proxy (volume based if available)
        capacity = 1.0
        if "volume" in features.columns:
            capacity = float(features["volume"][mask].median())

        # T-stat using Newey-West weighted standard error.
        # Overlap density adjustment is already captured structurally by NW variance above,
        # so we use raw sqrt(n_eff_w) for the denominator to prevent double-penalizing.
        t_stat = weighted_mean / (weighted_std / np.sqrt(max(1.0, n_eff_w)))

        # Strategy Sharpe (Scaling by realized trades per year)
        trades_per_year = n * (ann / len(features))
        trades_per_year = min(
            trades_per_year, ann
        )  # Cap at theoretical max to avoid sparse-trigger Sharpe inflation
        sharpe = (weighted_mean / weighted_std) * np.sqrt(trades_per_year)
        hit_rate = float((signed > 0).mean())
        mean_bps = weighted_mean
        cost_adj_bps = mean_bps - cost_bps
        p_value = _normal_p_value(t_stat)

        candidate = CandidateHypothesis(spec=spec, search_spec_name="evaluation")
        checked = FeasibilityCheckedHypothesis(
            candidate=candidate,
            feasibility=FeasibilityResult(valid=True),
        )
        evaluated = EvaluatedHypothesis(
            checked=checked,
            valid=True,
            metrics={
                "n": n,
                **split_counts,
                "mean_return_bps": round(mean_bps, 4),
                "t_stat": round(t_stat, 4),
                "sharpe": round(sharpe, 4),
                "hit_rate": round(hit_rate, 4),
                "cost_adjusted_return_bps": round(cost_adj_bps, 4),
                "p_value": round(p_value, 8),
                "p_value_raw": round(p_value, 8),
                # Downstream shrinkage/scoring stages may replace this with a
                # different multiplicity-input p-value. At the evaluator stage
                # it is intentionally the raw p-value.
                "p_value_for_fdr": round(p_value, 8),
                "mae_mean_bps": round(mae_mean * 10_000.0, 4),
                "mfe_mean_bps": round(mfe_mean * 10_000.0, 4),
                "robustness_score": round(robustness, 4),
                "stress_score": round(stress_score, 4),
                "kill_switch_count": ks_count,
                "capacity_proxy": capacity,
            },
        )
        
        row = evaluated.to_record()
        
        try:
            from project.research.validation.discovery_prechecks import compute_discovery_prechecks
            prechecks = compute_discovery_prechecks(
                spec=spec,
                features=features,
                mask=mask,
                fwd=fwd,
                event_weights=event_weights,
                signed=signed,
                regime_evals=regime_evals,
                n=n,
                cost_bps=cost_bps,
                mean_bps=mean_bps,
                t_stat=t_stat
            )
            for k, v in prechecks.items():
                row[k] = v
        except Exception as e:
            log.warning(f"Failed to inject prechecks: {e}")
            for k in METRICS_COLUMNS:
                if k not in row:
                    row[k] = np.nan
        
        if folds:
            fold_details, fold_aggs = evaluate_candidate_across_folds(
                signed_returns=signed,
                event_weights=event_weights,
                folds=folds,
                cost_bps=cost_bps
            )
            for k, v in fold_aggs.items():
                row[k] = v
            
            # Store detail rows independently to write them in phase2 engine
            if fold_details:
                h_id = row.get("hypothesis_id", "")
                trigger = row.get("trigger_key", "")
                for fd in fold_details:
                    fd["hypothesis_id"] = h_id
                    fd["trigger_key"] = trigger
                    fold_detail_rows.append(fd)
        
        rows.append({column: row.get(column, np.nan) for column in METRICS_COLUMNS if column in row})
        for c in METRICS_COLUMNS:
            if c not in rows[-1]:
                rows[-1][c] = np.nan

        # Phase 4.2 — Accumulate per-regime breakdown for every evaluated hypothesis.
        # The full regime_evals DataFrame is richer than the scalar robustness_score:
        # it exposes hypotheses where aggregate performance is weak but per-regime
        # performance is strong — the regime-specific alpha signal.
        if not regime_evals.empty and "t_stat" in regime_evals.columns:
            h_id = row.get("hypothesis_id", "")
            trigger = row.get("trigger_key", "")
            for _, r_row in regime_evals.iterrows():
                if r_row.get("valid", False):
                    regime_rows.append({
                        "hypothesis_id": h_id,
                        "trigger_key": trigger,
                        "template_id": spec.template_id,
                        "direction": spec.direction,
                        "horizon": spec.horizon,
                        "regime": str(r_row.get("regime", "")),
                        "n": int(r_row.get("n", 0)),
                        "mean_return_bps": float(r_row.get("mean_return_bps", 0.0)),
                        "t_stat": float(r_row.get("t_stat", 0.0)),
                        "hit_rate": float(r_row.get("hit_rate", 0.0)),
                    })

    df = pd.DataFrame(rows, columns=METRICS_COLUMNS)
    
    # Incorporate fold agg columns that we dynamically added to `rows` output above
    if folds and rows:
        fold_keys = [
            "fold_count", "fold_valid_count", "fold_sign_consistency", 
            "fold_median_oos_expectancy", "fold_worst_oos_expectancy", 
            "fold_median_after_cost_expectancy", "fold_median_t_stat", 
            "fold_fail_ratio"
        ]
        for c in fold_keys:
            if c not in df.columns:
                df[c] = pd.Series([r.get(c, np.nan) for r in rows], dtype=float)

    # Attach per-regime rows as a metadata attribute so callers can persist it
    # without changing the public return type.  run_hypothesis_search.py reads
    # this attribute and writes regime_breakdown.parquet alongside metrics.
    df.attrs["regime_breakdown"] = (
        pd.DataFrame(regime_rows) if regime_rows else pd.DataFrame(
            columns=["hypothesis_id", "trigger_key", "template_id", "direction",
                     "horizon", "regime", "n", "mean_return_bps", "t_stat", "hit_rate"]
        )
    )
    
    # Attach per-fold breakdown rows as metadata attribute similarly over candidates.
    df.attrs["fold_breakdown"] = (
        pd.DataFrame(fold_detail_rows) if fold_detail_rows else pd.DataFrame()
    )

    invalid_reason_counts = (
        df.loc[~df["valid"], "invalid_reason"]
        .fillna("unknown")
        .astype(str)
        .value_counts()
        .sort_index()
        .to_dict()
    )
    invalid_summary = (
        ", ".join(f"{reason}={count}" for reason, count in invalid_reason_counts.items())
        if invalid_reason_counts
        else "none"
    )
    log.info(
        "Evaluated %d hypotheses: %d valid, %d invalid (%s)",
        len(hypotheses),
        int(df["valid"].sum()),
        int((~df["valid"]).sum()),
        invalid_summary,
    )
    return df

def evaluate_candidate_across_folds(
    signed_returns: pd.Series,
    event_weights: pd.Series,
    folds: list[Any],
    cost_bps: float
) -> tuple[list[dict], dict]:
    fold_metrics = []
    
    for fold in folds:
        test_start = fold.test_split.start
        test_end = fold.test_split.end
        
        if pd.api.types.is_integer_dtype(signed_returns.index):
            t_start = int(pd.Timestamp(test_start).value // 10**6)
            t_end = int(pd.Timestamp(test_end).value // 10**6)
        else:
            t_start = test_start
            t_end = test_end
            
        mask = (signed_returns.index >= t_start) & (signed_returns.index <= t_end)
        
        fold_n = mask.sum()
        if fold_n < 3:
            fold_metrics.append({
                "fold_id": fold.fold_id,
                "valid": False,
                "n": int(fold_n)
            })
            continue
            
        fold_signed = signed_returns[mask]
        fold_w = event_weights[mask]
        w_sum = fold_w.sum()
        if w_sum <= 0:
            fold_metrics.append({"fold_id": fold.fold_id, "valid": False, "n": int(fold_n)})
            continue
            
        w_mean = float((fold_signed * fold_w).sum() / w_sum)
        
        # Simple weighted std for t-stat proxy
        v1 = w_sum
        v2 = (fold_w**2).sum()
        denom = v1 - (v2 / v1)
        if denom > 0:
            w_var = ((fold_w * (fold_signed - w_mean)**2).sum()) / denom
            w_std = np.sqrt(max(0.0, float(w_var)))
            n_eff = float((fold_w.sum()**2) / (fold_w**2).sum())
            t_stat = w_mean / (w_std / np.sqrt(max(1.0, n_eff))) if w_std > 1e-10 else 0.0
        else:
            t_stat = 0.0
            
        fold_metrics.append({
            "fold_id": fold.fold_id,
            "valid": True,
            "n": int(fold_n),
            "oos_expectancy_bps": w_mean,
            "after_cost_expectancy_bps": w_mean - cost_bps,
            "t_stat": t_stat,
            "sign": 1 if w_mean > 0 else (-1 if w_mean < 0 else 0)
        })
        
    valid_folds = [m for m in fold_metrics if m.get("valid", False)]
    fold_count = len(folds)
    valid_count = len(valid_folds)
    
    if valid_count == 0:
        return fold_metrics, {
            "fold_count": fold_count,
            "fold_valid_count": 0,
            "fold_sign_consistency": 0.0,
            "fold_median_oos_expectancy": 0.0,
            "fold_worst_oos_expectancy": 0.0,
            "fold_median_after_cost_expectancy": 0.0,
            "fold_median_t_stat": 0.0,
            "fold_fail_ratio": 1.0,
        }
        
    oos_list = [m["oos_expectancy_bps"] for m in valid_folds]
    after_cost_list = [m["after_cost_expectancy_bps"] for m in valid_folds]
    t_stat_list = [m["t_stat"] for m in valid_folds]
    
    pos_count = sum(1 for m in valid_folds if m["sign"] > 0)
    neg_count = sum(1 for m in valid_folds if m["sign"] < 0)
    dom_sign_count = max(pos_count, neg_count)
    
    fail_count = sum(1 for m in valid_folds if m["oos_expectancy_bps"] <= 0) if pos_count >= neg_count else sum(1 for m in valid_folds if m["oos_expectancy_bps"] >= 0)
    
    agg = {
        "fold_count": fold_count,
        "fold_valid_count": valid_count,
        "fold_sign_consistency": float(dom_sign_count / valid_count),
        "fold_median_oos_expectancy": float(np.median(oos_list)),
        "fold_worst_oos_expectancy": float(min(oos_list) if np.median(oos_list) > 0 else max(oos_list)),
        "fold_median_after_cost_expectancy": float(np.median(after_cost_list)),
        "fold_median_t_stat": float(np.median(t_stat_list)),
        "fold_fail_ratio": float(fail_count / valid_count),
    }
    
    return fold_metrics, agg
