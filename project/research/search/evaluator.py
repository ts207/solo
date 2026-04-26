"""
Batch hypothesis evaluator (Rich Version).

Evaluates a list of HypothesisSpec against a wide feature table and returns
a metrics DataFrame. Reuses the existing project.research infrastructure
(forward returns, cost model, sparsification) rather than reimplementing it.

The evaluator is trigger-type-agnostic: event, state, transition, and
feature_predicate triggers all resolve to a boolean mask over the feature table.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from project.core.timeframes import bars_per_year
from project.domain.hypotheses import HypothesisSpec
from project.research.helpers.shrinkage import _effective_sample_size, _time_decay_weights
from project.research.phase2_cost_model import expected_cost_per_trade_bps
from project.research.robustness.kill_switch import detect_kill_switches

# Robustness framework imports
from project.research.robustness.regime_evaluator import evaluate_by_regime
from project.research.robustness.regime_labeler import label_regimes
from project.research.robustness.robustness_scorer import compute_robustness_score
from project.research.robustness.stress_test import (
    STRESS_SCENARIOS,
    _apply_stress_mask,
    evaluate_stress_scenarios,
)
from project.research.search.evaluator_utils import (
    context_mask as _context_mask,
)
from project.research.search.evaluator_utils import (
    excursion_stats as _excursion_stats,
)
from project.research.search.evaluator_utils import (
    forward_log_returns as _forward_log_returns,
)
from project.research.search.evaluator_utils import (
    horizon_bars as _horizon_bars_func,
)
from project.research.search.evaluator_utils import (
    signed_returns_for_spec as _signed_returns_for_spec,
)
from project.research.search.evaluator_utils import (
    trigger_mask as _trigger_mask,
)

# Shared utilities
from project.research.search.feasibility import FeasibilityResult, check_hypothesis_feasibility
from project.research.search.stage_models import (
    CandidateHypothesis,
    EvaluatedHypothesis,
    FeasibilityCheckedHypothesis,
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
    "mean_return_gross_bps",
    "mean_return_net_bps",
    "expected_cost_bps_per_trade",
    "t_stat",
    "t_stat_gross",
    "t_stat_net",
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
    "log_wealth_contribution_bps",
    "funding_cost_bps_per_trade",
    "specificity_lift_bps",
    "specificity_lift_pass",
]

EVENT_TIMESTAMP_COLUMNS = [
    "hypothesis_id",
    "trigger_key",
    "event_timestamp",
    "split_label",
]

_EVALUATION_SPLIT_LABELS = {"train", "validation", "test"}
_SPLIT_LABEL_CODES = {"train": 1, "validation": 2, "test": 3}
_SPLIT_CODE_LABELS = {value: key for key, value in _SPLIT_LABEL_CODES.items()}


def _trigger_cache_key(spec: HypothesisSpec) -> str:
    condition = ""
    if spec.feature_condition is not None:
        try:
            condition = spec.feature_condition.label()
        except Exception:
            condition = repr(spec.feature_condition)
    return f"{spec.trigger.label()}||{condition}"


def _context_cache_key(context: dict[str, Any], *, use_context_quality: bool) -> str:
    items = tuple(sorted((str(k), str(v)) for k, v in context.items()))
    return f"{int(bool(use_context_quality))}:{items!r}"


@dataclass
class EvaluationContext:
    features: pd.DataFrame
    time_decay_tau_days: float | None = 60.0
    fwd_cache: dict[int, pd.Series] = field(default_factory=dict)
    trigger_cache: dict[str, pd.Series] = field(default_factory=dict)
    context_cache: dict[str, pd.Series | None] = field(default_factory=dict)
    shifted_mask_cache: dict[tuple[str, int, str], pd.Series] = field(default_factory=dict)
    split_compat_cache: dict[tuple[int, int], pd.DataFrame] = field(default_factory=dict)
    weights: pd.Series = field(init=False)
    regime_labels: pd.Series = field(init=False)
    stress_masks: dict[str, pd.Series | None] = field(init=False)
    position_lookup: pd.Series = field(init=False)
    split_codes: np.ndarray | None = field(init=False)

    def __post_init__(self) -> None:
        self.weights = pd.Series(1.0, index=self.features.index)
        if "timestamp" in self.features.columns and self.time_decay_tau_days:
            ref_ts = pd.to_datetime(self.features["timestamp"].max(), utc=True)
            self.weights = _time_decay_weights(
                self.features["timestamp"],
                ref_ts=ref_ts,
                tau_seconds=self.time_decay_tau_days * 86400.0,
                floor_weight=0.05,
            )
        self.regime_labels = label_regimes(self.features)
        self.stress_masks = {s["name"]: _apply_stress_mask(s, self.features) for s in STRESS_SCENARIOS}
        self.position_lookup = pd.Series(
            np.arange(len(self.features), dtype=int), index=self.features.index
        )
        if "split_label" in self.features.columns:
            labels = (
                self.features["split_label"]
                .astype(str)
                .str.strip()
                .str.lower()
                .map(_SPLIT_LABEL_CODES)
                .fillna(0)
                .astype(np.int8)
            )
            self.split_codes = labels.to_numpy()
        else:
            self.split_codes = None

    def forward_returns(self, horizon_bars: int) -> pd.Series:
        if horizon_bars not in self.fwd_cache:
            self.fwd_cache[horizon_bars] = _forward_log_returns(
                self.features["close"], horizon_bars
            )
        return self.fwd_cache[horizon_bars]

    def raw_trigger_mask(self, spec: HypothesisSpec) -> pd.Series:
        key = _trigger_cache_key(spec)
        if key in self.trigger_cache:
            return self.trigger_cache[key]
        mask = _trigger_mask(spec, self.features)
        if spec.feature_condition is not None:
            fc_spec = HypothesisSpec(
                trigger=spec.feature_condition,
                direction=spec.direction,
                horizon=spec.horizon,
                template_id=spec.template_id,
            )
            mask = mask & _trigger_mask(fc_spec, self.features)
        self.trigger_cache[key] = mask
        return mask

    def event_mask(
        self,
        spec: HypothesisSpec,
        *,
        use_context_quality: bool,
    ) -> tuple[pd.Series | None, str | None]:
        if spec.entry_lag < 1:
            return None, "entry_lag_guardrail"
        trigger_key = _trigger_cache_key(spec)
        context_key = ""
        if spec.context:
            context_key = _context_cache_key(spec.context, use_context_quality=use_context_quality)
        shifted_key = (trigger_key, int(spec.entry_lag), context_key)
        if shifted_key in self.shifted_mask_cache:
            return self.shifted_mask_cache[shifted_key], None

        mask = self.raw_trigger_mask(spec).astype("boolean").shift(
            spec.entry_lag, fill_value=False
        ).astype(bool)
        if spec.context:
            if context_key not in self.context_cache:
                self.context_cache[context_key] = _context_mask(
                    spec.context,
                    self.features,
                    use_context_quality=use_context_quality,
                )
            ctx_mask = self.context_cache[context_key]
            if ctx_mask is None:
                return None, "context_unresolvable"
            mask = mask & ctx_mask
        self.shifted_mask_cache[shifted_key] = mask
        return mask, None

    def split_labels_for_indices(
        self,
        indices: pd.Index,
        *,
        entry_lag_bars: int,
        horizon_bars: int,
    ) -> pd.Series:
        if self.split_codes is None or len(indices) == 0:
            return pd.Series(dtype=object, index=indices)

        key = (int(entry_lag_bars), int(horizon_bars))
        if key not in self.split_compat_cache:
            n = len(self.features)
            positions = np.arange(n, dtype=np.int64)
            entry_pos = positions + int(entry_lag_bars)
            future_pos = entry_pos + int(horizon_bars)
            valid = (entry_pos >= 0) & (future_pos < n)
            start_codes = np.zeros(n, dtype=np.int8)
            split_changes = np.zeros(n, dtype=np.int64)
            if n > 1:
                split_changes[1:] = self.split_codes[1:] != self.split_codes[:-1]
            change_prefix = np.cumsum(split_changes)
            if valid.any():
                start_codes[valid] = self.split_codes[entry_pos[valid]]
            no_window_change = np.zeros(n, dtype=bool)
            if valid.any():
                no_window_change[valid] = (
                    change_prefix[future_pos[valid]] == change_prefix[entry_pos[valid]]
                )
            compatible = valid & (start_codes > 0) & no_window_change
            labels = pd.Series("", index=self.features.index, dtype=object)
            for code, label in _SPLIT_CODE_LABELS.items():
                labels.iloc[np.where(compatible & (start_codes == code))[0]] = label
            self.split_compat_cache[key] = pd.DataFrame(
                {"compatible": compatible, "label": labels}, index=self.features.index
            )

        compat = self.split_compat_cache[key]
        present = indices.intersection(compat.index)
        labels = pd.Series("", index=indices, dtype=object)
        if len(present):
            valid_rows = compat.loc[present]
            labels.loc[present] = valid_rows["label"].where(
                valid_rows["compatible"].astype(bool), ""
            )
        return labels


def _weighted_newey_west_mean_std(
    values: pd.Series,
    weights: pd.Series,
    *,
    horizon_bars: int,
) -> tuple[float, float, float]:
    aligned = pd.concat(
        [pd.to_numeric(values, errors="coerce"), pd.to_numeric(weights, errors="coerce")],
        axis=1,
    ).dropna()
    if aligned.empty:
        return 0.0, 0.0, 0.0
    x = aligned.iloc[:, 0].astype(float)
    w = aligned.iloc[:, 1].astype(float).clip(lower=0.0)
    w_sum = float(w.sum())
    if w_sum <= 0.0:
        return 0.0, 0.0, 0.0
    mean = float((x * w).sum() / w_sum)
    v1 = w_sum
    v2 = float((w**2).sum())
    denom = v1 - (v2 / v1) if v1 > 0 else 0.0
    if denom <= 0.0:
        return mean, 0.0, 0.0
    weighted_var = float((w * (x - mean) ** 2).sum() / denom)
    nw_var = weighted_var
    n_samples = len(x)
    hbars = int(max(1, horizon_bars))
    if hbars > 1 and n_samples > hbars:
        x_demeaned = (x - mean).to_numpy()
        w_arr = w.to_numpy()
        cov_sum = 0.0
        for lag in range(1, hbars):
            kernel = 1.0 - (lag / hbars)
            w_lag = w_arr[lag:] * w_arr[:-lag]
            x_lag = x_demeaned[lag:] * x_demeaned[:-lag]
            cov_sum += 2.0 * kernel * float((w_lag * x_lag).sum() / denom)
        nw_var += cov_sum
    std = float(np.sqrt(max(0.0, nw_var)))
    n_eff = float(_effective_sample_size(w))
    t_stat = mean / (std / np.sqrt(max(1.0, n_eff))) if std >= 1e-10 else 0.0
    return mean, std, t_stat


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


def _null_row(spec: HypothesisSpec, n: int, reason: str = "unknown") -> dict[str, Any]:
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
            "mean_return_gross_bps": 0.0,
            "mean_return_net_bps": 0.0,
            "expected_cost_bps_per_trade": 0.0,
            "t_stat": 0.0,
            "t_stat_gross": 0.0,
            "t_stat_net": 0.0,
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
    hypotheses: list[HypothesisSpec],
    features: pd.DataFrame,
    *,
    cost_bps: float = 2.0,
    min_sample_size: int = 20,
    annualisation_factor: float | None = None,
    time_decay_tau_days: float | None = 60.0,
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

    eval_context = EvaluationContext(features, time_decay_tau_days=time_decay_tau_days)

    rows: list[dict[str, Any]] = []
    regime_rows: list[dict[str, Any]] = []  # Phase 4.2 — per-hypothesis regime breakdown
    fold_detail_rows: list[dict[str, Any]] = []
    event_timestamp_rows: list[dict[str, Any]] = []

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

        mask, mask_reason = eval_context.event_mask(
            spec,
            use_context_quality=use_context_quality,
        )
        if mask is None:
            rows.append(_null_row(spec, 0, mask_reason or "mask_unresolvable"))
            continue

        if not mask.any():
            rows.append(_null_row(spec, 0, "no_trigger_hits"))
            continue

        # Compute forward returns and extracts
        fwd = eval_context.forward_returns(hbars)
        event_returns = fwd[mask].dropna()
        split_labels = pd.Series(dtype=object)
        if "split_label" in features.columns and not event_returns.empty:
            split_labels = eval_context.split_labels_for_indices(
                event_returns.index,
                entry_lag_bars=int(spec.entry_lag),
                horizon_bars=int(hbars),
            )
            keep_mask = split_labels.astype(str).isin(_EVALUATION_SPLIT_LABELS)
            event_returns = event_returns.loc[split_labels.index[keep_mask]]
            split_labels = split_labels.loc[event_returns.index]
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
            split_counts["train_n_obs"] = int((split_labels == "train").sum())
            split_counts["validation_n_obs"] = int((split_labels == "validation").sum())
            split_counts["test_n_obs"] = int((split_labels == "test").sum())
            split_counts["validation_samples"] = split_counts["validation_n_obs"]
            split_counts["test_samples"] = split_counts["test_n_obs"]

        if n < min_sample_size:
            rows.append(_null_row(spec, n, "min_sample_size"))
            continue

        event_weights = eval_context.weights[mask].loc[event_returns.index]

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

        # 2. Gross and net expected returns with matched Newey-West HAC statistics.
        per_trade_cost_bps = expected_cost_per_trade_bps(
            features.loc[signed.index] if hasattr(features, "loc") else features,
            spec,
            cost_spec={"cost_bps": float(cost_bps)},
        ).reindex(signed.index).fillna(float(cost_bps)).astype(float)
        signed_net = signed.astype(float) - per_trade_cost_bps

        # T2.2 — subtract perp funding cost from net returns.
        # funding_rate_scaled is the 8-hour decimal rate (e.g. 0.0001 = 1 bps).
        # A long position pays positive funding; short position receives it.
        # Cost per trade = rate * direction_sign * (horizon_bars / 96 funding periods).
        _funding_col = next(
            (c for c in ("funding_rate_scaled", "funding_rate_realized", "funding_rate")
             if c in features.columns),
            None,
        )
        if _funding_col is not None:
            _funding_at_events = (
                pd.to_numeric(features[_funding_col], errors="coerce")
                .reindex(signed.index)
                .fillna(0.0)
            )
            _funding_periods = hbars / 96.0  # 8h periods per trade
            _funding_cost_bps = _funding_at_events * direction_sign * _funding_periods * 1e4
            signed_net = signed_net - _funding_cost_bps

        gross_mean_bps, gross_std, t_stat_gross = _weighted_newey_west_mean_std(
            signed,
            event_weights,
            horizon_bars=hbars,
        )
        net_mean_bps, net_std, t_stat_net = _weighted_newey_west_mean_std(
            signed_net,
            event_weights,
            horizon_bars=hbars,
        )
        weighted_mean = gross_mean_bps
        weighted_std = gross_std

        # Check for zero variance or too small sample early.  Constant costs preserve
        # variance, but dynamic costs can make gross/net variance differ; fail only
        # when both are degenerate.
        if gross_std < 1e-10 and net_std < 1e-10:
            rows.append(_null_row(spec, n, "low_variance"))
            continue

        # ── Enhanced Robustness Framework ──
        # 1. Per-Regime Evaluation
        regime_evals = evaluate_by_regime(
            spec,
            features,
            horizon_bars=hbars,
            min_n_per_regime=5,
            regime_labels=eval_context.regime_labels,
            event_mask=mask,
            forward_returns=fwd,
        )

        # 2. Composite Robustness Score
        robustness = compute_robustness_score(regime_evals, overall_direction=direction_sign)

        # 3. Stress Test Score
        stress_evals = evaluate_stress_scenarios(
            spec,
            features,
            horizon_bars=hbars,
            min_n=5,
            stress_masks=eval_context.stress_masks,
            event_mask=mask,
            forward_returns=fwd,
        )
        if not stress_evals.empty and stress_evals["valid"].any():
            valid_stress = stress_evals[stress_evals["valid"]]
            # Stress score is fraction of survived scenarios (t_stat > 1.0, a meaningful threshold)
            stress_survived = (valid_stress["t_stat"] > 1.0).sum()
            stress_score = float(stress_survived / len(valid_stress))
        else:
            stress_score = 0.0

        # 4. Kill-Switch Detection
        ks_df = detect_kill_switches(
            spec,
            features,
            horizon_bars=hbars,
            min_n=10,
            event_mask=mask,
            forward_returns=fwd,
        )
        ks_count = len(ks_df) if not ks_df.empty else 0

        # Excursions
        maes, mfes = _excursion_stats(features["close"], mask, hbars, direction_sign)
        mae_mean = float(maes.mean())
        mfe_mean = float(mfes.mean())

        # Capacity proxy (volume based if available)
        capacity = 1.0
        if "volume" in features.columns:
            capacity = float(features["volume"][mask].median())

        # Load-bearing search statistic is net of expected execution cost; gross is retained
        # as an explicit diagnostic to expose the cost shadow.
        t_stat = t_stat_net

        # Strategy Sharpe (Scaling by realized trades per year)
        trades_per_year = n * (ann / len(features))
        trades_per_year = min(
            trades_per_year, ann
        )  # Cap at theoretical max to avoid sparse-trigger Sharpe inflation
        sharpe_base_std = net_std if net_std >= 1e-10 else weighted_std
        sharpe = (net_mean_bps / sharpe_base_std) * np.sqrt(trades_per_year) if sharpe_base_std >= 1e-10 else 0.0
        hit_rate = float((signed_net > 0).mean())
        mean_bps = gross_mean_bps
        cost_adj_bps = net_mean_bps
        expected_cost_mean_bps = float(per_trade_cost_bps.mean()) if len(per_trade_cost_bps) else float(cost_bps)
        p_value = _normal_p_value(t_stat_net)

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
                "mean_return_gross_bps": round(gross_mean_bps, 4),
                "mean_return_net_bps": round(net_mean_bps, 4),
                "expected_cost_bps_per_trade": round(expected_cost_mean_bps, 4),
                "t_stat": round(t_stat, 4),
                "t_stat_gross": round(t_stat_gross, 4),
                "t_stat_net": round(t_stat_net, 4),
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
            row["after_cost_expectancy_bps"] = round(net_mean_bps, 4)
            row["mean_return_net_bps"] = round(net_mean_bps, 4)
            row["t_stat_net"] = round(t_stat_net, 4)
            row["t_stat_gross"] = round(t_stat_gross, 4)
        except Exception as e:
            log.warning(f"Failed to inject prechecks: {e}")
            for k in METRICS_COLUMNS:
                if k not in row:
                    row[k] = np.nan

        if folds:
            # Fold windows are defined in timestamp space. In this pipeline the
            # returns series is typically indexed by feature-row id, not epoch-ms,
            # so we must explicitly pass event timestamps for fold masking.
            event_timestamps = None
            if "timestamp" in features.columns and not signed.empty:
                try:
                    event_timestamps = pd.to_datetime(
                        features.loc[signed.index, "timestamp"],
                        utc=True,
                        errors="coerce",
                    )
                except Exception:
                    event_timestamps = None
            fold_details, fold_aggs = evaluate_candidate_across_folds(
                signed_returns=signed,
                event_weights=event_weights,
                folds=folds,
                cost_bps=cost_bps,
                timestamps=event_timestamps,
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

        if "timestamp" in features.columns and not event_returns.empty:
            try:
                timestamp_frame = pd.DataFrame(
                    {
                        "hypothesis_id": row.get("hypothesis_id", ""),
                        "trigger_key": row.get("trigger_key", ""),
                        "event_timestamp": pd.to_datetime(
                            features.loc[event_returns.index, "timestamp"],
                            utc=True,
                            errors="coerce",
                        ),
                    },
                    index=event_returns.index,
                )
                if not split_labels.empty:
                    timestamp_frame["split_label"] = split_labels.reindex(event_returns.index).astype(str)
                else:
                    timestamp_frame["split_label"] = ""
                timestamp_frame = timestamp_frame.dropna(subset=["event_timestamp"])
                if not timestamp_frame.empty:
                    event_timestamp_rows.extend(
                        timestamp_frame.drop_duplicates(
                            subset=["hypothesis_id", "event_timestamp", "split_label"]
                        )[EVENT_TIMESTAMP_COLUMNS].to_dict("records")
                    )
            except Exception:
                pass

        # M1 — expected log-wealth contribution at fractional-Kelly size.
        # U(h) = E[log(1 + f* * r_net)] approximated as f* * mu - 0.5 * f*^2 * sigma^2
        # using the Taylor expansion of log(1+x). f* = 0.5 (fractional Kelly default).
        _kelly_fraction = 0.5
        if net_std > 1e-10 and n > 0:
            _f_star = min(_kelly_fraction, abs(net_mean_bps / 1e4) / max((net_std / 1e4) ** 2, 1e-12))
            _f_star = min(_f_star, _kelly_fraction)
            _lwc = _f_star * (net_mean_bps / 1e4) - 0.5 * _f_star**2 * (net_std / 1e4) ** 2
        else:
            _lwc = 0.0
        row["log_wealth_contribution_bps"] = round(_lwc * 1e4, 4)

        if _funding_col is not None:
            row["funding_cost_bps_per_trade"] = round(
                float(_funding_cost_bps.mean()) if hasattr(_funding_cost_bps, "mean") else 0.0, 4
            )
        else:
            row["funding_cost_bps_per_trade"] = 0.0

        # M4 — Specificity lift: run regime-matched placebo test on high-quality candidates.
        # Only runs when |t_stat_net| > 2.0 to keep batch evaluation fast (50 draws).
        row["specificity_lift_bps"] = float("nan")
        row["specificity_lift_pass"] = False
        if abs(t_stat_net) > 2.0 and len(signed_net) >= 10:
            try:
                from project.research.placebo import build_placebo_series, evaluate_specificity_lift
                _event_mask_series = pd.Series(mask, index=features.index)
                _regime_series = eval_context.regime_labels.reindex(features.index)
                _placebo_list = build_placebo_series(
                    _event_mask_series,
                    _regime_series,
                    n=50,
                    random_seed=42,
                )
                _placebo_returns: list[pd.Series] = []
                for _ps in _placebo_list:
                    _ps_mask = _ps.fillna(False).astype(bool)
                    _ps_events = fwd[_ps_mask]
                    if len(_ps_events) >= 4:
                        _placebo_returns.append(_ps_events * direction_sign)
                _spec_result = evaluate_specificity_lift(
                    signed_net,
                    _placebo_returns,
                    kelly_fraction=0.5,
                    min_specificity_lift_bps=0.3,
                )
                row["specificity_lift_bps"] = _spec_result.get("specificity_lift_bps", float("nan"))
                row["specificity_lift_pass"] = bool(_spec_result.get("pass", False))
            except Exception as _spec_exc:
                log.debug("specificity_lift skipped: %s", _spec_exc)

        # Net/gross aliases are load-bearing for phase-2 gating and diagnostics.
        row["mean_return_gross_bps"] = row.get("mean_return_gross_bps", round(gross_mean_bps, 4))
        row["mean_return_net_bps"] = row.get("mean_return_net_bps", round(net_mean_bps, 4))
        row["expected_cost_bps_per_trade"] = row.get("expected_cost_bps_per_trade", round(expected_cost_mean_bps, 4))
        row["t_stat_gross"] = row.get("t_stat_gross", round(t_stat_gross, 4))
        row["t_stat_net"] = row.get("t_stat_net", round(t_stat_net, 4))
        row["t_stat"] = row.get("t_stat", round(t_stat_net, 4))
        row["cost_adjusted_return_bps"] = row.get("cost_adjusted_return_bps", round(net_mean_bps, 4))
        row["after_cost_expectancy_bps"] = row.get("after_cost_expectancy_bps", round(net_mean_bps, 4))
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
    df.attrs["candidate_event_timestamps"] = (
        pd.DataFrame(event_timestamp_rows, columns=EVENT_TIMESTAMP_COLUMNS).drop_duplicates(
            subset=["hypothesis_id", "event_timestamp", "split_label"]
        )
        if event_timestamp_rows
        else pd.DataFrame(columns=EVENT_TIMESTAMP_COLUMNS)
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
    cost_bps: float,
    timestamps: pd.Series | None = None,
) -> tuple[list[dict], dict]:
    fold_metrics = []

    def _to_utc_ts(value: Any) -> pd.Timestamp:
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            return ts.tz_localize("UTC")
        return ts.tz_convert("UTC")

    for fold in folds:
        test_start = _to_utc_ts(fold.test_split.start)
        test_end = _to_utc_ts(fold.test_split.end)

        if timestamps is not None:
            ts = pd.to_datetime(timestamps, utc=True, errors="coerce")
            # `ts` is aligned by index to signed_returns/event_weights.
            mask = ts.notna() & (ts >= test_start) & (ts <= test_end)
        else:
            # Fallback: attempt to interpret the signed_returns index as a time axis.
            # Prefer epoch-ms if the dtype is integer, else do direct comparisons.
            if pd.api.types.is_integer_dtype(signed_returns.index):
                t_start = int(test_start.value // 10**6)
                t_end = int(test_end.value // 10**6)
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
