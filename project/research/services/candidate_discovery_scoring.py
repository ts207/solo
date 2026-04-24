from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Callable, Optional

import numpy as np
import pandas as pd
import yaml

from project.domain.compiled_registry import get_domain_registry
from project.research import discovery
from project.research.gating import (
    bh_adjust,
    build_event_return_frame,
)
from project.research.multiplicity import simes_p_value
from project.research.validation import (
    apply_multiple_testing,
    assign_split_labels,
    assign_test_families,
    estimate_effect_from_frame,
    resolve_split_scheme,
)
from project.research.validation.falsification import generate_placebo_events
from project.research.validation.purging import compute_event_windows
from project.research.validation.regime_tests import evaluate_by_regime


def _canonical_grouping_for_event(event_type: object) -> str:
    token = str(event_type or "").strip().upper()
    if not token:
        return ""
    spec = get_domain_registry().get_event(token)
    if spec is None:
        return token
    return spec.research_family or spec.canonical_family or spec.canonical_regime or spec.event_type


def _json_array(values: list[object]) -> str:
    return json.dumps(values, separators=(",", ":"))


def _split_labels(frame: pd.DataFrame) -> pd.Series:
    if frame.empty:
        return pd.Series(dtype=object)
    if "split_label" not in frame.columns:
        return pd.Series(["train"] * len(frame), index=frame.index, dtype=object)
    return frame["split_label"].astype(str).str.strip().str.lower()


def _evaluation_mask(split_labels: pd.Series) -> pd.Series:
    if split_labels.empty:
        return pd.Series(dtype=bool)
    evaluation_mask = split_labels.isin(["validation", "test"])
    if not bool(evaluation_mask.any()):
        evaluation_mask = split_labels != "train"
    if not bool(evaluation_mask.any()):
        evaluation_mask = pd.Series(False, index=split_labels.index)
    return evaluation_mask.astype(bool)


def _split_frame(frame: pd.DataFrame, label: str) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=list(frame.columns))
    labels = _split_labels(frame)
    return frame.loc[labels == str(label).strip().lower()].copy()


def _evaluation_only_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=list(frame.columns))
    labels = _split_labels(frame)
    mask = _evaluation_mask(labels)
    if not bool(mask.any()):
        return pd.DataFrame(columns=list(frame.columns))
    return frame.loc[mask].copy()


def _float_mean(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return 0.0
    series = pd.to_numeric(frame[column], errors="coerce").dropna()
    return float(series.mean()) if not series.empty else 0.0


def _numeric_series(frame: pd.DataFrame, column: str, default: float = np.nan) -> pd.Series:
    if frame.empty:
        return pd.Series(dtype=float)
    if column not in frame.columns:
        return pd.Series([default] * len(frame), index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce")


def _t_stat(frame: pd.DataFrame, column: str = "forward_return") -> float:
    if frame.empty or column not in frame.columns:
        return 0.0
    series = pd.to_numeric(frame[column], errors="coerce").dropna()
    if len(series) < 2:
        return 0.0
    std = float(series.std(ddof=1) or 0.0)
    if std <= 0.0:
        return 0.0
    return float(series.mean() / (std / np.sqrt(len(series))))


def _regime_labels(frame: pd.DataFrame) -> pd.Series:
    if frame.empty:
        return pd.Series(dtype=object)
    for column in (
        "regime",
        "vol_regime",
        "liquidity_state",
        "market_liquidity_state",
        "depth_state",
    ):
        if column in frame.columns:
            values = frame[column].astype("object").where(frame[column].notna(), "unknown")
            return values.astype(str)
    return pd.Series(["unknown"] * len(frame), index=frame.index, dtype=object)


def _random_entry_events(
    events_df: pd.DataFrame, features_df: Optional[pd.DataFrame]
) -> pd.DataFrame:
    if (
        events_df.empty
        or features_df is None
        or features_df.empty
        or "timestamp" not in features_df.columns
    ):
        return pd.DataFrame()
    sampled_ts = pd.to_datetime(features_df["timestamp"], utc=True, errors="coerce").dropna()
    sampled_ts = sampled_ts.drop_duplicates().sort_values()
    if sampled_ts.empty:
        return pd.DataFrame()
    n = min(len(events_df), len(sampled_ts))
    sampled = sampled_ts.sample(n=n, random_state=0).sort_values().reset_index(drop=True)
    out = events_df.iloc[:n].copy().reset_index(drop=True)
    if "timestamp" in out.columns:
        out["timestamp"] = sampled.values
    if "enter_ts" in out.columns:
        out["enter_ts"] = sampled.values
    return out


def _optional_float(value: object) -> float | None:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return None
    return float(numeric)


def _cache_token(value: object) -> object:
    numeric = _optional_float(value)
    return numeric if numeric is not None else None


def _placebo_pass(observed_frame: pd.DataFrame, placebo_frame: pd.DataFrame) -> bool:
    if not isinstance(observed_frame, pd.DataFrame) or not isinstance(placebo_frame, pd.DataFrame):
        return False
    obs_val = observed_frame.get("forward_return")
    plc_val = placebo_frame.get("forward_return")
    if obs_val is None or plc_val is None:
        return False
    observed = pd.to_numeric(obs_val, errors="coerce").dropna()
    placebo = pd.to_numeric(plc_val, errors="coerce").dropna()
    if observed.empty or placebo.empty:
        return False
    observed_mean = float(observed.mean())
    placebo_mean = float(placebo.mean())
    observed_scale = max(abs(observed_mean) * 0.5, 1e-4)
    if np.sign(observed_mean) != 0.0 and np.sign(placebo_mean) != np.sign(observed_mean):
        return True
    return bool(abs(placebo_mean) < observed_scale)


def _build_confirmatory_evidence(
    *,
    return_frame: pd.DataFrame,
    delayed_frame: pd.DataFrame,
    shift_placebo_frame: pd.DataFrame,
    random_placebo_frame: pd.DataFrame,
    direction_placebo_frame: pd.DataFrame,
) -> dict[str, object]:
    if return_frame.empty:
        return {
            "returns_oos_combined": "[]",
            "pnl_series": "[]",
            "returns_raw": "[]",
            "costs_bps_series": "[]",
            "timestamps": "[]",
            "fold_scores": "[]",
            "validation_fold_scores": "[]",
            "regime_counts": "{}",
            "funding_carry_eval_coverage": 0.0,
            "mean_funding_carry_bps": 0.0,
            "mean_train_return": 0.0,
            "mean_validation_return": 0.0,
            "mean_test_return": 0.0,
            "train_t_stat": 0.0,
            "val_t_stat": 0.0,
            "oos1_t_stat": 0.0,
            "test_t_stat": 0.0,
            "sign_consistency": 0.0,
            "stability_score": 0.0,
            "gate_stability": False,
            "gate_delay_robustness": False,
            "gate_delayed_entry_stress": False,
            "gate_regime_stability": False,
            "pass_shift_placebo": False,
            "pass_random_entry_placebo": False,
            "pass_direction_reversal_placebo": False,
            "control_pass_rate": 1.0,
        }

    labels = _split_labels(return_frame)
    eval_mask = _evaluation_mask(labels)
    eval_frame = return_frame.loc[eval_mask].copy()
    train_frame = _split_frame(return_frame, "train")
    validation_frame = _split_frame(return_frame, "validation")
    test_frame = _split_frame(return_frame, "test")
    confirmatory_frame = test_frame if not test_frame.empty else eval_frame

    returns_oos = _numeric_series(confirmatory_frame, "forward_return").dropna()
    pnl_series = returns_oos.tolist()
    returns_raw = (
        _numeric_series(confirmatory_frame, "forward_return_raw")
        if "forward_return_raw" in confirmatory_frame.columns
        else _numeric_series(confirmatory_frame, "forward_return")
    ).dropna()
    costs_bps = (
        _numeric_series(confirmatory_frame, "cost_return", default=0.0).fillna(0.0) * 1e4
    ).tolist()
    funding_present = (
        confirmatory_frame.get(
            "funding_carry_present", pd.Series(False, index=confirmatory_frame.index)
        )
        if not confirmatory_frame.empty
        else pd.Series(dtype=bool)
    )
    funding_present = funding_present.fillna(False).astype(bool)
    funding_carry = _numeric_series(
        confirmatory_frame, "funding_carry_return", default=0.0
    ).fillna(0.0)
    funding_carry_eval_coverage = float(funding_present.mean()) if len(funding_present) else 0.0
    mean_funding_carry_bps = (
        float(funding_carry[funding_present].mean() * 1e4) if bool(funding_present.any()) else 0.0
    )
    if "event_ts" in confirmatory_frame.columns:
        event_ts = pd.to_datetime(confirmatory_frame["event_ts"], utc=True, errors="coerce").dropna()
        timestamps = [ts.isoformat() for ts in event_ts.tolist()]
    else:
        timestamps = []

    split_means = {
        "train": _float_mean(train_frame, "forward_return"),
        "validation": _float_mean(validation_frame, "forward_return"),
        "test": _float_mean(test_frame, "forward_return"),
    }
    fold_scores = [
        float(value)
        for value in (split_means["train"], split_means["validation"], split_means["test"])
        if np.isfinite(value)
    ]
    validation_fold_scores = [
        float(value)
        for value in (split_means["validation"], split_means["test"])
        if np.isfinite(value)
    ]
    eval_mean = float(returns_oos.mean()) if not returns_oos.empty else 0.0
    base_sign = np.sign(eval_mean) if abs(eval_mean) > 1e-12 else 0.0
    non_zero_folds = [np.sign(value) for value in fold_scores if abs(float(value)) > 1e-12]
    sign_consistency = (
        float(np.mean([sign == base_sign for sign in non_zero_folds]))
        if non_zero_folds and base_sign != 0.0
        else 0.0
    )
    eval_std = float(returns_oos.std(ddof=1)) if len(returns_oos) > 1 else 0.0
    stability_score = (
        float(sign_consistency * (abs(eval_mean) / max(eval_std, 1e-8)))
        if abs(eval_mean) > 1e-12
        else 0.0
    )
    gate_stability = bool(sign_consistency >= 0.5 and abs(eval_mean) > 1e-12)

    delayed_labels = _split_labels(delayed_frame)
    delayed_eval = delayed_frame.loc[_evaluation_mask(delayed_labels)].copy()
    delayed_mean = _float_mean(delayed_eval, "forward_return")
    gate_delay_robustness = bool(
        abs(delayed_mean) > 1e-12
        and base_sign != 0.0
        and np.sign(delayed_mean) == base_sign
        and abs(delayed_mean) >= abs(eval_mean) * 0.25
    )

    regime_series = _regime_labels(confirmatory_frame)
    regime_frame = confirmatory_frame.copy()
    regime_frame["regime"] = regime_series.values if not regime_series.empty else []
    regime_info = evaluate_by_regime(regime_frame, value_col="forward_return", regime_col="regime")
    regime_counts = {
        str(regime): int(details.get("n_obs", 0))
        for regime, details in dict(regime_info.get("by_regime", {})).items()
    }
    worst_regime_estimate = float(regime_info.get("worst_regime_estimate", 0.0) or 0.0)
    gate_regime_stability = bool(
        not bool(regime_info.get("regime_flip_flag", False))
        and (
            base_sign == 0.0
            or abs(worst_regime_estimate) <= 1e-12
            or np.sign(worst_regime_estimate) == base_sign
        )
    )

    shift_pass = _placebo_pass(eval_frame, _evaluation_only_frame(shift_placebo_frame))
    random_pass = _placebo_pass(eval_frame, _evaluation_only_frame(random_placebo_frame))
    direction_pass = _placebo_pass(eval_frame, _evaluation_only_frame(direction_placebo_frame))
    control_pass_rate = float(np.mean([not shift_pass, not random_pass, not direction_pass]))

    return {
        "returns_oos_combined": _json_array([float(value) for value in returns_oos.tolist()]),
        "pnl_series": _json_array([float(value) for value in pnl_series]),
        "returns_raw": _json_array([float(value) for value in returns_raw.tolist()]),
        "costs_bps_series": _json_array([float(value) for value in costs_bps]),
        "timestamps": _json_array(timestamps),
        "fold_scores": _json_array([float(value) for value in fold_scores]),
        "validation_fold_scores": _json_array([float(value) for value in validation_fold_scores]),
        "regime_counts": json.dumps(regime_counts, sort_keys=True),
        "funding_carry_eval_coverage": funding_carry_eval_coverage,
        "mean_funding_carry_bps": mean_funding_carry_bps,
        "mean_train_return": split_means["train"],
        "mean_validation_return": split_means["validation"],
        "mean_test_return": split_means["test"],
        "train_t_stat": _t_stat(train_frame),
        "val_t_stat": _t_stat(validation_frame),
        "oos1_t_stat": _t_stat(eval_frame),
        "test_t_stat": _t_stat(test_frame),
        "sign_consistency": sign_consistency,
        "stability_score": stability_score,
        "gate_stability": gate_stability,
        "gate_delay_robustness": gate_delay_robustness,
        "gate_delayed_entry_stress": gate_delay_robustness,
        "gate_regime_stability": gate_regime_stability,
        "pass_shift_placebo": shift_pass,
        "pass_random_entry_placebo": random_pass,
        "pass_direction_reversal_placebo": direction_pass,
        "control_pass_rate": control_pass_rate,
    }


def split_and_score_candidates(
    candidates: pd.DataFrame,
    events_df: pd.DataFrame,
    *,
    horizon_bars: int,
    split_scheme_id: str,
    purge_bars: int,
    embargo_bars: int,
    bar_duration_minutes: int,
    features_df: Optional[pd.DataFrame] = None,
    entry_lag_bars: int = 1,
    shift_labels_k: int = 0,
    cost_estimate: Optional[object] = None,
    cost_coordinate: Optional[dict[str, object]] = None,
    alpha: float = 0.05,
    build_event_return_frame_fn: Callable[..., pd.DataFrame] = build_event_return_frame,
    estimate_effect_from_frame_fn: Callable[..., object] = estimate_effect_from_frame,
) -> pd.DataFrame:
    if candidates.empty:
        return candidates.copy()

    working = events_df.copy()
    features_input = features_df if features_df is not None else pd.DataFrame()
    resolved_split_scheme_id, train_frac, validation_frac = resolve_split_scheme(split_scheme_id)
    time_col = (
        "enter_ts"
        if "enter_ts" in working.columns
        else ("timestamp" if "timestamp" in working.columns else None)
    )
    cost_coordinate_payload = dict(cost_coordinate or {})
    resolved_cost_digest = str(cost_coordinate_payload.get("config_digest", "") or "")
    resolved_execution_model_json = "{}"
    execution_model_payload = cost_coordinate_payload.get("execution_model")
    if isinstance(execution_model_payload, dict):
        resolved_execution_model_json = json.dumps(execution_model_payload, sort_keys=True)
    after_cost_includes_funding_carry = bool(
        cost_coordinate_payload.get("after_cost_includes_funding_carry", False)
    )
    round_trip_cost_bps = float(
        cost_coordinate_payload.get(
            "round_trip_cost_bps",
            2.0 * float(cost_coordinate_payload.get("cost_bps", 0.0) or 0.0),
        )
        or 0.0
    )

    if time_col is None:
        out = candidates.copy()
        out["p_value"] = np.nan
        out["p_value_raw"] = np.nan
        out["p_value_for_fdr"] = np.nan
        out["estimate_bps"] = np.nan
        out["stderr_bps"] = np.nan
        out["ci_low_bps"] = np.nan
        out["ci_high_bps"] = np.nan
        out["n_obs"] = 0
        out["n_clusters"] = 0
        out["split_scheme_id"] = resolved_split_scheme_id
        out["cost_config_digest"] = resolved_cost_digest
        out["execution_model_json"] = resolved_execution_model_json
        out["after_cost_includes_funding_carry"] = bool(after_cost_includes_funding_carry)
        out["round_trip_cost_bps"] = float(round_trip_cost_bps)
        out["funding_carry_eval_coverage"] = 0.0
        out["mean_funding_carry_bps"] = 0.0
        return out

    split_plan_id = (
        f"TVT_{int(round(train_frac * 100))}_{int(round(validation_frac * 100))}_"
        f"{100 - int(round((train_frac + validation_frac) * 100))}"
    )
    current_split_plan_id = (
        str(working.get("split_plan_id", pd.Series(dtype=object)).astype(str).iloc[0])
        if "split_plan_id" in working.columns and not working.empty
        else ""
    )
    if (
        "split_label" not in working.columns
        or working["split_label"].isna().all()
        or current_split_plan_id != split_plan_id
    ):
        working = assign_split_labels(
            working,
            time_col=time_col,
            train_frac=train_frac,
            validation_frac=validation_frac,
            embargo_bars=int(embargo_bars),
            purge_bars=int(purge_bars),
            bar_duration_minutes=int(bar_duration_minutes),
            split_col="split_label",
        )

    out = candidates.copy()
    out["split_scheme_id"] = str(resolved_split_scheme_id)
    out["split_plan_id"] = split_plan_id
    out["purge_bars_used"] = int(purge_bars)
    out["embargo_bars_used"] = int(embargo_bars)
    out["bar_duration_minutes"] = int(bar_duration_minutes)
    out["resolved_train_frac"] = float(train_frac)
    out["resolved_validation_frac"] = float(validation_frac)
    out["cost_config_digest"] = resolved_cost_digest
    out["execution_model_json"] = resolved_execution_model_json
    out["after_cost_includes_funding_carry"] = bool(after_cost_includes_funding_carry)
    out["round_trip_cost_bps"] = float(round_trip_cost_bps)
    if cost_estimate is not None:
        out["resolved_cost_bps"] = float(cost_estimate.cost_bps)
        out["fee_bps_per_side"] = float(cost_estimate.fee_bps_per_side)
        out["slippage_bps_per_fill"] = float(cost_estimate.slippage_bps_per_fill)
        out["avg_dynamic_cost_bps"] = float(cost_estimate.avg_dynamic_cost_bps)
        out["cost_input_coverage"] = float(cost_estimate.cost_input_coverage)
        out["cost_model_valid"] = bool(cost_estimate.cost_model_valid)
        out["cost_model_source"] = str(cost_estimate.cost_model_source)
        out["cost_regime_multiplier"] = float(cost_estimate.regime_multiplier)
    else:
        out["resolved_cost_bps"] = 0.0
        out["fee_bps_per_side"] = 0.0
        out["slippage_bps_per_fill"] = 0.0
        out["avg_dynamic_cost_bps"] = 0.0
        out["cost_input_coverage"] = 0.0
        out["cost_model_valid"] = True
        out["cost_model_source"] = "static"
        out["cost_regime_multiplier"] = 1.0

    time_col = (
        "enter_ts"
        if "enter_ts" in working.columns
        else ("timestamp" if "timestamp" in working.columns else "timestamp")
    )
    shift_placebo_events = (
        generate_placebo_events(working, time_col=time_col, shift_bars=1)
        if not working.empty and time_col in working.columns
        else pd.DataFrame()
    )
    random_placebo_events = _random_entry_events(working, features_input)

    source_events = {
        "observed": working,
        "shift_placebo": shift_placebo_events,
        "random_placebo": random_placebo_events,
    }
    prepared_source_events_cache: dict[tuple[object, ...], pd.DataFrame] = {}
    frame_cache: dict[tuple[object, ...], pd.DataFrame] = {}

    def _prepare_source_events_for_frame(
        source_frame: pd.DataFrame,
        *,
        source_kind: str,
        row_horizon_bars: int,
        frame_entry_lag_bars: int,
    ) -> pd.DataFrame:
        cache_key = (source_kind, int(row_horizon_bars), int(frame_entry_lag_bars))
        cached = prepared_source_events_cache.get(cache_key)
        if cached is not None:
            return cached
        if source_frame.empty or time_col not in source_frame.columns:
            prepared = pd.DataFrame(columns=list(source_frame.columns))
        else:
            prepared = compute_event_windows(
                source_frame,
                time_col=time_col,
                horizon_bars=int(row_horizon_bars),
                entry_lag_bars=int(frame_entry_lag_bars),
                bar_duration_minutes=int(bar_duration_minutes),
            )
            prepared = assign_split_labels(
                prepared,
                time_col=time_col,
                train_frac=train_frac,
                validation_frac=validation_frac,
                embargo_bars=int(embargo_bars),
                purge_bars=int(purge_bars),
                bar_duration_minutes=int(bar_duration_minutes),
                split_col="split_label",
                event_window_start_col="event_window_start",
                event_window_end_col="event_window_end",
                purge_mode="time" if int(row_horizon_bars) > 1 else "rows",
            )
        prepared_source_events_cache[cache_key] = prepared
        return prepared

    def _frame_key(
        *,
        source_kind: str,
        rule: str,
        row_horizon: str,
        canonical_family: str,
        row_horizon_bars: int,
        frame_entry_lag_bars: int,
        stop_loss_bps: object,
        take_profit_bps: object,
        stop_loss_atr_multipliers: object,
        take_profit_atr_multipliers: object,
        direction_value: object,
    ) -> tuple[object, ...]:
        return (
            source_kind,
            rule,
            row_horizon,
            canonical_family,
            int(row_horizon_bars),
            int(frame_entry_lag_bars),
            int(shift_labels_k),
            _cache_token(stop_loss_bps),
            _cache_token(take_profit_bps),
            _cache_token(stop_loss_atr_multipliers),
            _cache_token(take_profit_atr_multipliers),
            _cache_token(direction_value),
            float(round_trip_cost_bps if cost_estimate is not None else 0.0),
        )

    def _build_frame(
        *,
        source_kind: str,
        rule: str,
        row_horizon: str,
        canonical_family: str,
        row_horizon_bars: int,
        frame_entry_lag_bars: int,
        stop_loss_bps: object,
        take_profit_bps: object,
        stop_loss_atr_multipliers: object,
        take_profit_atr_multipliers: object,
        direction_value: object,
    ) -> pd.DataFrame:
        cache_key = _frame_key(
            source_kind=source_kind,
            rule=rule,
            row_horizon=row_horizon,
            canonical_family=canonical_family,
            row_horizon_bars=row_horizon_bars,
            frame_entry_lag_bars=frame_entry_lag_bars,
            stop_loss_bps=stop_loss_bps,
            take_profit_bps=take_profit_bps,
            stop_loss_atr_multipliers=stop_loss_atr_multipliers,
            take_profit_atr_multipliers=take_profit_atr_multipliers,
            direction_value=direction_value,
        )
        cached = frame_cache.get(cache_key)
        if cached is not None:
            return cached

        kwargs = {
            "rule": rule,
            "horizon": row_horizon,
            "canonical_family": canonical_family,
            "shift_labels_k": int(shift_labels_k),
            "entry_lag_bars": int(frame_entry_lag_bars),
            "horizon_bars_override": int(row_horizon_bars),
            "stop_loss_bps": _optional_float(stop_loss_bps),
            "take_profit_bps": _optional_float(take_profit_bps),
            "stop_loss_atr_multipliers": _optional_float(stop_loss_atr_multipliers),
            "take_profit_atr_multipliers": _optional_float(take_profit_atr_multipliers),
            "cost_bps": float(round_trip_cost_bps if cost_estimate is not None else 0.0),
            "direction_override": pd.to_numeric(direction_value, errors="coerce"),
        }
        prepared_events = _prepare_source_events_for_frame(
            source_events[source_kind],
            source_kind=source_kind,
            row_horizon_bars=int(row_horizon_bars),
            frame_entry_lag_bars=int(frame_entry_lag_bars),
        )
        frame = build_event_return_frame_fn(
            prepared_events,
            features_input,
            **kwargs,
        )
        frame_cache[cache_key] = frame
        return frame

    for idx, row in out.iterrows():
        row_horizon_bars = int(
            pd.to_numeric(row.get("horizon_bars", horizon_bars), errors="coerce") or horizon_bars
        )
        row_horizon = str(row.get("horizon", discovery.bars_to_timeframe(row_horizon_bars)))
        rule = str(row.get("rule_template", "continuation"))
        canonical_family = _canonical_grouping_for_event(
            row.get("canonical_event_type", row.get("event_type", ""))
        )
        direction_value = row.get("direction")
        stop_loss_bps = row.get("stop_loss_bps")
        take_profit_bps = row.get("take_profit_bps")
        stop_loss_atr_multipliers = row.get("stop_loss_atr_multipliers")
        take_profit_atr_multipliers = row.get("take_profit_atr_multipliers")
        direction_numeric = pd.to_numeric(direction_value, errors="coerce")
        direction_placebo_value = (
            -direction_numeric if pd.notna(direction_numeric) else direction_numeric
        )

        return_frame = _build_frame(
            source_kind="observed",
            rule=rule,
            row_horizon=row_horizon,
            canonical_family=canonical_family,
            row_horizon_bars=row_horizon_bars,
            frame_entry_lag_bars=int(entry_lag_bars),
            stop_loss_bps=stop_loss_bps,
            take_profit_bps=take_profit_bps,
            stop_loss_atr_multipliers=stop_loss_atr_multipliers,
            take_profit_atr_multipliers=take_profit_atr_multipliers,
            direction_value=direction_value,
        )
        delayed_frame = _build_frame(
            source_kind="observed",
            rule=rule,
            row_horizon=row_horizon,
            canonical_family=canonical_family,
            row_horizon_bars=row_horizon_bars,
            frame_entry_lag_bars=int(entry_lag_bars) + 1,
            stop_loss_bps=stop_loss_bps,
            take_profit_bps=take_profit_bps,
            stop_loss_atr_multipliers=stop_loss_atr_multipliers,
            take_profit_atr_multipliers=take_profit_atr_multipliers,
            direction_value=direction_value,
        )
        shift_placebo_frame = _build_frame(
            source_kind="shift_placebo",
            rule=rule,
            row_horizon=row_horizon,
            canonical_family=canonical_family,
            row_horizon_bars=row_horizon_bars,
            frame_entry_lag_bars=int(entry_lag_bars),
            stop_loss_bps=stop_loss_bps,
            take_profit_bps=take_profit_bps,
            stop_loss_atr_multipliers=stop_loss_atr_multipliers,
            take_profit_atr_multipliers=take_profit_atr_multipliers,
            direction_value=direction_value,
        )
        random_placebo_frame = _build_frame(
            source_kind="random_placebo",
            rule=rule,
            row_horizon=row_horizon,
            canonical_family=canonical_family,
            row_horizon_bars=row_horizon_bars,
            frame_entry_lag_bars=int(entry_lag_bars),
            stop_loss_bps=stop_loss_bps,
            take_profit_bps=take_profit_bps,
            stop_loss_atr_multipliers=stop_loss_atr_multipliers,
            take_profit_atr_multipliers=take_profit_atr_multipliers,
            direction_value=direction_value,
        )
        direction_placebo_frame = _build_frame(
            source_kind="observed",
            rule=rule,
            row_horizon=row_horizon,
            canonical_family=canonical_family,
            row_horizon_bars=row_horizon_bars,
            frame_entry_lag_bars=int(entry_lag_bars),
            stop_loss_bps=stop_loss_bps,
            take_profit_bps=take_profit_bps,
            stop_loss_atr_multipliers=stop_loss_atr_multipliers,
            take_profit_atr_multipliers=take_profit_atr_multipliers,
            direction_value=direction_placebo_value,
        )
        if return_frame.empty:
            eval_frame = pd.DataFrame(columns=["forward_return", "cluster_day"])
            train_frame = pd.DataFrame(columns=["forward_return", "cluster_day"])
            split_labels = pd.Series(dtype=object)
        else:
            split_labels = _split_labels(return_frame)
            evaluation_mask = _evaluation_mask(split_labels)
            eval_frame = return_frame.loc[
                evaluation_mask, ["forward_return", "cluster_day"]
            ].dropna(subset=["forward_return"])
            train_frame = return_frame.loc[
                split_labels == "train", ["forward_return", "cluster_day"]
            ].dropna(subset=["forward_return"])
        estimate = estimate_effect_from_frame_fn(
            eval_frame,
            value_col="forward_return",
            cluster_col="cluster_day",
            alpha=alpha,
            use_bootstrap_ci=True,
            n_boot=400,
        )
        out.at[idx, "estimate"] = float(estimate.estimate)
        out.at[idx, "estimate_bps"] = float(estimate.estimate * 1e4)
        out.at[idx, "stderr"] = float(estimate.stderr)
        out.at[idx, "stderr_bps"] = float(estimate.stderr * 1e4)
        out.at[idx, "ci_low"] = float(estimate.ci_low)
        out.at[idx, "ci_high"] = float(estimate.ci_high)
        out.at[idx, "ci_low_bps"] = float(estimate.ci_low * 1e4)
        out.at[idx, "ci_high_bps"] = float(estimate.ci_high * 1e4)
        out.at[idx, "p_value"] = float(estimate.p_value_raw)
        out.at[idx, "p_value_raw"] = float(estimate.p_value_raw)
        out.at[idx, "p_value_for_fdr"] = float(estimate.p_value_raw)
        out.at[idx, "n_obs"] = int(estimate.n_obs)
        out.at[idx, "sample_size"] = int(estimate.n_obs)
        out.at[idx, "n_clusters"] = int(estimate.n_clusters)
        out.at[idx, "estimation_method"] = str(estimate.method)
        out.at[idx, "cluster_col"] = str(estimate.cluster_col or "cluster_day")
        out.at[idx, "effect_split_basis"] = (
            "validation_test" if bool(split_labels.isin(["validation", "test"]).any()) else "none"
        )
        out.at[idx, "validation_n_obs"] = int((split_labels == "validation").sum())
        out.at[idx, "test_n_obs"] = int((split_labels == "test").sum())
        out.at[idx, "train_n_obs"] = int((split_labels == "train").sum())
        out.at[idx, "expectancy"] = (
            float(train_frame["forward_return"].mean()) if not train_frame.empty else 0.0
        )
        out.at[idx, "expectancy_bps"] = float(out.at[idx, "expectancy"] * 1e4)
        out.at[idx, "t_stat"] = (
            float(
                eval_frame["forward_return"].mean()
                / (eval_frame["forward_return"].std(ddof=1) / np.sqrt(len(eval_frame)))
            )
            if len(eval_frame) > 1 and float(eval_frame["forward_return"].std(ddof=1) or 0.0) > 0.0
            else 0.0
        )
        confirmatory = _build_confirmatory_evidence(
            return_frame=return_frame,
            delayed_frame=delayed_frame,
            shift_placebo_frame=shift_placebo_frame,
            random_placebo_frame=random_placebo_frame,
            direction_placebo_frame=direction_placebo_frame,
        )
        for column, value in confirmatory.items():
            out.at[idx, column] = value
    return out


def apply_validation_multiple_testing(candidates_df: pd.DataFrame) -> pd.DataFrame:
    if candidates_df.empty:
        return candidates_df.copy()
    out = candidates_df.copy()
    source_events = out.get(
        "canonical_event_type", out.get("event_type", pd.Series("", index=out.index))
    )
    out["primary_event_id"] = source_events.astype(str).str.strip().str.upper()
    out["event_family"] = source_events.map(_canonical_grouping_for_event)
    out["compat_event_family"] = (
        out.get("event_family", pd.Series("", index=out.index)).astype(str).str.strip().str.upper()
    )
    out["correction_frontier_id"] = (
        out.get("primary_event_id", pd.Series("", index=out.index)).astype(str).str.strip()
        + "::"
        + out.get("horizon", pd.Series("", index=out.index)).astype(str).str.strip()
    )
    out = assign_test_families(
        out,
        family_cols=["primary_event_id", "horizon"],
        out_col="correction_family_id",
    )
    out = apply_multiple_testing(
        out,
        p_col="p_value_raw",
        family_col="correction_family_id",
        method="bh",
        out_col="p_value_adj",
    )
    out = apply_multiple_testing(
        out,
        p_col="p_value_raw",
        family_col="correction_family_id",
        method="by",
        out_col="p_value_adj_by",
    )
    out = apply_multiple_testing(
        out,
        p_col="p_value_raw",
        family_col="correction_family_id",
        method="holm",
        out_col="p_value_adj_holm",
    )
    out["correction_method"] = "bh"
    out["q_value"] = pd.to_numeric(out.get("p_value_adj", np.nan), errors="coerce")
    out["q_value_by"] = pd.to_numeric(out.get("p_value_adj_by", np.nan), errors="coerce")
    out["q_value_family"] = out["q_value"]
    out["family_cluster_id"] = (
        out.get("symbol", pd.Series("", index=out.index)).astype(str).str.strip().str.upper()
        + "_"
        + out.get("event_type", pd.Series("", index=out.index)).astype(str).str.strip()
        + "_"
        + out.get("horizon", pd.Series("", index=out.index)).astype(str).str.strip()
        + "_"
        + out.get("state_id", pd.Series("", index=out.index)).astype(str).str.strip()
    )
    p_vals = pd.to_numeric(out.get("p_value_raw", np.nan), errors="coerce")
    eligible = out.loc[p_vals.notna()].copy()
    if not eligible.empty:
        cluster_simes = (
            eligible.groupby("family_cluster_id")["p_value_raw"]
            .apply(lambda s: simes_p_value(pd.to_numeric(s, errors="coerce")))
            .rename("p_value_cluster")
            .reset_index()
        )
        cluster_simes["q_value_cluster"] = bh_adjust(
            cluster_simes["p_value_cluster"].fillna(1.0).to_numpy()
        )
        p_mapping = dict(zip(cluster_simes["family_cluster_id"], cluster_simes["p_value_cluster"]))
        q_mapping = dict(zip(cluster_simes["family_cluster_id"], cluster_simes["q_value_cluster"]))
        out["p_value_cluster"] = out["family_cluster_id"].map(p_mapping)
        out["q_value_cluster"] = out["family_cluster_id"].map(q_mapping)
    else:
        out["p_value_cluster"] = np.nan
        out["q_value_cluster"] = np.nan
    out["is_discovery"] = out["q_value"].fillna(1.0) <= 0.10
    out["is_discovery_by"] = out["q_value_by"].fillna(1.0) <= 0.10
    out["is_discovery_cluster"] = out["q_value_cluster"].fillna(1.0) <= 0.10
    out["gate_multiplicity"] = out["is_discovery"].astype(bool)
    return out


def _candidate_run_id_from_phase2_path(path: Path) -> str:
    parts = list(path.parts)
    if "phase2" in parts:
        idx = parts.index("phase2")
        if idx + 1 < len(parts):
            return str(parts[idx + 1])
    return ""


def _historical_phase2_candidate_paths(data_root: Path, *, current_run_id: str) -> list[Path]:
    reports_root = Path(data_root) / "reports" / "phase2"
    if not reports_root.exists():
        return []
    discovered_by_run: dict[str, Path] = {}
    patterns = ["*/phase2_candidates.parquet", "*/search_engine/phase2_candidates.parquet"]
    for pattern in patterns:
        for path in reports_root.glob(pattern):
            run_id = _candidate_run_id_from_phase2_path(path)
            if not run_id or run_id == str(current_run_id):
                continue
            discovered_by_run.setdefault(run_id, path)
    return sorted(discovered_by_run.values())


def apply_historical_frontier_multiple_testing(
    candidates_df: pd.DataFrame,
    *,
    data_root: Path,
    current_run_id: str,
) -> pd.DataFrame:
    if candidates_df.empty:
        return candidates_df.copy()
    out = candidates_df.copy()
    if "event_family" not in out.columns:
        source_events = out.get(
            "canonical_event_type", out.get("event_type", pd.Series("", index=out.index))
        )
        out["event_family"] = source_events.map(_canonical_grouping_for_event)
    if "primary_event_id" not in out.columns:
        source_events = out.get(
            "canonical_event_type", out.get("event_type", pd.Series("", index=out.index))
        )
        out["primary_event_id"] = source_events.astype(str).str.strip().str.upper()
    if "compat_event_family" not in out.columns:
        out["compat_event_family"] = (
            out.get("event_family", pd.Series("", index=out.index))
            .astype(str)
            .str.strip()
            .str.upper()
        )
    if "correction_frontier_id" not in out.columns:
        out["correction_frontier_id"] = (
            out.get("primary_event_id", pd.Series("", index=out.index)).astype(str).str.strip()
            + "::"
            + out.get("horizon", pd.Series("", index=out.index)).astype(str).str.strip()
        )
    out["historical_frontier_test_count"] = 0
    out["q_value_historical_frontier"] = pd.to_numeric(out.get("q_value", np.nan), errors="coerce")
    out["gate_multiplicity_frontier"] = out.get("gate_multiplicity", False)

    historical_parts: list[pd.DataFrame] = []
    for path in _historical_phase2_candidate_paths(
        Path(data_root), current_run_id=str(current_run_id)
    ):
        try:
            hist = pd.read_parquet(path)
        except Exception:
            continue
        if hist.empty or "p_value_raw" not in hist.columns:
            continue
        if "event_family" not in hist.columns:
            source_events = hist.get(
                "canonical_event_type", hist.get("event_type", pd.Series("", index=hist.index))
            )
            hist["event_family"] = source_events.map(_canonical_grouping_for_event)
        if "primary_event_id" not in hist.columns:
            source_events = hist.get(
                "canonical_event_type", hist.get("event_type", pd.Series("", index=hist.index))
            )
            hist["primary_event_id"] = source_events.astype(str).str.strip().str.upper()
        hist["correction_frontier_id"] = (
            hist.get("primary_event_id", pd.Series("", index=hist.index)).astype(str).str.strip()
            + "::"
            + hist.get("horizon", pd.Series("", index=hist.index)).astype(str).str.strip()
        )
        hist = hist[["correction_frontier_id", "p_value_raw"]].copy()
        hist["p_value_raw"] = pd.to_numeric(hist["p_value_raw"], errors="coerce")
        hist = hist.dropna(subset=["p_value_raw"])
        if not hist.empty:
            historical_parts.append(hist)

    if not historical_parts:
        return out

    historical = pd.concat(historical_parts, ignore_index=True)
    current_p = pd.to_numeric(out.get("p_value_raw", np.nan), errors="coerce")
    if current_p.notna().sum() == 0:
        return out

    for frontier_id, group in out.groupby("correction_frontier_id"):
        current_idx = list(group.index)
        current_vals = pd.to_numeric(group.get("p_value_raw"), errors="coerce")
        current_vals = current_vals.dropna()
        hist_vals = pd.to_numeric(
            historical.loc[historical["correction_frontier_id"] == frontier_id, "p_value_raw"],
            errors="coerce",
        ).dropna()
        if current_vals.empty:
            continue
        pool = pd.concat(
            [hist_vals.reset_index(drop=True), current_vals.reset_index(drop=True)],
            ignore_index=True,
        )
        q_pool = bh_adjust(pool.fillna(1.0).to_numpy())
        q_current = q_pool[len(hist_vals) :]
        out.loc[current_idx, "historical_frontier_test_count"] = int(len(pool))
        out.loc[current_vals.index, "q_value_historical_frontier"] = q_current
        out.loc[current_vals.index, "gate_multiplicity_frontier"] = q_current <= 0.10

    local_q = pd.to_numeric(out.get("q_value", np.nan), errors="coerce")
    frontier_q = pd.to_numeric(out.get("q_value_historical_frontier", np.nan), errors="coerce")
    combined_q = np.where(
        local_q.notna() & frontier_q.notna(),
        np.maximum(local_q, frontier_q),
        np.where(frontier_q.notna(), frontier_q, local_q),
    )
    out["q_value_run_local"] = local_q
    out["q_value"] = combined_q
    out["gate_multiplicity_run_local"] = out.get("gate_multiplicity", False)
    out["gate_multiplicity"] = pd.Series(combined_q, index=out.index).fillna(1.0) <= 0.10
    out["is_discovery"] = out["gate_multiplicity"].astype(bool)
    out["correction_scope_policy"] = "historical_frontier_bh"
    return out


# --- Phase 2 V2 Scoring Components ---


def _row_get(row: object, key: str, default: object = np.nan) -> object:
    getter = getattr(row, "get", None)
    if callable(getter):
        return getter(key, default)
    try:
        return row[key]  # type: ignore[index]
    except Exception:
        return default


def _row_has(row: object, key: str) -> bool:
    try:
        return key in row  # type: ignore[operator]
    except Exception:
        return False


def score_falsification_precheck(row: object) -> tuple[float, list[str]]:
    penalty = 0.0
    flags = []

    mean_bps = _row_get(row, "mean_return_bps", np.nan)
    placebo_shift = _row_get(row, "placebo_shift_effect", np.nan)
    null_ratio = _row_get(row, "null_strength_ratio", np.nan)

    if pd.notna(mean_bps) and pd.notna(placebo_shift):
        if abs(placebo_shift) > abs(mean_bps):
            penalty += 2.0
            flags.append("placebo_exceeds_main")
        elif pd.notna(null_ratio) and null_ratio < 2.0:
            penalty += 1.0
            flags.append("weak_null_strength")

    reversal = _row_get(row, "direction_reversal_effect", np.nan)
    if pd.notna(reversal) and pd.notna(mean_bps) and abs(mean_bps) > 1e-10:
        # Guard: np.sign(0) == 0, which would falsely match any zero reversal
        if np.sign(mean_bps) != 0 and np.sign(reversal) == np.sign(mean_bps):
            penalty += 1.5
            flags.append("asymmetric_reversal_failure")

    return penalty, flags


def score_tradability_precheck(row: object, config: dict) -> tuple[float, list[str]]:
    score = 0.0
    flags = []

    survival_ratio = _row_get(row, "cost_survival_ratio", np.nan)
    if pd.notna(survival_ratio):
        if survival_ratio < 0.5:
            score -= 1.0
            flags.append("poor_cost_survival")
        elif survival_ratio > 1.5:
            score += 1.0

    turnover = _row_get(row, "turnover_proxy", np.nan)
    turnover_threshold = config.get("default_turnover_penalty_thresh", 0.8)
    if pd.notna(turnover) and turnover > turnover_threshold:
        score -= 1.0
        flags.append("high_turnover_penalty")

    coverage = _row_get(row, "coverage_ratio", np.nan)
    coverage_threshold = config.get("default_coverage_thresh", 0.01)
    if pd.notna(coverage) and coverage < coverage_threshold:
        score -= 0.5
        flags.append("low_coverage_penalty")

    return score, flags


def score_novelty_precheck(
    row: object, overlap_context: dict
) -> tuple[float, float, str, list[str]]:
    key = (
        str(_row_get(row, "event_family_key", "")),
        str(_row_get(row, "template_family_key", "")),
        str(_row_get(row, "direction_key", "")),
        str(_row_get(row, "horizon_bucket", "")),
    )
    cluster_id = "|".join(key)

    counts = overlap_context.get(cluster_id, 1)

    overlap_penalty = 0.0
    novelty_score = 1.0
    flags = []

    if counts > 3:
        overlap_penalty = 2.0
        novelty_score = 0.0
        flags.append("high_structural_overlap")
    elif counts > 1:
        overlap_penalty = 0.5
        novelty_score = 0.5
        flags.append("structural_duplicate_present")

    return novelty_score, overlap_penalty, cluster_id, flags


def score_support_component(row: object, config: dict) -> tuple[float, list[str]]:
    score = 0.0
    flags = []

    regime_support = _row_get(row, "regime_support_ratio", np.nan)
    min_support = config.get("min_acceptable_regime_support_ratio", 0.5)

    if pd.notna(regime_support):
        if regime_support < min_support:
            score -= 1.0
            flags.append("fragile_regime_support")
        else:
            score += 1.0  # symmetric with penalty: bonus is flat +1.0, not continuous

    return score, flags


def score_significance_component(row: object) -> float:
    t_stat = _row_get(row, "t_stat", np.nan)
    if pd.isna(t_stat):
        return 0.0
    return float(np.clip(abs(t_stat) / 2.0, 0.0, 3.0))


def score_fold_stability_precheck(row: object, config: dict) -> tuple[float, float, list[str]]:
    flags = []
    stability_penalty = 0.0
    evidence_bonus = 0.0

    fold_valid_count = _row_get(row, "fold_valid_count", np.nan)
    if not _row_has(row, "fold_valid_count") or pd.isna(fold_valid_count):
        return 0.0, 0.0, []

    valid_folds = int(fold_valid_count)
    if valid_folds < 1:
        flags.append("no_valid_oos_folds")
        return 0.0, 2.5, flags

    sign_consistency = float(_row_get(row, "fold_sign_consistency", 0.0))
    fail_ratio = float(_row_get(row, "fold_fail_ratio", 1.0))

    # 1. Sign Consistency (Bonus)
    if sign_consistency >= 0.8:
        evidence_bonus += 1.0
    elif sign_consistency < 0.5:
        stability_penalty += 1.0
        flags.append("unstable_sign_across_folds")

    # 2. Fail Ratio Penalty
    if fail_ratio >= 0.5:
        stability_penalty += 1.5
        flags.append("high_fold_fail_ratio")

    # 3. Validation Fold Concentration Check — penalize, not just flag
    if valid_folds < 3:
        stability_penalty += 1.0
        flags.append("insufficient_valid_folds")

    return evidence_bonus, stability_penalty, flags


def build_discovery_quality_score(row: object, overlap_context: dict, config: dict) -> dict:
    falsification_penalty, falsification_flags = score_falsification_precheck(row)
    tradability_score, tradability_flags = score_tradability_precheck(row, config)
    novelty_score, overlap_penalty, cluster_id, overlap_flags = score_novelty_precheck(
        row, overlap_context
    )
    support_score, support_flags = score_support_component(row, config)
    significance_score = score_significance_component(row)
    fold_bonus, fold_penalty, fold_flags = score_fold_stability_precheck(row, config)

    f_weight = config.get("falsification_weight", 1.0)
    t_weight = config.get("tradability_weight", 1.0)
    n_weight = config.get("novelty_weight", 1.0)
    o_weight = config.get("overlap_penalty_weight", 1.0)
    s_weight = config.get("fragility_penalty_weight", 1.0)

    fragility_penalty = 0.0
    if "fragile_regime_support" in support_flags:
        fragility_penalty = 1.0

    combined_score = (
        significance_score
        + support_score
        + fold_bonus
        + (tradability_score * t_weight)
        + (novelty_score * n_weight)
        - (falsification_penalty * f_weight)
        - (overlap_penalty * o_weight)
        - (fragility_penalty * s_weight)
        - fold_penalty
    )

    demotion_reasons = (
        falsification_flags + tradability_flags + overlap_flags + support_flags + fold_flags
    )
    rank_primary_reason = demotion_reasons[0] if demotion_reasons else "strong_baseline"
    if combined_score > 2.0 and not demotion_reasons:
        rank_primary_reason = "high_quality_discovery"

    return {
        "falsification_component": falsification_penalty,
        "tradability_component": tradability_score,
        "novelty_component": novelty_score,
        "support_component": support_score,
        "significance_component": significance_score,
        "fold_stability_bonus": fold_bonus,
        "fold_stability_penalty": fold_penalty,
        "overlap_penalty": overlap_penalty,
        "fragility_penalty": fragility_penalty,
        "discovery_quality_score": float(combined_score),
        "overlap_cluster_id": cluster_id,
        "duplicate_like_flag": overlap_penalty > 0,
        "falsification_reason": "|".join(falsification_flags),
        "tradability_reason": "|".join(tradability_flags),
        "overlap_reason": "|".join(overlap_flags),
        "rank_primary_reason": rank_primary_reason,
        "demotion_reason_codes": "|".join(demotion_reasons),
    }


def annotate_discovery_v2_scores(candidates: pd.DataFrame, config: dict) -> pd.DataFrame:
    if candidates.empty:
        return candidates

    out = candidates.copy()

    cluster_keys = (
        out.get("event_family_key", pd.Series("", index=out.index)).astype(str)
        + "|"
        + out.get("template_family_key", pd.Series("", index=out.index)).astype(str)
        + "|"
        + out.get("direction_key", pd.Series("", index=out.index)).astype(str)
        + "|"
        + out.get("horizon_bucket", pd.Series("", index=out.index)).astype(str)
    )
    overlap_context = cluster_keys.value_counts(dropna=False).to_dict()

    v2_metrics = []
    for row in out.itertuples(index=False):
        v2_metrics.append(build_discovery_quality_score(row._asdict(), overlap_context, config))

    v2_df = pd.DataFrame(v2_metrics, index=out.index)

    for col in v2_df.columns:
        out[col] = v2_df[col]

    return out


# ---------------------------------------------------------------------------
# Phase 3 — Concept-Ledger Aware Multiplicity Correction
# ---------------------------------------------------------------------------

_log = logging.getLogger(__name__)

_DEFAULT_LEDGER_CONFIG: dict = {
    "enabled": False,
    "lookback_days": 365,
    "recent_window_days": 90,
    "lineage_mode": "v1",
    "max_penalty": 3.0,
    "min_prior_tests_for_penalty": 3,
    "crowded_lineage_threshold": 20,
    "repeated_family_failure_threshold": 0.90,
    "low_family_success_threshold": 0.10,
    "low_family_success_min_tests": 5,
    "high_recent_test_density_threshold": 10,
}


def load_ledger_config(data_root: Path | None = None) -> dict:
    """Load Phase 3 ledger config from ``project/configs/discovery_ledger.yaml``.

    Falls back to :data:`_DEFAULT_LEDGER_CONFIG` when the file is absent or
    cannot be parsed.  Always returns a complete dict (every key present).
    """
    config = dict(_DEFAULT_LEDGER_CONFIG)
    try:
        if data_root is not None:
            # Try repo-relative config first
            candidate_paths = [
                Path(data_root).parent / "project" / "configs" / "discovery_ledger.yaml",
                Path(data_root) / "project" / "configs" / "discovery_ledger.yaml",
            ]
        else:
            candidate_paths = []

        # Also try path relative to this file's repo root
        _this_dir = Path(__file__).resolve().parent
        repo_candidate = (
            _this_dir.parent.parent.parent / "project" / "configs" / "discovery_ledger.yaml"
        )
        candidate_paths.append(repo_candidate)

        for cfg_path in candidate_paths:
            if cfg_path.exists():
                with cfg_path.open("r", encoding="utf-8") as fh:
                    raw = yaml.safe_load(fh)
                if isinstance(raw, dict):
                    inner = raw.get("discovery_scoring", {}).get("ledger_adjustment", {})
                    if isinstance(inner, dict):
                        config.update(inner)
                break
    except Exception as exc:
        _log.debug("Could not load discovery_ledger.yaml: %s", exc)

    return config


def is_ledger_scoring_enabled(data_root: Path | None = None) -> bool:
    """Return True when ledger-adjusted scoring is enabled in config."""
    return bool(load_ledger_config(data_root).get("enabled", False))


def attach_ledger_lineage_keys(candidates_df: pd.DataFrame) -> pd.DataFrame:
    """Add ``concept_lineage_key`` column to *candidates_df*.

    Purely additive — does not modify any existing column.  Safe to call
    on an empty DataFrame.
    """
    if candidates_df is None or candidates_df.empty:
        return candidates_df.copy() if candidates_df is not None else pd.DataFrame()

    from project.research.knowledge.concept_ledger import build_concept_lineage_key

    out = candidates_df.copy()
    # Use any pre-computed event_family column to improve key quality
    if "event_family" not in out.columns:
        if "canonical_event_type" in out.columns or "event_type" in out.columns:
            source = out.get(
                "canonical_event_type",
                out.get("event_type", pd.Series("", index=out.index)),
            )
            out["event_family"] = source.map(_canonical_grouping_for_event)

    keys = []
    for _, row in out.iterrows():
        keys.append(build_concept_lineage_key(dict(row)))
    out["concept_lineage_key"] = keys
    return out


def apply_ledger_multiplicity_correction(
    candidates_df: pd.DataFrame,
    *,
    data_root: Path,
    current_run_id: str,
    config: dict | None = None,
) -> pd.DataFrame:
    """Attach ledger-derived burden fields and compute ledger-adjusted scores.

    When the ledger adjustment is disabled (default), returns *candidates_df*
    unchanged except that ``concept_lineage_key`` is always attached (ledger
    writes need it).

    When enabled, appends these columns (all prefixed ``ledger_``):
        ledger_prior_test_count, ledger_prior_discovery_count,
        ledger_prior_promotion_count, ledger_recent_test_count,
        ledger_recent_failure_count, ledger_empirical_success_rate,
        ledger_family_density, ledger_multiplicity_penalty,
        ledger_adjusted_q_value, ledger_evidence_score,
        discovery_quality_score_v3.

    Existing ``q_value`` and ``gate_multiplicity`` are never overwritten.
    """
    if candidates_df is None or candidates_df.empty:
        return candidates_df.copy() if candidates_df is not None else pd.DataFrame()

    resolved_config = config if config is not None else load_ledger_config(data_root)
    enabled = bool(resolved_config.get("enabled", False))

    # Always attach lineage keys so ledger writes work regardless of flag
    out = attach_ledger_lineage_keys(candidates_df)

    if not enabled:
        return out

    from project.research.knowledge.concept_ledger import (
        default_ledger_path,
        load_concept_ledger,
        summarize_lineage_history,
    )

    ledger_path = default_ledger_path(data_root)
    ledger = load_concept_ledger(ledger_path, raise_on_error=True)

    # Filter out records from the *current* run so we don't count ourselves
    if not ledger.empty and "run_id" in ledger.columns:
        ledger = ledger[ledger["run_id"].astype(str) != str(current_run_id)].copy()

    lookback_days = int(resolved_config.get("lookback_days", 365))
    recent_window_days = int(resolved_config.get("recent_window_days", 90))
    max_penalty = float(resolved_config.get("max_penalty", 3.0))
    min_prior = int(resolved_config.get("min_prior_tests_for_penalty", 3))
    crowded_threshold = int(resolved_config.get("crowded_lineage_threshold", 20))
    fail_threshold = float(resolved_config.get("repeated_family_failure_threshold", 0.90))
    low_success_thr = float(resolved_config.get("low_family_success_threshold", 0.10))
    low_success_min = int(resolved_config.get("low_family_success_min_tests", 5))
    high_recent_thr = int(resolved_config.get("high_recent_test_density_threshold", 10))

    unique_keys = list(out["concept_lineage_key"].dropna().unique())
    summary = summarize_lineage_history(
        ledger,
        unique_keys,
        lookback_days=lookback_days,
        recent_window_days=recent_window_days,
    )
    summary_map: dict[str, dict] = {
        row["concept_lineage_key"]: row for row in summary.to_dict(orient="records")
    }

    # Descriptor columns
    desc_cols = [
        "ledger_prior_test_count",
        "ledger_prior_discovery_count",
        "ledger_prior_promotion_count",
        "ledger_recent_test_count",
        "ledger_recent_failure_count",
        "ledger_empirical_success_rate",
        "ledger_family_density",
    ]
    for col in desc_cols:
        out[col] = 0

    for col in desc_cols:
        if col == "ledger_empirical_success_rate":
            out[col] = out[col].astype(float)

    for col in desc_cols:
        out[col] = out["concept_lineage_key"].map(
            lambda k, col=col: summary_map.get(str(k), {}).get(col, 0)
        )

    # Scoring columns
    penalties: list[float] = []
    adj_q_vals: list[float] = []
    evidence_scores: list[float] = []
    v3_scores: list[float] = []

    for _, row in out.iterrows():
        prior_count = int(row.get("ledger_prior_test_count", 0) or 0)
        recent_count = int(row.get("ledger_recent_test_count", 0) or 0)
        recent_fail = int(row.get("ledger_recent_failure_count", 0) or 0)
        success_rate = float(row.get("ledger_empirical_success_rate", 0.0) or 0.0)

        # Compute penalty
        if prior_count < min_prior:
            penalty = 0.0
        else:
            empirical_fail_rate = 1.0 - success_rate
            recent_pressure = float(recent_fail) / float(max(recent_count, 1))
            # Normalize log1p to [0,1] over ~100 tests; unbounded growth was wrong.
            # Coefficients sum to 1.0: fail_rate 0.5, recent_pressure 0.3, age 0.2.
            # success_rate credit removed — it's already captured via (1 - fail_rate).
            _log_norm = float(np.log1p(100))
            normalized_age = min(float(np.log1p(prior_count)) / _log_norm, 1.0)
            raw_penalty = 0.5 * empirical_fail_rate + 0.3 * recent_pressure + 0.2 * normalized_age
            penalty = float(np.clip(raw_penalty * max_penalty, 0.0, max_penalty))

        # Adjusted q-value
        q_raw = pd.to_numeric(row.get("q_value", np.nan), errors="coerce")
        if pd.notna(q_raw):
            adj_q = float(np.clip(float(q_raw) * (1.0 + penalty), 0.0, 1.0))
        else:
            adj_q = float("nan")

        # Evidence score (v3)
        base_score = pd.to_numeric(row.get("discovery_quality_score", np.nan), errors="coerce")
        if pd.notna(base_score):
            ev_score = float(base_score) - penalty
            v3 = ev_score
        else:
            ev_score = float("nan")
            v3 = float("nan")

        penalties.append(penalty)
        adj_q_vals.append(adj_q)
        evidence_scores.append(ev_score)
        v3_scores.append(v3)

    out["ledger_multiplicity_penalty"] = penalties
    out["ledger_adjusted_q_value"] = adj_q_vals
    out["ledger_evidence_score"] = evidence_scores
    out["discovery_quality_score_v3"] = v3_scores

    # Reason codes — append ledger codes to existing demotion_reason_codes
    ledger_reason_parts: list[str] = []
    for idx, row in out.iterrows():
        prior_count = int(row.get("ledger_prior_test_count", 0) or 0)
        recent_count = int(row.get("ledger_recent_test_count", 0) or 0)
        success_rate = float(row.get("ledger_empirical_success_rate", 0.0) or 0.0)
        empirical_fail_rate = 1.0 - success_rate
        penalty = float(row.get("ledger_multiplicity_penalty", 0.0) or 0.0)

        codes: list[str] = []
        if prior_count >= crowded_threshold:
            codes.append("crowded_lineage")
        if empirical_fail_rate >= fail_threshold and prior_count >= min_prior:
            codes.append("repeated_family_failure")
        if (
            success_rate < low_success_thr
            and prior_count >= low_success_min
            and prior_count >= min_prior
        ):
            codes.append("low_empirical_family_success")
        if recent_count > high_recent_thr:
            codes.append("high_recent_test_density")
        if penalty > 0.0:
            codes.append("ledger_penalty_applied")

        ledger_reason_parts.append("|".join(codes))

    # Merge into existing demotion_reason_codes column if present
    if "demotion_reason_codes" in out.columns:
        existing_codes = out["demotion_reason_codes"].fillna("").astype(str)
        merged = []
        for existing, new in zip(existing_codes.tolist(), ledger_reason_parts):
            parts = [p for p in [existing.strip(), new.strip()] if p]
            merged.append("|".join(parts))
        out["demotion_reason_codes"] = merged
    else:
        out["demotion_reason_codes"] = ledger_reason_parts

    _log.info(
        "Ledger correction applied: %d candidates, %d with non-zero penalty",
        len(out),
        int((out["ledger_multiplicity_penalty"] > 0).sum()),
    )
    return out
