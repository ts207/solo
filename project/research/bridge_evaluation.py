from __future__ import annotations

import json
import logging
from typing import Any

import numpy as np
import pandas as pd

from project.core.coercion import safe_float, safe_int
from project.research.helpers.viability import (
    evaluate_low_capital_viability,
    evaluate_retail_constraints,
)

LOGGER = logging.getLogger(__name__)


def _row_float(row: pd.Series, key: str, default: float) -> float:
    value = row.get(key)
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return float(default)
    coerced = safe_float(value, default)
    return float(default if coerced is None else coerced)


def _row_int(row: pd.Series, key: str, default: int) -> int:
    value = row.get(key)
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return int(default)
    coerced = safe_int(value, default)
    return int(default if coerced is None else coerced)


def _series_from_row_value(value: Any) -> pd.Series:
    if value is None:
        return pd.Series(dtype=float)
    if isinstance(value, pd.Series):
        return pd.to_numeric(value, errors="coerce").dropna()
    if isinstance(value, np.ndarray):
        return pd.to_numeric(pd.Series(value.tolist()), errors="coerce").dropna()
    if isinstance(value, (list, tuple)):
        return pd.to_numeric(pd.Series(list(value)), errors="coerce").dropna()
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return pd.Series(dtype=float)
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return pd.Series(dtype=float)
        return _series_from_row_value(parsed)
    return pd.Series(dtype=float)


def _extract_row_series(row: pd.Series, candidates: list[str]) -> pd.Series:
    for key in candidates:
        if key in row and row.get(key) is not None:
            series = _series_from_row_value(row.get(key))
            if not series.empty:
                return series
    return pd.Series(dtype=float)


def _extract_repeated_fold_consistency(row: pd.Series) -> float:
    fold_candidates = [
        "fold_scores",
        "validation_fold_scores",
        "bridge_fold_scores",
        "bridge_validation_fold_scores",
        "walkforward_fold_scores",
    ]
    folds = _extract_row_series(row, fold_candidates)
    if folds.empty or len(folds) < 2:
        return float("nan")
    mean_abs = float(np.mean(np.abs(folds)))
    if mean_abs <= 1e-12:
        return float("nan")
    dispersion = float(np.std(folds, ddof=0) / mean_abs)
    return float(np.clip(1.0 - dispersion, 0.0, 1.0))


def _build_bridge_diagnostics(row: pd.Series, stressed_cost_multiplier: float) -> dict[str, Any]:
    from project.research.robustness import (
        evaluate_structural_breaks,
        evaluate_structural_robustness,
    )

    pnl_series = _extract_row_series(
        row, ["bridge_pnl_series", "validation_pnl_series", "pnl_series", "pnl_path"]
    )
    returns_raw = _extract_row_series(
        row, ["bridge_returns_raw", "returns_raw", "gross_returns_series"]
    )
    costs_bps = _extract_row_series(
        row, ["bridge_costs_bps_series", "costs_bps_series", "dynamic_cost_bps_series"]
    )
    entry_delay_pnl = _extract_row_series(
        row, ["bridge_entry_delay_pnl_series", "entry_delay_pnl_series"]
    )
    timestamps = _extract_row_series(row, [])
    for key in ["bridge_timestamps", "timestamps", "validation_timestamps", "ts_path"]:
        if key in row and row.get(key) is not None:
            raw = row.get(key)
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except json.JSONDecodeError:
                    raw = None
            if raw is not None:
                timestamps = pd.to_datetime(pd.Series(raw), errors="coerce").dropna()
                if not timestamps.empty:
                    break

    diagnostics: dict[str, Any] = {}
    if not pnl_series.empty:
        diagnostics.update(
            evaluate_structural_robustness(
                pnl_series,
                returns_raw=returns_raw if not returns_raw.empty else None,
                costs_bps=costs_bps if not costs_bps.empty else None,
                entry_delay_pnl=entry_delay_pnl if not entry_delay_pnl.empty else None,
                cost_multiplier=stressed_cost_multiplier,
            )
        )
        if not timestamps.empty and len(timestamps) == len(pnl_series):
            break_results = evaluate_structural_breaks(pnl_series, timestamps)
            diagnostics["gate_structural_break"] = bool(break_results.get("pass", False))
            diagnostics["structural_break_detected"] = break_results.get(
                "structural_break_detected", False
            )
        else:
            diagnostics["gate_structural_break"] = False
            diagnostics["structural_break_detected"] = np.nan
    else:
        diagnostics.update(
            {
                "structural_robustness_score": np.nan,
                "sign_retention_rate": np.nan,
                "robustness_panel_complete": False,
                "gate_structural_break": False,
                "structural_break_detected": np.nan,
            }
        )

    diagnostics["repeated_fold_consistency"] = _extract_repeated_fold_consistency(row)
    return diagnostics


def evaluate_microstructure_gate(
    row: pd.Series,
    *,
    max_spread_stress: float = 2.0,
    max_depth_depletion: float = 0.70,
    max_sweep_pressure: float = 2.5,
    max_abs_imbalance: float = 0.90,
    min_feature_coverage: float = 0.25,
) -> dict[str, bool]:
    spread = safe_float(row.get("micro_spread_stress"), np.nan)
    depth = safe_float(row.get("micro_depth_depletion"), np.nan)
    sweep = safe_float(row.get("micro_sweep_pressure"), np.nan)
    imbalance = safe_float(row.get("micro_abs_imbalance", row.get("micro_imbalance")), np.nan)
    coverage = safe_float(row.get("micro_feature_coverage"), np.nan)

    spread_pass = (not np.isfinite(spread)) or (spread <= max_spread_stress)
    depth_pass = (not np.isfinite(depth)) or (depth <= max_depth_depletion)
    sweep_pass = (not np.isfinite(sweep)) or (sweep <= max_sweep_pressure)
    imbalance_pass = (not np.isfinite(imbalance)) or (abs(imbalance) <= max_abs_imbalance)
    coverage_pass = (not np.isfinite(coverage)) or (coverage >= min_feature_coverage)

    gate_micro = bool(
        spread_pass and depth_pass and sweep_pass and imbalance_pass and coverage_pass
    )
    return {
        "gate_bridge_microstructure": gate_micro,
        "gate_bridge_micro_spread_stress": spread_pass,
        "gate_bridge_micro_depth_depletion": depth_pass,
        "gate_bridge_micro_sweep_pressure": sweep_pass,
        "gate_bridge_micro_imbalance": imbalance_pass,
        "gate_bridge_micro_feature_coverage": coverage_pass,
    }


def effective_cost_bps(row: pd.Series) -> float:
    avg_dynamic = max(0.0, _row_float(row, "avg_dynamic_cost_bps", np.nan))
    turnover = max(0.0, _row_float(row, "turnover_proxy_mean", np.nan))
    if avg_dynamic > 0.0:
        return float(avg_dynamic * max(turnover, 0.10))
    cost_ratio = max(0.0, _row_float(row, "cost_ratio", 0.0))
    after_cost = _row_float(
        row,
        "after_cost_expectancy_per_trade",
        _row_float(row, "expectancy_per_trade", 0.0),
    )
    gross_proxy_bps = abs(after_cost) * 10_000.0
    return float(max(0.0, gross_proxy_bps * min(1.0, cost_ratio))) if cost_ratio > 0.0 else 0.0


def bridge_metrics_for_row(row: pd.Series, stressed_cost_multiplier: float) -> dict[str, Any]:
    fallback_expectancy = _row_float(row, "expectancy", 0.0)
    eff_aft = _row_float(
        row,
        "after_cost_expectancy_per_trade",
        _row_float(row, "expectancy_per_trade", fallback_expectancy),
    )
    str_aft = _row_float(row, "stressed_after_cost_expectancy_per_trade", eff_aft)
    eff_cost = effective_cost_bps(row)
    entry_lag = _row_int(row, "entry_lag_bars", 1)
    turnover = max(0.01, _row_float(row, "turnover_proxy_mean", 0.5))
    maker_fill_prob = min(1.0, turnover * 1.5) if entry_lag == 1 else 1.0
    if eff_cost <= 0.0 and maker_fill_prob < 0.05:
        str_aft = eff_aft = -999.0
    elif entry_lag == 1:
        miss_prob = 1.0 - maker_fill_prob
        eff_aft -= miss_prob * 2.0 / 10000.0
        str_aft -= miss_prob * 3.0 / 10000.0
    fb_val_aft = float(eff_aft * 10_000.0)
    fb_val_str = float(str_aft * 10_000.0)
    if np.isfinite(fb_val_str) and fb_val_str != -9990000.0:
        fb_val_str = float(fb_val_aft - ((stressed_cost_multiplier - 1.0) * eff_cost))
    val_aft = _row_float(row, "bridge_validation_after_cost_bps", fb_val_aft)
    val_str = _row_float(row, "bridge_validation_stressed_after_cost_bps", fb_val_str)
    train_aft = _row_float(row, "bridge_train_after_cost_bps", val_aft)
    gross_edge = float(max(0.0, val_aft + eff_cost))
    val_trades = max(0, _row_int(row, "validation_samples", _row_int(row, "sample_size", 0)))

    diagnostics = _build_bridge_diagnostics(row, stressed_cost_multiplier)
    # Path evidence check
    pnl_series_raw = row.get("pnl_series", row.get("pnl_path", []))
    if isinstance(pnl_series_raw, str):
        try:
            pnl_series_raw = json.loads(pnl_series_raw)
        except json.JSONDecodeError:
            pnl_series_raw = []
    pnl_series = [v for v in pnl_series_raw if isinstance(v, (int, float))]
    has_pnl_path = bool(len(pnl_series) > 0 and not all(v == 0 for v in pnl_series))

    metrics_dict = {
        "bridge_train_after_cost_bps": train_aft,
        "bridge_validation_after_cost_bps": val_aft,
        "bridge_validation_stressed_after_cost_bps": val_str,
        "exp_costed_x0_5": val_aft
        + (
            0.0 * eff_cost
        ),  # fix: was 0.5 but logically we use 0, 1.0, 1.5, 2.0 based on common patterns
        "exp_costed_x1_0": val_aft,
        "exp_costed_x1_5": val_aft - (0.5 * eff_cost),
        "exp_costed_x2_0": val_aft - eff_cost,
        "bridge_validation_trades": val_trades,
        "bridge_effective_cost_bps_per_trade": eff_cost,
        "bridge_gross_edge_bps_per_trade": gross_edge,
        "bridge_edge_to_cost_ratio": gross_edge / max(eff_cost, 1e-9),
        "bridge_certified": bool(has_pnl_path),  # Strict check
        "bridge_has_path_evidence": has_pnl_path,
        **diagnostics,
    }
    return metrics_dict


def evaluate_bridge_performance(
    survivors: pd.DataFrame,
    *,
    event_type: str,
    base_lookup: dict[str, dict[str, float]],
    edge_cost_k: float,
    min_validation_trades: int,
    stressed_cost_multiplier: float,
    min_net_expectancy_bps: float,
    max_fee_plus_slippage_bps: float | None,
    max_daily_turnover_multiple: float | None,
    require_retail_viability: bool,
    micro_thresholds: dict[str, float],
    low_capital_contract: dict[str, Any] | None = None,
    enforce_low_capital_viability: bool = False,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    metrics_rows, overlay_rows = [], []
    for _, row in survivors.iterrows():
        candidate_id = str(row.get("candidate_id", "")).strip()
        candidate_type = str(row.get("candidate_type", "")).strip()
        overlay_base_id = str(row.get("overlay_base_candidate_id", "")).strip()
        metrics = bridge_metrics_for_row(row, stressed_cost_multiplier)
        overlay_delta = None
        base_turnover = np.nan
        if candidate_type == "overlay":
            base_key = overlay_base_id or f"BASE_TEMPLATE::{event_type.lower()}"
            base_metrics = base_lookup.get(base_key)
            if base_metrics is None:
                metrics_rows.append(
                    {
                        "candidate_id": candidate_id,
                        "bridge_eval_status": "rejected:missing_overlay_base",
                        "gate_bridge_tradable": False,
                    }
                )
                continue
            base_turnover = float(base_metrics.get("turnover_proxy_mean", np.nan))
            metrics["bridge_train_after_cost_bps"] -= float(
                base_metrics.get("bridge_train_after_cost_bps", 0.0) or 0.0
            )
            metrics["bridge_validation_after_cost_bps"] -= float(
                base_metrics.get("bridge_validation_after_cost_bps", 0.0) or 0.0
            )
            metrics["bridge_validation_stressed_after_cost_bps"] -= float(
                base_metrics.get("bridge_validation_stressed_after_cost_bps", 0.0) or 0.0
            )
            metrics["bridge_gross_edge_bps_per_trade"] = max(
                0.0,
                metrics["bridge_validation_after_cost_bps"]
                + metrics["bridge_effective_cost_bps_per_trade"],
            )
            metrics["bridge_edge_to_cost_ratio"] = metrics["bridge_gross_edge_bps_per_trade"] / max(
                metrics["bridge_effective_cost_bps_per_trade"], 1e-9
            )
            overlay_delta = {
                "candidate_id": candidate_id,
                "overlay_base_candidate_id": base_key,
                "delta_validation_after_cost_bps": metrics["bridge_validation_after_cost_bps"],
                "delta_validation_stressed_after_cost_bps": metrics[
                    "bridge_validation_stressed_after_cost_bps"
                ],
            }

        has_trades = metrics["bridge_validation_trades"] >= min_validation_trades
        aft_pos, str_pos = (
            metrics["bridge_validation_after_cost_bps"] > 0.0,
            metrics["bridge_validation_stressed_after_cost_bps"] > 0.0,
        )
        edge_ratio_gate = metrics["bridge_gross_edge_bps_per_trade"] >= (
            edge_cost_k * metrics["bridge_effective_cost_bps_per_trade"]
        )
        turnover_proxy = max(0.0, _row_float(row, "turnover_proxy_mean", 0.5))
        gate_turnover = (
            (np.isnan(base_turnover) or turnover_proxy <= (base_turnover + 1e-9))
            and turnover_proxy <= 1.0
            and str_pos
        )
        retail_row = row.copy()
        retail_row["bridge_validation_after_cost_bps"] = metrics[
            "bridge_validation_after_cost_bps"
        ]
        retail_row["bridge_effective_cost_bps_per_trade"] = metrics[
            "bridge_effective_cost_bps_per_trade"
        ]
        retail_row["turnover_proxy_mean"] = turnover_proxy
        retail_row["tob_coverage"] = _row_float(row, "tob_coverage", np.nan)
        retail_eval = evaluate_retail_constraints(
            retail_row,
            min_tob_coverage=0.0,
            min_net_expectancy_bps=min_net_expectancy_bps,
            max_fee_plus_slippage_bps=max_fee_plus_slippage_bps,
            max_daily_turnover_multiple=max_daily_turnover_multiple,
        )
        gate_retail = bool(retail_eval.get("gate_retail_viability", False))
        low_cap_eval = evaluate_low_capital_viability(
            row,
            low_capital_contract=low_capital_contract or {},
            baseline_after_cost_bps=metrics["bridge_validation_after_cost_bps"],
            effective_cost_bps=metrics["bridge_effective_cost_bps_per_trade"],
            turnover_proxy_mean=turnover_proxy,
        )
        gate_low_cap = bool(low_cap_eval.get("gate_low_capital_viability", True))

        micro_gates = evaluate_microstructure_gate(row, **micro_thresholds)
        gate_micro = micro_gates["gate_bridge_microstructure"]
        gate_tradable_wo_micro = (
            has_trades
            and aft_pos
            and str_pos
            and edge_ratio_gate
            and gate_turnover
            and (gate_retail if require_retail_viability else True)
            and (gate_low_cap if enforce_low_capital_viability else True)
        )
        gate_tradable = gate_tradable_wo_micro and gate_micro

        fail_reasons = []
        if not has_trades:
            fail_reasons.append("gate_bridge_has_trades_validation")
        if not aft_pos:
            fail_reasons.append("gate_bridge_after_cost_positive_validation")
        if not str_pos:
            fail_reasons.append("gate_bridge_after_cost_stressed_positive_validation")
        if not edge_ratio_gate:
            fail_reasons.append("gate_bridge_edge_cost_ratio")
        if not gate_turnover:
            fail_reasons.append("gate_bridge_turnover_controls")

        if not gate_micro:
            fail_reasons.append("gate_bridge_microstructure")
            for k, v in micro_gates.items():
                if not v:
                    fail_reasons.append(k)

        if require_retail_viability and not gate_retail:
            fail_reasons.append("gate_bridge_retail_viability")
            if not retail_eval.get("gate_net_expectancy", True):
                fail_reasons.append("gate_bridge_retail_net_expectancy")
            if not retail_eval.get("gate_cost_budget", True):
                fail_reasons.append("gate_bridge_retail_cost_budget")
            if not retail_eval.get("gate_turnover", True):
                fail_reasons.append("gate_bridge_retail_turnover")
            if not retail_eval.get("gate_tob_coverage", True):
                fail_reasons.append("gate_bridge_retail_tob_coverage")

        if enforce_low_capital_viability and not gate_low_cap:
            fail_reasons.append("gate_bridge_low_capital_viability")

        primary_fail = fail_reasons[0] if fail_reasons else ""
        metrics_rows.append(
            {
                "candidate_id": candidate_id,
                "symbol": str(row.get("symbol", "")).strip().upper(),
                "candidate_type": candidate_type,
                "overlay_base_candidate_id": overlay_base_id,
                "bridge_eval_status": "tradable"
                if gate_tradable
                else ("rejected:" + ",".join(fail_reasons)),
                **metrics,
                "gate_bridge_has_trades_validation": has_trades,
                "gate_bridge_after_cost_positive_validation": aft_pos,
                "gate_bridge_after_cost_stressed_positive_validation": str_pos,
                "gate_bridge_edge_cost_ratio": edge_ratio_gate,
                "gate_bridge_turnover_controls": gate_turnover,
                "gate_bridge_retail_viability": gate_retail,
                "tob_coverage": safe_float(retail_eval.get("tob_coverage"), np.nan),
                "turnover_proxy_mean": float(turnover_proxy),
                "gate_bridge_retail_net_expectancy": bool(
                    retail_eval.get("gate_net_expectancy", True)
                ),
                "gate_bridge_retail_cost_budget": bool(retail_eval.get("gate_cost_budget", True)),
                "gate_bridge_retail_turnover": bool(retail_eval.get("gate_turnover", True)),
                "gate_bridge_retail_tob_coverage": bool(retail_eval.get("gate_tob_coverage", True)),
                "gate_bridge_low_capital_viability": gate_low_cap,
                "low_capital_viability_score": safe_float(
                    low_cap_eval.get("low_capital_viability_score"), np.nan
                ),
                "low_capital_reject_reason_codes": ",".join(
                    low_cap_eval.get("low_capital_reject_reason_codes", [])
                ),
                "low_capital_estimated_position_notional_usd": safe_float(
                    low_cap_eval.get("low_capital_estimated_position_notional_usd"), np.nan
                ),
                "low_capital_required_min_notional_usd": safe_float(
                    low_cap_eval.get("low_capital_required_min_notional_usd"), np.nan
                ),
                "low_capital_min_order_ratio": safe_float(
                    low_cap_eval.get("low_capital_min_order_ratio"), np.nan
                ),
                "low_capital_estimated_position_notional_source": str(
                    low_cap_eval.get("low_capital_estimated_position_notional_source", "")
                ),
                "gate_bridge_tradable_without_microstructure": gate_tradable_wo_micro,
                "gate_bridge_tradable": gate_tradable,
                "bridge_fail_reasons": ",".join(fail_reasons),
                "selection_score_executed": metrics["bridge_validation_after_cost_bps"],
                "bridge_fail_gate_primary": primary_fail,
                "bridge_fail_reason_primary": f"failed_{primary_fail}" if primary_fail else "",
                **micro_gates,
            }
        )
        if overlay_delta:
            overlay_rows.append(overlay_delta)
    return metrics_rows, overlay_rows
