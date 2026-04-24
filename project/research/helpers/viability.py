from __future__ import annotations

import json
from typing import Any, Dict, Mapping, Optional

import numpy as np

from project.core.coercion import safe_float, safe_int


def _optional_finite(value: float) -> Optional[float]:
    return float(value) if np.isfinite(value) else None


def _delay_expectancy_map(row: Mapping[str, Any]) -> Dict[int, float]:
    payload = row.get("delay_expectancy_map")
    if isinstance(payload, str):
        text = payload.strip()
        if text:
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                payload = {}
        else:
            payload = {}
    if not isinstance(payload, Mapping):
        return {}
    out: Dict[int, float] = {}
    for key, value in payload.items():
        try:
            delay = int(float(key))
            val = float(value)
        except (TypeError, ValueError):
            continue
        if not np.isfinite(val):
            continue
        out[int(delay)] = float(val)
    return out


def _to_bps(value: float) -> float:
    if not np.isfinite(value):
        return float(np.nan)
    # Heuristic: small absolute values are decimal returns.
    if abs(value) <= 2.0:
        return float(value * 10_000.0)
    return float(value)


def evaluate_low_capital_viability(
    row: Mapping[str, Any],
    *,
    low_capital_contract: Mapping[str, Any],
    baseline_after_cost_bps: float | None = None,
    effective_cost_bps: float | None = None,
    turnover_proxy_mean: float | None = None,
) -> Dict[str, Any]:
    contract = dict(low_capital_contract or {})
    if not contract:
        return {
            "low_capital_viability_score": None,
            "low_capital_reject_reason_codes": [],
            "gate_low_capital_min_order_feasible": True,
            "gate_low_capital_cost_stress_2x": True,
            "gate_low_capital_cost_stress_3x": True,
            "gate_low_capital_latency_stress": True,
            "gate_low_capital_turnover_cap": True,
            "gate_low_capital_liquidity_sanity": True,
            "gate_low_capital_viability": True,
            "low_capital_stress_after_cost_2x_bps": None,
            "low_capital_stress_after_cost_3x_bps": None,
            "low_capital_stress_after_cost_latency_bps": None,
            "low_capital_estimated_position_notional_usd": None,
            "low_capital_required_min_notional_usd": None,
            "low_capital_min_order_ratio": None,
            "low_capital_estimated_position_notional_source": None,
        }

    baseline_bps = safe_float(baseline_after_cost_bps, default=np.nan)
    if not np.isfinite(baseline_bps):
        baseline_bps = safe_float(row.get("bridge_validation_after_cost_bps"), default=np.nan)
    if not np.isfinite(baseline_bps):
        baseline_bps = safe_float(row.get("net_expectancy_bps"), default=np.nan)
    if not np.isfinite(baseline_bps):
        baseline_bps = safe_float(row.get("after_cost_expectancy_per_trade"), default=np.nan)
        if np.isfinite(baseline_bps):
            baseline_bps = float(baseline_bps * 10_000.0)

    eff_cost_bps = safe_float(effective_cost_bps, default=np.nan)
    if not np.isfinite(eff_cost_bps):
        eff_cost_bps = safe_float(row.get("bridge_effective_cost_bps_per_trade"), default=np.nan)
    if not np.isfinite(eff_cost_bps):
        eff_cost_bps = safe_float(row.get("avg_dynamic_cost_bps"), default=np.nan)

    if np.isfinite(eff_cost_bps):
        eff_cost_bps = float(max(0.0, eff_cost_bps))
    else:
        eff_cost_bps = float(np.nan)

    turnover = safe_float(turnover_proxy_mean, default=np.nan)
    if not np.isfinite(turnover):
        turnover = safe_float(row.get("turnover_proxy_mean"), default=np.nan)

    stress_2x_bps = (
        baseline_bps - eff_cost_bps
        if np.isfinite(baseline_bps) and np.isfinite(eff_cost_bps)
        else float(np.nan)
    )
    stress_3x_bps = (
        baseline_bps - (2.0 * eff_cost_bps)
        if np.isfinite(baseline_bps) and np.isfinite(eff_cost_bps)
        else float(np.nan)
    )

    default_delay = safe_int(contract.get("entry_delay_bars_default"), 1)
    stress_delay = safe_int(contract.get("entry_delay_bars_stress"), max(2, default_delay))
    delay_map = _delay_expectancy_map(row)
    latency_stress_bps = float(np.nan)
    if stress_delay in delay_map:
        latency_stress_bps = _to_bps(float(delay_map[stress_delay]))
    elif np.isfinite(baseline_bps):
        lag_penalty_steps = float(max(0, stress_delay - default_delay))
        lag_penalty_bps = lag_penalty_steps * (eff_cost_bps if np.isfinite(eff_cost_bps) else 0.0)
        latency_stress_bps = float(baseline_bps - lag_penalty_bps)

    min_notional = safe_float(contract.get("min_position_notional_usd"), default=np.nan)
    min_notional_margin = safe_float(contract.get("min_notional_safety_margin"), default=np.nan)
    if not np.isfinite(min_notional_margin) or min_notional_margin <= 0.0:
        min_notional_margin = 1.0
    min_notional_required = (
        float(min_notional * min_notional_margin) if np.isfinite(min_notional) else float(np.nan)
    )

    account_equity = safe_float(contract.get("account_equity_usd"), default=np.nan)
    max_position_notional = safe_float(contract.get("max_position_notional_usd"), default=np.nan)
    max_trades_per_day = safe_float(contract.get("max_trades_per_day"), default=np.nan)
    max_turnover_per_day = safe_float(contract.get("max_turnover_per_day"), default=np.nan)
    estimated_position_notional = safe_float(
        row.get("estimated_position_notional_usd"), default=np.nan
    )
    estimated_position_notional_source = "row"
    if not np.isfinite(estimated_position_notional):
        turnover_for_est = turnover if np.isfinite(turnover) else max_turnover_per_day
        if (
            np.isfinite(turnover_for_est)
            and np.isfinite(account_equity)
            and np.isfinite(max_trades_per_day)
            and max_trades_per_day > 0.0
        ):
            estimated_position_notional = float(
                max(0.0, turnover_for_est) * max(0.0, account_equity) / max(max_trades_per_day, 1.0)
            )
            if np.isfinite(max_position_notional):
                estimated_position_notional = float(
                    min(estimated_position_notional, max_position_notional)
                )
            estimated_position_notional_source = "turnover_implied"
        else:
            candidates = [v for v in (max_position_notional, account_equity) if np.isfinite(v)]
            estimated_position_notional = float(min(candidates)) if candidates else float(np.nan)
            estimated_position_notional_source = "contract_cap"

    gate_min_order = bool(
        np.isfinite(estimated_position_notional)
        and np.isfinite(min_notional_required)
        and estimated_position_notional >= min_notional_required
    )
    min_order_ratio = (
        float(estimated_position_notional / min_notional_required)
        if np.isfinite(estimated_position_notional)
        and np.isfinite(min_notional_required)
        and min_notional_required > 0.0
        else float(np.nan)
    )
    gate_cost_2x = bool(np.isfinite(stress_2x_bps) and stress_2x_bps > 0.0)
    gate_cost_3x = bool(np.isfinite(stress_3x_bps) and stress_3x_bps > 0.0)
    gate_latency = bool(np.isfinite(latency_stress_bps) and latency_stress_bps > 0.0)

    max_turnover_day = safe_float(contract.get("max_turnover_per_day"), np.nan)
    gate_turnover = bool(
        np.isfinite(turnover) and np.isfinite(max_turnover_day) and turnover <= max_turnover_day
    )

    min_tob_coverage = safe_float(contract.get("require_top_book_coverage"), np.nan)
    tob_coverage = safe_float(row.get("tob_coverage"), np.nan)
    micro_feature_coverage = safe_float(row.get("micro_feature_coverage"), np.nan)
    observed_coverage = tob_coverage if np.isfinite(tob_coverage) else micro_feature_coverage
    spread_ceiling_bps = safe_float(contract.get("spread_ceiling_bps"), np.nan)
    spread_stress = safe_float(row.get("micro_spread_stress"), np.nan)
    # L2/top-of-book coverage is optional in non-TOB research runs. If no direct coverage
    # evidence exists, do not fail liquidity sanity on missing TOB alone; rely on spread and
    # the remaining low-capital gates instead.
    if not np.isfinite(min_tob_coverage):
        coverage_ok = True
    elif np.isfinite(observed_coverage):
        coverage_ok = bool(observed_coverage >= min_tob_coverage)
    else:
        coverage_ok = True
    # Spread sanity should only bind when direct spread evidence exists. In no-L2 runs,
    # missing spread telemetry is not a reason to fail liquidity sanity by itself.
    spread_ok = True
    if np.isfinite(spread_ceiling_bps) and np.isfinite(spread_stress):
        spread_ok = bool(spread_stress <= spread_ceiling_bps)
    gate_liquidity = bool(coverage_ok and spread_ok)

    gate_map = {
        "LOW_CAP_MIN_ORDER_FEASIBILITY": bool(gate_min_order),
        "LOW_CAP_COST_SURVIVAL_2X": bool(gate_cost_2x),
        "LOW_CAP_COST_SURVIVAL_3X": bool(gate_cost_3x),
        "LOW_CAP_LATENCY_STRESS": bool(gate_latency),
        "LOW_CAP_TURNOVER_CAP": bool(gate_turnover),
        "LOW_CAP_LIQUIDITY_SANITY": bool(gate_liquidity),
    }
    reject_codes = [code for code, passed in gate_map.items() if not passed]
    score = float(sum(1.0 for passed in gate_map.values() if passed) / max(len(gate_map), 1))

    return {
        "low_capital_viability_score": float(score),
        "low_capital_reject_reason_codes": reject_codes,
        "gate_low_capital_min_order_feasible": bool(gate_min_order),
        "gate_low_capital_cost_stress_2x": bool(gate_cost_2x),
        "gate_low_capital_cost_stress_3x": bool(gate_cost_3x),
        "gate_low_capital_latency_stress": bool(gate_latency),
        "gate_low_capital_turnover_cap": bool(gate_turnover),
        "gate_low_capital_liquidity_sanity": bool(gate_liquidity),
        "gate_low_capital_viability": bool(len(reject_codes) == 0),
        "low_capital_stress_after_cost_2x_bps": _optional_finite(stress_2x_bps),
        "low_capital_stress_after_cost_3x_bps": _optional_finite(stress_3x_bps),
        "low_capital_stress_after_cost_latency_bps": _optional_finite(latency_stress_bps),
        "low_capital_estimated_position_notional_usd": _optional_finite(
            estimated_position_notional
        ),
        "low_capital_required_min_notional_usd": _optional_finite(min_notional_required),
        "low_capital_min_order_ratio": _optional_finite(min_order_ratio),
        "low_capital_estimated_position_notional_source": str(
            estimated_position_notional_source
        ).strip()
        or "unknown",
    }


def evaluate_retail_constraints(
    row: Mapping[str, Any],
    *,
    min_tob_coverage: float = 0.0,
    min_net_expectancy_bps: float = 0.0,
    max_fee_plus_slippage_bps: float | None = None,
    max_daily_turnover_multiple: float | None = None,
) -> Dict[str, Any]:
    tob_coverage = safe_float(row.get("tob_coverage"), np.nan)

    net_expectancy_bps = safe_float(row.get("bridge_validation_after_cost_bps"), np.nan)
    if not np.isfinite(net_expectancy_bps):
        after_cost = safe_float(row.get("after_cost_expectancy_per_trade"), np.nan)
        if not np.isfinite(after_cost):
            after_cost = safe_float(row.get("expectancy_after_multiplicity"), np.nan)
        if np.isfinite(after_cost):
            net_expectancy_bps = float(after_cost * 10000.0)

    effective_cost_bps = safe_float(row.get("bridge_effective_cost_bps_per_trade"), np.nan)
    if not np.isfinite(effective_cost_bps):
        effective_cost_bps = safe_float(row.get("avg_dynamic_cost_bps"), np.nan)

    turnover_proxy_mean = safe_float(row.get("turnover_proxy_mean"), np.nan)

    gate_tob_coverage = True
    if float(min_tob_coverage) > 0.0:
        gate_tob_coverage = bool(
            np.isfinite(tob_coverage) and float(tob_coverage) >= float(min_tob_coverage)
        )

    gate_net_expectancy = True
    if float(min_net_expectancy_bps) > 0.0:
        gate_net_expectancy = bool(
            np.isfinite(net_expectancy_bps)
            and float(net_expectancy_bps) >= float(min_net_expectancy_bps)
        )

    gate_cost_budget = True
    if max_fee_plus_slippage_bps is not None and float(max_fee_plus_slippage_bps) > 0.0:
        gate_cost_budget = bool(
            np.isfinite(effective_cost_bps)
            and float(effective_cost_bps) <= float(max_fee_plus_slippage_bps)
        )

    gate_turnover = True
    if max_daily_turnover_multiple is not None and float(max_daily_turnover_multiple) > 0.0:
        gate_turnover = bool(
            np.isfinite(turnover_proxy_mean)
            and float(turnover_proxy_mean) <= float(max_daily_turnover_multiple)
        )

    gate_retail_viability = bool(
        gate_tob_coverage and gate_net_expectancy and gate_cost_budget and gate_turnover
    )

    return {
        "tob_coverage": _optional_finite(tob_coverage),
        "net_expectancy_bps": _optional_finite(net_expectancy_bps),
        "effective_cost_bps": _optional_finite(effective_cost_bps),
        "turnover_proxy_mean": _optional_finite(turnover_proxy_mean),
        "gate_tob_coverage": bool(gate_tob_coverage),
        "gate_net_expectancy": bool(gate_net_expectancy),
        "gate_cost_budget": bool(gate_cost_budget),
        "gate_turnover": bool(gate_turnover),
        "gate_retail_viability": bool(gate_retail_viability),
    }
