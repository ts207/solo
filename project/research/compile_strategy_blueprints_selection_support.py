from __future__ import annotations

from typing import Any, Dict, Tuple

import numpy as np

from project import PROJECT_ROOT
from project.core.coercion import as_bool, safe_float, safe_int
from project.research.utils.decision_safety import (
    coerce_numeric_nan,
    finite_ge,
    finite_le,
)


def candidate_id(row: Dict[str, object], idx: int) -> str:
    candidate_id = str(row.get("candidate_id", "")).strip()
    if candidate_id:
        return candidate_id
    event = str(row.get("event", row.get("event_type", "candidate"))).strip() or "candidate"
    return f"{event}_{idx}"


def load_gates_spec() -> Dict[str, Any]:
    from project.specs.gates import load_gates_spec

    return load_gates_spec(PROJECT_ROOT.parent)


def rank_key(row: Dict[str, object]) -> Tuple[float, float, float, float, str]:
    from project.research.helpers.selection import rank_key as selection_rank_key

    return selection_rank_key(row, safe_float_fn=safe_float, as_bool_fn=as_bool)


def passes_quality_floor(
    row: Dict[str, Any],
    *,
    strict_cost_fields: bool = False,
    min_events: int = 0,
    min_robustness: float = 0.0,
    require_positive_expectancy: bool = False,
    expected_cost_digest: str | None = None,
    min_tob_coverage: float = 0.0,
    min_net_expectancy_bps: float = 0.0,
    max_fee_plus_slippage_bps: float = 1e9,
    max_daily_turnover_multiple: float = 1e9,
) -> bool:
    n = safe_int(row.get("n_events"), 0)
    if n < min_events:
        return False
    robustness = coerce_numeric_nan(row.get("robustness_score"))
    if min_robustness > 0.0 and not finite_ge(robustness, min_robustness):
        return False
    tob_cov = coerce_numeric_nan(row.get("tob_coverage"))
    if min_tob_coverage > 0.0 and not finite_ge(tob_cov, min_tob_coverage):
        return False
    net_exp = coerce_numeric_nan(
        row.get(
            "bridge_validation_after_cost_bps",
            row.get("after_cost_expectancy_per_trade", 0.0) * 10_000.0,
        )
    )
    if min_net_expectancy_bps > 0.0 and not finite_ge(net_exp, min_net_expectancy_bps):
        return False
    if require_positive_expectancy and not finite_ge(net_exp, 1e-9):
        return False
    turnover = coerce_numeric_nan(row.get("turnover_proxy_mean"))
    turnover_cap = (
        float(max_daily_turnover_multiple)
        if max_daily_turnover_multiple is not None
        else float("inf")
    )
    if np.isfinite(turnover_cap) and not finite_le(turnover, turnover_cap):
        return False
    if expected_cost_digest is not None:
        actual_digest = str(row.get("cost_config_digest", "")).strip()
        if actual_digest and actual_digest != str(expected_cost_digest).strip():
            return False
    if strict_cost_fields:
        cost = coerce_numeric_nan(row.get("bridge_effective_cost_bps_per_trade"))
        cost_cap = (
            float(max_fee_plus_slippage_bps)
            if max_fee_plus_slippage_bps is not None
            else float("inf")
        )
        if np.isfinite(cost_cap) and not finite_le(cost, cost_cap):
            return False
    return True


def passes_fallback_gate(row: Dict[str, Any], gate_spec: Dict[str, Any]) -> bool:
    t_stat = coerce_numeric_nan(row.get("t_stat"))
    min_t = coerce_numeric_nan(gate_spec.get("min_t_stat"))
    if not np.isfinite(min_t):
        min_t = 0.0

    if not finite_ge(t_stat, min_t):
        return False

    exp_bps = (
        coerce_numeric_nan(
            row.get("after_cost_expectancy_per_trade", row.get("expectancy_bps", 0.0) / 10_000.0)
        )
        * 10_000.0
    )
    min_exp = coerce_numeric_nan(gate_spec.get("min_after_cost_expectancy_bps"))
    if not np.isfinite(min_exp):
        min_exp = 0.0

    if not finite_ge(exp_bps, min_exp):
        return False

    n = safe_int(row.get("n_events"), 0)
    min_n = safe_int(gate_spec.get("min_sample_size"), 0)
    if n < min_n:
        return False
    return True
