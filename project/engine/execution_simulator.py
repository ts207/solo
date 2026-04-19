from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping

import pandas as pd

from project.core.execution_costs import estimate_execution_model_v2_cost_bps
from project.engine.fill_model_v2 import FillModelConfig, FillModelRequest, estimate_fill_v2


@dataclass(frozen=True)
class ExecutionSimulationConfig:
    fee_bps_per_side: float = 4.0
    profile: str = "base"
    latency_ms: int = 250
    cost_tolerance_bps: float = 2.0
    passive_adverse_selection_bps: float = 0.2


def simulate_execution_event(
    request: FillModelRequest,
    market_state: Mapping[str, Any],
    config: ExecutionSimulationConfig | None = None,
) -> dict[str, Any]:
    cfg = config or ExecutionSimulationConfig()
    fill = estimate_fill_v2(
        request,
        market_state,
        FillModelConfig(
            fee_bps_per_side=cfg.fee_bps_per_side,
            profile=cfg.profile,
            latency_ms=cfg.latency_ms,
            passive_adverse_selection_bps=cfg.passive_adverse_selection_bps,
        ),
    )
    payload = asdict(fill)
    payload["model_family"] = "execution_simulator_v2"
    return payload


def simulate_execution_frame(
    frame: pd.DataFrame,
    turnover: pd.Series,
    config: Mapping[str, Any] | ExecutionSimulationConfig | None = None,
) -> pd.DataFrame:
    cfg = asdict(config) if isinstance(config, ExecutionSimulationConfig) else dict(config or {})
    cfg.setdefault("cost_model", "execution_simulator_v2")
    expected_cost = estimate_execution_model_v2_cost_bps(frame, turnover, cfg)
    return pd.DataFrame(
        {
            "expected_cost_bps": expected_cost,
            "model_family": "execution_simulator_v2",
        },
        index=expected_cost.index,
    )


def calibrate_execution_model_v2(
    base_config: Mapping[str, Any],
    observed_fills: pd.DataFrame,
) -> dict[str, Any]:
    calibrated = dict(base_config)
    if observed_fills.empty:
        calibrated.setdefault("cost_model", "execution_simulator_v2")
        return calibrated
    if "realized_fee_bps" in observed_fills:
        calibrated["base_fee_bps"] = float(
            pd.to_numeric(observed_fills["realized_fee_bps"], errors="coerce").dropna().median()
        )
    if "realized_slippage_bps" in observed_fills:
        calibrated["passive_adverse_selection_bps"] = max(
            0.0,
            float(
                pd.to_numeric(observed_fills["realized_slippage_bps"], errors="coerce")
                .dropna()
                .median()
            ),
        )
    calibrated["cost_model"] = "execution_simulator_v2"
    return calibrated


def compare_expected_realized_fill_costs(
    expected: pd.DataFrame,
    realized: pd.DataFrame,
    *,
    tolerance_bps: float,
) -> dict[str, float | bool]:
    if expected.empty or realized.empty:
        return {"within_tolerance": False, "mean_abs_gap_bps": float("inf"), "samples": 0.0}
    expected_cost = pd.to_numeric(expected["expected_cost_bps"], errors="coerce").reset_index(
        drop=True
    )
    realized_cost = pd.to_numeric(realized["realized_total_cost_bps"], errors="coerce").reset_index(
        drop=True
    )
    n = min(len(expected_cost), len(realized_cost))
    gaps = (expected_cost.iloc[:n] - realized_cost.iloc[:n]).abs()
    mean_gap = float(gaps.mean()) if n else float("inf")
    return {
        "within_tolerance": bool(mean_gap <= float(tolerance_bps)),
        "mean_abs_gap_bps": mean_gap,
        "samples": float(n),
    }
