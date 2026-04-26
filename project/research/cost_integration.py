from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pandas as pd

from project.core.execution_costs import resolve_execution_costs


def integrate_execution_costs(
    candidates: pd.DataFrame,
    symbol: str,
    base_fee_bps: float = 4.0,
    base_slippage_bps: float = 2.0,
    *,
    project_root: Path | None = None,
    config_paths: Sequence[str] | None = None,
    fees_bps: float | None = None,
    slippage_bps: float | None = None,
    cost_bps: float | None = None,
    stressed_cost_multiplier: float = 2.0,
) -> pd.DataFrame:
    """
    Integrate execution cost estimates into candidate rows.

    The helper resolves the canonical execution cost contract once, stores the
    resolved per-side and round-trip costs, and derives after-cost expectancy
    columns when a raw expectancy source is present.
    """
    repo_root = Path(project_root) if project_root is not None else Path(__file__).resolve().parents[2]
    resolved = resolve_execution_costs(
        project_root=repo_root,
        config_paths=config_paths,
        fees_bps=fees_bps if fees_bps is not None else base_fee_bps,
        slippage_bps=slippage_bps if slippage_bps is not None else base_slippage_bps,
        cost_bps=cost_bps,
    )

    out = candidates.copy()
    out["symbol"] = str(symbol).upper()
    out["cost_config_digest"] = resolved.config_digest
    out["fee_bps_per_side"] = float(resolved.fee_bps_per_side)
    out["slippage_bps_per_fill"] = float(resolved.slippage_bps_per_fill)
    out["resolved_cost_bps"] = float(resolved.cost_bps)
    out["cost_bps"] = float(resolved.cost_bps)
    out["round_trip_cost_bps"] = float(resolved.round_trip_cost_bps)

    raw_expectancy = None
    raw_is_bps = False
    if "expectancy_per_trade" in out.columns:
        raw_expectancy = pd.to_numeric(out["expectancy_per_trade"], errors="coerce")
    elif "expectancy_bps" in out.columns:
        raw_expectancy = pd.to_numeric(out["expectancy_bps"], errors="coerce") / 10000.0
        raw_is_bps = True
    elif "mean_return_bps" in out.columns:
        raw_expectancy = pd.to_numeric(out["mean_return_bps"], errors="coerce") / 10000.0
        raw_is_bps = True

    if raw_expectancy is not None:
        round_trip_cost_decimal = float(resolved.round_trip_cost_bps) / 10000.0
        after_cost = raw_expectancy - round_trip_cost_decimal
        stressed_after_cost = raw_expectancy - (
            round_trip_cost_decimal * float(stressed_cost_multiplier)
        )
        out["after_cost_expectancy_per_trade"] = after_cost
        out["stressed_after_cost_expectancy_per_trade"] = stressed_after_cost
        out["after_cost_expectancy"] = after_cost * 10000.0
        out["stressed_after_cost_expectancy"] = stressed_after_cost * 10000.0
        if raw_is_bps:
            out["raw_expectancy_bps"] = raw_expectancy * 10000.0
        out["gate_after_cost_positive"] = out["after_cost_expectancy"] > 0.0
        out["gate_after_cost_stressed_positive"] = out["stressed_after_cost_expectancy"] > 0.0

    return out
