from pathlib import Path

import pandas as pd

from project.core.execution_costs import estimate_execution_model_v2_cost_bps
from project.spec_registry import load_yaml_path


def apply_cost_model(
    candidates: pd.DataFrame, config_path: str, pnl_col: str = "pnl_bps"
) -> pd.DataFrame:
    """
    Apply fees and slippage to a DataFrame of trading candidates.

    Round-trip cost = (fee_bps_per_side + slippage_bps_per_fill) * 2
    Net PnL = Gross PnL - Round-trip cost

    Args:
        candidates: DataFrame with a P&L column (in bps)
        config_path: Path to YAML config containing 'fee_bps_per_side'
                    and 'slippage_bps_per_fill'
        pnl_col: Name of the column containing gross P&L in bps

    Returns:
        DataFrame with an added 'net_pnl_bps' column
    """
    if pnl_col not in candidates.columns:
        raise ValueError(f"Column '{pnl_col}' not found in DataFrame")

    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Cost config not found at: {config_path}")

    config = load_yaml_path(config_file)

    cost_model = str(config.get("cost_model", "")).strip().lower()
    if cost_model in {"execution_simulator_v2", "fill_model_v2"}:
        turnover = pd.to_numeric(
            candidates.get(
                "turnover",
                candidates.get("notional", pd.Series(1.0, index=candidates.index)),
            ),
            errors="coerce",
        ).fillna(0.0)
        side_cost = estimate_execution_model_v2_cost_bps(candidates, turnover, dict(config))
        round_trip_cost_series = side_cost * 2.0
        candidates["net_pnl_bps"] = candidates[pnl_col] - round_trip_cost_series
        candidates["round_trip_cost_bps"] = round_trip_cost_series
        candidates["execution_model_family"] = "execution_simulator_v2"
        return candidates

    fee = float(config.get("fee_bps_per_side", 0.0))
    slippage = float(config.get("slippage_bps_per_fill", 0.0))

    # 2 * (fee + slippage) for round trip
    round_trip_cost = (fee + slippage) * 2.0

    candidates["net_pnl_bps"] = candidates[pnl_col] - round_trip_cost
    candidates["round_trip_cost_bps"] = round_trip_cost

    return candidates
