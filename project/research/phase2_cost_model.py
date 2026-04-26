from __future__ import annotations

import argparse
from pathlib import Path

from project.core.execution_costs import resolve_execution_costs
from project.research.cost_calibration import ToBRegimeCostCalibrator


def _resolve_phase2_costs(
    args: argparse.Namespace,
    project_root: Path,
) -> tuple[float, dict]:
    """
    Resolve execution costs from spec configs (fees.yaml / pipeline.yaml).

    Returns:
        (cost_bps, cost_coordinate)
    """
    costs = resolve_execution_costs(
        project_root=project_root,
        config_paths=getattr(args, "config", []),
        fees_bps=getattr(args, "fees_bps", None),
        slippage_bps=getattr(args, "slippage_bps", None),
        cost_bps=getattr(args, "cost_bps", None),
    )
    coordinate = {
        "config_digest": costs.config_digest,
        "cost_bps": costs.cost_bps,
        "fee_bps_per_side": costs.fee_bps_per_side,
        "slippage_bps_per_fill": costs.slippage_bps_per_fill,
        "round_trip_cost_bps": costs.round_trip_cost_bps,
    }
    return costs.cost_bps, coordinate


def init_cost_calibrator(
    run_id: str,
    data_root: Path,
    cost_coordinate: dict[str, any],
    args: argparse.Namespace,
) -> ToBRegimeCostCalibrator:
    return ToBRegimeCostCalibrator(
        run_id=run_id,
        data_root=data_root,
        base_fee_bps=float(cost_coordinate["fee_bps_per_side"]),
        base_slippage_bps=float(cost_coordinate["slippage_bps_per_fill"]),
        static_cost_bps=float(cost_coordinate["cost_bps"]),
        mode=str(args.cost_calibration_mode),
        min_tob_coverage=float(args.cost_min_tob_coverage),
        tob_tolerance_minutes=int(args.cost_tob_tolerance_minutes),
    )


def expected_cost_per_trade_bps(
    features,
    hypothesis=None,
    *,
    cost_spec: dict | None = None,
):
    """Return expected per-trade execution cost in basis points.

    The phase-2 evaluator consumes this vector to compute net return statistics.
    The default implementation preserves the existing static round-trip cost
    contract while accepting optional dynamic columns for future calibrated cost
    models.
    """
    import pandas as pd

    idx = getattr(features, "index", None)
    cost_spec = dict(cost_spec or {})
    base_cost = float(
        cost_spec.get(
            "cost_bps",
            cost_spec.get("round_trip_cost_bps", cost_spec.get("static_cost_bps", 0.0)),
        )
    )
    if features is not None:
        for column in (
            "expected_cost_bps_per_trade",
            "round_trip_cost_bps",
            "cost_bps",
            "estimated_cost_bps",
        ):
            if column in getattr(features, "columns", []):
                values = pd.to_numeric(features[column], errors="coerce").fillna(base_cost)
                return values.astype(float)
    return pd.Series(base_cost, index=idx, dtype=float)
