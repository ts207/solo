from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

from project.core.execution_costs import resolve_execution_costs
from project.research.cost_calibration import ToBRegimeCostCalibrator


def _resolve_phase2_costs(
    args: argparse.Namespace,
    project_root: Path,
) -> Tuple[float, dict]:
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
    cost_coordinate: Dict[str, any],
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
