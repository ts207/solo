from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from pathlib import Path

from project.core.execution_costs import resolve_execution_costs
from project.research.cost_calibration import ToBRegimeCostCalibrator


@dataclass(frozen=True)
class CostDistribution:
    """Per-trade cost statistics used for p95-survival gating (T2.1)."""

    mean_bps: float
    std_bps: float
    p95_bps: float
    impact_bps_per_unit_size: float

    def survival_rate_at_p95(self, gross_mean_bps: float) -> float:
        """Fraction of p95-cost scenarios where gross edge still covers cost."""
        if self.p95_bps <= 0.0:
            return 1.0
        return 1.0 if gross_mean_bps > self.p95_bps else 0.0

    def as_dict(self) -> dict[str, float]:
        return {
            "cost_mean_bps": self.mean_bps,
            "cost_std_bps": self.std_bps,
            "cost_p95_bps": self.p95_bps,
            "cost_impact_bps_per_unit_size": self.impact_bps_per_unit_size,
        }


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


def cost_distribution_per_trade_bps(
    features,
    hypothesis=None,
    *,
    cost_spec: dict | None = None,
) -> CostDistribution:
    """Return a CostDistribution for the given features/hypothesis.

    Uses per-venue/symbol slippage statistics from cost_spec when available,
    falling back to a normal approximation of the static cost estimate.
    """
    import pandas as pd

    cost_spec = dict(cost_spec or {})
    base_cost = float(
        cost_spec.get(
            "cost_bps",
            cost_spec.get("round_trip_cost_bps", cost_spec.get("static_cost_bps", 0.0)),
        )
    )
    slippage_std = float(cost_spec.get("slippage_std_bps", base_cost * 0.5))
    impact_bps = float(cost_spec.get("impact_bps_per_unit_size", 0.0))

    mean_series = expected_cost_per_trade_bps(features, hypothesis, cost_spec=cost_spec)
    mean_bps = float(mean_series.mean()) if hasattr(mean_series, "mean") else base_cost

    # std and p95 from spec or heuristic (half of mean as lower bound)
    std_bps = max(slippage_std, mean_bps * 0.25)
    # Normal quantile at 95th pct: mean + 1.645 * std
    p95_bps = mean_bps + 1.645 * std_bps

    return CostDistribution(
        mean_bps=mean_bps,
        std_bps=std_bps,
        p95_bps=p95_bps,
        impact_bps_per_unit_size=impact_bps,
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
