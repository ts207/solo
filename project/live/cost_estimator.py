from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CostEstimate:
    expected_cost_bps: float | None
    source: str
    reasons: tuple[str, ...] = ()


def estimate_expected_cost_bps(
    *,
    spread_bps: float | None,
    taker_fee_bps: float,
) -> CostEstimate:
    if spread_bps is None:
        return CostEstimate(
            expected_cost_bps=None,
            source="missing",
            reasons=("missing_spread",),
        )
    if spread_bps < 0.0:
        return CostEstimate(
            expected_cost_bps=None,
            source="missing",
            reasons=("invalid_spread",),
        )
    return CostEstimate(
        expected_cost_bps=(float(spread_bps) / 2.0) + float(taker_fee_bps),
        source="spread_derived",
    )
