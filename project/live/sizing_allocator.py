from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping

from project.live.trade_valuator import TradeValuation


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


@dataclass(frozen=True)
class SizeAllocation:
    accepted: bool
    notional: float
    size_fraction: float
    participation_fraction: float
    reasons: tuple[str, ...] = ()

    @property
    def quantity_scale(self) -> float:
        return self.size_fraction


def compute_marginal_overlap(
    *,
    symbol: str,
    event_family: str,
    portfolio_state: Mapping[str, Any],
) -> float:
    """
    Fraction [0, 1] of the proposed trade's directional risk already captured
    by open positions, weighted by correlation proxies:

      same symbol + same family  → 1.00 (identical trigger and underlier)
      same symbol, diff family   → 0.60 (same underlier, different catalyst)
      diff symbol, same family   → 0.50 (same catalyst type, different coin)

    Normalised against gross_exposure so the result degrades gracefully as
    the portfolio grows.
    """
    symbol_exposures: dict[str, float] = dict(portfolio_state.get("symbol_exposures", {}))
    family_exposures: dict[str, float] = dict(portfolio_state.get("family_exposures", {}))
    gross_exposure = max(1.0, _finite(portfolio_state.get("gross_exposure"), 0.0))

    sym = str(symbol).upper()
    fam = str(event_family)
    same_symbol = abs(_finite(symbol_exposures.get(sym), 0.0))
    same_family = abs(_finite(family_exposures.get(fam), 0.0))

    # Decompose into non-overlapping buckets to avoid double-counting.
    same_both = min(same_symbol, same_family)
    symbol_only = max(0.0, same_symbol - same_both)
    family_only = max(0.0, same_family - same_both)

    correlated = 1.00 * same_both + 0.60 * symbol_only + 0.50 * family_only
    return min(1.0, correlated / gross_exposure)


def allocate_trade_size(
    *,
    valuation: TradeValuation,
    market_state: Mapping[str, Any],
    portfolio_state: Mapping[str, Any],
    base_size_fraction: float,
    max_notional_fraction: float,
    symbol: str = "",
    event_family: str = "",
) -> SizeAllocation:
    if not valuation.should_trade:
        return SizeAllocation(False, 0.0, 0.0, 0.0, valuation.reasons)

    balance = max(0.0, _finite(portfolio_state.get("available_balance"), 0.0))
    engine_cap = _finite(market_state.get("engine_allocated_notional"), 0.0)
    cap_notional = engine_cap if engine_cap > 0.0 else balance * max(0.0, max_notional_fraction)
    if cap_notional <= 0.0:
        return SizeAllocation(False, 0.0, 0.0, 0.0, ("zero_capital",))

    downside = max(1.0, valuation.expected_downside_bps)
    edge_ratio = max(0.0, valuation.expected_net_edge_bps / downside)
    edge_scale = min(1.0, edge_ratio)
    confidence_scale = max(0.0, min(1.0, valuation.edge_confidence))
    fill_scale = max(0.0, min(1.0, valuation.fill_probability))
    slippage_bps = _finite(
        market_state.get("realized_slippage_bps")
        or market_state.get("current_slippage_bps")
        or market_state.get("expected_cost_bps"),
        0.0,
    )
    slippage_scale = 1.0 / (1.0 + max(0.0, slippage_bps) / 10.0)
    explicit_overlap = (
        portfolio_state.get("marginal_overlap")
        or portfolio_state.get("thesis_overlap")
        or market_state.get("thesis_overlap")
    )
    if explicit_overlap is not None:
        overlap = _finite(explicit_overlap, 0.0)
    elif symbol:
        overlap = compute_marginal_overlap(
            symbol=symbol,
            event_family=event_family,
            portfolio_state=portfolio_state,
        )
    else:
        overlap = 0.0
    overlap_scale = max(0.20, 1.0 - max(0.0, min(1.0, overlap)))
    downside_scale = 1.0 / (1.0 + max(0.0, downside - valuation.expected_net_edge_bps) / 50.0)
    requested = max(0.0, float(base_size_fraction))
    fraction = requested * edge_scale * confidence_scale * fill_scale
    fraction *= slippage_scale * overlap_scale * downside_scale
    fraction = max(0.0, min(1.0, fraction))
    notional = cap_notional * fraction

    depth = _finite(
        market_state.get("top_of_book_depth_usd")
        or market_state.get("depth_usd")
        or market_state.get("liquidity_available"),
        0.0,
    )
    participation_cap = max(
        0.0,
        min(1.0, _finite(market_state.get("participation_cap"), 0.10)),
    )
    if depth > 0.0 and participation_cap > 0.0:
        max_participation_notional = depth * participation_cap
        if notional > max_participation_notional:
            notional = max_participation_notional
            fraction = notional / cap_notional if cap_notional > 0.0 else 0.0

    if notional <= 0.0:
        return SizeAllocation(False, 0.0, 0.0, 0.0, ("zero_edge_adjusted_size",))
    participation = notional / depth if depth > 0.0 else 0.0
    return SizeAllocation(True, float(notional), float(fraction), float(participation))
