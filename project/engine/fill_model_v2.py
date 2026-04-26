from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from project.core.execution_costs import estimate_fill_probability_v2, estimate_slippage_bps_v2


@dataclass(frozen=True)
class FillModelRequest:
    symbol: str
    side: str
    quantity: float
    order_type: str = "market"
    urgency: str = "aggressive"
    limit_price: float | None = None


@dataclass(frozen=True)
class FillModelConfig:
    fee_bps_per_side: float = 4.0
    profile: str = "base"
    latency_ms: int = 250
    passive_adverse_selection_bps: float = 0.2


@dataclass(frozen=True)
class FillModelResult:
    symbol: str
    side: str
    requested_quantity: float
    filled_quantity: float
    residual_quantity: float
    fill_probability: float
    expected_fill_price: float
    expected_slippage_bps: float
    expected_fee_bps: float
    expected_total_cost_bps: float
    latency_ms: int
    route: str


def estimate_fill_v2(
    request: FillModelRequest,
    market_state: Mapping[str, Any],
    config: FillModelConfig | None = None,
) -> FillModelResult:
    cfg = config or FillModelConfig()
    side = str(request.side).strip().upper()
    bid = float(market_state.get("bid", market_state.get("best_bid", 0.0)) or 0.0)
    ask = float(market_state.get("ask", market_state.get("best_ask", 0.0)) or 0.0)
    mid = float(market_state.get("mid_price", 0.0) or 0.0)
    if mid <= 0.0:
        mid = (
            (bid + ask) / 2.0 if bid > 0.0 and ask > 0.0 else float(market_state.get("price", 0.0))
        )
    spread_bps = float(
        market_state.get(
            "spread_bps",
            ((ask - bid) / max(mid, 1e-12)) * 10_000.0 if bid > 0.0 and ask > 0.0 else 1.0,
        )
    )
    liquidity = float(
        market_state.get(
            "depth_usd",
            market_state.get("liquidity_available", market_state.get("top_of_book_depth_usd", 1e6)),
        )
        or 1e6
    )
    vol_regime_bps = float(
        market_state.get("vol_regime_bps", market_state.get("volatility_bps", 0.0)) or 0.0
    )
    notional = abs(float(request.quantity)) * max(mid, 1.0)
    fill_probability = estimate_fill_probability_v2(
        order_size=notional,
        liquidity_available=liquidity,
        spread_bps=spread_bps,
        vol_regime_bps=vol_regime_bps,
        urgency=request.urgency,
        profile=cfg.profile,
    )
    slippage_bps = estimate_slippage_bps_v2(
        order_size=notional,
        spread_bps=spread_bps,
        liquidity_available=liquidity,
        vol_regime_bps=vol_regime_bps,
        urgency=request.urgency,
        profile=cfg.profile,
        passive_adverse_selection_bps=cfg.passive_adverse_selection_bps,
    )
    if request.urgency == "passive" and request.limit_price is not None:
        base_price = float(request.limit_price)
    elif request.urgency == "passive":
        base_price = bid if side == "BUY" and bid > 0.0 else ask if ask > 0.0 else mid
    elif side == "BUY":
        base_price = ask if ask > 0.0 else mid
    else:
        base_price = bid if bid > 0.0 else mid
    direction = 1.0 if side == "BUY" else -1.0
    fill_price = base_price * (1.0 + direction * (slippage_bps / 10_000.0))
    requested_qty = abs(float(request.quantity))
    filled_qty = (
        requested_qty if request.urgency == "aggressive" else requested_qty * fill_probability
    )
    residual_qty = max(0.0, requested_qty - filled_qty)
    return FillModelResult(
        symbol=str(request.symbol).upper(),
        side=side,
        requested_quantity=requested_qty,
        filled_quantity=filled_qty,
        residual_quantity=residual_qty,
        fill_probability=fill_probability,
        expected_fill_price=float(fill_price),
        expected_slippage_bps=slippage_bps,
        expected_fee_bps=float(cfg.fee_bps_per_side),
        expected_total_cost_bps=float(cfg.fee_bps_per_side) + slippage_bps,
        latency_ms=int(cfg.latency_ms),
        route=str(request.urgency),
    )
