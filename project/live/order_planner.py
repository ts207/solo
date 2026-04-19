from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping

from project.live.contracts.trade_intent import TradeIntent
from project.live.oms import LiveOrder, OrderSide, OrderType
from project.live.venue_rules import VenueSymbolRules, check_and_normalize_order


@dataclass(frozen=True)
class OrderPlan:
    accepted: bool
    client_order_id: str
    order: LiveOrder | None
    plan: Dict[str, Any]
    blocked_by: str = ""


def _resolve_order_quantity(
    *,
    size_fraction: float,
    entry_price: float,
    available_balance: float,
    max_notional_fraction: float,
) -> float:
    balance = max(0.0, float(available_balance))
    cap_fraction = max(0.0, float(max_notional_fraction))
    size = max(0.0, float(size_fraction))
    notional = balance * cap_fraction * size
    if entry_price <= 0.0 or notional <= 0.0:
        return 0.0
    return notional / entry_price


def build_order_plan(
    *,
    intent: TradeIntent,
    client_order_id: str,
    market_state: Mapping[str, Any],
    portfolio_state: Mapping[str, Any],
    max_notional_fraction: float = 0.10,
    order_type: OrderType = OrderType.MARKET,
    venue_rules: VenueSymbolRules | None = None,
) -> OrderPlan:
    if intent.action not in {"probe", "trade_small", "trade_normal"}:
        return OrderPlan(
            accepted=False,
            client_order_id=client_order_id,
            order=None,
            plan={"action": intent.action, "size_fraction": intent.size_fraction},
            blocked_by="non_trade_action",
        )
    if intent.side not in {"buy", "sell"}:
        return OrderPlan(
            accepted=False,
            client_order_id=client_order_id,
            order=None,
            plan={"action": intent.action, "size_fraction": intent.size_fraction},
            blocked_by="missing_trade_side",
        )
    entry_price = float(
        market_state.get("mid_price")
        or market_state.get("last_price")
        or market_state.get("close")
        or 0.0
    )
    engine_notional = float(market_state.get("engine_allocated_notional", 0.0) or 0.0)
    if engine_notional > 0.0 and entry_price > 0.0:
        quantity = engine_notional / entry_price
    else:
        available_balance = float(portfolio_state.get("available_balance", 0.0) or 0.0)
        quantity = _resolve_order_quantity(
            size_fraction=float(intent.size_fraction),
            entry_price=entry_price,
            available_balance=available_balance,
            max_notional_fraction=float(max_notional_fraction),
        )
    if quantity <= 0.0:
        return OrderPlan(
            accepted=False,
            client_order_id=client_order_id,
            order=None,
            plan={"entry_price": entry_price, "available_balance": available_balance},
            blocked_by="zero_quantity",
        )
    side = OrderSide.BUY if intent.side == "buy" else OrderSide.SELL
    reduce_only = bool(intent.metadata.get("reduce_only", False))
    post_only = bool(intent.metadata.get("post_only", False))
    price = entry_price if order_type == OrderType.LIMIT else None
    venue_rule_diagnostics: Dict[str, Any] = {}
    if venue_rules is not None:
        venue_check = check_and_normalize_order(
            rules=venue_rules,
            order_type=order_type.name.lower(),
            side=intent.side,
            quantity=quantity,
            reference_price=entry_price,
            limit_price=price,
            reduce_only=reduce_only,
            post_only=post_only,
        )
        venue_rule_diagnostics = dict(venue_check.diagnostics or {})
        if not venue_check.accepted:
            return OrderPlan(
                accepted=False,
                client_order_id=client_order_id,
                order=None,
                plan={
                    "entry_price": entry_price,
                    "requested_quantity": quantity,
                    "size_fraction": float(intent.size_fraction),
                    "action": intent.action,
                    "venue_rule_reasons": list(venue_check.reasons),
                    "venue_rules": venue_rule_diagnostics,
                },
                blocked_by=venue_check.blocked_by,
            )
        quantity = float(venue_check.quantity)
        price = venue_check.price if venue_check.price is not None else price

    metadata = {
        "strategy": str(intent.thesis_id or "promoted_thesis"),
        "signal_timestamp": str(market_state.get("timestamp", "")),
        "volatility_regime": str(market_state.get("canonical_regime", "")),
        "microstructure_regime": str(market_state.get("microstructure_regime", "")),
        "expected_entry_price": float(entry_price),
        "expected_return_bps": float(
            intent.metadata.get("expected_return_bps", market_state.get("expected_return_bps", 0.0))
        ),
        "expected_adverse_bps": float(
            intent.metadata.get(
                "expected_adverse_bps", market_state.get("expected_adverse_bps", 0.0)
            )
        ),
        "expected_cost_bps": float(market_state.get("expected_cost_bps", 0.0) or 0.0),
        "expected_net_edge_bps": float(market_state.get("expected_net_edge_bps", 0.0) or 0.0),
        "realized_fee_bps": 0.0,
        "thesis_id": str(intent.thesis_id),
        "trade_intent_action": intent.action,
        "overlap_group_id": str(intent.metadata.get("overlap_group_id", "")),
        "governance_tier": str(intent.metadata.get("governance_tier", "")),
        "operational_role": str(intent.metadata.get("operational_role", "")),
        "active_episode_ids": list(intent.metadata.get("active_episode_ids", [])),
        "reduce_only": reduce_only,
        "post_only": post_only,
        "venue_rules": venue_rule_diagnostics,
    }
    order = LiveOrder(
        client_order_id=client_order_id,
        symbol=intent.symbol,
        side=side,
        order_type=order_type,
        quantity=float(quantity),
        price=price,
        metadata=metadata,
    )
    return OrderPlan(
        accepted=True,
        client_order_id=client_order_id,
        order=order,
        plan={
            "entry_price": entry_price,
            "quantity": quantity,
            "size_fraction": float(intent.size_fraction),
            "action": intent.action,
        },
    )
