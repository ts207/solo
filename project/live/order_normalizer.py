from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from project.live.venue_rules import VenueRuleCheck, VenueSymbolRules, check_and_normalize_order


@dataclass(frozen=True)
class NormalizedOrderResult:
    accepted: bool
    order: Any
    blocked_by: str = ""
    reasons: tuple[str, ...] = ()
    diagnostics: dict[str, Any] | None = None
    venue_check: VenueRuleCheck | None = None


def _reference_price(order: Any, market_state: Mapping[str, Any] | None = None) -> float:
    state = dict(market_state or {})
    metadata = getattr(order, "metadata", None) or {}
    candidates = (
        getattr(order, "price", None),
        state.get("mid_price"),
        state.get("mid"),
        state.get("last_price"),
        state.get("close"),
        metadata.get("expected_entry_price"),
    )
    for candidate in candidates:
        try:
            value = float(candidate)
        except (TypeError, ValueError):
            continue
        if value > 0.0:
            return value
    return 0.0


def normalize_order_for_venue(
    order: Any,
    *,
    venue_rules: VenueSymbolRules | None,
    market_state: Mapping[str, Any] | None = None,
    apply: bool = True,
) -> NormalizedOrderResult:
    if venue_rules is None:
        return NormalizedOrderResult(accepted=True, order=order)

    metadata = getattr(order, "metadata", None) or {}
    order_type = getattr(getattr(order, "order_type", None), "name", "").lower()
    side = getattr(getattr(order, "side", None), "name", "").lower()
    limit_price = getattr(order, "price", None)
    if order_type == "limit" and limit_price is None:
        return NormalizedOrderResult(
            accepted=False,
            order=order,
            blocked_by="venue_rules",
            reasons=("missing_limit_price",),
            diagnostics={"venue_rule_source": venue_rules.source},
        )

    venue_check = check_and_normalize_order(
        rules=venue_rules,
        order_type=order_type,
        side=side,
        quantity=float(getattr(order, "quantity", 0.0) or 0.0),
        reference_price=_reference_price(order, market_state),
        limit_price=float(limit_price) if limit_price is not None else None,
        reduce_only=bool(metadata.get("reduce_only", False)),
        post_only=bool(metadata.get("post_only", False)),
    )
    if not venue_check.accepted:
        return NormalizedOrderResult(
            accepted=False,
            order=order,
            blocked_by=venue_check.blocked_by,
            reasons=tuple(venue_check.reasons),
            diagnostics=dict(venue_check.diagnostics or {}),
            venue_check=venue_check,
        )

    if apply:
        order.quantity = float(venue_check.quantity)
        order.remaining_quantity = max(0.0, float(order.quantity) - float(order.filled_quantity))
        if venue_check.price is not None:
            order.price = float(venue_check.price)

    return NormalizedOrderResult(
        accepted=True,
        order=order,
        diagnostics=dict(venue_check.diagnostics or {}),
        venue_check=venue_check,
    )
