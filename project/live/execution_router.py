from __future__ import annotations

import inspect
from dataclasses import dataclass, replace
from typing import Any, Mapping

from project.live.oms import LiveOrder, OrderType
from project.live.order_book_view import OrderBookView
from project.live.order_normalizer import NormalizedOrderResult, normalize_order_for_venue
from project.live.venue_rules import VenueSymbolRules


class ExecutionRouterError(RuntimeError):
    """Raised when an order cannot be routed truthfully to a venue."""


@dataclass(frozen=True)
class ExecutionRoute:
    route_type: str
    order_type: OrderType
    time_in_force: str | None = None
    reduce_only: bool = False
    post_only: bool = False
    price: float | None = None

    @property
    def is_passive(self) -> bool:
        return self.route_type == "post_only" or self.post_only


@dataclass(frozen=True)
class ExecutionSubmission:
    accepted: bool
    order: LiveOrder
    route: ExecutionRoute
    venue_submitted: bool
    venue_response: Any = None
    exchange_order_id: str | None = None
    normalization: NormalizedOrderResult | None = None


def _supports_kwarg(callable_obj: Any, key: str) -> bool:
    try:
        params = inspect.signature(callable_obj).parameters
    except (TypeError, ValueError):
        return False
    return key in params


def _filter_supported_kwargs(callable_obj: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    try:
        params = inspect.signature(callable_obj).parameters
    except (TypeError, ValueError):
        return kwargs
    if any(param.kind == inspect.Parameter.VAR_KEYWORD for param in params.values()):
        return kwargs
    return {key: value for key, value in kwargs.items() if key in params}


def _extract_exchange_order_id(venue_response: Any) -> str | None:
    if not isinstance(venue_response, Mapping):
        return None
    nested = venue_response.get("result")
    response = nested if isinstance(nested, Mapping) else venue_response
    raw = (
        response.get("orderId")
        or response.get("order_id")
        or response.get("clientOrderId")
        or response.get("origClientOrderId")
        or response.get("orderLinkId")
        or ""
    )
    value = str(raw).strip()
    return value or None


class ExecutionRouter:
    def __init__(self, exchange_client: Any | None = None):
        self.exchange_client = exchange_client

    def build_route(
        self,
        order: LiveOrder,
        *,
        market_state: Mapping[str, Any] | None = None,
    ) -> ExecutionRoute:
        metadata = dict(order.metadata or {})
        reduce_only = bool(metadata.get("reduce_only", False))
        post_only = bool(metadata.get("post_only", False))
        preference = str(
            metadata.get("route_preference")
            or (market_state or {}).get("route_preference")
            or ""
        ).strip().lower()
        book = OrderBookView.from_market_state(market_state, symbol=order.symbol)

        if post_only or preference == "passive":
            price = (
                order.price
                if order.price is not None
                else book.passive_limit_price(order.side.name)
            )
            if price is None:
                raise ExecutionRouterError(
                    f"passive route for {order.client_order_id} requires a limit price or bid/ask"
                )
            order.order_type = OrderType.LIMIT
            order.price = float(price)
            order.metadata["post_only"] = True
            return ExecutionRoute(
                route_type="post_only",
                order_type=OrderType.LIMIT,
                time_in_force=str(metadata.get("time_in_force") or "GTX"),
                reduce_only=reduce_only,
                post_only=True,
                price=float(order.price),
            )

        if reduce_only and order.order_type == OrderType.MARKET:
            return ExecutionRoute(
                route_type="reduce_only",
                order_type=OrderType.MARKET,
                reduce_only=True,
            )

        if order.order_type == OrderType.LIMIT:
            if order.price is None:
                price = book.aggressive_limit_price(order.side.name)
                if price is None:
                    raise ExecutionRouterError(
                        f"limit route for {order.client_order_id} requires a price or bid/ask"
                    )
                order.price = float(price)
            return ExecutionRoute(
                route_type="limit",
                order_type=OrderType.LIMIT,
                time_in_force=str(metadata.get("time_in_force") or "GTC"),
                reduce_only=reduce_only,
                post_only=False,
                price=float(order.price),
            )

        return ExecutionRoute(
            route_type="market",
            order_type=OrderType.MARKET,
            reduce_only=reduce_only,
        )

    async def submit(
        self,
        order: LiveOrder,
        *,
        market_state: Mapping[str, Any] | None = None,
        venue_rules: VenueSymbolRules | None = None,
    ) -> ExecutionSubmission:
        route = self.build_route(order, market_state=market_state)
        normalization = normalize_order_for_venue(
            order,
            venue_rules=venue_rules,
            market_state=market_state,
            apply=True,
        )
        if not normalization.accepted:
            raise ExecutionRouterError(
                f"venue rules rejected order {order.client_order_id}: "
                f"{','.join(normalization.reasons)}"
            )
        if route.order_type == OrderType.LIMIT:
            route = replace(route, price=order.price)

        if self.exchange_client is None:
            return ExecutionSubmission(
                accepted=True,
                order=order,
                route=route,
                venue_submitted=False,
                normalization=normalization,
            )

        if route.order_type == OrderType.MARKET:
            creator = getattr(self.exchange_client, "create_market_order", None)
            if creator is None:
                raise ExecutionRouterError("exchange client does not expose create_market_order")
            kwargs = {
                "symbol": order.symbol,
                "side": order.side.name,
                "quantity": order.quantity,
                "reduce_only": route.reduce_only,
            }
            if _supports_kwarg(creator, "new_client_order_id"):
                kwargs["new_client_order_id"] = order.client_order_id
            response = await creator(**_filter_supported_kwargs(creator, kwargs))
        else:
            creator = getattr(self.exchange_client, "create_limit_order", None)
            if creator is None:
                raise ExecutionRouterError("exchange client does not expose create_limit_order")
            if order.price is None:
                raise ExecutionRouterError("limit submission requires price")
            kwargs = {
                "symbol": order.symbol,
                "side": order.side.name,
                "quantity": order.quantity,
                "price": float(order.price),
                "time_in_force": route.time_in_force or "GTC",
                "reduce_only": route.reduce_only,
                "post_only": route.post_only,
            }
            if _supports_kwarg(creator, "new_client_order_id"):
                kwargs["new_client_order_id"] = order.client_order_id
            response = await creator(**_filter_supported_kwargs(creator, kwargs))

        return ExecutionSubmission(
            accepted=True,
            order=order,
            route=route,
            venue_submitted=True,
            venue_response=response,
            exchange_order_id=_extract_exchange_order_id(response),
            normalization=normalization,
        )
