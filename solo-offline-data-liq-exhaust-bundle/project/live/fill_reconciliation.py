from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from project.live.oms import LiveOrder, OrderStatus


@dataclass(frozen=True)
class FillEvent:
    client_order_id: str
    fill_qty: float
    fill_price: float
    venue_fill_id: str = ""


@dataclass(frozen=True)
class OrderLifecycleUpdate:
    client_order_id: str
    status: OrderStatus
    exchange_order_id: str | None = None
    reason: str = ""
    raw: dict[str, Any] | None = None


def reconcile_fill(order: LiveOrder, event: FillEvent) -> LiveOrder:
    if event.client_order_id != order.client_order_id:
        raise ValueError(
            f"fill event {event.client_order_id} does not match order {order.client_order_id}"
        )
    order.apply_fill(float(event.fill_qty), float(event.fill_price))
    return order


def reconcile_order_update(order: LiveOrder, update: OrderLifecycleUpdate) -> LiveOrder:
    if update.client_order_id != order.client_order_id:
        raise ValueError(
            f"lifecycle update {update.client_order_id} does not match order "
            f"{order.client_order_id}"
        )
    order.update_status(update.status, exchange_id=update.exchange_order_id)
    if update.reason:
        order.metadata["last_lifecycle_reason"] = update.reason
    if update.raw is not None:
        order.metadata["last_lifecycle_raw"] = dict(update.raw)
    return order


def apply_cancel_replace(
    order: LiveOrder,
    *,
    new_client_order_id: str | None = None,
    new_quantity: float | None = None,
    new_price: float | None = None,
) -> LiveOrder:
    if new_quantity is not None:
        requested_quantity = float(new_quantity)
        if requested_quantity < float(order.filled_quantity):
            raise ValueError("replacement quantity cannot be below already-filled quantity")
        order.quantity = requested_quantity
        order.remaining_quantity = max(0.0, order.quantity - order.filled_quantity)
    if new_price is not None:
        order.price = float(new_price)
    if new_client_order_id:
        order.metadata["replaced_by_client_order_id"] = str(new_client_order_id)
    order.update_status(OrderStatus.PENDING_NEW)
    return order
