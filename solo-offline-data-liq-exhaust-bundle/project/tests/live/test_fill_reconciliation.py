from __future__ import annotations

import pytest

from project.live.fill_reconciliation import (
    FillEvent,
    OrderLifecycleUpdate,
    apply_cancel_replace,
    reconcile_fill,
    reconcile_order_update,
)
from project.live.oms import LiveOrder, OrderSide, OrderStatus, OrderType


def test_fill_reconciliation_tracks_partial_and_final_fill() -> None:
    order = LiveOrder("cid_fill_1", "BTCUSDT", OrderSide.BUY, OrderType.LIMIT, 1.0, 100.0)

    reconcile_fill(order, FillEvent("cid_fill_1", 0.4, 100.0))
    assert order.status == OrderStatus.PARTIALLY_FILLED
    assert order.filled_quantity == 0.4
    assert order.remaining_quantity == 0.6

    reconcile_fill(order, FillEvent("cid_fill_1", 0.6, 101.0))
    assert order.status == OrderStatus.FILLED
    assert order.avg_fill_price == pytest.approx(100.6)


def test_reconcile_order_update_records_reject_reason() -> None:
    order = LiveOrder("cid_fill_2", "BTCUSDT", OrderSide.BUY, OrderType.LIMIT, 1.0, 100.0)

    reconcile_order_update(
        order,
        OrderLifecycleUpdate(
            "cid_fill_2",
            OrderStatus.REJECTED,
            exchange_order_id="ex-reject-1",
            reason="price_filter",
            raw={"code": -1013},
        ),
    )

    assert order.status == OrderStatus.REJECTED
    assert order.exchange_order_id == "ex-reject-1"
    assert order.metadata["last_lifecycle_reason"] == "price_filter"
    assert order.metadata["last_lifecycle_raw"] == {"code": -1013}


def test_cancel_replace_preserves_already_filled_quantity() -> None:
    order = LiveOrder("cid_fill_3", "BTCUSDT", OrderSide.BUY, OrderType.LIMIT, 1.0, 100.0)
    reconcile_fill(order, FillEvent("cid_fill_3", 0.3, 100.0))

    apply_cancel_replace(
        order,
        new_client_order_id="cid_fill_3_r1",
        new_quantity=0.8,
        new_price=99.5,
    )

    assert order.status == OrderStatus.PENDING_NEW
    assert order.quantity == 0.8
    assert order.filled_quantity == 0.3
    assert order.remaining_quantity == 0.5
    assert order.price == 99.5
    assert order.metadata["replaced_by_client_order_id"] == "cid_fill_3_r1"

    with pytest.raises(ValueError, match="below already-filled"):
        apply_cancel_replace(order, new_quantity=0.2)
