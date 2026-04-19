"""
Live Order Management System (OMS) state machine.

Tracks the lifecycle of an order from submission to terminal state (FILLED, CANCELLED, REJECTED).
"""

from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Dict, List, Optional

import pandas as pd

from project.engine.strategy_executor import StrategyResult, build_live_order_metadata
from project.live.execution_attribution import (
    ExecutionAttributionRecord,
    build_execution_attribution_record,
    summarize_execution_attribution,
)
from project.live.health_checks import evaluate_pretrade_microstructure_gate

LOGGER = logging.getLogger(__name__)

_LIVE_STRATEGY_RESULT_REQUIRED_METADATA = frozenset(
    {
        "engine_execution_lag_bars_used",
        "strategy_effective_lag_bars",
        "fp_def_version",
        "live_order_metadata_template",
    }
)

_LIVE_EXECUTABLE_SPEC_REQUIRED_METADATA = frozenset(
    {
        "runtime_provenance_validated",
        "runtime_provenance_source",
        "run_id",
        "candidate_id",
        "blueprint_id",
        "source_path",
        "compiler_version",
        "generated_at_utc",
        "ontology_spec_hash",
        "promotion_track",
        "wf_status",
    }
)

_LIVE_ORDER_METADATA_TEMPLATE_KEYS = frozenset(
    {
        "strategy",
        "signal_timestamp",
        "volatility_regime",
        "microstructure_regime",
        "expected_entry_price",
        "expected_return_bps",
        "expected_adverse_bps",
        "expected_cost_bps",
        "expected_net_edge_bps",
        "realized_fee_bps",
    }
)


class OrderSubmissionBlocked(RuntimeError):
    """Raised when a pre-trade guard rejects an order before OMS activation."""


class OrderSubmissionFailed(RuntimeError):
    """Raised when a live order cannot be truthfully submitted."""


class OrderNeutralizationFailed(RuntimeError):
    """Raised when emergency cancel/flatten actions fail during live unwind."""


class OrderStatus(Enum):
    PENDING_NEW = auto()
    NEW = auto()
    PARTIALLY_FILLED = auto()
    FILLED = auto()
    PENDING_CANCEL = auto()
    CANCELLED = auto()
    REJECTED = auto()
    EXPIRED = auto()


class OrderSide(Enum):
    BUY = auto()
    SELL = auto()


class OrderType(Enum):
    LIMIT = auto()
    MARKET = auto()


_TERMINAL_STATES = frozenset(
    {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.EXPIRED}
)


@dataclass
class LiveOrder:
    client_order_id: str
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: float
    price: Optional[float] = None
    stop_price: Optional[float] = None

    # State
    status: OrderStatus = OrderStatus.PENDING_NEW
    filled_quantity: float = 0.0
    remaining_quantity: float = 0.0
    avg_fill_price: float = 0.0
    exchange_order_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Timestamps
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self):
        if self.remaining_quantity == 0:
            self.remaining_quantity = self.quantity

    def update_status(self, new_status: OrderStatus, exchange_id: Optional[str] = None):
        if self.status in _TERMINAL_STATES and new_status != self.status:
            LOGGER.warning(
                "Ignoring status transition %s -> %s for order %s (already terminal)",
                self.status.name,
                new_status.name,
                self.client_order_id,
            )
            return
        self.status = new_status
        if exchange_id:
            self.exchange_order_id = exchange_id
        self.updated_at = datetime.now(timezone.utc)

    def apply_fill(self, fill_qty: float, fill_price: float):
        if self.status in _TERMINAL_STATES:
            LOGGER.warning(
                "Ignoring fill for terminal order %s in status %s",
                self.client_order_id,
                self.status.name,
            )
            return
        total_filled = self.filled_quantity + fill_qty
        # Update WAP
        self.avg_fill_price = (
            (self.avg_fill_price * self.filled_quantity) + (fill_price * fill_qty)
        ) / total_filled

        self.filled_quantity = total_filled
        self.remaining_quantity = max(0.0, self.quantity - self.filled_quantity)

        if self.remaining_quantity <= 1e-10:
            self.update_status(OrderStatus.FILLED)
        else:
            self.update_status(OrderStatus.PARTIALLY_FILLED)


class OrderManager:
    def __init__(self, exchange_client: Any | None = None):
        self.active_orders: Dict[str, LiveOrder] = {}  # client_order_id -> LiveOrder
        self.order_history: List[LiveOrder] = []
        self.execution_attribution: List[ExecutionAttributionRecord] = []
        self.exchange_client = exchange_client

    def add_order(self, order: LiveOrder):
        self.active_orders[order.client_order_id] = order

    def get_order(self, client_order_id: str) -> Optional[LiveOrder]:
        return self.active_orders.get(client_order_id)

    @staticmethod
    def evaluate_microstructure_gate(
        *,
        spread_bps: float | None,
        depth_usd: float | None,
        tob_coverage: float | None,
        max_spread_bps: float,
        min_depth_usd: float,
        min_tob_coverage: float,
    ) -> Dict[str, Any]:
        return evaluate_pretrade_microstructure_gate(
            spread_bps=spread_bps,
            depth_usd=depth_usd,
            tob_coverage=tob_coverage,
            max_spread_bps=max_spread_bps,
            min_depth_usd=min_depth_usd,
            min_tob_coverage=min_tob_coverage,
        )

    @staticmethod
    def _submit_market_order_kwargs(order: LiveOrder) -> Dict[str, Any]:
        reduce_only = bool((order.metadata or {}).get("reduce_only", False))
        return {
            "symbol": order.symbol,
            "side": order.side.name,
            "quantity": order.quantity,
            "reduce_only": reduce_only,
        }

    def _market_order_call_kwargs(self, order: LiveOrder) -> Dict[str, Any]:
        kwargs = self._submit_market_order_kwargs(order)
        try:
            sig = inspect.signature(self.exchange_client.create_market_order)  # type: ignore[union-attr]
            params = sig.parameters
            if "new_client_order_id" in params or "newClientOrderId" in params:
                kwargs["new_client_order_id"] = order.client_order_id
        except Exception:
            pass
        return kwargs

    def submit_order(
        self,
        order: LiveOrder,
        *,
        kill_switch_manager: Any | None = None,
        market_state: Optional[Dict[str, float]] = None,
        max_spread_bps: float = 5.0,
        min_depth_usd: float = 25_000.0,
        min_tob_coverage: float = 0.80,
    ) -> Dict[str, Any]:
        """
        Activate an order only if live microstructure is tradable.

        ``add_order`` remains the raw state mutation primitive. New live order
        flow should prefer ``submit_order`` so the kill-switch can block unsafe
        conditions before the order becomes active.
        """
        gate = None
        if kill_switch_manager is not None:
            snapshot = dict(market_state or {})
            gate = kill_switch_manager.check_microstructure(
                spread_bps=snapshot.get("spread_bps"),
                depth_usd=snapshot.get("depth_usd", snapshot.get("liquidity_available")),
                tob_coverage=snapshot.get("tob_coverage"),
                max_spread_bps=max_spread_bps,
                min_depth_usd=min_depth_usd,
                min_tob_coverage=min_tob_coverage,
            )
            if not gate["is_tradable"]:
                order.update_status(OrderStatus.REJECTED)
                self.order_history.append(order)
                raise OrderSubmissionBlocked(
                    f"order {order.client_order_id} blocked by microstructure gate: "
                    f"{','.join(gate['reasons'])}"
                )

        if self.exchange_client is not None:
            raise OrderSubmissionFailed(
                "exchange-backed order managers require await submit_order_async(...) "
                "before reporting acceptance"
            )

        self.add_order(order)
        return {
            "accepted": True,
            "client_order_id": order.client_order_id,
            "gate": gate,
            "venue_submitted": False,
        }

    async def submit_order_async(
        self,
        order: LiveOrder,
        *,
        kill_switch_manager: Any | None = None,
        market_state: Optional[Dict[str, float]] = None,
        max_spread_bps: float = 5.0,
        min_depth_usd: float = 25_000.0,
        min_tob_coverage: float = 0.80,
    ) -> Dict[str, Any]:
        gate = None
        if kill_switch_manager is not None:
            snapshot = dict(market_state or {})
            gate = kill_switch_manager.check_microstructure(
                spread_bps=snapshot.get("spread_bps"),
                depth_usd=snapshot.get("depth_usd", snapshot.get("liquidity_available")),
                tob_coverage=snapshot.get("tob_coverage"),
                max_spread_bps=max_spread_bps,
                min_depth_usd=min_depth_usd,
                min_tob_coverage=min_tob_coverage,
            )
            if not gate["is_tradable"]:
                order.update_status(OrderStatus.REJECTED)
                self.order_history.append(order)
                raise OrderSubmissionBlocked(
                    f"order {order.client_order_id} blocked by microstructure gate: "
                    f"{','.join(gate['reasons'])}"
                )

        if self.exchange_client is None:
            self.add_order(order)
            return {
                "accepted": True,
                "client_order_id": order.client_order_id,
                "gate": gate,
                "venue_submitted": False,
            }

        if order.order_type != OrderType.MARKET:
            order.update_status(OrderStatus.REJECTED)
            self.order_history.append(order)
            raise OrderSubmissionFailed(
                f"exchange-backed OMS does not support {order.order_type.name} orders yet"
            )

        try:
            venue_kwargs = self._market_order_call_kwargs(order)
            try:
                sig = inspect.signature(self.exchange_client.create_market_order)
                params = sig.parameters
                if "new_client_order_id" in params or "newClientOrderId" in params:
                    venue_kwargs["new_client_order_id"] = order.client_order_id
            except Exception:
                pass
            venue_response = await self.exchange_client.create_market_order(**venue_kwargs)
        except Exception as exc:
            order.update_status(OrderStatus.REJECTED)
            self.order_history.append(order)
            raise OrderSubmissionFailed(
                f"venue rejected order {order.client_order_id}: {exc}"
            ) from exc

        exchange_order_id = None
        if isinstance(venue_response, dict):
            exchange_order_id = (
                str(
                    venue_response.get("orderId")
                    or venue_response.get("clientOrderId")
                    or venue_response.get("origClientOrderId")
                    or ""
                ).strip()
                or None
            )
        self.add_order(order)
        order.update_status(OrderStatus.NEW, exchange_id=exchange_order_id)
        return {
            "accepted": True,
            "client_order_id": order.client_order_id,
            "gate": gate,
            "venue_submitted": True,
            "venue_response": venue_response,
        }

    async def cancel_all_orders(self, symbol: Optional[str] = None):
        """Cancel all active orders for a symbol or ALL symbols via exchange client."""
        if not self.exchange_client:
            LOGGER.warning("No exchange client configured; skipping cancel_all_orders.")
            return

        symbols_to_cancel = (
            [symbol] if symbol else list(set(o.symbol for o in self.active_orders.values()))
        )
        failures: List[str] = []
        for sym in symbols_to_cancel:
            try:
                await self.exchange_client.cancel_all_open_orders(sym)
                LOGGER.info(f"Cancelled all open orders for {sym}")
            except Exception as e:
                LOGGER.error(f"Failed to cancel orders for {sym}: {e}")
                failures.append(f"{sym}: {e}")
        if failures:
            raise OrderNeutralizationFailed(
                "Failed to cancel all open orders during emergency unwind: " + "; ".join(failures)
            )

    async def flatten_all_positions(self, state_store: Any, symbol: Optional[str] = None):
        """Submit reactive market orders to close all positions in the state store."""
        if not self.exchange_client:
            LOGGER.warning("No exchange client configured; skipping flatten_all_positions.")
            return

        positions = state_store.account.positions
        symbols = [symbol.upper()] if symbol else list(positions.keys())
        failures: List[str] = []

        for sym in symbols:
            pos = positions.get(sym)
            if not pos or abs(pos.quantity) <= 1e-10:
                continue

            side = "SELL" if pos.side == "LONG" else "BUY"
            try:
                await self.exchange_client.create_market_order(
                    symbol=sym, side=side, quantity=pos.quantity, reduce_only=True
                )
                LOGGER.info(f"Submitted flattening order for {sym}: {side} {pos.quantity}")
            except Exception as e:
                LOGGER.error(f"Failed to flatten position for {sym}: {e}")
                failures.append(f"{sym}: {e}")
        if failures:
            raise OrderNeutralizationFailed(
                "Failed to flatten all positions during emergency unwind: " + "; ".join(failures)
            )

    def on_order_update(self, client_order_id: str, status: OrderStatus, **kwargs):
        order = self.get_order(client_order_id)
        if not order:
            LOGGER.warning(f"Received update for unknown order {client_order_id}")
            return

        previous_status = order.status
        order.update_status(status, exchange_id=kwargs.get("exchange_order_id"))
        if order.status != status:
            return

        if status in (
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        ):
            if client_order_id in self.active_orders:
                self.order_history.append(order)
                del self.active_orders[client_order_id]
            elif previous_status not in _TERMINAL_STATES:
                self.order_history.append(order)

    def on_fill(self, client_order_id: str, fill_qty: float, fill_price: float):
        order = self.get_order(client_order_id)
        if not order:
            LOGGER.warning(f"Received fill for unknown order {client_order_id}")
            return

        if order.status in _TERMINAL_STATES and order.status != OrderStatus.FILLED:
            LOGGER.warning(
                "Ignoring fill for terminal order %s in status %s",
                client_order_id,
                order.status.name,
            )
            return

        order.apply_fill(fill_qty, fill_price)

        if order.status == OrderStatus.FILLED:
            self._record_execution_attribution(order)
            if client_order_id in self.active_orders:
                self.order_history.append(order)
                del self.active_orders[client_order_id]

    def _record_execution_attribution(self, order: LiveOrder) -> None:
        metadata = dict(order.metadata or {})
        required = {"expected_entry_price", "expected_return_bps", "expected_adverse_bps"}
        if not required.issubset(metadata):
            return

        record = build_execution_attribution_record(
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            strategy=str(metadata.get("strategy", "")),
            thesis_id=str(metadata.get("thesis_id", "")),
            overlap_group_id=str(metadata.get("overlap_group_id", "")),
            governance_tier=str(metadata.get("governance_tier", "")),
            operational_role=str(metadata.get("operational_role", "")),
            active_episode_ids=list(metadata.get("active_episode_ids", [])),
            volatility_regime=str(metadata.get("volatility_regime", "")),
            microstructure_regime=str(metadata.get("microstructure_regime", "")),
            side=order.side.name,
            quantity=order.quantity,
            signal_timestamp=str(metadata.get("signal_timestamp", order.created_at.isoformat())),
            expected_entry_price=float(metadata["expected_entry_price"]),
            realized_fill_price=float(order.avg_fill_price),
            expected_return_bps=float(metadata["expected_return_bps"]),
            expected_adverse_bps=float(metadata["expected_adverse_bps"]),
            expected_cost_bps=float(metadata.get("expected_cost_bps", 0.0) or 0.0),
            realized_fee_bps=float(metadata.get("realized_fee_bps", 0.0) or 0.0),
        )
        self.execution_attribution.append(record)

    def summarize_execution_quality(self) -> Dict[str, float]:
        return summarize_execution_attribution(self.execution_attribution)

    async def close(self) -> None:
        closer = getattr(self.exchange_client, "close", None)
        if closer is None:
            return
        result = closer()
        if inspect.isawaitable(result):
            await result


def _validate_live_strategy_result_provenance(result: StrategyResult) -> None:
    metadata = getattr(result, "strategy_metadata", None)
    if not isinstance(metadata, dict):
        raise OrderSubmissionBlocked(
            "live trading requires a StrategyResult with validated runtime provenance"
        )

    missing = sorted(_LIVE_STRATEGY_RESULT_REQUIRED_METADATA - set(metadata.keys()))
    if missing:
        raise OrderSubmissionBlocked(
            "live trading requires validated runtime provenance fields: " + ", ".join(missing)
        )

    template = metadata.get("live_order_metadata_template")
    if not isinstance(template, dict):
        raise OrderSubmissionBlocked(
            "live trading requires a validated live_order_metadata_template mapping"
        )

    template_missing = sorted(_LIVE_ORDER_METADATA_TEMPLATE_KEYS - set(template.keys()))
    if template_missing:
        raise OrderSubmissionBlocked(
            "live trading requires validated live order metadata fields: "
            + ", ".join(template_missing)
        )

    contract_source = str(metadata.get("contract_source", "")).strip()
    provenance_source = str(metadata.get("runtime_provenance_source", "")).strip()
    is_dsl_runtime = str(getattr(result, "name", "")).startswith("dsl_interpreter_v1__")
    if contract_source == "dsl_blueprint" or (
        is_dsl_runtime
        and not provenance_source
        and not bool(metadata.get("runtime_provenance_validated"))
    ):
        raise OrderSubmissionBlocked(
            "live trading requires executable_strategy_spec-backed provenance for DSL strategies"
        )

    if (
        provenance_source == "executable_strategy_spec"
        or contract_source == "executable_strategy_spec"
    ):
        missing_exec = sorted(_LIVE_EXECUTABLE_SPEC_REQUIRED_METADATA - set(metadata.keys()))
        if missing_exec:
            raise OrderSubmissionBlocked(
                "live trading requires executable strategy provenance fields: "
                + ", ".join(missing_exec)
            )
        if not bool(metadata.get("runtime_provenance_validated")):
            raise OrderSubmissionBlocked(
                "live trading requires validated executable strategy provenance"
            )
        required_non_empty = (
            "run_id",
            "candidate_id",
            "blueprint_id",
            "source_path",
            "compiler_version",
            "generated_at_utc",
            "ontology_spec_hash",
            "promotion_track",
            "wf_status",
        )
        blank = [key for key in required_non_empty if not str(metadata.get(key, "")).strip()]
        if blank:
            raise OrderSubmissionBlocked(
                "live trading requires non-empty executable strategy provenance fields: "
                + ", ".join(blank)
            )
        if provenance_source != "executable_strategy_spec":
            raise OrderSubmissionBlocked(
                "live trading requires executable_strategy_spec-backed runtime provenance"
            )


def build_live_order_from_strategy_result(
    result: StrategyResult,
    *,
    client_order_id: str,
    timestamp: pd.Timestamp | None = None,
    order_type: OrderType = OrderType.MARKET,
    realized_fee_bps: float = 0.0,
) -> LiveOrder | None:
    if not isinstance(result, StrategyResult):
        raise OrderSubmissionBlocked(
            "live trading requires a project.engine.strategy_executor.StrategyResult"
        )

    frame = result.data.copy()
    if frame.empty:
        return None

    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    if timestamp is None:
        row = frame.iloc[-1]
    else:
        ts = pd.Timestamp(timestamp)
        ts = ts.tz_convert("UTC") if ts.tz is not None else ts.tz_localize("UTC")
        matched = frame.loc[frame["timestamp"] == ts]
        if matched.empty:
            raise KeyError(f"timestamp not found in strategy result: {ts}")
        row = matched.iloc[-1]

    current_target = float(row.get("target_position", 0.0) or 0.0)
    prior_position = float(row.get("prior_executed_position", 0.0) or 0.0)
    expected_entry_price = float(row.get("fill_price", row.get("close", 0.0)) or 0.0)
    delta_notional = current_target - prior_position
    if abs(delta_notional) <= 1e-12 or expected_entry_price <= 0.0:
        return None

    _validate_live_strategy_result_provenance(result)

    side = OrderSide.BUY if delta_notional > 0 else OrderSide.SELL
    quantity = abs(delta_notional) / expected_entry_price
    metadata = build_live_order_metadata(
        result,
        timestamp=row["timestamp"],
        realized_fee_bps=realized_fee_bps,
    )
    price = float(expected_entry_price) if order_type == OrderType.LIMIT else None

    return LiveOrder(
        client_order_id=client_order_id,
        symbol=str(row.get("symbol", "")).upper(),
        side=side,
        order_type=order_type,
        quantity=float(quantity),
        price=price,
        metadata=metadata,
    )
