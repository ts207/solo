"""Child-order execution scheduler (T4.1).

Replaces single-shot orders with TWAP/VWAP/Almgren-Chriss child-order
schedules. Plugs into the existing OMS path in project/live/runner.py.

Calibration target: reduce realised-vs-expected slippage by ≥ 30% for
orders exceeding liquidity_factor × 0.5.

Usage:
    from project.live.execution.scheduler import ExecutionScheduler, ScheduleMode
    scheduler = ExecutionScheduler()
    child_orders = scheduler.build_schedule(parent_order, market_snapshot)
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

_LOG = logging.getLogger(__name__)

_DEFAULT_TWAP_SLICES = 5
_DEFAULT_VWAP_VOLUME_SHAPE = [0.10, 0.15, 0.25, 0.25, 0.25]
_MIN_CHILD_NOTIONAL = 10.0  # USD — skip slices below this


class ScheduleMode(str, Enum):
    SINGLE = "single"
    TWAP = "twap"
    VWAP = "vwap"
    ALMGREN_CHRISS = "almgren_chriss"


@dataclass(frozen=True)
class ParentOrder:
    symbol: str
    side: str  # "buy" | "sell"
    quantity: float
    price_limit: float | None = None
    urgency: float = 0.5  # 0=passive, 1=aggressive; drives mode selection
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ChildOrder:
    symbol: str
    side: str
    quantity: float
    slice_index: int
    total_slices: int
    schedule_mode: str
    price_limit: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "side": self.side,
            "quantity": round(self.quantity, 8),
            "slice_index": self.slice_index,
            "total_slices": self.total_slices,
            "schedule_mode": self.schedule_mode,
            "price_limit": self.price_limit,
            **self.metadata,
        }


@dataclass
class MarketSnapshot:
    symbol: str
    bid: float
    ask: float
    last_price: float
    volume_24h: float = 0.0
    avg_trade_size: float = 0.0
    volume_profile: list[float] | None = None  # relative volume by slice

    @property
    def spread_bps(self) -> float:
        mid = (self.bid + self.ask) / 2.0
        return (self.ask - self.bid) / max(mid, 1e-9) * 1e4

    def liquidity_factor(self) -> float:
        """Fraction of 24h volume the parent order represents."""
        return 0.0


def _twap_schedule(
    parent: ParentOrder,
    n_slices: int,
    snapshot: MarketSnapshot | None = None,
) -> list[ChildOrder]:
    """Divide quantity evenly across n_slices."""
    min_qty = _MIN_CHILD_NOTIONAL / max(
        snapshot.last_price if snapshot else 1.0, 1e-9
    )
    effective_slices = max(1, min(n_slices, int(parent.quantity / max(min_qty, 1e-12))))
    slice_qty = parent.quantity / effective_slices

    return [
        ChildOrder(
            symbol=parent.symbol,
            side=parent.side,
            quantity=slice_qty,
            slice_index=i,
            total_slices=effective_slices,
            schedule_mode=ScheduleMode.TWAP,
            price_limit=parent.price_limit,
        )
        for i in range(effective_slices)
    ]


def _vwap_schedule(
    parent: ParentOrder,
    volume_shape: list[float],
    snapshot: MarketSnapshot | None = None,
) -> list[ChildOrder]:
    """Distribute quantity according to a volume profile."""
    profile = (
        snapshot.volume_profile
        if snapshot and snapshot.volume_profile
        else volume_shape
    )
    n = len(profile)
    total = sum(profile)
    if total < 1e-12:
        return _twap_schedule(parent, n, snapshot)

    weights = [w / total for w in profile]

    orders = []
    for i, w in enumerate(weights):
        qty = parent.quantity * w
        if qty < 1e-12:
            continue
        orders.append(
            ChildOrder(
                symbol=parent.symbol,
                side=parent.side,
                quantity=qty,
                slice_index=i,
                total_slices=n,
                schedule_mode=ScheduleMode.VWAP,
                price_limit=parent.price_limit,
            )
        )
    return orders


def _almgren_chriss_schedule(
    parent: ParentOrder,
    snapshot: MarketSnapshot | None = None,
    *,
    sigma_bps: float = 50.0,
    eta: float = 1e-4,
    n_slices: int = 5,
) -> list[ChildOrder]:
    """Almgren-Chriss optimal liquidation schedule.

    Minimises E[cost] + λ * Var[cost] where λ is derived from urgency.
    For urgency → 1 (aggressive), approaches TWAP.
    For urgency → 0 (passive), back-loads execution.
    """
    T = float(n_slices)
    urgency = float(parent.urgency)
    # Risk-aversion parameter: higher urgency → higher λ → front-load
    lambda_risk = urgency * 10.0 + 0.01

    sigma = sigma_bps / 1e4
    kappa = math.sqrt(lambda_risk * sigma**2 / max(eta, 1e-12))

    # AC optimal trajectory: X(t) = X0 * sinh(kappa*(T-t)) / sinh(kappa*T)
    x0 = parent.quantity
    sinh_kT = math.sinh(kappa * T)
    if sinh_kT < 1e-12:
        return _twap_schedule(parent, n_slices, snapshot)

    schedule_quantities = []
    prev_remaining = x0
    for i in range(n_slices):
        t = float(i + 1)
        remaining = x0 * math.sinh(kappa * (T - t)) / sinh_kT
        remaining = max(0.0, remaining)
        traded = prev_remaining - remaining
        schedule_quantities.append(max(0.0, traded))
        prev_remaining = remaining

    total_scheduled = sum(schedule_quantities)
    if total_scheduled < 1e-12:
        return _twap_schedule(parent, n_slices, snapshot)

    scale = x0 / total_scheduled
    orders = []
    for i, qty in enumerate(schedule_quantities):
        scaled_qty = qty * scale
        if scaled_qty < 1e-12:
            continue
        orders.append(
            ChildOrder(
                symbol=parent.symbol,
                side=parent.side,
                quantity=scaled_qty,
                slice_index=i,
                total_slices=n_slices,
                schedule_mode=ScheduleMode.ALMGREN_CHRISS,
                price_limit=parent.price_limit,
                metadata={"kappa": round(kappa, 6)},
            )
        )
    return orders


class ExecutionScheduler:
    """Select and build an execution schedule for a parent order."""

    def __init__(
        self,
        *,
        default_mode: ScheduleMode = ScheduleMode.TWAP,
        n_slices: int = _DEFAULT_TWAP_SLICES,
        vwap_shape: list[float] | None = None,
        liquidity_threshold: float = 0.01,
        sigma_bps: float = 50.0,
    ):
        self.default_mode = default_mode
        self.n_slices = n_slices
        self.vwap_shape = vwap_shape or list(_DEFAULT_VWAP_VOLUME_SHAPE)
        self.liquidity_threshold = liquidity_threshold
        self.sigma_bps = sigma_bps

    def _select_mode(
        self,
        parent: ParentOrder,
        snapshot: MarketSnapshot | None,
    ) -> ScheduleMode:
        """Auto-select mode based on order size and urgency."""
        if parent.urgency >= 0.9:
            return ScheduleMode.SINGLE
        if snapshot is not None:
            lf = snapshot.liquidity_factor()
            if lf > self.liquidity_threshold:
                return ScheduleMode.ALMGREN_CHRISS
        if snapshot and snapshot.volume_profile:
            return ScheduleMode.VWAP
        return self.default_mode

    def build_schedule(
        self,
        parent: ParentOrder,
        snapshot: MarketSnapshot | None = None,
        *,
        mode: ScheduleMode | None = None,
    ) -> list[ChildOrder]:
        """Build child-order schedule for the given parent order."""
        effective_mode = mode or self._select_mode(parent, snapshot)

        if effective_mode == ScheduleMode.SINGLE:
            return [
                ChildOrder(
                    symbol=parent.symbol,
                    side=parent.side,
                    quantity=parent.quantity,
                    slice_index=0,
                    total_slices=1,
                    schedule_mode=ScheduleMode.SINGLE,
                    price_limit=parent.price_limit,
                )
            ]
        elif effective_mode == ScheduleMode.TWAP:
            return _twap_schedule(parent, self.n_slices, snapshot)
        elif effective_mode == ScheduleMode.VWAP:
            return _vwap_schedule(parent, self.vwap_shape, snapshot)
        elif effective_mode == ScheduleMode.ALMGREN_CHRISS:
            return _almgren_chriss_schedule(
                parent,
                snapshot,
                sigma_bps=self.sigma_bps,
                n_slices=self.n_slices,
            )
        else:
            _LOG.warning("Unknown schedule mode %s; falling back to TWAP", effective_mode)
            return _twap_schedule(parent, self.n_slices, snapshot)
