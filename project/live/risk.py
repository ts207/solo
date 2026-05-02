from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

from project.portfolio.risk_budget import calculate_execution_quality_multiplier

_LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class PerThesisCap:
    """Per-thesis risk overrides.  Zeros mean 'use global cap'."""

    thesis_id: str
    max_notional: float = 0.0  # per-order max notional for this thesis
    max_position_notional: float = 0.0  # maximum open position notional
    max_daily_loss: float = 0.0  # daily loss limit (absolute USD)
    max_active_orders: int = 0  # max simultaneous live orders
    max_active_positions: int = 0  # max simultaneous open positions


@dataclass(frozen=True)
class RuntimeRiskCaps:
    # Safe-by-default: meaningful paper/trading exposure must come from an explicit cap profile.
    max_gross_exposure: float = 0.0
    max_symbol_exposure: float = 0.0
    max_family_exposure: float = 0.0
    max_active_theses: int = 0
    max_order_notional: float = 0.0  # hard per-order ceiling (all theses)
    max_daily_loss: float = 0.0  # global daily loss limit (0 = no limit)
    reject_on_breach: bool = True  # If False, clip to cap
    slippage_budget_bps: float = 0.0  # 0 disables execution-quality slippage scaling
    min_fill_rate: float = 0.0  # 0 disables execution-quality fill-rate scaling
    min_execution_quality_multiplier: float = 0.10
    # Per-family risk budgets
    per_family_caps: dict[str, float] = field(default_factory=dict)
    # Per-thesis overrides indexed by thesis_id
    per_thesis: dict[str, PerThesisCap] = field(default_factory=dict)


@dataclass(frozen=True)
class CapBreachEvent:
    timestamp: str
    thesis_id: str
    symbol: str
    # gross|symbol|family|count|per_thesis_notional|per_thesis_daily_loss|
    # per_thesis_positions|per_thesis_orders|order_notional|global_daily_loss|
    # overlap_group_exclusive
    cap_type: str
    attempted_value: float
    cap_value: float
    action: str  # rejected, clipped


@dataclass
class DailyLossLedger:
    """
    Tracks realized + unrealized loss incurred today.

    Reset at UTC midnight or explicit operator reset.
    """

    _realized_loss: float = 0.0
    _unrealized_loss: float = 0.0
    _date: date = field(default_factory=lambda: datetime.now(UTC).date())
    # Per-thesis realized loss today
    _per_thesis: dict[str, float] = field(default_factory=dict)

    def _maybe_roll(self) -> None:
        today = datetime.now(UTC).date()
        if today != self._date:
            self._date = today
            self._realized_loss = 0.0
            self._unrealized_loss = 0.0
            self._per_thesis.clear()

    def record_fill_pnl(self, thesis_id: str, pnl: float) -> None:
        """Call after each fill is attributed.  pnl < 0 = loss."""
        self._maybe_roll()
        if pnl < 0.0:
            self._realized_loss += abs(pnl)
            self._per_thesis[thesis_id] = self._per_thesis.get(thesis_id, 0.0) + abs(pnl)

    def update_unrealized(self, total_unrealized_pnl: float) -> None:
        """Call on each account snapshot.  Negative unrealized PnL contributes."""
        self._maybe_roll()
        self._unrealized_loss = max(0.0, -float(total_unrealized_pnl))

    def global_loss_today(self) -> float:
        self._maybe_roll()
        return self._realized_loss + self._unrealized_loss

    def thesis_loss_today(self, thesis_id: str) -> float:
        self._maybe_roll()
        return self._per_thesis.get(thesis_id, 0.0)

    def reset(self) -> None:
        self._realized_loss = 0.0
        self._unrealized_loss = 0.0
        self._per_thesis.clear()
        self._date = datetime.now(UTC).date()


class RiskEnforcer:
    def __init__(self, caps: RuntimeRiskCaps):
        self.caps = caps
        self.breach_history: list[CapBreachEvent] = []
        self.daily_loss = DailyLossLedger()

    def _reject(
        self,
        timestamp: str,
        thesis_id: str,
        symbol: str,
        cap_type: str,
        attempted: float,
        cap_val: float,
    ) -> CapBreachEvent:
        ev = CapBreachEvent(
            timestamp=timestamp,
            thesis_id=thesis_id,
            symbol=symbol,
            cap_type=cap_type,
            attempted_value=attempted,
            cap_value=cap_val,
            action="rejected",
        )
        self.breach_history.append(ev)
        return ev

    def _clip(
        self,
        timestamp: str,
        thesis_id: str,
        symbol: str,
        cap_type: str,
        attempted: float,
        cap_val: float,
    ) -> CapBreachEvent:
        ev = CapBreachEvent(
            timestamp=timestamp,
            thesis_id=thesis_id,
            symbol=symbol,
            cap_type=cap_type,
            attempted_value=attempted,
            cap_value=cap_val,
            action="clipped",
        )
        self.breach_history.append(ev)
        return ev

    def _execution_quality_multiplier(self, portfolio_state: dict[str, Any]) -> float:
        raw_quality = portfolio_state.get("execution_quality", {})
        quality = raw_quality if isinstance(raw_quality, dict) else {}
        explicit_quality = portfolio_state.get(
            "execution_quality_multiplier",
            quality.get("multiplier", quality.get("quality_multiplier")),
        )
        realized_slippage = quality.get(
            "realized_slippage_bps",
            quality.get("avg_slippage_bps", quality.get("slippage_bps")),
        )
        fill_rate = quality.get("fill_rate", quality.get("recent_fill_rate"))
        has_config = self.caps.slippage_budget_bps > 0.0 or self.caps.min_fill_rate > 0.0
        if explicit_quality is None and not has_config:
            return 1.0
        return calculate_execution_quality_multiplier(
            realized_slippage_bps=(None if realized_slippage is None else float(realized_slippage)),
            slippage_budget_bps=(
                self.caps.slippage_budget_bps if self.caps.slippage_budget_bps > 0.0 else None
            ),
            fill_rate=None if fill_rate is None else float(fill_rate),
            min_fill_rate=self.caps.min_fill_rate if self.caps.min_fill_rate > 0.0 else None,
            explicit_quality=None if explicit_quality is None else float(explicit_quality),
            min_multiplier=self.caps.min_execution_quality_multiplier,
        )

    def check_and_apply_caps(
        self,
        *,
        thesis_id: str,
        symbol: str,
        family: str,
        attempted_notional: float,
        portfolio_state: dict[str, Any],
        active_thesis_ids: list[str],
        timestamp: str,
        active_order_count_by_thesis: dict[str, int] | None = None,
        active_position_count_by_thesis: dict[str, int] | None = None,
        thesis_overlap_group: str | None = None,
        active_overlap_groups: set[str] | None = None,
    ) -> tuple[float, CapBreachEvent | None]:
        """
        Enforce risk caps on a single trade intent.
        Returns (effective_notional, Optional breach event).

        Control hierarchy (spec §D):
          1. per-order global ceiling
          2. per-thesis notional cap
          3. per-thesis daily loss
          4. per-thesis active orders
          5. per-thesis active positions
          6. max active theses (count)
          6b. overlap group exclusivity (unified policy)
          7. per-symbol cap
          8. per-family notional cap (specific budget)
          9. global family exposure cap
          10. gross exposure cap
        """
        effective_notional = float(attempted_notional)
        last_breach: CapBreachEvent | None = None
        per = self.caps.per_thesis.get(thesis_id)

        # 1. Hard per-order ceiling (global)
        if self.caps.max_order_notional > 0.0 and effective_notional > self.caps.max_order_notional:
            if self.caps.reject_on_breach:
                return 0.0, self._reject(
                    timestamp,
                    thesis_id,
                    symbol,
                    "order_notional",
                    effective_notional,
                    self.caps.max_order_notional,
                )
            effective_notional = self.caps.max_order_notional
            last_breach = self._clip(
                timestamp,
                thesis_id,
                symbol,
                "order_notional",
                attempted_notional,
                self.caps.max_order_notional,
            )

        # 2. Per-thesis notional cap
        if per and per.max_notional > 0.0 and effective_notional > per.max_notional:
            if self.caps.reject_on_breach:
                return 0.0, self._reject(
                    timestamp,
                    thesis_id,
                    symbol,
                    "per_thesis_notional",
                    effective_notional,
                    per.max_notional,
                )
            effective_notional = per.max_notional
            last_breach = self._clip(
                timestamp,
                thesis_id,
                symbol,
                "per_thesis_notional",
                attempted_notional,
                per.max_notional,
            )

        # 3. Per-thesis daily loss
        if per and per.max_daily_loss > 0.0:
            loss = self.daily_loss.thesis_loss_today(thesis_id)
            if loss >= per.max_daily_loss:
                return 0.0, self._reject(
                    timestamp, thesis_id, symbol, "per_thesis_daily_loss", loss, per.max_daily_loss
                )

        # 3b. Global daily loss limit
        if self.caps.max_daily_loss > 0.0:
            global_loss = self.daily_loss.global_loss_today()
            if global_loss >= self.caps.max_daily_loss:
                return 0.0, self._reject(
                    timestamp,
                    thesis_id,
                    symbol,
                    "global_daily_loss",
                    global_loss,
                    self.caps.max_daily_loss,
                )

        # 4. Per-thesis active order count
        if per and per.max_active_orders > 0 and active_order_count_by_thesis is not None:
            current_orders = active_order_count_by_thesis.get(thesis_id, 0)
            if current_orders >= per.max_active_orders:
                return 0.0, self._reject(
                    timestamp,
                    thesis_id,
                    symbol,
                    "per_thesis_orders",
                    float(current_orders + 1),
                    float(per.max_active_orders),
                )

        # 5. Per-thesis active position count
        if per and per.max_active_positions > 0 and active_position_count_by_thesis is not None:
            current_pos = active_position_count_by_thesis.get(thesis_id, 0)
            if current_pos >= per.max_active_positions:
                return 0.0, self._reject(
                    timestamp,
                    thesis_id,
                    symbol,
                    "per_thesis_positions",
                    float(current_pos + 1),
                    float(per.max_active_positions),
                )

        # 6. Max Active Theses
        if thesis_id not in active_thesis_ids:
            if len(active_thesis_ids) >= self.caps.max_active_theses:
                return 0.0, self._reject(
                    timestamp,
                    thesis_id,
                    symbol,
                    "count",
                    float(len(active_thesis_ids) + 1),
                    float(self.caps.max_active_theses),
                )

        # 6b. Overlap Group Exclusivity
        group_id = str(thesis_overlap_group or "").strip()
        if group_id and active_overlap_groups and thesis_id not in active_thesis_ids:
            if group_id in active_overlap_groups:
                return 0.0, self._reject(
                    timestamp,
                    thesis_id,
                    symbol,
                    "overlap_group_exclusive",
                    1.0,
                    0.0,
                )

        # 6c. Execution quality degradation scales risk down before exposure caps.
        execution_quality_mult = self._execution_quality_multiplier(portfolio_state)
        if execution_quality_mult < 0.999999 and effective_notional > 0.0:
            scaled_notional = effective_notional * execution_quality_mult
            last_breach = self._clip(
                timestamp,
                thesis_id,
                symbol,
                "execution_quality",
                effective_notional,
                scaled_notional,
            )
            effective_notional = scaled_notional

        # 7. Per-Symbol Cap
        current_symbol_notional = portfolio_state.get("symbol_exposures", {}).get(symbol, 0.0)
        total_symbol_notional = abs(current_symbol_notional) + abs(effective_notional)
        if total_symbol_notional > self.caps.max_symbol_exposure:
            available = max(0.0, self.caps.max_symbol_exposure - abs(current_symbol_notional))
            if self.caps.reject_on_breach:
                return 0.0, self._reject(
                    timestamp,
                    thesis_id,
                    symbol,
                    "symbol",
                    total_symbol_notional,
                    self.caps.max_symbol_exposure,
                )
            effective_notional = available
            return effective_notional, self._clip(
                timestamp,
                thesis_id,
                symbol,
                "symbol",
                total_symbol_notional,
                self.caps.max_symbol_exposure,
            )

        # 8. Per-Family Notional Cap (Specific Budget)
        family_cap = self.caps.per_family_caps.get(family, 0.0)
        if family_cap > 0.0:
            current_family_notional = portfolio_state.get("family_exposures", {}).get(family, 0.0)
            total_family_notional = abs(current_family_notional) + abs(effective_notional)
            if total_family_notional > family_cap:
                available = max(0.0, family_cap - abs(current_family_notional))
                if self.caps.reject_on_breach:
                    return 0.0, self._reject(
                        timestamp,
                        thesis_id,
                        symbol,
                        "per_family_notional",
                        total_family_notional,
                        family_cap,
                    )
                effective_notional = available
                return effective_notional, self._clip(
                    timestamp,
                    thesis_id,
                    symbol,
                    "per_family_notional",
                    total_family_notional,
                    family_cap,
                )

        # 9. Global Family Cap
        current_family_notional = portfolio_state.get("family_exposures", {}).get(family, 0.0)
        total_family_notional = abs(current_family_notional) + abs(effective_notional)
        if total_family_notional > self.caps.max_family_exposure:
            available = max(0.0, self.caps.max_family_exposure - abs(current_family_notional))
            if self.caps.reject_on_breach:
                return 0.0, self._reject(
                    timestamp,
                    thesis_id,
                    symbol,
                    "family",
                    total_family_notional,
                    self.caps.max_family_exposure,
                )
            effective_notional = available
            return effective_notional, self._clip(
                timestamp,
                thesis_id,
                symbol,
                "family",
                total_family_notional,
                self.caps.max_family_exposure,
            )

        # 10. Max Gross Exposure
        current_gross = portfolio_state.get("gross_exposure", 0.0)
        total_gross = current_gross + abs(effective_notional)
        if total_gross > self.caps.max_gross_exposure:
            available = max(0.0, self.caps.max_gross_exposure - current_gross)
            if self.caps.reject_on_breach:
                return 0.0, self._reject(
                    timestamp, thesis_id, symbol, "gross", total_gross, self.caps.max_gross_exposure
                )
            effective_notional = available
            return effective_notional, self._clip(
                timestamp, thesis_id, symbol, "gross", total_gross, self.caps.max_gross_exposure
            )

        return effective_notional, last_breach
