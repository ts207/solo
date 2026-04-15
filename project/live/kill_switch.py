"""
Live Kill-Switch and Unwind Orchestration.

Monitors drift and account health to trigger automated de-risking (kill-switches)
and position unwinding.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum, auto

# Imported lazily to avoid a circular import (audit_log -> kill_switch -> audit_log)
# Use TYPE_CHECKING to satisfy type checkers without causing import cycles.
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

import pandas as pd

from project.live.health_checks import evaluate_pretrade_microstructure_gate
from project.live.oms import LiveOrder, OrderSide, OrderType
from project.live.state import LiveStateStore

if TYPE_CHECKING:
    from project.live.audit_log import AuditLog

LOGGER = logging.getLogger(__name__)


class KillSwitchReason(Enum):
    FEATURE_DRIFT = auto()
    EXECUTION_DRIFT = auto()
    EXCESSIVE_DRAWDOWN = auto()
    EXCHANGE_DISCONNECT = auto()
    STALE_DATA = auto()
    MICROSTRUCTURE_BREAKDOWN = auto()
    ACCOUNT_SYNC_LOSS = auto()
    MANUAL = auto()


@dataclass
class KillSwitchStatus:
    is_active: bool = False
    reason: Optional[KillSwitchReason] = None
    triggered_at: Optional[datetime] = None
    message: str = ""
    recovery_streak: int = 0
    peak_equity: float = 0.0


class KillSwitchManager:
    TIER1_FEATURES = [
        "vol_regime",
        "ms_spread_state",
        "funding_abs_pct",
        "basis_zscore",
        "oi_delta_1h",
        "spread_bps",
    ]
    PSI_ERROR_THRESHOLD = 0.25
    PSI_WARN_THRESHOLD = 0.10

    def __init__(
        self,
        state_store: LiveStateStore,
        *,
        microstructure_recovery_streak: int = 3,
        audit_log: "AuditLog | None" = None,
    ):
        self.state_store = state_store
        self.status = KillSwitchStatus()
        self._on_trigger_callbacks: List[Callable[[KillSwitchReason, str], None]] = []
        self.microstructure_recovery_streak = max(1, int(microstructure_recovery_streak))
        self._lock = threading.RLock()
        self._audit_log = audit_log
        self._load_persisted_status()

    # ------------------------------------------------------------------
    # Per-entity operator controls (thesis / symbol / family)
    # ------------------------------------------------------------------

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _emit_audit(self, **kwargs: Any) -> None:
        if self._audit_log is None:
            return
        try:
            from project.live.audit_log import KillSwitchEvent as _KSEvent

            self._audit_log.append(_KSEvent(**kwargs))
        except Exception as exc:
            LOGGER.error("Failed to write kill switch audit event: %s", exc)

    def disable_thesis(
        self, thesis_id: str, reason: str = "operator_disable", *, operator: str = ""
    ) -> None:
        """Disable a single thesis.  Durable across restart."""
        LOGGER.warning("THESIS DISABLED: %s — %s", thesis_id, reason)
        self.state_store.set_entity_disabled("thesis", thesis_id, reason=reason, at=self._now_iso())
        self._emit_audit(
            action="thesis_disabled",
            scope=f"thesis:{thesis_id}",
            reason=reason,
            operator=operator,
        )

    def resume_thesis(self, thesis_id: str, *, operator: str = "") -> None:
        LOGGER.info("Thesis %s re-enabled.", thesis_id)
        self.state_store.set_entity_enabled("thesis", thesis_id)
        self._emit_audit(
            action="thesis_resumed",
            scope=f"thesis:{thesis_id}",
            reason="operator_resume",
            operator=operator,
        )

    def disable_symbol(
        self, symbol: str, reason: str = "operator_disable", *, operator: str = ""
    ) -> None:
        """Block all theses from trading a given symbol."""
        LOGGER.warning("SYMBOL DISABLED: %s — %s", symbol, reason)
        self.state_store.set_entity_disabled(
            "symbol", symbol.upper(), reason=reason, at=self._now_iso()
        )
        self._emit_audit(
            action="symbol_disabled",
            scope=f"symbol:{symbol.upper()}",
            reason=reason,
            operator=operator,
        )

    def resume_symbol(self, symbol: str, *, operator: str = "") -> None:
        LOGGER.info("Symbol %s re-enabled.", symbol)
        self.state_store.set_entity_enabled("symbol", symbol.upper())
        self._emit_audit(
            action="symbol_resumed",
            scope=f"symbol:{symbol.upper()}",
            reason="operator_resume",
            operator=operator,
        )

    def disable_family(
        self, family: str, reason: str = "operator_disable", *, operator: str = ""
    ) -> None:
        """Block all theses belonging to an event family."""
        LOGGER.warning("FAMILY DISABLED: %s — %s", family, reason)
        self.state_store.set_entity_disabled(
            "family", family.upper(), reason=reason, at=self._now_iso()
        )
        self._emit_audit(
            action="family_disabled",
            scope=f"family:{family.upper()}",
            reason=reason,
            operator=operator,
        )

    def resume_family(self, family: str, *, operator: str = "") -> None:
        LOGGER.info("Family %s re-enabled.", family)
        self.state_store.set_entity_enabled("family", family.upper())
        self._emit_audit(
            action="family_resumed",
            scope=f"family:{family.upper()}",
            reason="operator_resume",
            operator=operator,
        )

    def is_thesis_blocked(
        self,
        thesis_id: str,
        symbol: str,
        family: str = "",
    ) -> tuple[bool, str]:
        """
        Return (blocked: bool, reason: str).

        Checks (in priority order):
          1. Global kill switch
          2. Symbol-level disable
          3. Family-level disable
          4. Per-thesis disable
        """
        # 1. Global
        with self._lock:
            if self.status.is_active:
                reason = self.status.reason.name if self.status.reason else "GLOBAL_KILL"
                return True, f"global_kill:{reason}"

        # 2. Symbol
        sym_key = str(symbol or "").strip().upper()
        if sym_key and self.state_store.is_entity_disabled("symbol", sym_key):
            return True, f"symbol_disabled:{sym_key}"

        # 3. Family
        fam_key = str(family or "").strip().upper()
        if fam_key and self.state_store.is_entity_disabled("family", fam_key):
            return True, f"family_disabled:{fam_key}"

        # 4. Thesis
        if self.state_store.is_entity_disabled("thesis", thesis_id):
            return True, f"thesis_disabled:{thesis_id}"

        return False, ""

    def register_callback(self, callback: Callable[[KillSwitchReason, str], None]):
        self._on_trigger_callbacks.append(callback)

    def _serialize_status(self) -> Dict[str, Any]:
        return {
            "is_active": bool(self.status.is_active),
            "reason": self.status.reason.name if self.status.reason is not None else None,
            "triggered_at": self.status.triggered_at.isoformat()
            if self.status.triggered_at
            else None,
            "message": str(self.status.message),
            "recovery_streak": int(self.status.recovery_streak),
            "peak_equity": float(self.status.peak_equity),
        }

    def _persist_status(self) -> None:
        with self._lock:
            self.state_store.set_kill_switch_snapshot(self._serialize_status())

    def _load_persisted_status(self) -> None:
        with self._lock:
            snapshot = self.state_store.get_kill_switch_snapshot()
        reason_name = snapshot.get("reason")
        reason = None
        if reason_name:
            try:
                reason = KillSwitchReason[str(reason_name)]
            except KeyError:
                LOGGER.warning("Unknown persisted kill-switch reason %r; ignoring.", reason_name)
        triggered_at_raw = snapshot.get("triggered_at")
        triggered_at = None
        if triggered_at_raw:
            try:
                triggered_at = datetime.fromisoformat(str(triggered_at_raw))
            except ValueError:
                LOGGER.warning(
                    "Invalid persisted kill-switch timestamp %r; ignoring.", triggered_at_raw
                )
        self.status = KillSwitchStatus(
            is_active=bool(snapshot.get("is_active", False)),
            reason=reason,
            triggered_at=triggered_at,
            message=str(snapshot.get("message", "")),
            recovery_streak=int(snapshot.get("recovery_streak", 0) or 0),
            peak_equity=float(snapshot.get("peak_equity", 0.0)),
        )

    def trigger(self, reason: KillSwitchReason, message: str = ""):
        callbacks: List[Callable[[KillSwitchReason, str], None]] = []
        with self._lock:
            if self.status.is_active:
                return

            self.status = KillSwitchStatus(
                is_active=True,
                reason=reason,
                triggered_at=datetime.now(timezone.utc),
                message=message,
                recovery_streak=0,
                peak_equity=self.status.peak_equity,  # preserve high-water mark
            )
            self._persist_status()
            callbacks = list(self._on_trigger_callbacks)
        LOGGER.critical(f"KILL-SWITCH TRIGGERED: {reason.name} - {message}")

        for cb in callbacks:
            try:
                cb(reason, message)
            except Exception as e:
                LOGGER.error(f"Error in kill-switch callback: {e}")

    def reset(self):
        with self._lock:
            self.status = KillSwitchStatus(
                is_active=False,
                peak_equity=self.status.peak_equity,  # preserve high-water mark
            )
            self._persist_status()
        LOGGER.info("Kill-switch reset.")

    def check_drawdown(self, max_drawdown_pct: float = 0.10):
        """Trigger if current drawdown from peak equity exceeds limit."""
        with self._lock:
            equity = self.state_store.account.wallet_balance
            unrealized = self.state_store.account.total_unrealized_pnl
            current_total_equity = equity + unrealized

            # Update high-water mark
            if current_total_equity > self.status.peak_equity:
                self.status.peak_equity = current_total_equity
                self._persist_status()

            peak = max(self.status.peak_equity, 1e-9)
            drawdown = (peak - current_total_equity) / peak

            if drawdown > max_drawdown_pct:
                self.trigger(
                    KillSwitchReason.EXCESSIVE_DRAWDOWN,
                    f"Drawdown {drawdown:.2%} exceeded limit {max_drawdown_pct:.2%}"
                    f" (Peak: {peak:.2f}, Current: {current_total_equity:.2f})",
                )

    def evaluate_microstructure_gate(
        self,
        *,
        spread_bps: float | None,
        depth_usd: float | None,
        tob_coverage: float | None,
        max_spread_bps: float,
        min_depth_usd: float,
        min_tob_coverage: float,
    ) -> Dict[str, object]:
        gate = evaluate_pretrade_microstructure_gate(
            spread_bps=spread_bps,
            depth_usd=depth_usd,
            tob_coverage=tob_coverage,
            max_spread_bps=max_spread_bps,
            min_depth_usd=min_depth_usd,
            min_tob_coverage=min_tob_coverage,
        )
        with self._lock:
            if self.status.is_active and self.status.reason not in {
                None,
                KillSwitchReason.MICROSTRUCTURE_BREAKDOWN,
            }:
                gate["is_tradable"] = False
                gate["reasons"] = list(gate.get("reasons", [])) + ["kill_switch_active"]
                gate["recovery_streak"] = int(self.status.recovery_streak)
                gate["required_recovery_streak"] = int(self.microstructure_recovery_streak)
            elif (
                self.status.is_active
                and self.status.reason == KillSwitchReason.MICROSTRUCTURE_BREAKDOWN
            ):
                gate["is_tradable"] = False
                gate["reasons"] = list(gate.get("reasons", [])) + ["microstructure_cooldown"]
                gate["recovery_streak"] = int(self.status.recovery_streak)
                gate["required_recovery_streak"] = int(self.microstructure_recovery_streak)
        return gate

    def check_microstructure(
        self,
        *,
        spread_bps: float | None,
        depth_usd: float | None,
        tob_coverage: float | None,
        max_spread_bps: float,
        min_depth_usd: float,
        min_tob_coverage: float,
    ) -> Dict[str, object]:
        """
        Trigger a live kill-switch when market microstructure exits the tradable envelope.
        """
        gate = evaluate_pretrade_microstructure_gate(
            spread_bps=spread_bps,
            depth_usd=depth_usd,
            tob_coverage=tob_coverage,
            max_spread_bps=max_spread_bps,
            min_depth_usd=min_depth_usd,
            min_tob_coverage=min_tob_coverage,
        )
        with self._lock:
            if self.status.is_active and self.status.reason not in {
                None,
                KillSwitchReason.MICROSTRUCTURE_BREAKDOWN,
            }:
                gate["is_tradable"] = False
                gate["reasons"] = list(gate.get("reasons", [])) + ["kill_switch_active"]
                gate["recovery_streak"] = int(self.status.recovery_streak)
                gate["required_recovery_streak"] = int(self.microstructure_recovery_streak)
                return gate

            if not gate["is_tradable"]:
                if (
                    self.status.is_active
                    and self.status.reason == KillSwitchReason.MICROSTRUCTURE_BREAKDOWN
                ):
                    self.status.recovery_streak = 0
                    self._persist_status()
                    gate["recovery_streak"] = 0
                    gate["required_recovery_streak"] = int(self.microstructure_recovery_streak)
                    return gate

                self.status.recovery_streak = 0
                self._persist_status()
                details = ",".join(gate["reasons"]) or "microstructure_breakdown"
                self.trigger(
                    KillSwitchReason.MICROSTRUCTURE_BREAKDOWN,
                    (
                        f"Pre-trade microstructure gate failed ({details}): "
                        f"spread_bps={gate['spread_bps']}, depth_usd={gate['depth_usd']}, "
                        f"tob_coverage={gate['tob_coverage']}"
                    ),
                )
                gate["recovery_streak"] = 0
                gate["required_recovery_streak"] = int(self.microstructure_recovery_streak)
                return gate

            if (
                self.status.is_active
                and self.status.reason == KillSwitchReason.MICROSTRUCTURE_BREAKDOWN
            ):
                self.status.recovery_streak += 1
                if self.status.recovery_streak < self.microstructure_recovery_streak:
                    gate["is_tradable"] = False
                    gate["reasons"] = ["microstructure_cooldown"]
                    gate["recovery_streak"] = int(self.status.recovery_streak)
                    gate["required_recovery_streak"] = int(self.microstructure_recovery_streak)
                    self.status.message = (
                        "Microstructure recovery in progress "
                        f"({self.status.recovery_streak}/{self.microstructure_recovery_streak})"
                    )
                    self._persist_status()
                    return gate
                self.reset()
                gate["recovered"] = True

            gate["recovery_streak"] = int(self.status.recovery_streak)
            gate["required_recovery_streak"] = int(self.microstructure_recovery_streak)
        return gate

    def check_feature_drift(
        self,
        research_features: pd.DataFrame,
        live_features: pd.DataFrame,
        threshold: float | None = None,
    ) -> Dict[str, Any]:
        """
        Check for feature drift between research baseline and live features.
        Triggers kill-switch if PSI exceeds PSI_ERROR_THRESHOLD on any tier-1 feature.
        """
        error_threshold = self.PSI_ERROR_THRESHOLD if threshold is None else threshold
        drift_results: Dict[str, Any] = {
            "drifted_features": [],
            "psi_scores": {},
            "triggered": False,
        }

        for feature in self.TIER1_FEATURES:
            if feature not in research_features.columns or feature not in live_features.columns:
                continue

            research_samples = research_features[feature].dropna()
            live_samples = live_features[feature].dropna()

            if research_samples.empty or live_samples.empty:
                continue

            from project.live.drift import calculate_feature_drift

            drift = calculate_feature_drift(
                research_samples,
                live_samples,
                threshold=self.PSI_WARN_THRESHOLD,
            )

            psi = drift.get("psi", 0.0)
            drift_results["psi_scores"][feature] = psi

            if psi > error_threshold:
                drift_results["drifted_features"].append(feature)

        if drift_results["drifted_features"]:
            drift_results["triggered"] = True
            features_str = ", ".join(drift_results["drifted_features"])
            psi_str = ", ".join(f"{k}={v:.3f}" for k, v in drift_results["psi_scores"].items())
            self.trigger(
                KillSwitchReason.FEATURE_DRIFT,
                f"Feature drift detected: {features_str} (PSI: {psi_str})",
            )

        return drift_results


class UnwindOrchestrator:
    """
    Handles the actual closing of positions once a kill-switch is triggered.
    """

    def __init__(self, state_store: LiveStateStore, oms_manager: Any):
        self.state_store = state_store
        self.oms_manager = oms_manager
        self.is_unwinding = False
        self._lock = asyncio.Lock()

    async def unwind_all(self):
        """
        Produce market-sell/market-buy orders for all active positions.

        Sequentially unwinds positions to avoid overwhelming liquidity.
        Uses deterministic client order IDs so retries remain idempotent.
        """
        async with self._lock:
            if self.is_unwinding:
                return
            self.is_unwinding = True
        try:
            # 1. Cancel all open orders first
            await self.oms_manager.cancel_all_orders()

            # 2. Get positions
            positions = self.state_store.account.positions
            if not positions:
                LOGGER.info("No positions to unwind.")
                return

            # Sort by size (USD notional) descending to reduce risk fastest
            sorted_positions = sorted(
                positions.values(),
                key=lambda p: abs(p.quantity * p.mark_price),
                reverse=True,
            )

            # 3. Flatten positions sequentially with retry logic
            for pos in sorted_positions:
                if abs(pos.quantity) <= 1e-10:
                    continue

                symbol = pos.symbol
                side = OrderSide.SELL if pos.side == "LONG" else OrderSide.BUY
                client_order_id = f"kill-{symbol}-{uuid.uuid4().hex}"

                LOGGER.info("Unwinding %s %s %s", symbol, side.name, pos.quantity)

                max_retries = 5
                backoff = 0.5
                for attempt in range(max_retries):
                    try:
                        current_pos = self.state_store.account.positions.get(symbol)
                        if not current_pos or abs(current_pos.quantity) <= 1e-10:
                            LOGGER.info("Position %s already flat", symbol)
                            break

                        order = LiveOrder(
                            client_order_id=client_order_id,
                            symbol=symbol,
                            side=side,
                            order_type=OrderType.MARKET,
                            quantity=float(current_pos.quantity),
                            metadata={"idempotency_key": client_order_id, "reduce_only": True},
                        )
                        submitter = getattr(self.oms_manager, "submit_order_async", None)
                        if callable(submitter):
                            await submitter(order)
                        else:
                            await self.oms_manager.exchange_client.create_market_order(
                                symbol=symbol,
                                side=side.name,
                                quantity=float(current_pos.quantity),
                                reduce_only=True,
                                new_client_order_id=client_order_id,
                            )

                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2.0, 5.0)

                        current_pos = self.state_store.account.positions.get(symbol)
                        if not current_pos or abs(current_pos.quantity) <= 1e-10:
                            LOGGER.info("Successfully unwound %s", symbol)
                            break

                        LOGGER.warning("Partial unwind for %s, retrying...", symbol)

                    except Exception as e:
                        LOGGER.error("Unwind attempt %s failed for %s: %s", attempt + 1, symbol, e)
                        await asyncio.sleep(min(backoff, 5.0))
                        backoff = min(backoff * 2.0, 5.0)

            LOGGER.warning("Emergency unwind orchestration completed.")
        except Exception as e:
            LOGGER.error("Error during emergency unwind: %s", e)
        finally:
            self.is_unwinding = False
