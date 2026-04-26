"""
Live state tracking for account and positions.

Provides a unified 'LiveState' container to track balance, active positions,
and exchange status in real-time.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from project.core.exceptions import DataIntegrityError


@dataclass
class PositionState:
    symbol: str
    side: str  # "LONG" | "SHORT"
    quantity: float
    entry_price: float
    mark_price: float
    unrealized_pnl: float
    liquidation_price: float | None = None
    leverage: float = 1.0
    margin_type: str = "ISOLATED"
    cluster_id: int | None = None
    update_time: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self):
        self.symbol = self.symbol.upper()
        self.side = self.side.upper()


@dataclass
class AccountState:
    wallet_balance: float = 0.0
    margin_balance: float = 0.0
    available_balance: float = 0.0
    total_unrealized_pnl: float = 0.0
    positions: dict[str, PositionState] = field(default_factory=dict)
    update_time: datetime = field(default_factory=lambda: datetime.now(UTC))
    exchange_status: str = "NORMAL"  # NORMAL | DEGRADED | DOWN

    def update_position(self, pos: PositionState):
        self.positions[pos.symbol] = pos
        self._recalculate_totals()

    def remove_position(self, symbol: str):
        if symbol.upper() in self.positions:
            del self.positions[symbol.upper()]
            self._recalculate_totals()

    def _recalculate_totals(self):
        self.total_unrealized_pnl = sum(p.unrealized_pnl for p in self.positions.values())
        self.update_time = datetime.now(UTC)


@dataclass
class KillSwitchSnapshot:
    is_active: bool = False
    reason: str | None = None
    triggered_at: str | None = None
    message: str = ""
    recovery_streak: int = 0


def _write_snapshot_blocking(target: Path, payload: str) -> None:
    """Write snapshot payload to disk. Runs in a thread pool via asyncio.to_thread."""
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(payload, encoding="utf-8")


class LiveStateStore:
    """Thread-safe store for live account state and persisted snapshots."""

    def __init__(self, *, snapshot_path: str | Path | None = None):
        self.account = AccountState()
        self._last_snapshot_time: datetime | None = None
        self.kill_switch = KillSwitchSnapshot()
        self._snapshot_path = Path(snapshot_path) if snapshot_path is not None else None
        self._lock = threading.RLock()
        # Per-entity disable state — keys are thesis_id / symbol / family
        # Value: {"disabled": bool, "reason": str, "at": str}
        self.thesis_disable_state: dict[str, dict[str, Any]] = {}
        self.symbol_disable_state: dict[str, dict[str, Any]] = {}
        self.family_disable_state: dict[str, dict[str, Any]] = {}

    def _maybe_persist(self) -> None:
        if self._snapshot_path is not None:
            self.save_snapshot(self._snapshot_path)

    def update_from_exchange_snapshot(self, data: dict[str, Any]) -> None:
        """
        Update state from a full exchange account/position snapshot.
        Expected format: typical CCXT or Binance account information.
        """
        with self._lock:
            self.account.wallet_balance = float(
                data.get("wallet_balance", self.account.wallet_balance)
            )
            self.account.margin_balance = float(
                data.get("margin_balance", self.account.margin_balance)
            )
            self.account.available_balance = float(
                data.get("available_balance", self.account.available_balance)
            )
            self.account.exchange_status = str(
                data.get("exchange_status", self.account.exchange_status)
            )

            if "positions" in data:
                positions_raw = list(data.get("positions", []))
                seen_symbols: set[str] = set()
                for p in positions_raw:
                    qty = float(p.get("quantity", 0.0))
                    symbol = str(p.get("symbol")).upper()
                    if not symbol:
                        continue
                    seen_symbols.add(symbol)
                    if qty == 0:
                        self.account.remove_position(symbol)
                    else:
                        pos = PositionState(
                            symbol=symbol,
                            side="LONG" if qty > 0 else "SHORT",
                            quantity=abs(qty),
                            entry_price=float(p.get("entry_price", 0.0)),
                            mark_price=float(p.get("mark_price", p.get("entry_price", 0.0))),
                            unrealized_pnl=float(p.get("unrealized_pnl", 0.0)),
                            liquidation_price=float(p.get("liquidation_price", 0.0))
                            if p.get("liquidation_price")
                            else None,
                            leverage=float(p.get("leverage", 1.0) or 1.0),
                            margin_type=str(p.get("margin_type", "ISOLATED")),
                            cluster_id=p.get("cluster_id"),
                        )
                        self.account.update_position(pos)
                for symbol in list(self.account.positions):
                    if symbol not in seen_symbols:
                        self.account.remove_position(symbol)

            self.account._recalculate_totals()
            self._last_snapshot_time = self.account.update_time
            self._maybe_persist()

    def reconcile(
        self,
        exchange_data: dict[str, Any],
        balance_tolerance_usd: float = 0.10,
        qty_tolerance_fraction: float = 1e-4,
    ) -> list[str]:
        """
        Compare current state with exchange data and return a list of discrepancies.
        Discrepancies are returned as human-readable error messages.
        """
        errors = []

        # 1. Compare Wallet Balance with an absolute USD tolerance.
        exchange_wallet = float(exchange_data.get("wallet_balance", 0.0))
        if abs(self.account.wallet_balance - exchange_wallet) > float(balance_tolerance_usd):
            errors.append(
                f"Wallet balance mismatch: local={self.account.wallet_balance}, "
                f"exchange={exchange_wallet}"
            )

        # 2. Compare Positions with quantity-relative tolerance.
        exchange_positions = {
            str(p["symbol"]).upper(): p for p in exchange_data.get("positions", [])
        }
        local_positions = self.account.positions

        all_symbols = set(exchange_positions.keys()) | set(local_positions.keys())
        for sym in all_symbols:
            e_pos = exchange_positions.get(sym)
            l_pos = local_positions.get(sym)

            e_qty = float(e_pos["quantity"]) if e_pos else 0.0
            l_qty = float(l_pos.quantity) if l_pos else 0.0
            if l_pos and l_pos.side == "SHORT":
                l_qty = -l_qty

            scale = max(abs(e_qty), abs(l_qty), 1.0)
            abs_tol = max(scale * float(qty_tolerance_fraction), 1e-8)
            if abs(e_qty - l_qty) > abs_tol:
                errors.append(
                    f"Position mismatch for {sym}: local_qty={l_qty}, exchange_qty={e_qty}"
                )

        return errors

    def set_kill_switch_snapshot(self, snapshot: dict[str, Any]) -> None:
        self.kill_switch = KillSwitchSnapshot(
            is_active=bool(snapshot.get("is_active", False)),
            reason=str(snapshot["reason"]) if snapshot.get("reason") else None,
            triggered_at=str(snapshot["triggered_at"]) if snapshot.get("triggered_at") else None,
            message=str(snapshot.get("message", "")),
            recovery_streak=int(snapshot.get("recovery_streak", 0) or 0),
        )
        self._maybe_persist()

    def get_kill_switch_snapshot(self) -> dict[str, Any]:
        return {
            "is_active": bool(self.kill_switch.is_active),
            "reason": self.kill_switch.reason,
            "triggered_at": self.kill_switch.triggered_at,
            "message": self.kill_switch.message,
            "recovery_streak": int(self.kill_switch.recovery_streak),
        }

    # ------------------------------------------------------------------
    # Per-entity disable state (thesis / symbol / family)
    # ------------------------------------------------------------------

    def set_entity_disabled(self, scope: str, key: str, *, reason: str, at: str) -> None:
        """Mark an entity (thesis/symbol/family) as disabled."""
        with self._lock:
            target = self._entity_store(scope)
            target[key] = {"disabled": True, "reason": reason, "at": at}
            self._maybe_persist()

    def set_entity_enabled(self, scope: str, key: str) -> None:
        """Re-enable a previously disabled entity."""
        with self._lock:
            target = self._entity_store(scope)
            if key in target:
                target[key] = {"disabled": False, "reason": "", "at": ""}
                self._maybe_persist()

    def is_entity_disabled(self, scope: str, key: str) -> bool:
        with self._lock:
            entry = self._entity_store(scope).get(key, {})
            return bool(entry.get("disabled", False))

    def get_entity_state(self, scope: str, key: str) -> dict[str, Any]:
        with self._lock:
            return dict(self._entity_store(scope).get(key, {}))

    def _entity_store(self, scope: str) -> dict[str, dict[str, Any]]:
        if scope == "thesis":
            return self.thesis_disable_state
        if scope == "symbol":
            return self.symbol_disable_state
        if scope == "family":
            return self.family_disable_state
        raise ValueError(f"Unknown entity scope: {scope!r}")

    def to_snapshot(self) -> dict[str, Any]:
        return {
            "account": {
                "wallet_balance": float(self.account.wallet_balance),
                "margin_balance": float(self.account.margin_balance),
                "available_balance": float(self.account.available_balance),
                "total_unrealized_pnl": float(self.account.total_unrealized_pnl),
                "exchange_status": str(self.account.exchange_status),
                "update_time": self.account.update_time.isoformat(),
                "positions": [
                    {
                        "symbol": pos.symbol,
                        "side": pos.side,
                        "quantity": float(pos.quantity),
                        "entry_price": float(pos.entry_price),
                        "mark_price": float(pos.mark_price),
                        "unrealized_pnl": float(pos.unrealized_pnl),
                        "liquidation_price": pos.liquidation_price,
                        "leverage": float(pos.leverage),
                        "margin_type": pos.margin_type,
                        "cluster_id": pos.cluster_id,
                        "update_time": pos.update_time.isoformat(),
                    }
                    for pos in self.account.positions.values()
                ],
            },
            "kill_switch": self.get_kill_switch_snapshot(),
            "thesis_disable_state": dict(self.thesis_disable_state),
            "symbol_disable_state": dict(self.symbol_disable_state),
            "family_disable_state": dict(self.family_disable_state),
            "last_snapshot_time": (
                self._last_snapshot_time.isoformat()
                if self._last_snapshot_time is not None
                else None
            ),
        }

    @classmethod
    def from_snapshot(cls, snapshot: dict[str, Any]) -> LiveStateStore:
        store = cls()
        account = dict(snapshot.get("account", {}))
        store.account.wallet_balance = float(account.get("wallet_balance", 0.0))
        store.account.margin_balance = float(account.get("margin_balance", 0.0))
        store.account.available_balance = float(account.get("available_balance", 0.0))
        store.account.total_unrealized_pnl = float(account.get("total_unrealized_pnl", 0.0))
        store.account.exchange_status = str(account.get("exchange_status", "NORMAL"))
        account_update_time = account.get("update_time")
        if account_update_time:
            store.account.update_time = datetime.fromisoformat(str(account_update_time))
        store.account.positions = {}
        for raw in list(account.get("positions", [])):
            pos = PositionState(
                symbol=str(raw.get("symbol", "")),
                side=str(raw.get("side", "LONG")),
                quantity=float(raw.get("quantity", 0.0)),
                entry_price=float(raw.get("entry_price", 0.0)),
                mark_price=float(raw.get("mark_price", 0.0)),
                unrealized_pnl=float(raw.get("unrealized_pnl", 0.0)),
                liquidation_price=(
                    float(raw["liquidation_price"])
                    if raw.get("liquidation_price") is not None
                    else None
                ),
                leverage=float(raw.get("leverage", 1.0)),
                margin_type=str(raw.get("margin_type", "ISOLATED")),
                cluster_id=raw.get("cluster_id"),
            )
            update_time = raw.get("update_time")
            if update_time:
                pos.update_time = datetime.fromisoformat(str(update_time))
            store.account.positions[pos.symbol] = pos
        store.set_kill_switch_snapshot(dict(snapshot.get("kill_switch", {})))
        for scope in ("thesis", "symbol", "family"):
            raw = snapshot.get(f"{scope}_disable_state", {})
            if isinstance(raw, dict):
                store._entity_store(scope).update(raw)
        last_snapshot_time = snapshot.get("last_snapshot_time")
        if last_snapshot_time:
            store._last_snapshot_time = datetime.fromisoformat(str(last_snapshot_time))
        return store

    def save_snapshot(self, path: str | Path) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(self.to_snapshot(), indent=2, sort_keys=True), encoding="utf-8"
        )
        return target

    async def save_snapshot_async(self, path: str | Path) -> Path:
        """Non-blocking snapshot write for use inside async tasks (e.g. kill-switch handler).

        Delegates the blocking file I/O to a thread pool so the event loop is not
        stalled during the write.
        """
        import asyncio

        target = Path(path)
        payload = json.dumps(self.to_snapshot(), indent=2, sort_keys=True)
        await asyncio.to_thread(_write_snapshot_blocking, target, payload)
        return target

    @classmethod
    def load_snapshot(cls, path: str | Path) -> LiveStateStore:
        source = Path(path)
        try:
            payload = json.loads(source.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DataIntegrityError(f"Failed to read live state snapshot {source}: {exc}") from exc
        if not isinstance(payload, dict):
            raise DataIntegrityError(f"Live state snapshot {source} must be a JSON object")
        store = cls.from_snapshot(payload)
        store._snapshot_path = source
        return store
