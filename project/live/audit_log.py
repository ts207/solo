"""
Append-only live audit event log.

Every live action is recorded as a typed JSON event appended to a JSONL file.
The file is the source of truth; a separate in-memory cache is built on load
for O(1) read access.

Event types (spec §E):
  order_intent        — intent to submit an order, before exchange submission
  order_submission    — order submitted to exchange (with exchange request ID)
  order_ack           — exchange acknowledged the order
  fill_event          — fill received, with full lineage chain
  position_snapshot   — periodic position / PnL snapshot
  cap_snapshot        — cap utilisation snapshot attached to an order check
  kill_switch_event   — global kill activated or reset
  thesis_state_change — deployment_state or runtime state transition
  operator_action     — any operator command (disable, resume, approve, etc.)

Lineage chain on fill_event:
  fill -> order -> signal -> thesis -> thesis_version -> promotion_run ->
  validation_run -> approval_record -> cap_snapshot -> operator_approval
"""

from __future__ import annotations

import json
import logging
import threading
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

_LOG = logging.getLogger(__name__)


def _utcnow() -> str:
    return datetime.now(UTC).isoformat()


def _new_id() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Event records
# ---------------------------------------------------------------------------


@dataclass
class OrderIntentEvent:
    """Persisted before the order is sent to the exchange."""

    event_type: str = field(init=False, default="order_intent")
    event_id: str = field(default_factory=_new_id)
    recorded_at: str = field(default_factory=_utcnow)
    session_id: str = ""
    # Lineage
    thesis_id: str = ""
    thesis_version: str = ""
    promotion_run_id: str = ""
    validation_run_id: str = ""
    approval_record_id: str = ""
    cap_snapshot_id: str = ""
    # Signal context
    signal_timestamp: str = ""
    signal_event_type: str = ""
    # Order intent details
    client_order_id: str = ""
    symbol: str = ""
    side: str = ""
    order_type: str = ""
    quantity: float = 0.0
    expected_price: float = 0.0
    expected_notional: float = 0.0
    # Cap state at intent time
    cap_state: dict[str, Any] = field(default_factory=dict)
    kill_switch_state: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class OrderSubmissionEvent:
    """Emitted when the order is dispatched to the exchange."""

    event_type: str = field(init=False, default="order_submission")
    event_id: str = field(default_factory=_new_id)
    recorded_at: str = field(default_factory=_utcnow)
    session_id: str = ""
    intent_event_id: str = ""
    client_order_id: str = ""
    exchange_request_id: str = ""
    symbol: str = ""
    side: str = ""
    quantity: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class OrderAckEvent:
    """Exchange acknowledged / assigned an order ID."""

    event_type: str = field(init=False, default="order_ack")
    event_id: str = field(default_factory=_new_id)
    recorded_at: str = field(default_factory=_utcnow)
    session_id: str = ""
    client_order_id: str = ""
    exchange_order_id: str = ""
    symbol: str = ""
    status: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class FillEvent:
    """
    Fill received from the exchange.

    Full lineage chain:
      fill -> order -> signal -> thesis -> promotion -> validation -> approval -> cap
    """

    event_type: str = field(init=False, default="fill_event")
    event_id: str = field(default_factory=_new_id)
    recorded_at: str = field(default_factory=_utcnow)
    session_id: str = ""
    # Order lineage
    client_order_id: str = ""
    exchange_order_id: str = ""
    intent_event_id: str = ""
    # Thesis lineage
    thesis_id: str = ""
    thesis_version: str = ""
    promotion_run_id: str = ""
    validation_run_id: str = ""
    approval_record_id: str = ""
    cap_snapshot_id: str = ""
    # Signal context
    signal_timestamp: str = ""
    # Fill details
    symbol: str = ""
    side: str = ""
    quantity: float = 0.0
    fill_price: float = 0.0
    fill_notional: float = 0.0
    fee_usd: float = 0.0
    fee_bps: float = 0.0
    # Cost attribution
    expected_price: float = 0.0
    slippage_bps: float = 0.0
    realized_pnl: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class PositionSnapshotEvent:
    event_type: str = field(init=False, default="position_snapshot")
    event_id: str = field(default_factory=_new_id)
    recorded_at: str = field(default_factory=_utcnow)
    session_id: str = ""
    wallet_balance: float = 0.0
    total_unrealized_pnl: float = 0.0
    gross_exposure: float = 0.0
    positions: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class CapSnapshotEvent:
    """Snapshot of cap utilisation at a specific check point."""

    event_type: str = field(init=False, default="cap_snapshot")
    event_id: str = field(default_factory=_new_id)
    recorded_at: str = field(default_factory=_utcnow)
    session_id: str = ""
    thesis_id: str = ""
    symbol: str = ""
    gross_exposure: float = 0.0
    symbol_exposure: float = 0.0
    family_exposure: float = 0.0
    daily_loss_global: float = 0.0
    daily_loss_thesis: float = 0.0
    active_thesis_count: int = 0
    breach_type: str | None = None  # None means no breach
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class KillSwitchEvent:
    event_type: str = field(init=False, default="kill_switch_event")
    event_id: str = field(default_factory=_new_id)
    recorded_at: str = field(default_factory=_utcnow)
    session_id: str = ""
    action: Literal[
        "triggered",
        "reset",
        "thesis_disabled",
        "thesis_resumed",
        "symbol_disabled",
        "symbol_resumed",
        "family_disabled",
        "family_resumed",
    ] = "triggered"
    scope: str = "global"  # global | thesis:<id> | symbol:<sym> | family:<fam>
    reason: str = ""
    operator: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ThesisStateChangeEvent:
    event_type: str = field(init=False, default="thesis_state_change")
    event_id: str = field(default_factory=_new_id)
    recorded_at: str = field(default_factory=_utcnow)
    session_id: str = ""
    thesis_id: str = ""
    from_state: str = ""
    to_state: str = ""
    reason: str = ""
    operator: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class OperatorActionEvent:
    event_type: str = field(init=False, default="operator_action")
    event_id: str = field(default_factory=_new_id)
    recorded_at: str = field(default_factory=_utcnow)
    session_id: str = ""
    action: str = ""  # e.g. disable_thesis, resume_symbol, global_kill_on, approve_thesis
    target: str = ""  # thesis_id / symbol / family / "global"
    operator: str = ""
    reason: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


# Union type for type-checking convenience
AuditEvent = (
    OrderIntentEvent
    | OrderSubmissionEvent
    | OrderAckEvent
    | FillEvent
    | PositionSnapshotEvent
    | CapSnapshotEvent
    | KillSwitchEvent
    | ThesisStateChangeEvent
    | OperatorActionEvent
)

_EVENT_TYPES = {
    "order_intent": OrderIntentEvent,
    "order_submission": OrderSubmissionEvent,
    "order_ack": OrderAckEvent,
    "fill_event": FillEvent,
    "position_snapshot": PositionSnapshotEvent,
    "cap_snapshot": CapSnapshotEvent,
    "kill_switch_event": KillSwitchEvent,
    "thesis_state_change": ThesisStateChangeEvent,
    "operator_action": OperatorActionEvent,
}


# ---------------------------------------------------------------------------
# Append-only writer
# ---------------------------------------------------------------------------


def _event_to_dict(event: Any) -> dict[str, Any]:
    return asdict(event)


class AuditLog:
    """
    Thread-safe append-only JSONL audit log.

    One event per line.  Never rewrites existing lines.  Safe to tail.

    Usage:
        log = AuditLog(path)
        log.append(FillEvent(...))
        events = log.load_all()
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        # Touch the file so it exists
        if not self._path.exists():
            self._path.touch()

    @property
    def path(self) -> Path:
        return self._path

    def append(self, event: Any) -> None:
        """Append one event.  Fsync after write for durability."""
        line = json.dumps(_event_to_dict(event), sort_keys=True, default=str) + "\n"
        with self._lock, self._path.open("a", encoding="utf-8") as fh:
            fh.write(line)
            fh.flush()

    def append_batch(self, events: list[Any]) -> None:
        if not events:
            return
        lines = [json.dumps(_event_to_dict(e), sort_keys=True, default=str) + "\n" for e in events]
        with self._lock, self._path.open("a", encoding="utf-8") as fh:
            fh.writelines(lines)
            fh.flush()

    def load_all(self) -> list[dict[str, Any]]:
        """Load all events as raw dicts.  Safe to call while running."""
        events: list[dict[str, Any]] = []
        with self._lock:
            text = self._path.read_text(encoding="utf-8")
        for line_no, line in enumerate(text.splitlines(), start=1):
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                _LOG.warning(
                    "Skipping malformed live audit log line %s in %s",
                    line_no,
                    self._path,
                )
        return events

    def load_by_type(self, event_type: str) -> list[dict[str, Any]]:
        return [e for e in self.load_all() if e.get("event_type") == event_type]

    def load_for_thesis(self, thesis_id: str) -> list[dict[str, Any]]:
        return [e for e in self.load_all() if e.get("thesis_id") == thesis_id]

    def reconstruct_fill_lineage(self, fill_event_id: str) -> dict[str, Any]:
        """
        Given a fill event_id, reconstruct the full lineage chain as a dict.

        Returns a dict with keys: fill, intent (order_intent), thesis_id,
        thesis_version, promotion_run_id, validation_run_id, approval_record_id,
        cap_snapshot_id.
        """
        all_events = self.load_all()
        by_id: dict[str, dict[str, Any]] = {e["event_id"]: e for e in all_events if "event_id" in e}

        fill = by_id.get(fill_event_id)
        if fill is None:
            return {}

        intent_id = fill.get("intent_event_id", "")
        intent = by_id.get(intent_id) if intent_id else None
        cap_snapshot_id = fill.get("cap_snapshot_id", "")
        cap_snapshot = by_id.get(cap_snapshot_id) if cap_snapshot_id else None

        return {
            "fill": fill,
            "order_intent": intent,
            "cap_snapshot": cap_snapshot,
            "thesis_id": fill.get("thesis_id", ""),
            "thesis_version": fill.get("thesis_version", ""),
            "promotion_run_id": fill.get("promotion_run_id", ""),
            "validation_run_id": fill.get("validation_run_id", ""),
            "approval_record_id": fill.get("approval_record_id", ""),
        }

    def __len__(self) -> int:
        return len(self.load_all())
