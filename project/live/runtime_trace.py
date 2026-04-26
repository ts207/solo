from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class RuntimeTrace:
    """End-to-end trace: event detected → thesis matched → intent formed → OMS submitted."""

    trace_id: str
    run_id: str
    symbol: str
    event_type: str
    event_bar_index: int

    thesis_id: str
    template_id: str
    direction: str
    horizon: str

    intent_notional: float
    allocated_notional: float
    risk_multiplier: float

    oms_order_id: str = ""
    oms_status: str = "pending"

    reasons: tuple[str, ...] = ()
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def was_submitted(self) -> bool:
        return bool(self.oms_order_id)

    @property
    def was_allocated(self) -> bool:
        return self.allocated_notional > 0.0

    def summary(self) -> str:
        status = self.oms_status.upper() if self.was_submitted else "NOT_SUBMITTED"
        return (
            f"[{status}] trace={self.trace_id} event={self.event_type} "
            f"thesis={self.thesis_id} allocated={self.allocated_notional:.0f} "
            f"order={self.oms_order_id or 'none'}"
        )


@dataclass(frozen=True)
class ThesisArbitrationResult:
    """Records how the engine arbitrated between competing theses for a given event."""

    event_type: str
    symbol: str
    bar_index: int
    candidate_thesis_ids: tuple[str, ...]
    selected_thesis_id: str | None
    selection_reason: str
    rejected: tuple[str, ...] = ()

    @property
    def had_candidates(self) -> bool:
        return len(self.candidate_thesis_ids) > 0

    @property
    def was_resolved(self) -> bool:
        return self.selected_thesis_id is not None


@dataclass(frozen=True)
class ReconciliationStateTransition:
    """Records a state machine transition during thesis reconciliation."""

    thesis_id: str
    symbol: str
    from_state: str
    to_state: str
    trigger: str
    bar_index: int
    position_delta_usd: float = 0.0
    notes: str = ""

    _VALID_STATES = frozenset(
        {
            "inactive",
            "pending_entry",
            "active",
            "pending_exit",
            "exited",
            "error",
            "suspended",
        }
    )

    def __post_init__(self) -> None:
        if self.from_state not in self._VALID_STATES:
            raise ValueError(
                f"thesis {self.thesis_id}: invalid from_state {self.from_state!r}"
            )
        if self.to_state not in self._VALID_STATES:
            raise ValueError(
                f"thesis {self.thesis_id}: invalid to_state {self.to_state!r}"
            )

    @property
    def is_entry_transition(self) -> bool:
        return self.from_state in {"inactive", "pending_entry"} and self.to_state == "active"

    @property
    def is_exit_transition(self) -> bool:
        return self.to_state in {"exited", "pending_exit"}
