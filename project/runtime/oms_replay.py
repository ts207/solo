from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from typing import Any

from project.runtime.hashing import hash_records
from project.runtime.normalized_event import NormalizedEvent

_MAX_ISSUE_EXAMPLES = 20
_TERMINAL_STATES = {"filled", "rejected", "canceled"}


@dataclass
class _OrderState:
    state: str
    last_source_seq: int


def _is_execution_related(event: NormalizedEvent) -> bool:
    if str(event.provenance).strip().lower() == "execution":
        return True
    if str(event.role).strip().lower() == "execution":
        return True
    token = str(event.event_type).strip().lower()
    if "oms_" in token or token.startswith("order_"):
        return True
    source = str(event.source_id).strip().lower()
    return ("oms" in source) or ("order" in source)


def _action_from_event_type(event_type: str) -> str:
    token = str(event_type).strip().lower()
    if any(x in token for x in ("submit", "new_order", "create_order")):
        return "submit"
    if any(x in token for x in ("ack", "accepted")):
        return "ack"
    if any(x in token for x in ("fill", "trade")):
        if "partial" in token:
            return "partial_fill"
        return "fill"
    if any(x in token for x in ("reject", "rejected")):
        return "reject"
    if any(x in token for x in ("cancel", "canceled", "cancelled")):
        return "cancel"
    return "other"


def _push_issue(
    counters: dict[str, int],
    examples: list[str],
    *,
    key: str,
    message: str,
    max_examples: int,
) -> None:
    counters[key] = int(counters.get(key, 0)) + 1
    if len(examples) < int(max_examples):
        examples.append(str(message))


def audit_oms_replay(
    events: Iterable[NormalizedEvent],
    *,
    hashing_spec: Mapping[str, Any],
    max_examples: int = _MAX_ISSUE_EXAMPLES,
) -> dict[str, Any]:
    counters: dict[str, int] = {
        "missing_order_id": 0,
        "order_source_seq_regression": 0,
        "ack_without_submit": 0,
        "fill_without_submit": 0,
        "cancel_without_submit": 0,
        "event_after_terminal": 0,
        "invalid_transition": 0,
    }
    examples: list[str] = []
    order_state: dict[str, _OrderState] = {}
    replay_rows: list[dict[str, Any]] = []
    execution_events_seen = 0

    for event in events:
        if not _is_execution_related(event):
            continue
        execution_events_seen += 1
        action = _action_from_event_type(event.event_type)
        order_id = str(event.order_id).strip()
        if action != "other" and not order_id:
            _push_issue(
                counters,
                examples,
                key="missing_order_id",
                message=f"event_id={event.event_id} action={action} missing order_id",
                max_examples=max_examples,
            )
            order_id = f"__missing_order_id__:{event.event_id}"
        elif not order_id:
            order_id = f"__event_only__:{event.event_id}"

        current = order_state.get(order_id)
        if current is None:
            current = _OrderState(state="none", last_source_seq=int(event.source_seq))
            order_state[order_id] = current
        else:
            if int(event.source_seq) < int(current.last_source_seq):
                _push_issue(
                    counters,
                    examples,
                    key="order_source_seq_regression",
                    message=(
                        f"order_id={order_id} source_seq regressed "
                        f"{current.last_source_seq}->{int(event.source_seq)} event_id={event.event_id}"
                    ),
                    max_examples=max_examples,
                )
            current.last_source_seq = max(int(current.last_source_seq), int(event.source_seq))

        state_before = str(current.state)
        if state_before in _TERMINAL_STATES and action in {
            "submit",
            "ack",
            "fill",
            "partial_fill",
            "reject",
            "cancel",
        }:
            _push_issue(
                counters,
                examples,
                key="event_after_terminal",
                message=(
                    f"order_id={order_id} terminal_state={state_before} "
                    f"received action={action} event_id={event.event_id}"
                ),
                max_examples=max_examples,
            )

        if action == "submit":
            current.state = "submitted"
        elif action == "ack":
            if state_before == "none":
                _push_issue(
                    counters,
                    examples,
                    key="ack_without_submit",
                    message=f"order_id={order_id} ack without submit event_id={event.event_id}",
                    max_examples=max_examples,
                )
            if state_before not in _TERMINAL_STATES:
                current.state = "acked"
        elif action in {"fill", "partial_fill"}:
            if state_before == "none":
                _push_issue(
                    counters,
                    examples,
                    key="fill_without_submit",
                    message=f"order_id={order_id} fill without submit event_id={event.event_id}",
                    max_examples=max_examples,
                )
            if state_before not in _TERMINAL_STATES:
                current.state = "partially_filled" if action == "partial_fill" else "filled"
        elif action == "cancel":
            if state_before == "none":
                _push_issue(
                    counters,
                    examples,
                    key="cancel_without_submit",
                    message=f"order_id={order_id} cancel without submit event_id={event.event_id}",
                    max_examples=max_examples,
                )
            if state_before not in _TERMINAL_STATES:
                current.state = "canceled"
        elif action == "reject":
            if state_before not in {"none", "submitted", "acked", "partially_filled"}:
                _push_issue(
                    counters,
                    examples,
                    key="invalid_transition",
                    message=(
                        f"order_id={order_id} reject from invalid state={state_before} "
                        f"event_id={event.event_id}"
                    ),
                    max_examples=max_examples,
                )
            current.state = "rejected"

        replay_rows.append(
            {
                "order_id": order_id,
                "event_id": str(event.event_id),
                "event_type": str(event.event_type),
                "action": str(action),
                "state_before": state_before,
                "state_after": str(current.state),
                "source_id": str(event.source_id),
                "source_seq": int(event.source_seq),
                "event_time_us": int(event.event_time_us),
                "recv_time_us": int(event.recv_time_us),
                "instrument_id": str(event.instrument_id),
                "venue_id": str(event.venue_id),
            }
        )

    violation_count = int(sum(int(v) for v in counters.values()))
    replay_digest = hash_records(replay_rows, hashing_spec=hashing_spec) if replay_rows else ""
    order_states = {order_id: asdict(state) for order_id, state in sorted(order_state.items())}
    if execution_events_seen == 0:
        status = "no_execution_events"
    elif violation_count == 0:
        status = "pass"
    else:
        status = "failed"
    return {
        "status": status,
        "execution_event_count": int(execution_events_seen),
        "order_count": len(order_states),
        "violation_count": int(violation_count),
        "violations_by_type": counters,
        "violation_examples": examples[: int(max_examples)],
        "replay_digest": str(replay_digest),
        "terminal_order_states": order_states,
        "replay_rows": replay_rows,
    }
