from __future__ import annotations

from project.runtime.hashing import load_hashing_spec
from project.runtime.normalized_event import NormalizedEvent
from project.runtime.oms_replay import audit_oms_replay
from project.tests.conftest import PROJECT_ROOT


def _event(
    *,
    event_id: str,
    event_type: str,
    order_id: str,
    source_seq: int,
    recv_time_us: int,
) -> NormalizedEvent:
    return NormalizedEvent(
        event_id=event_id,
        event_type=event_type,
        lane_id="exec_1s",
        source_id="oms:BTCUSDT",
        source_seq=int(source_seq),
        event_time_us=int(recv_time_us) - 1000,
        recv_time_us=int(recv_time_us),
        instrument_id="BTCUSDT",
        venue_id="binance",
        role="execution",
        provenance="execution",
        order_id=order_id,
    )


def test_oms_replay_passes_for_valid_lifecycle():
    hashing_spec = load_hashing_spec(PROJECT_ROOT.parent)
    events = [
        _event(
            event_id="e1", event_type="oms_submit", order_id="o1", source_seq=1, recv_time_us=10
        ),
        _event(event_id="e2", event_type="oms_ack", order_id="o1", source_seq=2, recv_time_us=20),
        _event(event_id="e3", event_type="oms_fill", order_id="o1", source_seq=3, recv_time_us=30),
    ]
    out = audit_oms_replay(events, hashing_spec=hashing_spec)
    assert out["status"] == "pass"
    assert int(out["violation_count"]) == 0
    assert str(out["replay_digest"]).startswith("blake2b_256:")


def test_oms_replay_detects_invalid_order_transitions():
    hashing_spec = load_hashing_spec(PROJECT_ROOT.parent)
    events = [
        _event(event_id="e1", event_type="oms_fill", order_id="o1", source_seq=2, recv_time_us=20),
        _event(event_id="e2", event_type="oms_ack", order_id="o1", source_seq=1, recv_time_us=21),
        _event(event_id="e3", event_type="oms_submit", order_id="", source_seq=5, recv_time_us=50),
    ]
    out = audit_oms_replay(events, hashing_spec=hashing_spec)
    assert out["status"] == "failed"
    assert int(out["violation_count"]) >= 2
    by_type = dict(out["violations_by_type"])
    assert int(by_type.get("fill_without_submit", 0)) >= 1
    assert int(by_type.get("order_source_seq_regression", 0)) >= 1
    assert int(by_type.get("missing_order_id", 0)) >= 1
