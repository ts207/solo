from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from project.core.coercion import as_bool, safe_float, safe_int
from project.runtime.firewall import (
    AccessRequest,
    _str_list,
    audit_access_requests,
    evaluate_access,
)
from project.runtime.hashing import (
    _canonical_json_bytes,
    _hash_bytes,
    _record_sort_key,
    compute_artifact_hashes,
    compute_run_hash,
    hash_file_sha256,
    hash_record,
    hash_records,
)
from project.runtime.invariants import (
    _first_timestamp_us,
    _to_us,
    run_runtime_postflight_audit,
    run_watermark_audit,
)
from project.runtime.normalized_event import (
    NormalizedEvent,
    event_to_record,
    events_to_records,
    normalize_event_rows,
    to_us,
)
from project.runtime.oms_replay import (
    _action_from_event_type,
    _is_execution_related,
    audit_oms_replay,
)
from project.runtime.replay import determinism_replay_check
from project.runtime.timebase import (
    DEFAULT_LANE_ID,
    NEG_INF_US,
    WatermarkCfg,
    WatermarkTracker,
    lane_cfg_map,
)


def test_coercion_helpers_handle_good_bad_and_missing_values():
    assert safe_float("3.5") == 3.5
    assert safe_int("7.9") == 7
    assert safe_float("bad", 1.25, context="x") == 1.25
    assert safe_int(None, None) is None
    assert as_bool(True) is True
    assert as_bool("yes") is True
    assert as_bool("off") is False
    assert as_bool(None) is False


def test_hashing_helpers_cover_canonicalization_and_file_hashing(tmp_path: Path):
    payload = {"b": 2, "a": [1, float("nan"), float("inf")], "c": {"x": 1}}
    canon = _canonical_json_bytes(payload, ensure_ascii=False)
    assert b'"a"' in canon and b'"b"' in canon
    assert _hash_bytes(b"abc", algorithm="sha256").startswith("sha256:")
    assert _hash_bytes(b"abc", algorithm="blake2b_256").startswith("blake2b_256:")
    with pytest.raises(ValueError):
        _hash_bytes(b"abc", algorithm="unknown")
    assert _record_sort_key({"a": 2, "b": "x"}, ["b", "a"]) == ("x", 2)

    record_hash = hash_record({"z": 1}, hashing_spec={"algorithm": "sha256", "canonicalization": {"ensure_ascii": True}})
    assert record_hash.startswith("sha256:")

    ordered_a = [{"id": 2, "source_seq": 2}, {"id": 1, "source_seq": 1}]
    ordered_b = list(reversed(ordered_a))
    spec = {"algorithm": "sha256", "record_sort_keys": ["source_seq", "id"], "canonicalization": {"ensure_ascii": True}}
    assert hash_records(ordered_a, hashing_spec=spec) == hash_records(ordered_b, hashing_spec=spec)

    one = tmp_path / "one.txt"
    one.write_text("hello", encoding="utf-8")
    assert hash_file_sha256(one).startswith("sha256:")
    assert compute_artifact_hashes([one, tmp_path / "missing.txt"])[str(one)].startswith("sha256:")

    manifest = {
        "git_commit": "abc",
        "data_hash": "sha256:data",
        "spec_hashes": {"a": "1"},
        "ontology_spec_hash": "sha256:o",
        "feature_schema_hash": "sha256:f",
        "objective_spec_hash": "sha256:o2",
        "retail_profile_spec_hash": "sha256:r",
        "runtime_invariants_spec_hash": "sha256:ri",
        "runtime_lanes_hash": "sha256:rl",
        "runtime_firewall_hash": "sha256:rf",
        "runtime_hashing_hash": "sha256:rh",
        "runtime_postflight_status": "pass",
        "runtime_watermark_violation_count": 0,
        "runtime_normalization_issue_count": 0,
        "runtime_firewall_violation_count": 0,
        "determinism_status": "pass",
        "replay_digest": "sha256:replay",
        "oms_replay_status": "pass",
        "oms_replay_violation_count": 0,
        "oms_replay_digest": "sha256:oms",
    }
    run_hash = compute_run_hash(manifest=manifest, artifact_hashes={str(one): "sha256:x"}, hashing_spec=spec)
    assert run_hash.startswith("sha256:")


def test_replay_check_covers_empty_pass_and_failed_variants():
    empty = determinism_replay_check([], hashing_spec={"algorithm": "sha256"})
    assert empty["status"] == "no_runtime_events"

    rows = [
        {"event_id": "b", "source_seq": 2, "tick_time": 2},
        {"event_id": "a", "source_seq": 1, "tick_time": 1},
    ]
    out = determinism_replay_check(rows, hashing_spec={"algorithm": "sha256", "record_sort_keys": ["source_seq", "event_id"]})
    assert out["status"] == "pass"
    assert out["variant_digests"]["canonical"] == out["variant_digests"]["reverse"]

    out2 = determinism_replay_check(rows, hashing_spec={"algorithm": "sha256"})
    assert out2["tick_count"] == 2
    assert out2["replay_digest"].startswith("sha256:")


def test_to_us_normalization_and_event_row_conversion():
    dt = datetime(2026, 3, 25, 12, 0, tzinfo=timezone.utc)
    assert to_us("2026-03-25T12:00:00Z") == int(dt.timestamp() * 1_000_000)
    assert to_us(dt) == int(dt.timestamp() * 1_000_000)
    assert to_us(1_700_000_000) == 1_700_000_000_000_000
    assert to_us(1_700_000_000_000) == 1_700_000_000_000_000
    assert to_us(float("nan")) is None

    class FakeTs:
        def to_pydatetime(self):
            return dt

    assert to_us(FakeTs()) == int(dt.timestamp() * 1_000_000)
    assert _to_us("2026-03-25T12:00:00Z") == int(dt.timestamp() * 1_000_000)
    assert _first_timestamp_us({"a": None, "b": "2026-03-25T12:00:00Z"}, ["a", "b"]) == int(dt.timestamp() * 1_000_000)

    rows = [
        {"event_type": "x", "symbol": "BTC", "enter_ts": 200, "detected_ts": 250, "source_seq": 2, "event_id": "b"},
        {"event_type": "x", "symbol": "BTC", "enter_ts": 100, "detected_ts": 180, "source_seq": 1, "event_id": "a"},
        {"event_type": "x", "symbol": "BTC"},  # missing times -> issue
    ]
    events, issues = normalize_event_rows(rows)
    assert len(events) == 2
    assert len(issues) == 1
    assert events[0].event_id == "a"
    assert events[1].event_id == "b"
    assert events[0].lane_id == DEFAULT_LANE_ID
    assert event_to_record(events[0])["event_id"] == "a"
    assert len(events_to_records(events)) == 2

    limited, _ = normalize_event_rows(rows, max_events=1)
    assert len(limited) == 1


def test_firewall_audit_covers_all_failure_paths():
    spec = {
        "roles": {
            "alpha": {"allowed_provenance": ["market", "execution"], "allow_exec_state": True},
            "execution": {"allowed_provenance": ["execution"], "allow_exec_state": True},
        },
        "constraints": {"forbid_posttrade_for_alpha": True},
    }

    assert _str_list(["A", "B "]) == ["a", "b"]
    assert _str_list("X") == ["x"]

    ok, msg = evaluate_access(AccessRequest(role="alpha", provenance="market", is_exec_state=False, event_id="e1"), firewall_spec=spec)
    assert ok and msg == ""

    cases = [
        AccessRequest(role="unknown", provenance="market", is_exec_state=False, event_id="e2"),
        AccessRequest(role="execution", provenance="market", is_exec_state=False, event_id="e3"),
        AccessRequest(role="alpha", provenance="execution", is_exec_state=False, event_id="e5"),
    ]
    outcomes = [evaluate_access(req, firewall_spec=spec) for req in cases]
    assert outcomes[0][0] is False and "unknown role" in outcomes[0][1]
    assert outcomes[1][0] is False and "not allowed" in outcomes[1][1]
    assert outcomes[2][0] is False and "post-trade" in outcomes[2][1]

    no_exec_spec = {
        "roles": {"alpha": {"allowed_provenance": ["market"], "allow_exec_state": False}},
        "constraints": {},
    }
    ok2, msg2 = evaluate_access(
        AccessRequest(role="alpha", provenance="market", is_exec_state=True, event_id="e4"),
        firewall_spec=no_exec_spec,
    )
    assert ok2 is False and "cannot access execution state" in msg2

    audit = audit_access_requests(cases + [AccessRequest(role="alpha", provenance="market", is_exec_state=False, event_id="e6")], firewall_spec=spec)
    assert audit["status"] == "failed"
    assert audit["event_count"] == 4
    assert audit["violation_count"] >= 3


def test_timebase_lane_cfg_and_watermark_tracker():
    cfg_map = lane_cfg_map({"lanes": [{"lane_id": "l1", "watermark": {"max_lateness_us": 10, "idle_source_policy": "allow_advance", "idle_timeout_us": 5}}]})
    assert "l1" in cfg_map
    assert DEFAULT_LANE_ID in cfg_map

    tracker = WatermarkTracker(WatermarkCfg(max_lateness_us=10, idle_source_policy="allow_advance", idle_timeout_us=5))
    tracker.observe("s1", 100, 100)
    assert tracker.lane_watermark(101, ["s1"]) == 90
    # unseen source stalls if requested
    stall = WatermarkTracker(WatermarkCfg(max_lateness_us=10, idle_source_policy="stall", idle_timeout_us=5))
    assert stall.lane_watermark(101, ["missing"]) == NEG_INF_US


def test_watermark_audit_and_runtime_postflight_audit(tmp_path: Path):
    ok_rows = [
        {"event_id": "e1", "event_type": "x", "enter_ts": 100, "detected_ts": 120},
        {"event_id": "e2", "event_type": "x", "enter_ts": 200, "detected_ts": 220},
    ]
    ok = run_watermark_audit(ok_rows, max_lateness_us=50)
    assert ok["status"] == "pass"

    bad = run_watermark_audit(
        [
            {"event_id": "e1", "event_type": "x", "enter_ts": 300, "detected_ts": 100},
            {"event_id": "e2", "event_type": "x", "enter_ts": 50, "detected_ts": 40},
        ],
        max_lateness_us=10,
    )
    assert bad["status"] == "failed"
    assert bad["violation_count"] >= 1

    data_root = tmp_path / "data"
    run_id = "r1"
    runtime_dir = data_root / "runs" / run_id / "runtime"
    runtime_dir.mkdir(parents=True)
    (runtime_dir / "determinism_replay.json").write_text(json.dumps({"status": "pass", "replay_digest": "sha256:det"}), encoding="utf-8")
    (runtime_dir / "oms_replay.json").write_text(json.dumps({"status": "pass", "replay_digest": "sha256:oms", "violation_count": 2}), encoding="utf-8")

    df = pd.DataFrame(ok_rows)
    post = run_runtime_postflight_audit(data_root=data_root, run_id=run_id, determinism_replay_checks=True, events_df=df, source_path="events.csv")
    assert post["status"] == "failed"
    assert post["determinism_replay_checks_status"] == "pass"
    assert post["determinism_status"] == "pass"
    assert post["oms_replay_status"] == "pass"
    assert post["oms_replay_violation_count"] == 2
    assert post["watermark_status"] == "pass"
    assert post["event_count"] == 2

    missing = run_runtime_postflight_audit(source_path="missing.csv")
    assert missing["status"] == "failed"


def _event(event_id: str, event_type: str, order_id: str, source_seq: int, recv_time_us: int) -> NormalizedEvent:
    return NormalizedEvent(
        event_id=event_id,
        event_type=event_type,
        lane_id="exec_1s",
        source_id="oms:BTCUSDT",
        source_seq=source_seq,
        event_time_us=recv_time_us - 1000,
        recv_time_us=recv_time_us,
        instrument_id="BTCUSDT",
        venue_id="binance",
        role="execution",
        provenance="execution",
        order_id=order_id,
    )


def test_oms_replay_actions_and_violation_branches():
    assert _is_execution_related(_event("e", "oms_submit", "o", 1, 10)) is True
    assert _is_execution_related(NormalizedEvent("e", "x", "l", "m", 1, 1, 1, "BTC", "binance", "alpha", "market")) is False
    assert _action_from_event_type("oms_partial_fill") == "partial_fill"
    assert _action_from_event_type("oms_cancelled") == "cancel"

    no_exec = audit_oms_replay(
        [NormalizedEvent("e", "signal", "l", "src", 1, 10, 10, "BTC", "binance", "alpha", "market")],
        hashing_spec={"algorithm": "sha256"},
    )
    assert no_exec["status"] == "no_execution_events"

    pass_out = audit_oms_replay(
        [
            _event("e1", "oms_submit", "o1", 1, 10),
            _event("e2", "oms_ack", "o1", 2, 20),
            _event("e3", "oms_fill", "o1", 3, 30),
        ],
        hashing_spec={"algorithm": "sha256"},
    )
    assert pass_out["status"] == "pass"
    assert pass_out["violation_count"] == 0

    failed = audit_oms_replay(
        [
            _event("e4", "oms_fill", "o2", 2, 20),
            _event("e5", "oms_ack", "o2", 1, 21),
            _event("e6", "oms_cancel", "", 3, 25),
        ],
        hashing_spec={"algorithm": "sha256"},
    )
    assert failed["status"] == "failed"
    assert failed["violation_count"] >= 2
    assert failed["violations_by_type"]["fill_without_submit"] >= 1
