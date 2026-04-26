from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from project.runtime.firewall import AccessRequest, evaluate_access
from project.runtime.hashing import hash_record, hash_records
from project.runtime.normalized_event import NormalizedEvent, event_to_record
from project.runtime.timebase import DEFAULT_LANE_ID, NEG_INF_US, WatermarkTracker, lane_cfg_map

_MAX_ISSUE_EXAMPLES = 20


def _firewall_counter_key(message: str) -> str:
    lowered = str(message).strip().lower()
    if "unknown role" in lowered:
        return "unknown_role"
    if "execution state" in lowered or "post-trade" in lowered:
        return "exec_state_forbidden"
    if "invalid firewall" in lowered:
        return "invalid_firewall_spec"
    return "provenance_forbidden"


def run_causal_lane_ticks(
    events: Iterable[NormalizedEvent],
    *,
    lanes_spec: Mapping[str, Any],
    firewall_spec: Mapping[str, Any],
    hashing_spec: Mapping[str, Any],
) -> dict[str, Any]:
    lane_cfgs = lane_cfg_map(lanes_spec)
    trackers = {lane_id: WatermarkTracker(cfg) for lane_id, cfg in lane_cfgs.items()}
    sources_seen: dict[str, set[str]] = {lane_id: set() for lane_id in lane_cfgs}
    prev_wm_by_lane: dict[str, int] = {lane_id: NEG_INF_US for lane_id in lane_cfgs}

    watermark_counters = {
        "future_event_time": 0,
        "decision_before_watermark": 0,
        "watermark_monotonicity": 0,
        "unknown_lane": 0,
    }
    watermark_examples: list[str] = []
    firewall_counters = {
        "unknown_role": 0,
        "provenance_forbidden": 0,
        "exec_state_forbidden": 0,
        "invalid_firewall_spec": 0,
    }
    firewall_examples: list[str] = []
    max_observed_lag_us = 0

    ticks: list[dict[str, Any]] = []

    for idx, event in enumerate(events):
        lane_id = event.lane_id if event.lane_id in trackers else DEFAULT_LANE_ID
        if event.lane_id not in trackers:
            watermark_counters["unknown_lane"] += 1
            if len(watermark_examples) < _MAX_ISSUE_EXAMPLES:
                watermark_examples.append(
                    f"unknown lane_id '{event.lane_id}' for event_id={event.event_id}; fallback={DEFAULT_LANE_ID}"
                )
        tracker = trackers[lane_id]
        seen = sources_seen.setdefault(lane_id, set())
        seen.add(event.source_id)
        decision_time_us = int(event.recv_time_us)
        tracker.observe(event.source_id, event.event_time_us, event.recv_time_us)
        watermark_time_us = tracker.lane_watermark(decision_time_us, seen)

        lag_us = max(0, int(event.recv_time_us) - int(event.event_time_us))
        if lag_us > max_observed_lag_us:
            max_observed_lag_us = lag_us

        if int(event.event_time_us) > int(event.recv_time_us):
            watermark_counters["future_event_time"] += 1
            if len(watermark_examples) < _MAX_ISSUE_EXAMPLES:
                watermark_examples.append(
                    f"event_id={event.event_id} event_time_us={event.event_time_us} > recv_time_us={event.recv_time_us}"
                )
        if watermark_time_us > NEG_INF_US // 2:
            prev_wm = int(prev_wm_by_lane.get(lane_id, NEG_INF_US))
            if int(watermark_time_us) < prev_wm:
                watermark_counters["watermark_monotonicity"] += 1
                if len(watermark_examples) < _MAX_ISSUE_EXAMPLES:
                    watermark_examples.append(
                        f"lane={lane_id} watermark regressed {prev_wm}->{int(watermark_time_us)} at event_id={event.event_id}"
                    )
            prev_wm_by_lane[lane_id] = int(watermark_time_us)
            if int(decision_time_us) < int(watermark_time_us):
                watermark_counters["decision_before_watermark"] += 1
                if len(watermark_examples) < _MAX_ISSUE_EXAMPLES:
                    watermark_examples.append(
                        f"lane={lane_id} decision_time_us={decision_time_us} < watermark_time_us={int(watermark_time_us)} event_id={event.event_id}"
                    )

        req = AccessRequest(
            role=str(event.role).strip().lower() or "alpha",
            provenance=str(event.provenance).strip().lower() or "market",
            is_exec_state=str(event.provenance).strip().lower() == "execution",
            event_id=str(event.event_id),
        )
        firewall_ok, firewall_msg = evaluate_access(req, firewall_spec=firewall_spec)
        if not firewall_ok:
            key = _firewall_counter_key(firewall_msg)
            firewall_counters[key] += 1
            if len(firewall_examples) < _MAX_ISSUE_EXAMPLES:
                firewall_examples.append(str(firewall_msg))

        tick = {
            "tick_seq": int(idx),
            "tick_time": int(decision_time_us),
            "decision_time_us": int(decision_time_us),
            "watermark_time_us": int(watermark_time_us),
            "lane_id": str(lane_id),
            "event_id": str(event.event_id),
            "source_id": str(event.source_id),
            "source_seq": int(event.source_seq),
            "instrument_id": str(event.instrument_id),
            "venue_id": str(event.venue_id),
            "event_time_us": int(event.event_time_us),
            "recv_time_us": int(event.recv_time_us),
            "role": str(event.role),
            "provenance": str(event.provenance),
            "firewall_ok": bool(firewall_ok),
        }
        tick["tick_hash"] = hash_record(tick, hashing_spec=hashing_spec)
        ticks.append(tick)

    watermark_violation_count = int(sum(int(v) for v in watermark_counters.values()))
    firewall_violation_count = int(sum(int(v) for v in firewall_counters.values()))
    replay_digest = hash_records(ticks, hashing_spec=hashing_spec) if ticks else ""
    status = "pass" if (watermark_violation_count + firewall_violation_count) == 0 else "failed"
    return {
        "status": status,
        "event_count": len(ticks),
        "tick_count": len(ticks),
        "watermark_violation_count": int(watermark_violation_count),
        "watermark_violations_by_type": watermark_counters,
        "watermark_violation_examples": watermark_examples,
        "firewall_violation_count": int(firewall_violation_count),
        "firewall_violations_by_type": firewall_counters,
        "firewall_violation_examples": firewall_examples,
        "max_observed_lag_us": int(max_observed_lag_us),
        "replay_digest": str(replay_digest),
        "ticks": ticks,
    }


def events_to_tick_records(events: Iterable[NormalizedEvent]) -> list[dict[str, Any]]:
    return [event_to_record(event) for event in events]
