from __future__ import annotations

from project.core.coercion import safe_float, safe_int, as_bool

from dataclasses import dataclass
from typing import Dict, Iterable, Mapping

NEG_INF_US = -(2**62)
DEFAULT_LANE_ID = "alpha_5s"


@dataclass(frozen=True)
class WatermarkCfg:
    max_lateness_us: int
    idle_source_policy: str
    idle_timeout_us: int


@dataclass
class _SourceState:
    max_event_time_seen: int
    last_recv_time_seen: int


class WatermarkTracker:
    def __init__(self, cfg: WatermarkCfg) -> None:
        self.cfg = cfg
        self._sources: Dict[str, _SourceState] = {}

    def observe(self, source_id: str, event_time_us: int, recv_time_us: int) -> None:
        current = self._sources.get(source_id)
        if current is None:
            self._sources[source_id] = _SourceState(int(event_time_us), int(recv_time_us))
            return
        if int(event_time_us) > int(current.max_event_time_seen):
            current.max_event_time_seen = int(event_time_us)
        if int(recv_time_us) > int(current.last_recv_time_seen):
            current.last_recv_time_seen = int(recv_time_us)

    def lane_watermark(self, decision_time_us: int, sources_seen: Iterable[str]) -> int:
        source_ids = sorted(set(str(s).strip() for s in sources_seen if str(s).strip()))
        if not source_ids:
            return NEG_INF_US

        max_late = int(self.cfg.max_lateness_us)
        idle_timeout = int(self.cfg.idle_timeout_us)
        idle_policy = str(self.cfg.idle_source_policy).strip().lower()

        wms: list[int] = []
        for source_id in source_ids:
            state = self._sources.get(source_id)
            if state is None:
                if idle_policy == "stall":
                    return NEG_INF_US
                continue
            is_idle = (
                idle_timeout > 0
                and (int(decision_time_us) - int(state.last_recv_time_seen)) > idle_timeout
            )
            if is_idle:
                if idle_policy == "stall":
                    return NEG_INF_US
                if idle_policy == "allow_advance":
                    continue
            wms.append(int(state.max_event_time_seen) - max_late)

        if wms:
            return min(wms)
        all_seen = [int(state.max_event_time_seen) - max_late for state in self._sources.values()]
        return min(all_seen) if all_seen else NEG_INF_US


def lane_cfg_map(lanes_spec: Mapping[str, object]) -> Dict[str, WatermarkCfg]:
    out: Dict[str, WatermarkCfg] = {}
    lanes = lanes_spec.get("lanes")
    if not isinstance(lanes, list):
        lanes = []
    for lane in lanes:
        if not isinstance(lane, dict):
            continue
        lane_id = str(lane.get("lane_id", "")).strip()
        if not lane_id:
            continue
        watermark = lane.get("watermark")
        if not isinstance(watermark, dict):
            watermark = {}
        out[lane_id] = WatermarkCfg(
            max_lateness_us=safe_int(watermark.get("max_lateness_us"), 0),
            idle_source_policy=str(watermark.get("idle_source_policy", "stall")).strip().lower(),
            idle_timeout_us=safe_int(watermark.get("idle_timeout_us"), 0),
        )
    if DEFAULT_LANE_ID not in out:
        out[DEFAULT_LANE_ID] = WatermarkCfg(
            max_lateness_us=5_000_000,
            idle_source_policy="stall",
            idle_timeout_us=0,
        )
    return out
