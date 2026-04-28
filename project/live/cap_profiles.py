from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CapProfile:
    profile_id: str
    max_notional: float
    max_position_notional: float
    max_daily_loss: float
    max_open_positions: int
    max_orders_per_day: int
    allow_pyramiding: bool
    kill_switch_on_daily_loss: bool


TINY_LIVE_V1 = CapProfile(
    profile_id="tiny_live_v1",
    max_notional=50.0,
    max_position_notional=50.0,
    max_daily_loss=10.0,
    max_open_positions=1,
    max_orders_per_day=3,
    allow_pyramiding=False,
    kill_switch_on_daily_loss=True,
)

_PROFILES = {
    "tiny_live_v1": TINY_LIVE_V1,
}


def get_cap_profile(profile_id: str) -> CapProfile | None:
    return _PROFILES.get(profile_id)


def validate_cap_profile_id(profile_id: str) -> bool:
    return profile_id in _PROFILES
