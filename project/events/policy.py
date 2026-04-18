from __future__ import annotations

DEPLOYABLE_CORE_EVENT_TYPES = frozenset(
    {
        "BASIS_DISLOC",
        "FND_DISLOC",
        "LIQUIDATION_CASCADE",
        "LIQUIDITY_SHOCK",
        "LIQUIDITY_STRESS_DIRECT",
        "LIQUIDITY_VACUUM",
        "SPOT_PERP_BASIS_SHOCK",
        "VOL_SHOCK",
        "VOL_SPIKE",
    }
)

LIVE_SAFE_EVENT_TYPES = DEPLOYABLE_CORE_EVENT_TYPES

RETROSPECTIVE_EVENT_TYPES = frozenset(
    {
        "FUNDING_FLIP",
        "FEE_REGIME_CHANGE_EVENT",
    }
)

LEGACY_EVENT_TYPES = frozenset(
    {
        "BASIS_SNAPBACK",
        "CROSS_VENUE_CATCHUP",
        "FUNDING_EXTREME_BREAKOUT",
        "FUNDING_EXTREME_STAGNATION",
        "LIQUIDATION_EXHAUSTION_REVERSAL",
        "DEPTH_RECOVERY_EVENT",
        "IMBALANCE_ABSORPTION_REVERSAL",
        "OI_VOL_DIVERGENCE",
        "OI_VOL_COMPRESSION_BUILDUP",
    }
)


def is_live_safe_event_type(event_type: str) -> bool:
    return str(event_type).strip().upper() in LIVE_SAFE_EVENT_TYPES


def is_retrospective_event_type(event_type: str) -> bool:
    return str(event_type).strip().upper() in RETROSPECTIVE_EVENT_TYPES


def is_legacy_event_type(event_type: str) -> bool:
    return str(event_type).strip().upper() in LEGACY_EVENT_TYPES


__all__ = [
    "DEPLOYABLE_CORE_EVENT_TYPES",
    "LEGACY_EVENT_TYPES",
    "LIVE_SAFE_EVENT_TYPES",
    "RETROSPECTIVE_EVENT_TYPES",
    "is_legacy_event_type",
    "is_live_safe_event_type",
    "is_retrospective_event_type",
]
