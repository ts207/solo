from __future__ import annotations

LIVE_SAFE_EVENT_TYPES = frozenset(
    {
        "BASIS_DISLOC",
        "CROSS_VENUE_DESYNC",
        "FND_DISLOC",
        "SPOT_PERP_BASIS_SHOCK",
        "VOL_SPIKE",
        "VOL_RELAXATION_START",
        "VOL_CLUSTER_SHIFT",
        "RANGE_COMPRESSION_END",
        "BREAKOUT_TRIGGER",
        "VOL_SHOCK",
        "LIQUIDITY_STRESS_DIRECT",
        "LIQUIDITY_STRESS_PROXY",
        "LIQUIDITY_GAP_PRINT",
        "LIQUIDITY_VACUUM",
        "OI_SPIKE_POSITIVE",
        "OI_SPIKE_NEGATIVE",
        "OI_FLUSH",
        "DELEVERAGING_WAVE",
        "LIQUIDATION_CASCADE",
        "RANGE_BREAKOUT",
        "FALSE_BREAKOUT",
        "PULLBACK_PIVOT",
        "SUPPORT_RESISTANCE_BREAK",
    }
)

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
    "LEGACY_EVENT_TYPES",
    "LIVE_SAFE_EVENT_TYPES",
    "RETROSPECTIVE_EVENT_TYPES",
    "is_legacy_event_type",
    "is_live_safe_event_type",
    "is_retrospective_event_type",
]
