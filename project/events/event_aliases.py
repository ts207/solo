from __future__ import annotations

EVENT_ALIASES = {
    "BASIS_DISLOCATION": "BASIS_DISLOC",
    "VOL_REGIME_SHIFT": "VOL_REGIME_SHIFT_EVENT",
    "DEPTH_COLLAPSE": "DEPTH_STRESS_PROXY",
    "ABSORPTION_EVENT": "ABSORPTION_PROXY",
}
EXECUTABLE_EVENT_ALIASES = {
    "ABSORPTION_PROXY": "ABSORPTION_EVENT",
    "DEPTH_STRESS_PROXY": "DEPTH_COLLAPSE",
    "LIQUIDITY_STRESS_DIRECT": "LIQUIDITY_SHOCK",
    "LIQUIDITY_STRESS_PROXY": "LIQUIDITY_SHOCK",
}


def resolve_event_alias(event_type: str) -> str:
    normalized = str(event_type).strip().upper()
    return EVENT_ALIASES.get(normalized, normalized)


def resolve_executable_event_alias(event_type: str) -> str:
    normalized = str(event_type).strip().upper()
    return EXECUTABLE_EVENT_ALIASES.get(normalized, normalized)
