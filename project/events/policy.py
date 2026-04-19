from __future__ import annotations

DEPLOYABLE_CORE_EVENT_TYPES: frozenset[str] = frozenset(
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


def _domain_runtime_eligible_set() -> frozenset[str]:
    from project.domain.compiled_registry import get_domain_registry
    return frozenset(get_domain_registry().runtime_eligible_event_ids())


def is_live_safe_event_type(event_type: str) -> bool:
    domain_set = _domain_runtime_eligible_set()
    if domain_set:
        return str(event_type).strip().upper() in domain_set
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


def runtime_eligible_event_ids_from_domain() -> frozenset[str]:
    """Return the runtime-eligible event set derived from the compiled domain registry.

    This is the authoritative source for runtime eligibility. The hardcoded
    DEPLOYABLE_CORE_EVENT_TYPES set above should match this set; use
    assert_policy_domain_parity() in tests to enforce it.
    """
    from project.domain.compiled_registry import get_domain_registry
    return frozenset(get_domain_registry().runtime_eligible_event_ids())


def assert_policy_domain_parity() -> list[str]:
    """Return issues where DEPLOYABLE_CORE_EVENT_TYPES diverges from compiled domain.

    An empty list means the hardcoded policy set and the domain are in sync.
    """
    domain_set = runtime_eligible_event_ids_from_domain()
    issues: list[str] = []
    for event_type in sorted(DEPLOYABLE_CORE_EVENT_TYPES - domain_set):
        issues.append(f"policy has {event_type} but domain does not mark it runtime_eligible")
    for event_type in sorted(domain_set - DEPLOYABLE_CORE_EVENT_TYPES):
        issues.append(f"domain marks {event_type} runtime_eligible but policy does not")
    return issues
