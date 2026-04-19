from __future__ import annotations

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


def runtime_eligible_event_ids_from_domain() -> frozenset[str]:
    """Return the authoritative runtime-eligible event set from compiled domain."""

    from project.domain.compiled_registry import get_domain_registry

    return frozenset(get_domain_registry().runtime_eligible_event_ids())


DEPLOYABLE_CORE_EVENT_TYPES: frozenset[str] = runtime_eligible_event_ids_from_domain()
LIVE_SAFE_EVENT_TYPES = DEPLOYABLE_CORE_EVENT_TYPES


def is_live_safe_event_type(event_type: str) -> bool:
    return str(event_type).strip().upper() in runtime_eligible_event_ids_from_domain()


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
    "runtime_eligible_event_ids_from_domain",
]


def assert_policy_domain_parity() -> list[str]:
    """Return issues where compatibility aliases diverge from compiled domain.

    An empty list means policy aliases are derived from the same runtime-eligible domain set.
    """
    domain_set = runtime_eligible_event_ids_from_domain()
    issues: list[str] = []
    for event_type in sorted(DEPLOYABLE_CORE_EVENT_TYPES - domain_set):
        issues.append(f"policy has {event_type} but domain does not mark it runtime_eligible")
    for event_type in sorted(domain_set - DEPLOYABLE_CORE_EVENT_TYPES):
        issues.append(f"domain marks {event_type} runtime_eligible but policy does not")
    return issues
