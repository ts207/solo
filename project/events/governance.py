from __future__ import annotations

from functools import lru_cache
from typing import Any, Iterable, Mapping

from project.events.contract_registry import load_active_event_contracts
from project.events.event_aliases import resolve_event_alias

TRIGGER_ELIGIBLE_ROLES: frozenset[str] = frozenset({"trigger", "confirm"})
PROMOTION_BLOCKING_ROLES: frozenset[str] = frozenset(
    {"context", "filter", "research_only", "sequence_component"}
)
PROMOTION_BLOCKING_DISPOSITIONS: frozenset[str] = frozenset(
    {
        "context_only",
        "research_only",
        "repair_before_promotion",
        "inactive",
        "deprecated",
        "alias_only",
    }
)
PLANNING_DEFAULT_TIERS: tuple[str, ...] = ("A", "B")
PLANNING_DEFAULT_ROLES: tuple[str, ...] = ("trigger", "confirm")
PLANNING_EXCLUDED_EVIDENCE_MODES: frozenset[str] = frozenset({"proxy", "indirect", "derived", "inferred"})

_TIER_RANK: dict[str, int] = {"A": 0, "B": 1, "C": 2, "D": 3, "X": 4}


def _clean_token(value: Any) -> str:
    return str(value or "").strip()


@lru_cache(maxsize=256)
def get_event_governance_metadata(event_type: str) -> dict[str, Any]:
    token = resolve_event_alias(_clean_token(event_type).upper())
    contract = load_active_event_contracts().get(token, {})
    role = _clean_token(contract.get("operational_role")).lower()
    tier = _clean_token(contract.get("tier")).upper()
    disposition = _clean_token(contract.get("deployment_disposition")).lower()
    runtime_category = _clean_token(contract.get("runtime_category")) or "active_runtime_event"
    evidence_mode = _clean_token(contract.get("evidence_mode")).lower()
    asset_scope = _clean_token(contract.get("asset_scope")).lower()

    role_blocked = role in PROMOTION_BLOCKING_ROLES
    tier_blocked = tier in {"C", "D", "X"}
    disposition_blocked = disposition in PROMOTION_BLOCKING_DISPOSITIONS
    runtime_blocked = runtime_category != "active_runtime_event"
    trade_trigger_eligible = not (role_blocked or tier_blocked or disposition_blocked or runtime_blocked)

    descriptive_only = role in {"context", "filter", "research_only", "sequence_component"}
    requires_stronger_evidence = bool(
        descriptive_only
        or evidence_mode in {"proxy", "indirect", "derived", "inferred"}
        or "cross_asset" in asset_scope
    )

    if runtime_blocked:
        block_reason = f"runtime_category={runtime_category}"
    elif disposition_blocked:
        block_reason = f"deployment_disposition={disposition}"
    elif role_blocked:
        block_reason = f"operational_role={role or 'unspecified'}"
    elif tier_blocked:
        block_reason = f"tier={tier or 'unspecified'}"
    else:
        block_reason = ""

    rank_penalty = float(_TIER_RANK.get(tier or "D", _TIER_RANK["D"]))
    if descriptive_only:
        rank_penalty += 1.5
    if disposition_blocked:
        rank_penalty += 1.0
    if requires_stronger_evidence:
        rank_penalty += 0.5

    return {
        "event_type": token,
        "tier": tier or "D",
        "operational_role": role or "trigger",
        "deployment_disposition": disposition or "review_required",
        "runtime_category": runtime_category,
        "evidence_mode": evidence_mode or "unspecified",
        "asset_scope": asset_scope or "single_asset",
        "event_is_descriptive": descriptive_only,
        "event_is_trade_trigger": trade_trigger_eligible,
        "trade_trigger_eligible": trade_trigger_eligible,
        "requires_stronger_evidence": requires_stronger_evidence,
        "promotion_block_reason": block_reason,
        "rank_penalty": rank_penalty,
    }


def event_matches_filters(
    event_type: str,
    *,
    tiers: Iterable[str] | None = None,
    roles: Iterable[str] | None = None,
    deployment_dispositions: Iterable[str] | None = None,
    trade_trigger_eligible: bool | None = None,
) -> bool:
    meta = get_event_governance_metadata(event_type)
    tier_set = {str(t).strip().upper() for t in (tiers or []) if str(t).strip()}
    role_set = {str(r).strip().lower() for r in (roles or []) if str(r).strip()}
    disposition_set = {
        str(d).strip().lower() for d in (deployment_dispositions or []) if str(d).strip()
    }
    if tier_set and str(meta["tier"]).upper() not in tier_set:
        return False
    if role_set and str(meta["operational_role"]).lower() not in role_set:
        return False
    if disposition_set and str(meta["deployment_disposition"]).lower() not in disposition_set:
        return False
    if trade_trigger_eligible is not None and bool(meta["trade_trigger_eligible"]) != bool(
        trade_trigger_eligible
    ):
        return False
    return True


def filter_event_ids_by_governance(
    event_ids: Iterable[str],
    *,
    tiers: Iterable[str] | None = None,
    roles: Iterable[str] | None = None,
    deployment_dispositions: Iterable[str] | None = None,
    trade_trigger_eligible: bool | None = None,
) -> tuple[str, ...]:
    out = []
    for event_id in event_ids:
        if event_matches_filters(
            event_id,
            tiers=tiers,
            roles=roles,
            deployment_dispositions=deployment_dispositions,
            trade_trigger_eligible=trade_trigger_eligible,
        ):
            out.append(resolve_event_alias(_clean_token(event_id).upper()))
    return tuple(sorted(dict.fromkeys(out)))


def default_planning_event_ids(event_ids: Iterable[str]) -> tuple[str, ...]:
    base = filter_event_ids_by_governance(
        event_ids,
        tiers=PLANNING_DEFAULT_TIERS,
        roles=PLANNING_DEFAULT_ROLES,
        trade_trigger_eligible=True,
    )
    return tuple(
        event_id
        for event_id in base
        if str(get_event_governance_metadata(event_id).get("evidence_mode", "")).lower()
        not in PLANNING_EXCLUDED_EVIDENCE_MODES
    )


def promotion_event_metadata(event_type: str, row: Mapping[str, Any] | None = None) -> dict[str, Any]:
    meta = dict(get_event_governance_metadata(event_type))
    row = row or {}
    if "event_is_descriptive" in row:
        meta["event_is_descriptive"] = bool(row.get("event_is_descriptive")) or bool(
            meta["event_is_descriptive"]
        )
    if "event_is_trade_trigger" in row:
        meta["event_is_trade_trigger"] = bool(row.get("event_is_trade_trigger")) and bool(
            meta["event_is_trade_trigger"]
        )
    meta["trade_trigger_eligible"] = bool(meta["event_is_trade_trigger"])
    return meta
