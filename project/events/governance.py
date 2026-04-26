from __future__ import annotations

from collections.abc import Iterable, Mapping
from functools import lru_cache
from typing import Any

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
PLANNING_EXCLUDED_BANDS: frozenset[str] = frozenset({"context_only", "composite_or_fragile"})

_TIER_RANK: dict[str, int] = {"A": 0, "B": 1, "C": 2, "D": 3, "X": 4}


def _clean_token(value: Any) -> str:
    return str(value or "").strip()


@lru_cache(maxsize=256)
def get_event_governance_metadata(event_type: str) -> dict[str, Any]:
    token = resolve_event_alias(_clean_token(event_type).upper())
    contract_row = load_active_event_contracts().get(token, {})
    role = _clean_token(contract_row.get("operational_role")).lower()
    tier = _clean_token(contract_row.get("tier")).upper()
    disposition = _clean_token(contract_row.get("deployment_disposition")).lower()
    runtime_category = _clean_token(contract_row.get("runtime_category")) or "active_runtime_event"
    evidence_mode = _clean_token(contract_row.get("evidence_mode")).lower()
    asset_scope = _clean_token(contract_row.get("asset_scope")).lower()

    detector_contract = None
    try:
        from project.events.registry import get_detector_contract

        detector_contract = get_detector_contract(token)
    except Exception:
        detector_contract = None

    if detector_contract is not None:
        role = str(detector_contract.role).lower()
        evidence_mode = str(detector_contract.evidence_mode).lower()
        runtime_category = "active_runtime_event" if detector_contract.runtime_default else "gated_event"
        detector_band = str(detector_contract.detector_band).lower()
    else:
        detector_band = ""

    role_blocked = role in PROMOTION_BLOCKING_ROLES
    tier_blocked = tier in {"C", "D", "X"}
    disposition_blocked = disposition in PROMOTION_BLOCKING_DISPOSITIONS
    runtime_blocked = runtime_category != "active_runtime_event"

    descriptive_only = role in {"context", "filter", "research_only", "sequence_component", "composite"}
    requires_stronger_evidence = bool(
        descriptive_only
        or evidence_mode in {"proxy", "indirect", "derived", "inferred", "inferred_cross_asset"}
        or "cross_asset" in asset_scope
    )

    trade_trigger_eligible = not (role_blocked or tier_blocked or disposition_blocked or runtime_blocked)
    if detector_contract is not None:
        descriptive_only = bool(
            detector_contract.context_only
            or detector_contract.composite
            or detector_contract.research_only
            or detector_contract.role in {"context", "research_only", "composite"}
            or detector_contract.detector_band in {"context_only", "composite_or_fragile"}
        )
        requires_stronger_evidence = bool(
            descriptive_only
            or evidence_mode in {"proxy", "indirect", "derived", "inferred", "inferred_cross_asset"}
            or "cross_asset" in asset_scope
            or detector_contract.event_version != "v2"
        )
        trade_trigger_eligible = bool(
            detector_contract.role == "trigger"
            and (detector_contract.promotion_eligible or detector_contract.primary_anchor_eligible)
        )
        runtime_blocked = not bool(detector_contract.runtime_default)

    if detector_contract is not None and detector_contract.event_version != "v2":
        block_reason = "legacy_v1_retired"
    elif runtime_blocked:
        block_reason = f"runtime_category={runtime_category}"
    elif disposition_blocked:
        block_reason = f"deployment_disposition={disposition}"
    elif role_blocked or descriptive_only:
        block_reason = f"operational_role={role or 'unspecified'}"
    elif tier_blocked:
        block_reason = f"tier={tier or 'unspecified'}"
    elif detector_contract is not None and not trade_trigger_eligible:
        block_reason = "detector_contract_not_eligible"
    else:
        block_reason = ""

    rank_penalty = float(_TIER_RANK.get(tier or "D", _TIER_RANK["D"]))
    if descriptive_only:
        rank_penalty += 1.5
    if disposition_blocked:
        rank_penalty += 1.0
    if requires_stronger_evidence:
        rank_penalty += 0.5
    if detector_contract is not None and detector_contract.event_version != "v2":
        rank_penalty += 1.0

    payload = {
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
    if detector_contract is not None:
        payload.update(
            {
                "event_version": detector_contract.event_version,
                "detector_band": detector_contract.detector_band,
                "planning_default": detector_contract.planning_default,
                "runtime_default": detector_contract.runtime_default,
                "promotion_eligible": detector_contract.promotion_eligible,
                "primary_anchor_eligible": detector_contract.primary_anchor_eligible,
            }
        )
    elif detector_band:
        payload["detector_band"] = detector_band
    return payload


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
    out: list[str] = []
    for event_id in event_ids:
        token = resolve_event_alias(_clean_token(event_id).upper())
        meta = get_event_governance_metadata(token)
        if not bool(meta.get("planning_default", False)):
            continue
        if str(meta.get("detector_band", "")).lower() in PLANNING_EXCLUDED_BANDS:
            continue
        if str(meta.get("evidence_mode", "")).lower() in PLANNING_EXCLUDED_EVIDENCE_MODES:
            continue
        if not event_matches_filters(
            token,
            tiers=PLANNING_DEFAULT_TIERS,
            roles=PLANNING_DEFAULT_ROLES,
            trade_trigger_eligible=True,
        ):
            continue
        out.append(token)
    return tuple(sorted(dict.fromkeys(out)))


def governed_default_planning_event_ids() -> tuple[str, ...]:
    from project.events.registry import list_planning_eligible_detectors

    return default_planning_event_ids(
        contract.event_name for contract in list_planning_eligible_detectors()
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
