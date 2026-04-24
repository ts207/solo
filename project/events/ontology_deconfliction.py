from __future__ import annotations

import json
from typing import Dict

import pandas as pd

from project.domain.compiled_registry import get_domain_registry
from project.events.event_aliases import resolve_event_alias

_ONTOLOGY_COLUMNS = [
    "raw_event_type",
    "canonical_regime",
    "subtype",
    "phase",
    "evidence_mode",
    "deconflict_priority",
    "is_composite",
    "is_context_tag",
    "is_strategy_construct",
]


def _event_bundle_map() -> Dict[str, Dict[str, object]]:
    registry = get_domain_registry()
    out: Dict[str, Dict[str, object]] = {}
    for event_type in registry.event_ids:
        spec = registry.get_event(event_type)
        if spec is None:
            continue
        out[event_type] = {
            "raw_event_type": event_type,
            "canonical_regime": spec.canonical_regime,
            "subtype": spec.subtype,
            "phase": spec.phase,
            "evidence_mode": spec.evidence_mode,
            "deconflict_priority": spec.deconflict_priority,
            "is_composite": spec.is_composite,
            "is_context_tag": spec.is_context_tag,
            "is_strategy_construct": spec.is_strategy_construct,
        }
    return out


def attach_canonical_event_bundle(events: pd.DataFrame) -> pd.DataFrame:
    if events is None or events.empty or "event_type" not in events.columns:
        return events.copy()
    out = events.copy()
    bundles = _event_bundle_map()
    canonical_event_type = out["event_type"].astype(str).map(resolve_event_alias)
    mapped = canonical_event_type.str.upper().map(bundles)
    for column in _ONTOLOGY_COLUMNS:
        out[column] = mapped.map(
            lambda row, key=column: row.get(key) if isinstance(row, dict) else None
        )
    out["raw_event_type"] = out["raw_event_type"].fillna(canonical_event_type.str.upper())
    return out


def deconflict_event_episodes(events: pd.DataFrame) -> pd.DataFrame:
    out = attach_canonical_event_bundle(events)
    if out.empty or "timestamp" not in out.columns or "symbol" not in out.columns:
        return out
    if "event_id" not in out.columns:
        out["event_id"] = [f"event_{i:08d}" for i in range(len(out))]

    ranked = out.copy()
    ranked["_layer_penalty"] = (
        ranked["is_strategy_construct"].fillna(False).astype(int) * 100
        + ranked["is_context_tag"].fillna(False).astype(int) * 10
        + ranked["is_composite"].fillna(False).astype(int)
    )
    ranked["_group_key"] = (
        ranked["symbol"].astype(str).str.upper()
        + "||"
        + ranked["timestamp"].astype(str)
        + "||"
        + ranked["canonical_regime"].fillna(ranked["event_type"]).astype(str)
        + "||"
        + ranked["subtype"].fillna("").astype(str)
        + "||"
        + ranked["phase"].fillna("").astype(str)
    )
    ranked = ranked.sort_values(
        ["_group_key", "deconflict_priority", "_layer_penalty", "event_type"],
        ascending=[True, False, True, True],
    ).reset_index(drop=True)
    raw_lists = ranked.groupby("_group_key")["raw_event_type"].apply(
        lambda s: json.dumps(sorted({str(value) for value in s if str(value)}))
    )
    deduped = ranked.drop_duplicates(subset=["_group_key"], keep="first").copy()
    deduped["raw_event_types"] = deduped["_group_key"].map(raw_lists)
    deduped = deduped.drop(columns=["_group_key", "_layer_penalty"])
    return deduped.reset_index(drop=True)
