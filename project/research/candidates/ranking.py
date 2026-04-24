from __future__ import annotations

import json
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from project.core.coercion import safe_float
from project.events.governance import get_event_governance_metadata

SOURCE_PRIORITY = {
    "promoted_blueprint": 0,
    "edge_candidate": 1,
    "alpha_bundle": 2,
}


def behavior_equivalence_key(row: Dict[str, object]) -> str:
    payload = {
        "base_strategy": str(row.get("base_strategy", "")),
        "event": str(row.get("event", "")),
        "condition": str(row.get("condition", "")),
        "action": str(row.get("action", "")),
        "candidate_symbol": str(row.get("candidate_symbol", "")),
        "deployment_symbols": sorted(str(symbol) for symbol in row.get("deployment_symbols", [])),
        "risk_controls": row.get("risk_controls", {}),
    }
    return json.dumps(payload, sort_keys=True)


def count_by_source(rows: List[Dict[str, object]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in rows:
        source = str(row.get("source_type", "unknown")).strip() or "unknown"
        counts[source] = counts.get(source, 0) + 1
    return counts


def _sort_metric_desc(value: object) -> float:
    v = safe_float(value, np.nan)
    return float("inf") if not pd.notna(v) else -float(v)


def candidate_rank_key(row: Dict[str, object]) -> Tuple[float, float, float, float, int, str]:
    quality_score = safe_float(
        row.get("selection_score_executed"),
        safe_float(row.get("quality_score"), safe_float(row.get("selection_score"), np.nan)),
    )
    expectancy = safe_float(
        row.get("expectancy_after_multiplicity"),
        safe_float(row.get("expectancy_per_trade"), np.nan),
    )
    robustness = safe_float(row.get("robustness_score"), np.nan)
    event_token = str(row.get("event_type", row.get("event", ""))).strip().upper()
    governance = get_event_governance_metadata(event_token) if event_token and event_token != "ALPHA_BUNDLE" else {"rank_penalty": -1.0}
    rank_penalty = float(governance.get("rank_penalty", 0.0))
    source_priority = SOURCE_PRIORITY.get(str(row.get("source_type", row.get("source", ""))), 99)
    return (
        _sort_metric_desc(quality_score),
        _sort_metric_desc(expectancy),
        _sort_metric_desc(robustness),
        rank_penalty,
        source_priority,
        str(row.get("strategy_candidate_id", "")),
    )
