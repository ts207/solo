from __future__ import annotations

from typing import Any, Dict, Mapping

import pandas as pd


PREPARE_EVENTS_ATTR = "phase2_prepare_diagnostics"


def _json_scalar(value: Any) -> Any:
    if isinstance(value, (str, bool, int, float)) or value is None:
        return value
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if pd.isna(value):
        return None
    return str(value)


def _jsonify_mapping(payload: Mapping[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, value in payload.items():
        if isinstance(value, Mapping):
            out[str(key)] = _jsonify_mapping(value)
        elif isinstance(value, list):
            out[str(key)] = [_json_scalar(item) for item in value]
        else:
            out[str(key)] = _json_scalar(value)
    return out


def split_counts(df: pd.DataFrame, split_col: str = "split_label") -> Dict[str, int]:
    if df.empty or split_col not in df.columns:
        return {"train": 0, "validation": 0, "test": 0}
    counts = df[split_col].astype(str).str.lower().value_counts(dropna=False).to_dict()
    return {
        "train": int(counts.get("train", 0)),
        "validation": int(counts.get("validation", 0)),
        "test": int(counts.get("test", 0)),
    }


def attach_prepare_events_diagnostics(df: pd.DataFrame, payload: Mapping[str, Any]) -> pd.DataFrame:
    df.attrs[PREPARE_EVENTS_ATTR] = _jsonify_mapping(payload)
    return df


def get_prepare_events_diagnostics(df: pd.DataFrame) -> Dict[str, Any]:
    payload = df.attrs.get(PREPARE_EVENTS_ATTR, {})
    if isinstance(payload, Mapping):
        return _jsonify_mapping(payload)
    return {}


def build_prepare_events_diagnostics(
    *,
    run_id: str,
    event_type: str,
    symbols_requested: list[str],
    raw_event_count: int,
    canonical_episode_count: int,
    split_counts_payload: Mapping[str, Any],
    loaded_from_fallback_file: bool,
    holdout_integrity_failed: bool,
    resplit_attempted: bool,
    returned_empty_due_to_holdout: bool,
    min_validation_events: int,
    min_test_events: int,
    returned_rows: int,
) -> Dict[str, Any]:
    return _jsonify_mapping(
        {
            "run_id": run_id,
            "event_type": event_type,
            "symbols_requested": symbols_requested,
            "raw_event_count": int(raw_event_count),
            "canonical_episode_count": int(canonical_episode_count),
            "split_counts": dict(split_counts_payload),
            "loaded_from_fallback_file": bool(loaded_from_fallback_file),
            "holdout_integrity_failed": bool(holdout_integrity_failed),
            "resplit_attempted": bool(resplit_attempted),
            "returned_empty_due_to_holdout": bool(returned_empty_due_to_holdout),
            "min_validation_events": int(min_validation_events),
            "min_test_events": int(min_test_events),
            "returned_rows": int(returned_rows),
        }
    )


def build_search_engine_diagnostics(
    *,
    run_id: str,
    discovery_profile: str,
    search_spec: str,
    timeframe: str,
    symbols_requested: list[str],
    primary_symbol: str,
    feature_rows: int,
    feature_columns: int,
    event_flag_rows: int,
    event_flag_columns_merged: int,
    hypotheses_generated: int,
    feasible_hypotheses: int,
    rejected_hypotheses: int,
    rejection_reason_counts: Mapping[str, Any],
    metrics_rows: int,
    valid_metrics_rows: int,
    rejected_invalid_metrics: int,
    rejected_by_min_n: int,
    rejected_by_min_t_stat: int,
    bridge_candidates_rows: int,
    multiplicity_discoveries: int,
    min_t_stat: float,
    min_n: int,
    search_budget: int | None,
    use_context_quality: bool,
    gate_funnel: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "run_id": run_id,
        "discovery_profile": discovery_profile,
        "search_spec": search_spec,
        "timeframe": timeframe,
        "symbols_requested": symbols_requested,
        "primary_symbol": primary_symbol,
        "feature_rows": int(feature_rows),
        "feature_columns": int(feature_columns),
        "event_flag_rows": int(event_flag_rows),
        "event_flag_columns_merged": int(event_flag_columns_merged),
        "hypotheses_generated": int(hypotheses_generated),
        "feasible_hypotheses": int(feasible_hypotheses),
        "rejected_hypotheses": int(rejected_hypotheses),
        "rejection_reason_counts": dict(rejection_reason_counts),
        "metrics_rows": int(metrics_rows),
        "valid_metrics_rows": int(valid_metrics_rows),
        "rejected_invalid_metrics": int(rejected_invalid_metrics),
        "rejected_by_min_n": int(rejected_by_min_n),
        "rejected_by_min_t_stat": int(rejected_by_min_t_stat),
        "bridge_candidates_rows": int(bridge_candidates_rows),
        "multiplicity_discoveries": int(multiplicity_discoveries),
        "min_t_stat": float(min_t_stat),
        "min_n": int(min_n),
        "search_budget": int(search_budget) if search_budget is not None else None,
        "use_context_quality": bool(use_context_quality),
    }
    if gate_funnel:
        payload["gate_funnel"] = dict(gate_funnel)
    return _jsonify_mapping(payload)
