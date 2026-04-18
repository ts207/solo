from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Sequence

import pandas as pd
import yaml

from project.domain.compiled_registry import get_domain_registry
from project.research.search.evaluator import evaluate_hypothesis_batch
from project.research.search.generator import generate_hypotheses_with_audit
from project.research.search.search_feature_utils import load_search_feature_frame


def _load_search_space_doc(search_space_path: Path | None) -> Dict[str, Any]:
    if search_space_path is None or not Path(search_space_path).exists():
        return {}
    payload = yaml.safe_load(Path(search_space_path).read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _expected_event_ids_from_search_space_doc(search_space_doc: Dict[str, Any]) -> list[str]:
    from project.spec_validation import expand_triggers

    expanded = expand_triggers(dict(search_space_doc or {}))
    expected: list[str] = []
    seen: set[str] = set()
    for raw_event_id in list(expanded.get("events", []) or []):
        event_id = str(raw_event_id or "").strip().upper()
        if event_id and event_id not in seen and get_domain_registry().has_event(event_id):
            expected.append(event_id)
            seen.add(event_id)
    return expected


def _first_valid_row(metrics: pd.DataFrame) -> Dict[str, Any]:
    if metrics.empty:
        return {}
    valid = metrics[
        metrics.get("valid", pd.Series(False, index=metrics.index)).fillna(False).astype(bool)
    ]
    row = valid.iloc[0] if not valid.empty else metrics.iloc[0]
    return row.to_dict()


def compare_context_modes(
    *,
    hypotheses: Sequence[Any],
    features: pd.DataFrame,
    min_sample_size: int = 30,
) -> Dict[str, Any]:
    if not hypotheses or features.empty or "close" not in features.columns:
        return {
            "schema_version": "context_mode_comparison_v1",
            "hard_label": {
                "evaluated_rows": 0,
                "selected": {},
            },
            "confidence_aware": {
                "evaluated_rows": 0,
                "selected": {},
            },
            "delta": {
                "n": 0.0,
                "validation_n_obs": 0.0,
                "test_n_obs": 0.0,
                "t_stat": 0.0,
                "robustness_score": 0.0,
                "stress_score": 0.0,
            },
            "selection_changed": False,
            "selection_outcome_changed": False,
        }

    hard_metrics = evaluate_hypothesis_batch(
        list(hypotheses),
        features,
        min_sample_size=min_sample_size,
        use_context_quality=False,
    )
    quality_metrics = evaluate_hypothesis_batch(
        list(hypotheses),
        features,
        min_sample_size=min_sample_size,
        use_context_quality=True,
    )
    hard_row = _first_valid_row(hard_metrics)
    quality_row = _first_valid_row(quality_metrics)

    def _get(row: Dict[str, Any], key: str, default: float = 0.0) -> float:
        try:
            return float(row.get(key, default))
        except (TypeError, ValueError):
            return float(default)

    hard_hypothesis_id = str(hard_row.get("hypothesis_id", "")).strip()
    quality_hypothesis_id = str(quality_row.get("hypothesis_id", "")).strip()
    hard_valid = bool(hard_row.get("valid")) if hard_row else False
    quality_valid = bool(quality_row.get("valid")) if quality_row else False

    return {
        "schema_version": "context_mode_comparison_v1",
        "hard_label": {
            "evaluated_rows": int(len(hard_metrics)),
            "selected": hard_row,
        },
        "confidence_aware": {
            "evaluated_rows": int(len(quality_metrics)),
            "selected": quality_row,
        },
        "delta": {
            "n": _get(quality_row, "n") - _get(hard_row, "n"),
            "validation_n_obs": _get(quality_row, "validation_n_obs")
            - _get(hard_row, "validation_n_obs"),
            "test_n_obs": _get(quality_row, "test_n_obs") - _get(hard_row, "test_n_obs"),
            "t_stat": _get(quality_row, "t_stat") - _get(hard_row, "t_stat"),
            "robustness_score": _get(quality_row, "robustness_score")
            - _get(hard_row, "robustness_score"),
            "stress_score": _get(quality_row, "stress_score") - _get(hard_row, "stress_score"),
        },
        "selection_changed": bool(
            hard_hypothesis_id
            and quality_hypothesis_id
            and hard_hypothesis_id != quality_hypothesis_id
        ),
        "selection_outcome_changed": bool(
            (hard_hypothesis_id == quality_hypothesis_id and hard_hypothesis_id)
            and (hard_valid != quality_valid)
        ),
    }


def build_context_mode_comparison_payload(
    *,
    data_root: Path,
    run_id: str,
    symbols: Sequence[str],
    timeframe: str = "5m",
    min_sample_size: int = 30,
    search_space_path: Path | None = None,
    event_registry_override: str | None = None,
) -> Dict[str, Any]:
    normalized_symbols = [str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()]
    search_space_doc = _load_search_space_doc(search_space_path)
    expected_event_ids = _expected_event_ids_from_search_space_doc(search_space_doc)
    features = load_search_feature_frame(
        run_id=run_id,
        symbols=normalized_symbols,
        timeframe=str(timeframe),
        data_root=data_root,
        expected_event_ids=expected_event_ids,
        event_registry_override=event_registry_override,
    )
    hypotheses, _ = generate_hypotheses_with_audit(
        search_space_path=search_space_path,
        features=None if features.empty else features,
    )
    payload = compare_context_modes(
        hypotheses=hypotheses,
        features=features,
        min_sample_size=int(min_sample_size),
    )
    payload["run_id"] = run_id
    payload["symbols"] = normalized_symbols
    payload["timeframe"] = str(timeframe)
    if search_space_path is not None:
        payload["search_space_path"] = str(search_space_path)
    if event_registry_override:
        payload["event_registry_override"] = str(event_registry_override)
    return payload


def write_context_mode_comparison_report(
    *,
    out_path: Path,
    comparison: Dict[str, Any],
) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(comparison, indent=2, sort_keys=True), encoding="utf-8")
    return out_path
