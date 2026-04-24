"""
Distributed hypothesis search runner.

Partitions a list of HypothesisSpec instances into chunks and evaluates each
chunk via multiprocessing.Pool. Optimized to avoid redundant to_dict calls.
"""

from __future__ import annotations

import logging
import multiprocessing
from typing import Any, List, Optional, Sequence, Tuple

import pandas as pd

from project.core.column_registry import ColumnRegistry
from project.domain.compiled_registry import get_domain_registry
from project.domain.hypotheses import HypothesisSpec
from project.events.event_specs import EVENT_REGISTRY_SPECS
from project.research.robustness.regime_labeler import REGIME_DIMENSIONS
from project.research.robustness.stress_test import STRESS_SCENARIOS
from project.research.search.evaluator_utils import (
    _CONTEXT_CONFIDENCE_COLUMN_BY_FAMILY,
    _CONTEXT_ENTROPY_COLUMN_BY_FAMILY,
    load_context_state_map,
)
from project.research.search.evaluator import evaluate_hypothesis_batch, METRICS_COLUMNS


log = logging.getLogger(__name__)

_COMMON_REQUIRED_COLUMNS = (
    "close",
    "timestamp",
    "split_label",
    "symbol",
    "time_open",
    "time_close",
)

_ROBUSTNESS_ALIAS_COLUMNS = (
    "carry_state_code",
    "trending_state",
    "chop_state",
    "bull_trend_regime",
    "bear_trend_regime",
    "prob_spread_tight",
    "prob_spread_wide",
    "spread_elevated_state",
)


def _trigger_required_columns(trigger) -> set[str]:
    if trigger is None:
        return set()

    trigger_type = getattr(trigger, "trigger_type", None)
    if trigger_type == "event":
        event_id = str(getattr(trigger, "event_id", "") or "").upper()
        spec_event = EVENT_REGISTRY_SPECS.get(event_id)
        signal_col = spec_event.signal_column if spec_event else None
        return set(ColumnRegistry.event_cols(event_id, signal_col=signal_col)) | set(
            ColumnRegistry.event_direction_cols(event_id)
        )
    if trigger_type == "state":
        return set(ColumnRegistry.state_cols(getattr(trigger, "state_id", "") or ""))
    if trigger_type == "transition":
        return set(ColumnRegistry.state_cols(getattr(trigger, "from_state", "") or "")) | set(
            ColumnRegistry.state_cols(getattr(trigger, "to_state", "") or "")
        )
    if trigger_type == "feature_predicate":
        return set(ColumnRegistry.feature_cols(getattr(trigger, "feature", "") or ""))
    if trigger_type == "sequence":
        return set(ColumnRegistry.sequence_cols(getattr(trigger, "sequence_id", "") or ""))
    if trigger_type == "interaction":
        return set(ColumnRegistry.interaction_cols(getattr(trigger, "interaction_id", "") or ""))
    return set()


def _context_required_columns(context: dict[str, str] | None) -> set[str]:
    if not context:
        return set()

    required: set[str] = set()
    try:
        context_state_map = load_context_state_map()
    except Exception:
        context_state_map = {}

    for family, label in context.items():
        state_id = context_state_map.get((family, label))
        if state_id:
            required.update(ColumnRegistry.state_cols(state_id))
        family_key = str(family).strip().lower()
        confidence_col = _CONTEXT_CONFIDENCE_COLUMN_BY_FAMILY.get(family_key)
        entropy_col = _CONTEXT_ENTROPY_COLUMN_BY_FAMILY.get(family_key)
        if confidence_col:
            required.add(confidence_col)
        if entropy_col:
            required.add(entropy_col)
    return required


def _robustness_required_columns() -> set[str]:
    required: set[str] = set()
    required.update(_ROBUSTNESS_ALIAS_COLUMNS)

    for cfg in REGIME_DIMENSIONS.values():
        for state_id in cfg.get("states", {}):
            required.update(ColumnRegistry.state_cols(state_id))

    for scenario in STRESS_SCENARIOS:
        feature_name = str(scenario.get("feature", "")).strip()
        if feature_name:
            required.update(ColumnRegistry.feature_cols(feature_name))
            required.add(feature_name)

    try:
        kill_switch_candidates = get_domain_registry().kill_switch_candidates()
    except Exception:
        kill_switch_candidates = ()
    for feature_name in kill_switch_candidates:
        token = str(feature_name).strip()
        if token:
            required.update(ColumnRegistry.feature_cols(token))
            required.add(token)

    return required


def _required_columns_for_chunk(chunk: Sequence[HypothesisSpec]) -> set[str]:
    required = set(_COMMON_REQUIRED_COLUMNS)
    required.update(_robustness_required_columns())
    for hypothesis in chunk:
        required.update(_trigger_required_columns(getattr(hypothesis, "trigger", None)))
        required.update(_trigger_required_columns(getattr(hypothesis, "feature_condition", None)))
        required.update(_context_required_columns(getattr(hypothesis, "context", None)))
    return required


def _slice_chunk_features(chunk: Sequence[HypothesisSpec], features: pd.DataFrame) -> pd.DataFrame:
    required = _required_columns_for_chunk(chunk)
    valid_cols = [column for column in features.columns if column in required]
    return features[valid_cols] if valid_cols else features


def _evaluate_chunk(
    args: Tuple[Sequence[HypothesisSpec], pd.DataFrame, int, bool, Optional[List[Any]]],
) -> pd.DataFrame:
    """Worker function: unpack and evaluate a chunk of hypotheses."""
    chunk, features, min_sample_size, use_context_quality, folds = args
    if features.empty:
        return pd.DataFrame(columns=METRICS_COLUMNS)
    return evaluate_hypothesis_batch(
        list(chunk),
        features,
        min_sample_size=min_sample_size,
        use_context_quality=use_context_quality,
        folds=folds,
    )


def run_distributed_search(
    hypotheses: List[HypothesisSpec],
    features: pd.DataFrame,
    *,
    n_workers: Optional[int] = None,
    chunk_size: int = 256,
    min_sample_size: int = 20,
    use_context_quality: bool = True,
    folds: list[Any] | None = None,
) -> pd.DataFrame:
    """
    Evaluate hypotheses against features, optionally in parallel.
    """
    if not hypotheses:
        return pd.DataFrame(columns=METRICS_COLUMNS)

    if features is None or features.empty:
        return pd.DataFrame(columns=METRICS_COLUMNS)

    effective_workers = n_workers if n_workers is not None else multiprocessing.cpu_count()
    try:
        effective_workers = max(1, int(effective_workers))
    except Exception:
        effective_workers = 1

    chunks: list[list[HypothesisSpec]] = [
        hypotheses[i : i + int(chunk_size)] for i in range(0, len(hypotheses), int(chunk_size))
    ]
    if not chunks:
        return pd.DataFrame(columns=METRICS_COLUMNS)

    if effective_workers == 1 or len(chunks) == 1:
        parts = [
            evaluate_hypothesis_batch(
                chunk,
                features,
                min_sample_size=min_sample_size,
                use_context_quality=use_context_quality,
                folds=folds,
            )
            for chunk in chunks
        ]
    else:
        try:
            # Note: Passing DataFrame directly works efficiently on Unix via fork (copy-on-write).
            # On Windows/MacOS (spawn), this will pickle the DataFrame which is still
            # more efficient than to_dict("records").
            with multiprocessing.Pool(effective_workers) as pool:
                # OOM Fix (SL-001): Only pass the subset of columns that these specific hypotheses need
                # rather than the full feature dataframe. This prevents massive memory duplication.
                args_list = []
                for chunk in chunks:
                    chunk_features = _slice_chunk_features(chunk, features)
                    args_list.append(
                        (chunk, chunk_features, min_sample_size, use_context_quality, folds)
                    )

                parts = pool.map(_evaluate_chunk, args_list)
        except Exception as exc:
            log.warning(
                "Multiprocessing in run_distributed_search (workers=%d, chunks=%d) failed: %s. "
                "Falling back to sequential execution.",
                effective_workers,
                len(chunks),
                exc,
                exc_info=True,
            )
            parts = [
                evaluate_hypothesis_batch(
                    chunk,
                    features,
                    min_sample_size=min_sample_size,
                    use_context_quality=use_context_quality,
                    folds=folds,
                )
                for chunk in chunks
            ]

    non_empty_parts = [p for p in parts if p is not None and not p.empty]
    if not non_empty_parts:
        return pd.DataFrame(columns=METRICS_COLUMNS)

    normalized_parts = []
    combined_folds = []
    combined_event_timestamps = []
    for p in non_empty_parts:
        if "fold_breakdown" in p.attrs:
            combined_folds.append(p.attrs["fold_breakdown"])
        if "candidate_event_timestamps" in p.attrs:
            combined_event_timestamps.append(p.attrs["candidate_event_timestamps"])
        expected_cols = set(METRICS_COLUMNS)
        if p.columns.tolist() != list(METRICS_COLUMNS):
            for col in expected_cols - set(p.columns):
                p = p.copy()
                p[col] = None
            p = p[list(METRICS_COLUMNS)]
        p.attrs = {}
        normalized_parts.append(p)

    combined = pd.concat(normalized_parts, ignore_index=True)
    if combined_folds:
        combined.attrs["fold_breakdown"] = pd.concat(combined_folds, ignore_index=True)
    if combined_event_timestamps:
        combined.attrs["candidate_event_timestamps"] = pd.concat(
            combined_event_timestamps, ignore_index=True
        ).drop_duplicates(subset=["hypothesis_id", "event_timestamp", "split_label"])

    if "hypothesis_id" in combined.columns:
        combined = combined.drop_duplicates(subset=["hypothesis_id"]).reset_index(drop=True)
    return combined
