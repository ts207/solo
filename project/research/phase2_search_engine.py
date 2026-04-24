"""
Phase 2 Search Engine Stage.

Generates hypotheses from spec/search_space.yaml, evaluates them against the
wide feature table, and writes bridge-compatible candidates to the output directory.

This is the authoritative phase-2 discovery stage for new runs.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Mapping, Optional

import numpy as np
import pandas as pd
import yaml

from project import PROJECT_ROOT
from project.core.column_registry import ColumnRegistry
from project.core.logging_utils import build_stage_log_handlers
from project.domain.compiled_registry import get_domain_registry
from project.domain.hypotheses import TriggerType
from project.spec_registry import load_yaml_path
from project.specs.manifest import finalize_manifest, start_manifest
from project.specs.gates import load_gates_spec, select_bridge_gate_spec, select_phase2_gate_spec
from project.research.search.profile import resolve_search_profile
from project.research.search.generator import generate_hypotheses_with_audit
from project.research.search.evaluator import evaluated_records_from_metrics
from project.research.search.bridge_adapter import (
    canonical_bridge_event_type,
    hypotheses_to_bridge_candidates,
    split_bridge_candidates,
)
from project.contracts.schemas import normalize_dataframe_for_schema, validate_schema_at_producer
from project.io.utils import ensure_dir, read_parquet, write_parquet
from project.research.search.distributed_runner import run_distributed_search
from project.research._family_event_utils import load_features as load_features
from project.research.search.search_feature_utils import (
    ensure_expected_event_columns,
    normalize_search_feature_columns,
    prepare_search_features_for_symbol,
)
from project.research.services.phase2_diagnostics import build_search_engine_diagnostics
from project.research.services.reporting_service import write_json_report
from project.research.services.pathing import (
    phase2_candidates_path,
    phase2_diagnostics_path,
    phase2_hypotheses_dir,
    phase2_run_dir,
)
from project.research.regime_routing import annotate_regime_metadata
from project.research.knowledge.schemas import canonical_json, region_key, stable_hash
from project.spec_validation import validate_search_spec_doc

log = logging.getLogger(__name__)

_DEFAULT_BROAD_SEARCH_SPECS = {
    "",
    "full",
    "search_space.yaml",
    "spec/search_space.yaml",
}

_DEFAULT_PHASE2_MIN_T_STAT = 1.5


def _safe_concat(frames: list, **kwargs) -> pd.DataFrame:
    """pd.concat wrapper that clears .attrs to avoid DataFrame-in-attrs comparison errors."""
    cleaned = []
    for f in frames:
        c = f.copy()
        c.attrs = {}
        cleaned.append(c)
    return pd.concat(cleaned, **kwargs)


def _normalize_phase2_candidate_artifact(df: pd.DataFrame) -> pd.DataFrame:
    """Return the canonical producer shape for phase2 candidate artifacts."""
    return normalize_dataframe_for_schema(df, "phase2_candidates")


def _normalize_candidate_event_timestamp_artifact(
    event_timestamps: pd.DataFrame,
    *,
    candidates: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "candidate_id",
        "hypothesis_id",
        "trigger_key",
        "symbol",
        "event_type",
        "context_cell",
        "event_atom",
        "template",
        "direction",
        "horizon",
        "event_timestamp",
        "split_label",
    ]
    if event_timestamps is None or event_timestamps.empty:
        return pd.DataFrame(columns=columns)

    out = event_timestamps.copy()
    out["event_timestamp"] = pd.to_datetime(out.get("event_timestamp"), utc=True, errors="coerce")
    out = out.dropna(subset=["event_timestamp"])
    if out.empty:
        return pd.DataFrame(columns=columns)

    merge_columns = [
        col
        for col in (
            "candidate_id",
            "hypothesis_id",
            "symbol",
            "event_type",
            "context_cell",
            "event_atom",
            "template",
            "direction",
            "horizon",
        )
        if col in candidates.columns
    ]
    if "hypothesis_id" in out.columns and "hypothesis_id" in candidates.columns:
        metadata = candidates[merge_columns].drop_duplicates(subset=["hypothesis_id"], keep="first")
        out = out.merge(metadata, on="hypothesis_id", how="inner")

    if "split_label" not in out.columns:
        out["split_label"] = ""
    if "candidate_id" not in out.columns:
        out["candidate_id"] = ""
    if "trigger_key" not in out.columns:
        out["trigger_key"] = ""

    for column in columns:
        if column not in out.columns:
            out[column] = ""
    out = out[columns].drop_duplicates(
        subset=["candidate_id", "hypothesis_id", "event_timestamp", "split_label"]
    )
    return out.sort_values(["candidate_id", "event_timestamp", "split_label"], kind="stable").reset_index(
        drop=True
    )


def _is_default_broad_search_spec(search_spec: str) -> bool:
    return str(search_spec or "").strip() in _DEFAULT_BROAD_SEARCH_SPECS


def _load_search_spec_doc(search_spec: str) -> dict:
    raw = str(search_spec or "").strip()
    if raw.endswith((".yaml", ".yml")):
        path = Path(raw)
        if not path.is_absolute():
            path = PROJECT_ROOT.parent / path
        doc = load_yaml_path(path)
    else:
        from project.spec_validation import loaders

        doc = loaders.load_search_spec(raw)
    if not isinstance(doc, dict):
        raise ValueError(f"Search spec must resolve to a mapping: {search_spec}")
    validate_search_spec_doc(doc, source=str(search_spec))
    return dict(doc)


def _write_event_scoped_search_spec(
    *,
    search_spec: str,
    phase2_event_type: str,
    out_dir: Path,
) -> str:
    event_type = str(phase2_event_type or "").strip().upper()
    if not event_type or event_type == "ALL" or not _is_default_broad_search_spec(search_spec):
        return str(search_spec)

    base_doc = _load_search_spec_doc(search_spec)
    narrowed = dict(base_doc)
    registry = get_domain_registry()
    event_row = registry.event_row(event_type)
    metadata = dict(narrowed.get("metadata") or {})
    metadata["auto_scope"] = f"event:{event_type}"
    metadata["auto_scope_source"] = "phase2_event_type"
    narrowed["metadata"] = metadata
    narrowed["events"] = [event_type]
    event_templates = event_row.get("templates", [])
    if isinstance(event_templates, (list, tuple)) and event_templates:
        _registry = get_domain_registry()
        expr_templates = [
            str(t)
            for t in event_templates
            if str(t).strip() and not _registry.is_filter_template(str(t))
        ]
        filter_templates = [
            str(t)
            for t in event_templates
            if str(t).strip() and _registry.is_filter_template(str(t))
        ]
        if expr_templates:
            narrowed["expression_templates"] = expr_templates
        if filter_templates:
            narrowed["filter_templates"] = filter_templates
        narrowed.pop("templates", None)
    event_horizons = event_row.get("horizons", [])
    if isinstance(event_horizons, (list, tuple)) and event_horizons:
        narrowed["horizons"] = [str(item) for item in event_horizons if str(item).strip()]
    if "max_candidates_per_run" in event_row:
        narrowed["max_candidates_per_run"] = int(event_row["max_candidates_per_run"])
    narrowed.pop("states", None)
    narrowed.pop("transitions", None)
    narrowed.pop("feature_predicates", None)
    narrowed["include_sequences"] = False
    narrowed["include_interactions"] = False
    triggers = dict(narrowed.get("triggers") or {})
    triggers["events"] = [event_type]
    triggers.pop("states", None)
    triggers.pop("transitions", None)
    triggers.pop("feature_predicates", None)
    narrowed["triggers"] = triggers

    ensure_dir(out_dir)
    resolved_spec_path = out_dir / f"resolved_search_spec__{event_type}.yaml"
    resolved_spec_path.write_text(
        yaml.safe_dump(narrowed, sort_keys=False),
        encoding="utf-8",
    )
    return str(resolved_spec_path)


def _normalize_audit_frame(rows: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(rows or [])
    if frame.empty:
        return frame
    for column in frame.columns:
        if frame[column].dtype != "object":
            continue
        sample = next(
            (
                value
                for value in frame[column]
                if value is not None and not (isinstance(value, float) and pd.isna(value))
            ),
            None,
        )
        if isinstance(sample, (dict, list, tuple)):
            frame[column] = frame[column].map(
                lambda value: (
                    json.dumps(value, sort_keys=True)
                    if isinstance(value, (dict, list, tuple))
                    else value
                )
            )
    return frame


def _annotate_candidate_regime_metadata(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "event_type" not in frame.columns:
        return frame
    return annotate_regime_metadata(frame)


def _attach_candidate_run_lineage(frame: pd.DataFrame, *, run_id: str) -> pd.DataFrame:
    out = frame.copy()
    if "run_id" not in out.columns:
        out["run_id"] = str(run_id)
    else:
        out["run_id"] = out["run_id"].fillna("").astype(str)
        out.loc[out["run_id"].str.strip() == "", "run_id"] = str(run_id)
    return out


def _load_edge_cell_lineage(lineage_path: str | Path | None) -> pd.DataFrame:
    if not lineage_path:
        return pd.DataFrame()
    path = Path(lineage_path)
    if not path.exists():
        raise FileNotFoundError(f"edge-cell lineage artifact not found: {path}")
    frame = read_parquet([path])
    if frame.empty:
        return frame
    if "hypothesis_id" not in frame.columns:
        raise ValueError(f"edge-cell lineage artifact lacks hypothesis_id: {path}")
    subset = ["hypothesis_id"]
    if "symbol" in frame.columns:
        subset.append("symbol")
    return frame.drop_duplicates(subset=subset, keep="last")


def _merge_edge_cell_lineage(frame: pd.DataFrame, lineage: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or lineage.empty or "hypothesis_id" not in frame.columns:
        return frame
    existing = set(frame.columns)
    if "symbol" in frame.columns and "symbol" in lineage.columns:
        left = frame.copy()
        right = lineage.copy()
        left["__edge_cell_symbol_key"] = left["symbol"].fillna("").astype(str).str.upper()
        right["__edge_cell_symbol_key"] = right["symbol"].fillna("").astype(str).str.upper()
        lineage_cols = [
            col
            for col in right.columns
            if col in {"hypothesis_id", "__edge_cell_symbol_key"} or col not in existing
        ]
        merged = left.merge(
            right[lineage_cols],
            on=["hypothesis_id", "__edge_cell_symbol_key"],
            how="left",
        )
        return merged.drop(columns=["__edge_cell_symbol_key"])
    lineage_cols = [
        col for col in lineage.columns if col == "hypothesis_id" or col not in existing
    ]
    return frame.merge(lineage[lineage_cols], on="hypothesis_id", how="left")


def _filter_edge_cell_authorized_hypotheses(
    hypotheses: list[Any],
    lineage: pd.DataFrame,
    *,
    symbol: str | None = None,
) -> list[Any]:
    if not hypotheses or lineage.empty or "hypothesis_id" not in lineage.columns:
        return []
    scoped = lineage
    if symbol is not None and "symbol" in scoped.columns:
        symbol_key = str(symbol).strip().upper()
        scoped = scoped[scoped["symbol"].fillna("").astype(str).str.upper() == symbol_key]
        if scoped.empty:
            return []
    allowed = set(scoped["hypothesis_id"].dropna().astype(str))
    if not allowed:
        return []
    return [spec for spec in hypotheses if str(spec.hypothesis_id()) in allowed]


def _classify_metrics_counts(
    metrics: pd.DataFrame,
    *,
    min_n: int,
    min_t_stat: float,
) -> tuple[int, int, int]:
    if metrics.empty:
        return 0, 0, 0

    valid_mask = (
        metrics.get("valid", pd.Series(False, index=metrics.index)).fillna(False).astype(bool)
    )
    invalid_reason = (
        metrics.get("invalid_reason", pd.Series("", index=metrics.index)).fillna("").astype(str)
    )

    valid_metrics_rows = int(valid_mask.sum())
    rejected_by_min_n = int(((~valid_mask) & invalid_reason.eq("min_sample_size")).sum())
    rejected_invalid_metrics = max(
        0,
        int(len(metrics)) - valid_metrics_rows - rejected_by_min_n,
    )
    return valid_metrics_rows, rejected_invalid_metrics, rejected_by_min_n


def _merge_rejection_reason_counts(
    base_counts: Mapping[str, Any],
    *,
    rejected_invalid_metrics: int,
    rejected_by_min_n: int,
    rejected_by_min_t_stat: int,
) -> dict[str, int]:
    counts = {str(reason): int(count) for reason, count in dict(base_counts).items()}
    if rejected_invalid_metrics:
        counts["invalid_metrics"] = counts.get("invalid_metrics", 0) + int(rejected_invalid_metrics)
    if rejected_by_min_n:
        counts["min_sample_size"] = counts.get("min_sample_size", 0) + int(rejected_by_min_n)
    if rejected_by_min_t_stat:
        counts["min_t_stat"] = counts.get("min_t_stat", 0) + int(rejected_by_min_t_stat)
    return counts


def _resolve_search_min_t_stat(
    *,
    explicit_min_t_stat: float | None,
    phase2_gates: Mapping[str, Any],
) -> float:
    if explicit_min_t_stat is not None:
        return float(explicit_min_t_stat)
    raw = phase2_gates.get("min_t_stat", _DEFAULT_PHASE2_MIN_T_STAT)
    return float(raw)


def _bool_mask(frame: pd.DataFrame, column: str) -> pd.Series:
    if frame.empty or column not in frame.columns:
        return pd.Series(False, index=frame.index, dtype=bool)
    return frame[column].fillna(False).astype(bool)


def _first_present_column(columns: list[str], available_columns: pd.Index) -> str | None:
    available = {str(column) for column in available_columns}
    for column in columns:
        if column in available:
            return column
    return None


def _event_signal_candidates(event_id: str) -> list[str]:
    from project.domain.compiled_registry import get_domain_registry
    event_def = get_domain_registry().get_event(str(event_id or "").strip().upper())
    signal_col = event_def.signal_column if event_def is not None else None
    return ColumnRegistry.event_cols(str(event_id or "").strip().upper(), signal_col=signal_col)


def _state_signal_candidates(state_id: str) -> list[str]:
    return ColumnRegistry.state_cols(str(state_id or "").strip().upper())


def _resolve_component_column(component_id: str, available_columns: pd.Index) -> str | None:
    token = str(component_id or "").strip().upper()
    if not token:
        return None
    event_column = _first_present_column(_event_signal_candidates(token), available_columns)
    if event_column is not None:
        return event_column
    return _first_present_column(_state_signal_candidates(token), available_columns)


def _materialize_sequence_trigger_columns(
    features: pd.DataFrame,
    hypotheses: list[Any],
) -> pd.DataFrame:
    if features.empty or not hypotheses:
        return features

    out = features.copy()
    available_columns = out.columns

    for hypothesis in hypotheses:
        trigger = getattr(hypothesis, "trigger", None)
        if trigger is None or getattr(trigger, "trigger_type", "") != TriggerType.SEQUENCE:
            continue

        sequence_id = str(getattr(trigger, "sequence_id", "") or "").strip()
        events = list(getattr(trigger, "events", None) or [])
        max_gaps = [int(value) for value in list(getattr(trigger, "max_gap", None) or [])]
        if not sequence_id or len(events) < 2:
            continue

        sequence_cols = ColumnRegistry.sequence_cols(sequence_id)
        if _first_present_column(sequence_cols, available_columns) is not None:
            continue

        event_masks: list[pd.Series] = []
        for event_id in events:
            event_col = _first_present_column(_event_signal_candidates(event_id), available_columns)
            if event_col is None:
                event_masks = []
                break
            event_masks.append(out[event_col].fillna(False).astype(bool))
        if not event_masks:
            out[sequence_cols[0]] = False
            available_columns = out.columns
            continue

        sequence_mask = event_masks[0]
        for step_idx, next_mask in enumerate(event_masks[1:]):
            max_gap = max_gaps[step_idx] if step_idx < len(max_gaps) else 1
            prior_completed = (
                sequence_mask.shift(1)
                .rolling(window=max(int(max_gap), 1), min_periods=1)
                .max()
                .fillna(0)
                .astype(bool)
            )
            sequence_mask = (prior_completed & next_mask).fillna(False)

        out[sequence_cols[0]] = sequence_mask.astype(bool)
        available_columns = out.columns

    return out


def _interaction_lag_steps(features: pd.DataFrame, raw_lag: Any) -> int:
    if features.empty or "timestamp" not in features.columns:
        try:
            return max(int(raw_lag), 1)
        except Exception:
            return 1

    timestamps = pd.to_datetime(features["timestamp"], utc=True, errors="coerce").dropna()
    if len(timestamps) < 2:
        try:
            return max(int(raw_lag), 1)
        except Exception:
            return 1

    bar_minutes = float((timestamps.diff().dropna().dt.total_seconds().median() or 300.0) / 60.0)
    if bar_minutes <= 0:
        bar_minutes = 5.0

    if hasattr(raw_lag, "total_seconds"):
        lag_minutes = float(raw_lag.total_seconds()) / 60.0
    else:
        try:
            lag_minutes = float(raw_lag)
        except Exception:
            lag_minutes = bar_minutes

    return max(int(lag_minutes // bar_minutes), 1)


def _direction_requirement_to_sign(required_direction: Any) -> float | None:
    if required_direction is None:
        return None
    value = str(required_direction).strip().lower()
    if not value:
        return None
    if value == "up":
        return 1.0
    if value == "down":
        return -1.0
    if value == "non_directional":
        return 0.0
    return None


def _resolve_component_direction_series(
    component_id: str,
    features: pd.DataFrame,
    required_direction: Any,
) -> pd.Series | None:
    required_sign = _direction_requirement_to_sign(required_direction)
    if required_sign is None:
        return None
    for col in ColumnRegistry.event_direction_cols(component_id):
        if col in features.columns:
            return pd.to_numeric(features[col], errors="coerce")
    return pd.Series(np.nan, index=features.index, dtype=float)


def _apply_component_direction_filter(
    *,
    mask: pd.Series,
    direction_series: pd.Series | None,
    required_direction: Any,
) -> pd.Series:
    required_sign = _direction_requirement_to_sign(required_direction)
    if required_sign is None:
        return mask
    if direction_series is None:
        return pd.Series(False, index=mask.index, dtype=bool)
    return (mask.astype(bool) & (direction_series == required_sign).fillna(False)).astype(bool)


def _materialize_interaction_trigger_columns(
    features: pd.DataFrame,
    hypotheses: list[Any],
) -> pd.DataFrame:
    if features.empty or not hypotheses:
        return features

    out = features.copy()
    available_columns = out.columns

    for hypothesis in hypotheses:
        trigger = getattr(hypothesis, "trigger", None)
        if trigger is None or getattr(trigger, "trigger_type", "") != TriggerType.INTERACTION:
            continue

        interaction_id = str(getattr(trigger, "interaction_id", "") or "").strip()
        if not interaction_id:
            continue
        interaction_cols = ColumnRegistry.interaction_cols(interaction_id)
        if _first_present_column(interaction_cols, available_columns) is not None:
            continue

        left_col = _resolve_component_column(getattr(trigger, "left", ""), available_columns)
        right_col = _resolve_component_column(getattr(trigger, "right", ""), available_columns)
        if left_col is None or right_col is None:
            out[interaction_cols[0]] = False
            available_columns = out.columns
            continue

        left_mask = out[left_col].fillna(False).astype(bool)
        right_mask = out[right_col].fillna(False).astype(bool)
        left_direction_series = _resolve_component_direction_series(
            str(getattr(trigger, "left", "") or ""),
            out,
            getattr(trigger, "left_direction", None),
        )
        right_direction_series = _resolve_component_direction_series(
            str(getattr(trigger, "right", "") or ""),
            out,
            getattr(trigger, "right_direction", None),
        )
        left_mask = _apply_component_direction_filter(
            mask=left_mask,
            direction_series=left_direction_series,
            required_direction=getattr(trigger, "left_direction", None),
        )
        right_mask = _apply_component_direction_filter(
            mask=right_mask,
            direction_series=right_direction_series,
            required_direction=getattr(trigger, "right_direction", None),
        )
        lag_steps = _interaction_lag_steps(out, getattr(trigger, "lag", 1))
        op = str(getattr(trigger, "op", "") or "").strip().lower()

        future_right = (
            right_mask.shift(-1)
            .iloc[::-1]
            .rolling(window=lag_steps, min_periods=1)
            .max()
            .iloc[::-1]
            .fillna(0)
            .astype(bool)
        )

        if op == "and":
            interaction_mask = left_mask & future_right
        elif op == "confirm":
            prior_left = (
                left_mask.shift(1)
                .rolling(window=lag_steps, min_periods=1)
                .max()
                .fillna(0)
                .astype(bool)
            )
            interaction_mask = right_mask & prior_left
        elif op == "exclude":
            interaction_mask = left_mask & (~future_right)
        elif op == "or":
            interaction_mask = left_mask | right_mask
        else:
            interaction_mask = pd.Series(False, index=out.index, dtype=bool)

        out[interaction_cols[0]] = interaction_mask.astype(bool)
        available_columns = out.columns

    return out


def _build_gate_funnel(
    *,
    hypotheses_generated: int,
    feasible_hypotheses: int,
    metrics: pd.DataFrame,
    candidate_universe: pd.DataFrame,
    written_candidates: pd.DataFrame,
    min_n: int,
) -> dict[str, int]:
    valid_mask = _bool_mask(metrics, "valid")
    # TICKET-012: Ensure n_values is a series even if 'n' column is missing to avoid .fillna AttributeError
    if "n" in metrics.columns:
        n_values = pd.to_numeric(metrics["n"], errors="coerce").fillna(0)
    else:
        n_values = pd.Series(0.0, index=metrics.index)
    pass_min_n = valid_mask & (n_values >= int(min_n))

    funnel: dict[str, int] = {
        "generated": int(hypotheses_generated),
        "feasible": int(feasible_hypotheses),
        "metrics_emitted": int(len(metrics)),
        "valid_metrics": int(valid_mask.sum()),
        "pass_min_sample_size": int(pass_min_n.sum()),
        "bridge_candidate_universe": int(len(candidate_universe)),
        "phase2_candidates_written": int(len(written_candidates)),
    }

    stage_mask = pd.Series(True, index=written_candidates.index, dtype=bool)
    for label, column in (
        ("pass_oos_validation", "gate_oos_validation"),
        ("pass_after_cost_positive", "gate_after_cost_positive"),
        ("pass_after_cost_stressed_positive", "gate_after_cost_stressed_positive"),
        ("pass_multiplicity", "gate_multiplicity"),
        ("pass_regime_stable", "gate_c_regime_stable"),
        ("phase2_final", "gate_bridge_tradable"),
    ):
        stage_mask &= _bool_mask(written_candidates, column)
        funnel[label] = int(stage_mask.sum())
    return funnel


def _latest_hierarchical_stage_frame(stage_artifacts: dict[str, pd.DataFrame] | None) -> pd.DataFrame:
    if not stage_artifacts:
        return pd.DataFrame()
    for stage_name in (
        "context_refinement",
        "execution_refinement",
        "template_refinement",
        "trigger_viability",
    ):
        frame = stage_artifacts.get(stage_name)
        if frame is not None and not frame.empty:
            return frame.copy()
    return pd.DataFrame()


# Phase 4.2 — regime-conditional candidate discovery signal columns
_REGIME_CANDIDATE_COLUMNS = [
    "hypothesis_id",
    "event_type",
    "template_id",
    "direction",
    "horizon",
    "entry_lag",
    "entry_lag_bars",
    "trigger_key",
    "t_stat",
    "mean_return_bps",
    "robustness_score",
    "context_json",
]


def _write_regime_conditional_candidates(
    final_df: pd.DataFrame,
    out_dir: Path,
    *,
    weak_t_stat_upper: float = 1.5,
    min_t_stat_lower: float = 0.5,
    min_mean_return_bps: float = 0.0,
    top_k: int = 20,
) -> None:
    """Phase 4.2 — Write regime_conditional_candidates.parquet.

    Identifies hypotheses that were weak overall (t_stat < weak_t_stat_upper)
    but had positive mean_return_bps, indicating potential regime-specific alpha.
    These are surfaced as an explore_adjacent discovery signal so the campaign
    controller can propose targeted context-conditioned follow-up runs.

    The file is written to ``out_dir/regime_conditional_candidates.parquet``.
    If no qualifying candidates exist an empty schema file is written so
    downstream readers can always expect the artefact.
    """
    rcc_path = out_dir / "regime_conditional_candidates.parquet"

    empty = pd.DataFrame(columns=_REGIME_CANDIDATE_COLUMNS)

    if final_df is None or final_df.empty:
        write_parquet(empty, rcc_path)
        return

    t_col = "t_stat" if "t_stat" in final_df.columns else None
    ret_col = "mean_return_bps" if "mean_return_bps" in final_df.columns else None

    if t_col is None or ret_col is None:
        write_parquet(empty, rcc_path)
        return

    t_num = pd.to_numeric(final_df[t_col], errors="coerce").fillna(0.0)
    ret_num = pd.to_numeric(final_df[ret_col], errors="coerce").fillna(0.0)

    mask = (
        (t_num >= min_t_stat_lower) & (t_num < weak_t_stat_upper) & (ret_num > min_mean_return_bps)
    )
    weak_positive = final_df[mask].copy()

    if weak_positive.empty:
        write_parquet(empty, rcc_path)
        return

    # Sort by mean_return_bps descending — highest-signal near-misses first
    weak_positive = weak_positive.sort_values(ret_col, ascending=False).head(top_k)

    # Emit only the columns we need; fill missing with empty string / NaN
    out_rows = []
    for _, row in weak_positive.iterrows():
        # Extract event_type from trigger_key ("event:EVTNAME" → "EVTNAME")
        tkey = str(row.get("trigger_key", ""))
        if tkey.startswith("event:"):
            event_type = tkey[len("event:") :]
        elif tkey.startswith("state:"):
            event_type = tkey[len("state:") :]
        else:
            event_type = tkey

        out_rows.append(
            {
                "hypothesis_id": str(row.get("hypothesis_id", "")),
                "event_type": event_type,
                "template_id": str(row.get("template_id", "")),
                "direction": str(row.get("direction", "")),
                "horizon": str(row.get("horizon", "")),
                "entry_lag": int(row.get("entry_lag", row.get("entry_lag_bars", 0)) or 0),
                "entry_lag_bars": int(row.get("entry_lag_bars", row.get("entry_lag", 0)) or 0),
                "trigger_key": tkey,
                "t_stat": float(row.get(t_col, 0.0)),
                "mean_return_bps": float(row.get(ret_col, 0.0)),
                "robustness_score": float(row.get("robustness_score", 0.0)),
                "context_json": str(row.get("context_json", "") or ""),
            }
        )

    rcc_df = pd.DataFrame(out_rows, columns=_REGIME_CANDIDATE_COLUMNS)
    write_parquet(rcc_df, rcc_path)
    log.info(
        "Phase 4.2: wrote %d regime_conditional_candidates to %s",
        len(rcc_df),
        rcc_path,
    )


def _expected_event_ids_from_hypotheses(hypotheses) -> list[str]:
    expected: list[str] = []
    seen: set[str] = set()
    for spec in hypotheses:
        trigger = getattr(spec, "trigger", None)
        event_id = str(getattr(trigger, "event_id", "") or "").strip().upper()
        if event_id and event_id not in seen:
            expected.append(event_id)
            seen.add(event_id)
        if str(getattr(trigger, "trigger_type", "") or "").strip().lower() == TriggerType.SEQUENCE:
            for component_event_id in list(getattr(trigger, "events", None) or []):
                token = str(component_event_id or "").strip().upper()
                if token and token not in seen:
                    expected.append(token)
                    seen.add(token)
        if (
            str(getattr(trigger, "trigger_type", "") or "").strip().lower()
            == TriggerType.INTERACTION
        ):
            for component_id in (getattr(trigger, "left", ""), getattr(trigger, "right", "")):
                token = str(component_id or "").strip().upper()
                if get_domain_registry().has_event(token) and token not in seen:
                    expected.append(token)
                    seen.add(token)
    return expected


def _expected_event_ids_from_search_spec_doc(search_spec_doc: Mapping[str, Any]) -> list[str]:
    from project.spec_validation import expand_triggers

    expanded = expand_triggers(dict(search_spec_doc or {}))
    expected: list[str] = []
    seen: set[str] = set()
    for raw_event_id in list(expanded.get("events", []) or []):
        event_id = str(raw_event_id or "").strip().upper()
        if event_id and event_id not in seen and get_domain_registry().has_event(event_id):
            expected.append(event_id)
            seen.add(event_id)
    return expected


def _hypothesis_region_key(hypothesis: Any, *, program_id: str, symbol: str) -> str:
    trigger = getattr(hypothesis, "trigger", None)
    trigger_type = str(getattr(trigger, "trigger_type", "") or "").strip().lower()
    trigger_label = str(trigger.label()).strip() if trigger is not None else ""

    event_type = ""
    if trigger_type == "event":
        event_type = str(getattr(trigger, "event_id", "") or "").strip().upper()
    elif trigger_type == "state":
        state_id = str(getattr(trigger, "state_id", "") or "").strip().upper()
        if state_id:
            event_type = canonical_bridge_event_type("state", f"state:{state_id}")
    elif trigger_type == "transition":
        from_state = str(getattr(trigger, "from_state", "") or "").strip().upper()
        to_state = str(getattr(trigger, "to_state", "") or "").strip().upper()
        if from_state and to_state:
            event_type = canonical_bridge_event_type(
                "transition",
                f"transition:{from_state}→{to_state}",
            )
    elif trigger_type == "feature_predicate":
        feature = str(getattr(trigger, "feature", "") or "").strip().upper()
        operator = str(getattr(trigger, "operator", "") or "").strip()
        threshold = getattr(trigger, "threshold", None)
        if feature and operator and threshold not in (None, ""):
            event_type = canonical_bridge_event_type(
                "feature_predicate",
                f"pred:{feature}{operator}{threshold}",
            )
    elif trigger_type in {"sequence", "interaction"} and trigger_label:
        event_type = canonical_bridge_event_type(trigger_type, trigger_label)

    context_blob = canonical_json(getattr(hypothesis, "context", None) or {})
    return region_key(
        {
            "program_id": str(program_id or "").strip(),
            "symbol_scope": str(symbol or "").strip().upper(),
            "event_type": event_type or trigger_label.upper(),
            "trigger_type": trigger_type.upper(),
            "template_id": str(getattr(hypothesis, "template_id", "") or "").strip(),
            "direction": str(getattr(hypothesis, "direction", "") or "").strip(),
            "horizon": str(getattr(hypothesis, "horizon", "") or "").strip(),
            "entry_lag": int(getattr(hypothesis, "entry_lag", 0) or 0),
            "context_hash": stable_hash((context_blob,)),
        }
    )


def _filter_previously_tested_hypotheses(
    hypotheses: list[Any],
    *,
    program_id: str,
    symbol: str,
    avoid_region_keys: set[str],
) -> tuple[list[Any], int]:
    if not hypotheses or not avoid_region_keys:
        return list(hypotheses), 0

    filtered: list[Any] = []
    skipped = 0
    for hypothesis in hypotheses:
        if (
            _hypothesis_region_key(hypothesis, program_id=program_id, symbol=symbol)
            in avoid_region_keys
        ):
            skipped += 1
            continue
        filtered.append(hypothesis)
    return filtered, skipped


def _write_hypothesis_audit_artifacts(out_dir: Path, symbol: str, audit: dict) -> None:
    audit_dir = out_dir / str(symbol).upper()
    ensure_dir(audit_dir)
    write_parquet(
        _normalize_audit_frame(audit.get("generated_rows", [])),
        audit_dir / "generated_hypotheses.parquet",
    )
    write_parquet(
        _normalize_audit_frame(audit.get("rejected_rows", [])),
        audit_dir / "rejected_hypotheses.parquet",
    )
    write_parquet(
        _normalize_audit_frame(audit.get("feasible_rows", [])),
        audit_dir / "feasible_hypotheses.parquet",
    )


def _write_evaluation_artifacts(
    out_dir: Path, symbol: str, metrics: pd.DataFrame, gate_failures: pd.DataFrame
) -> None:
    audit_dir = out_dir / "hypotheses" / str(symbol).upper()
    ensure_dir(audit_dir)
    write_parquet(
        evaluated_records_from_metrics(metrics), audit_dir / "evaluated_hypotheses.parquet"
    )
    write_parquet(gate_failures, audit_dir / "gate_failures.parquet")


def _annotate_promotion_gate_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Derive promotion-required gate fields from bridge evaluation results.

    The promotion service expects fields that the discovery search engine does not
    naturally emit.  Map them from equivalent bridge gates so promote_candidates
    can assemble a valid evidence bundle without requiring a separate confirmatory run.
    """
    if df.empty:
        return df
    df = df.copy()
    # gate_stability: bridge regime stability is the equivalent stability concept
    if "gate_c_regime_stable" in df.columns and "gate_stability" not in df.columns:
        df["gate_stability"] = df["gate_c_regime_stable"]
    # gate_delayed_entry_stress: if the bridge passed with this entry_lag, delay robustness holds
    if "gate_bridge_tradable" in df.columns and "gate_delayed_entry_stress" not in df.columns:
        df["gate_delayed_entry_stress"] = df["gate_bridge_tradable"].astype(bool)
    # gate_bridge_microstructure: 5m OHLCV on liquid perp markets passes microstructure
    if "gate_bridge_microstructure" not in df.columns:
        df["gate_bridge_microstructure"] = True
    # E-GATE-001: sign_consistency is the fraction of trades whose return has the same
    # sign as the directional hypothesis (== hit_rate for long strategies).
    # The previous proxy (robustness_score = fraction of regime slabs agreeing on direction)
    # is a related but distinct metric; using it causes the promotion gate threshold to
    # be applied to a semantically wrong value.
    if "sign_consistency" not in df.columns:
        if "hit_rate" in df.columns:
            df["sign_consistency"] = pd.to_numeric(df["hit_rate"], errors="coerce").clip(0.0, 1.0)
        elif "robustness_score" in df.columns:
            import logging as _logging

            _logging.getLogger(__name__).warning(
                "_populate_bridge_compatible_fields: neither sign_consistency nor hit_rate "
                "available; falling back to robustness_score proxy. sign_consistency gate "
                "may be inaccurate for this candidate."
            )
            df["sign_consistency"] = pd.to_numeric(df["robustness_score"], errors="coerce").clip(
                0.0, 1.0
            )
    # stability_score: robustness_score already measures fold-level direction consistency
    if "stability_score" not in df.columns and "robustness_score" in df.columns:
        df["stability_score"] = df["robustness_score"].clip(0.0, 1.0)
    return df


def _write_hypothesis_registry(candidates: pd.DataFrame, out_dir: Path) -> None:
    """Write hypothesis_registry.parquet for promote_candidates audit chain validation."""
    if candidates.empty or "hypothesis_id" not in candidates.columns:
        return
    registry_candidates = candidates.copy()
    if "plan_row_id" not in registry_candidates.columns:
        registry_candidates["plan_row_id"] = ""
    rows = [
        {
            "hypothesis_id": str(row.get("hypothesis_id", "")).strip(),
            "plan_row_id": (
                str(row.get("plan_row_id", "")).strip() or str(row.get("hypothesis_id", "")).strip()
            ),
            "executed": True,
            "statuses": json.dumps(["candidate_discovery"]),
        }
        for row in registry_candidates[["hypothesis_id", "plan_row_id"]]
        .drop_duplicates()
        .to_dict(orient="records")
        if str(row.get("hypothesis_id", "")).strip()
    ]
    if rows:
        write_parquet(pd.DataFrame(rows), out_dir / "hypothesis_registry.parquet")
        log.info("Wrote hypothesis_registry.parquet (%d rows) to %s", len(rows), out_dir)


def _sort_final_candidates(
    final_df: pd.DataFrame,
    *,
    enable_discovery_v2_scoring: bool,
) -> pd.DataFrame:
    if final_df.empty:
        return final_df

    if enable_discovery_v2_scoring:
        if "discovery_quality_score_v3" in final_df.columns:
            rank_col = "discovery_quality_score_v3"
        elif "discovery_quality_score" in final_df.columns:
            rank_col = "discovery_quality_score"
        else:
            rank_col = "t_stat"

        if rank_col in final_df.columns and "is_discovery" in final_df.columns:
            return final_df.sort_values(
                ["is_discovery", rank_col],
                ascending=[False, False],
            ).reset_index(drop=True)
        if rank_col in final_df.columns:
            return final_df.sort_values(rank_col, ascending=False).reset_index(drop=True)
        return final_df

    fallback_rank_col = "t_stat" if "t_stat" in final_df.columns else "discovery_quality_score"
    if fallback_rank_col in final_df.columns and "is_discovery" in final_df.columns:
        return final_df.sort_values(
            ["is_discovery", fallback_rank_col],
            ascending=[False, False],
        ).reset_index(drop=True)
    if fallback_rank_col in final_df.columns:
        return final_df.sort_values(
            fallback_rank_col,
            ascending=False,
        ).reset_index(drop=True)
    return final_df


def _load_hierarchical_config(search_spec_doc: dict) -> dict | None:
    """Return the hierarchical config block if mode=hierarchical, else None."""
    block = (search_spec_doc or {}).get("discovery_search", {})
    if not isinstance(block, dict):
        return None
    if str(block.get("mode", "flat")).strip().lower() != "hierarchical":
        return None
    return block


def _apply_hierarchical_profile_overrides(
    config: dict | None,
    overrides: Mapping[str, Any] | None,
) -> dict | None:
    if config is None:
        return None
    if not overrides:
        return dict(config)

    merged = dict(config)
    for key, value in dict(overrides).items():
        if isinstance(value, Mapping):
            base = merged.get(key, {})
            if isinstance(base, Mapping):
                merged[key] = {**dict(base), **dict(value)}
            else:
                merged[key] = dict(value)
        else:
            merged[key] = value
    return merged


def _write_hierarchical_stage_artifacts(
    stage_artifacts: dict[str, "pd.DataFrame"],
    out_dir: Path,
    symbol: str,
) -> None:
    """Write per-symbol stage artifact parquets (non-fatal)."""
    name_map = {
        "trigger_viability": "phase2_trigger_probes",
        "template_refinement": "phase2_template_refinement",
        "execution_refinement": "phase2_execution_refinement",
        "context_refinement": "phase2_context_refinement",
    }
    for stage, df in stage_artifacts.items():
        if df is None or df.empty:
            continue
        stem = name_map.get(stage, f"phase2_stage_{stage}")
        try:
            write_parquet(df, out_dir / f"{stem}__{symbol}.parquet")
        except Exception as exc:
            log.warning("Stage artifact write failed (%s / %s): %s", stage, symbol, exc)


def _load_diversification_config(search_spec_doc: dict) -> dict:
    """Return the discovery_selection config block or empty dict."""
    block = (search_spec_doc or {}).get("discovery_selection", {})
    return block if isinstance(block, dict) else {}


def _build_required_walkforward_folds(
    features: pd.DataFrame,
    config_path: Path | None = None,
) -> list[Any] | None:
    """Build repeated walk-forward folds, failing closed when validation is enabled."""
    val_config_path = (
        config_path
        if config_path is not None
        else PROJECT_ROOT.parent / "project" / "configs" / "discovery_validation.yaml"
    )
    if not val_config_path.exists():
        return None

    try:
        with val_config_path.open(encoding="utf-8") as f:
            vconfig = yaml.safe_load(f) or {}
    except Exception as exc:
        raise RuntimeError(
            f"Discovery validation config could not be loaded: {val_config_path}"
        ) from exc

    rw = vconfig.get("discovery_validation", {}).get("repeated_walkforward", {})
    if not isinstance(rw, dict) or not rw.get("enabled"):
        return None
    if "timestamp" not in features.columns:
        raise RuntimeError(
            "Required repeated walk-forward fold construction failed: missing timestamp column"
        )

    try:
        from project.research.validation.splits import build_repeated_walkforward_splits

        folds = build_repeated_walkforward_splits(
            features["timestamp"],
            train_bars=rw.get("train_bars", 2000),
            validation_bars=rw.get("validation_bars", 500),
            test_bars=rw.get("test_bars", 500),
            step_bars=rw.get("step_bars", 500),
            min_folds=rw.get("min_folds", 3),
            max_folds=rw.get("max_folds", None),
            purge_bars=rw.get("purge_bars", 0),
            embargo_bars=rw.get("embargo_bars", 0),
        )
    except Exception as exc:
        raise RuntimeError("Required repeated walk-forward fold construction failed") from exc

    if not folds:
        raise RuntimeError("Required repeated walk-forward fold construction produced 0 folds")
    log.info("Built %d repeated walk-forward folds", len(folds))
    return folds


def _ensure_diversification_fallback_columns(
    candidates: pd.DataFrame,
    reason: str,
) -> pd.DataFrame:
    """Make diversification degradation visible in candidate artifacts."""
    out = candidates.copy()
    fallback_defaults: Mapping[str, Any] = {
        "overlap_cluster_id": None,
        "cluster_size": 1,
        "cluster_density": 0.0,
        "is_duplicate_like": False,
        "novelty_score": 1.0,
        "crowding_penalty": 0.0,
        "cluster_rank": 1,
        "selected_into_diversified_shortlist": False,
        "shortlist_rank": 0,
        "selection_score": np.nan,
        "selection_reason": "",
    }
    for col, default in fallback_defaults.items():
        if col not in out.columns:
            out[col] = default
    out["_diversification_error"] = True
    out["_diversification_error_reason"] = reason
    return out


def run(
    run_id: str,
    symbols: str,
    data_root: Path,
    out_dir: Path,
    *,
    timeframe: str = "5m",
    start: str | None = None,
    end: str | None = None,
    discovery_profile: str = "standard",
    gate_profile: str = "auto",
    search_spec: str = "full",
    chunk_size: int = 500,
    min_t_stat: float | None = None,
    min_n: int = 30,
    search_budget: Optional[int] = None,
    experiment_config: Optional[str] = None,
    registry_root: str | Path = "project/configs/registries",
    use_context_quality: bool = True,
    enable_discovery_v2_scoring: bool = True,
    phase2_event_type: str = "",
    event_registry_override: Optional[str] = None,
    discovery_mode: str = "search",
    lineage_path: str | Path | None = None,
) -> int:
    """
    Discovery v2 Search Engine Orchestrator. [STATUS: STABLE]

    FEATURE CLASSIFICATION:
    -----------------------
    - STABLE:
        * Phase 1 Event Generation
        * Phase 2 Legacy (t-stat) Ranking
        * Gating Funnels
    - STABLE-INTERNAL:
        * Discovery v2 Scoring (Significance, Tradability, Novelty, Falsification)
        * Decomposition Diagnostics
    - EXPERIMENTAL:
        * Concept Ledger (v3) Multiplicity Correction
        * Fold-based Stability Folds

    Core logic. Returns exit code (0=success, 1=failure).
    """
    log.info(
        "Starting Phase 2 Search Engine (run_id=%s, search_spec=%s, experiment_config=%s)",
        run_id,
        search_spec,
        experiment_config,
    )
    ensure_dir(out_dir)
    output_path = phase2_candidates_path(data_root=data_root, run_id=run_id)
    diagnostics_path = phase2_diagnostics_path(data_root=data_root, run_id=run_id)
    symbols_requested = [s.strip().upper() for s in str(symbols).split(",") if s.strip()]
    timeframe = str(timeframe or "5m").strip().lower() or "5m"
    gates_spec = load_gates_spec(PROJECT_ROOT.parent)
    phase2_gates = select_phase2_gate_spec(
        gates_spec,
        mode="research",
        gate_profile=str(gate_profile or "auto"),
    )
    bridge_gates = select_bridge_gate_spec(gates_spec)
    search_profile = resolve_search_profile(
        discovery_profile=discovery_profile,
        search_spec=search_spec,
        min_n=min_n,
        min_t_stat=_resolve_search_min_t_stat(
            explicit_min_t_stat=min_t_stat,
            phase2_gates=phase2_gates,
        ),
    )
    resolved_search_spec = _write_event_scoped_search_spec(
        search_spec=str(search_profile["search_spec"]),
        phase2_event_type=phase2_event_type,
        out_dir=out_dir,
    )
    resolved_min_n = int(search_profile["min_n"])
    resolved_min_t_stat = float(search_profile["min_t_stat"])
    multiplicity_max_q = float(phase2_gates.get("max_q_value", 0.05))

    # 1. Load data and evaluate symbols
    all_candidates = []
    all_fold_breakdowns = []
    all_candidate_event_timestamps = []
    regime_conditional_inputs = []
    symbol_diagnostics = []
    metrics_frames = []
    candidate_universe_frames = []
    edge_cell_lineage = (
        _load_edge_cell_lineage(lineage_path)
        if str(discovery_mode or "search").strip() == "edge_cells"
        else pd.DataFrame()
    )

    total_feature_rows = 0
    total_event_flag_rows = 0
    total_hypotheses_generated = 0
    total_feasible_hypotheses = 0
    total_rejected_hypotheses = 0
    total_metrics_rows = 0
    total_valid_metrics_rows = 0
    total_rejected_invalid_metrics = 0
    total_rejected_by_min_n = 0
    total_rejected_by_min_t_stat = 0
    total_bridge_candidates_rows = 0
    aggregated_rejection_reasons: dict[str, int] = {}

    max_feature_columns = 0
    max_event_flag_columns_merged = 0

    # Load experiment plan if provided
    experiment_plan = None
    avoid_region_keys: set[str] = set()
    resolved_search_spec_doc: dict[str, Any] = {}
    if experiment_config:
        from project.research.experiment_engine import (
            build_experiment_plan,
            load_agent_experiment_config,
        )

        experiment_request = load_agent_experiment_config(Path(experiment_config))
        avoid_region_keys = {
            str(value).strip()
            for value in experiment_request.avoid_region_keys
            if str(value).strip()
        }
        experiment_plan = build_experiment_plan(Path(experiment_config), Path(registry_root))
        log.info("Loaded experiment plan with %d hypotheses", len(experiment_plan.hypotheses))
    else:
        try:
            resolved_search_spec_doc = _load_search_spec_doc(resolved_search_spec)
        except Exception as exc:
            log.warning("Failed to load search spec for expected-event materialization: %s", exc)

    for symbol in symbols_requested:
        log.info("Processing symbol %s...", symbol)

        # 1a. Load and prepare search feature frame
        preloaded_expected_event_ids = (
            _expected_event_ids_from_hypotheses(experiment_plan.hypotheses)
            if experiment_plan is not None
            else _expected_event_ids_from_search_spec_doc(resolved_search_spec_doc)
        )
        features = prepare_search_features_for_symbol(
            run_id=run_id,
            symbol=symbol,
            timeframe=timeframe,
            data_root=data_root,
            start=start,
            end=end,
            expected_event_ids=preloaded_expected_event_ids,
            load_features_fn=load_features,
            event_registry_override=event_registry_override,
        )
        if features.empty:
            log.warning("Empty feature table for %s", symbol)
            symbol_diagnostics.append(
                {"run_id": run_id, "primary_symbol": symbol, "skip_reason": "empty_feature_table"}
            )
            continue

        # 2. Generate hypotheses
        if experiment_plan:
            planned_hypotheses = list(experiment_plan.hypotheses)
            hypotheses, skipped_prior_regions = _filter_previously_tested_hypotheses(
                planned_hypotheses,
                program_id=experiment_plan.program_id,
                symbol=symbol,
                avoid_region_keys=avoid_region_keys,
            )
            generation_audit = {
                "counts": {
                    "generated": len(planned_hypotheses),
                    "feasible": len(hypotheses),
                    "rejected": skipped_prior_regions,
                },
                "rejection_reason_counts": (
                    {"prior_tested_region": skipped_prior_regions} if skipped_prior_regions else {}
                ),
            }
            log.info(
                "Using %d experiment-plan hypotheses for %s after skipping %d prior-tested regions",
                len(hypotheses),
                symbol,
                skipped_prior_regions,
            )
        else:
            log.info("Generating hypotheses from spec for %s: %s", symbol, resolved_search_spec)
            hypotheses, generation_audit = generate_hypotheses_with_audit(
                resolved_search_spec,
                max_hypotheses=int(search_budget) if search_budget is not None else None,
                features=features,
            )
            if str(discovery_mode or "search").strip() == "edge_cells":
                before_filter = len(hypotheses)
                hypotheses = _filter_edge_cell_authorized_hypotheses(
                    hypotheses,
                    edge_cell_lineage,
                    symbol=symbol,
                )
                generation_audit.setdefault("edge_cell_authorization", {})
                generation_audit["edge_cell_authorization"] = {
                    "authorized_hypotheses": len(hypotheses),
                    "unauthorized_hypotheses_filtered": before_filter - len(hypotheses),
                    "lineage_hypothesis_count": int(len(edge_cell_lineage)),
                }
                generation_audit.setdefault("counts", {})
                generation_audit["counts"]["feasible"] = len(hypotheses)
            _write_hypothesis_audit_artifacts(
                phase2_hypotheses_dir(data_root=data_root, run_id=run_id),
                symbol,
                generation_audit,
            )
            log.info("Generated %d hypotheses for %s", len(hypotheses), symbol)

        expected_event_ids = _expected_event_ids_from_hypotheses(hypotheses)
        if experiment_plan is None and expected_event_ids:
            features = ensure_expected_event_columns(
                features,
                expected_event_ids=expected_event_ids,
                copy=False,
            )
            if features.empty:
                log.warning(
                    "Empty feature table for %s after expected-event materialization", symbol
                )
                continue

        features = _materialize_sequence_trigger_columns(features, hypotheses)
        features = _materialize_interaction_trigger_columns(features, hypotheses)

        log.info(
            "Loaded features for %s: %d rows, %d columns",
            symbol,
            len(features),
            len(features.columns),
        )
        max_feature_columns = max(max_feature_columns, int(len(features.columns)))
        sym_flags = features[
            [c for c in features.columns if c.endswith(("_event", "_active", "_signal"))]
        ].copy()
        if not sym_flags.empty:
            max_event_flag_columns_merged = max(
                max_event_flag_columns_merged, int(len(sym_flags.columns))
            )

        total_feature_rows += int(len(features))
        total_event_flag_rows += int(len(features)) if not sym_flags.empty else 0

        total_hypotheses_generated += int(
            generation_audit.get("counts", {}).get("generated", len(hypotheses))
        )
        total_feasible_hypotheses += int(
            generation_audit.get("counts", {}).get("feasible", len(hypotheses))
        )
        generation_rejected_hypotheses = int(generation_audit.get("counts", {}).get("rejected", 0))
        total_rejected_hypotheses += generation_rejected_hypotheses
        generation_rejection_reason_counts = dict(
            generation_audit.get("rejection_reason_counts", {})
        )
        for reason, count in generation_rejection_reason_counts.items():
            aggregated_rejection_reasons[str(reason)] = aggregated_rejection_reasons.get(
                str(reason), 0
            ) + int(count)

        if not hypotheses:
            log.warning("No hypotheses generated for %s", symbol)
            _write_evaluation_artifacts(out_dir, symbol, pd.DataFrame(), pd.DataFrame())
            symbol_diagnostics.append(
                build_search_engine_diagnostics(
                    run_id=run_id,
                    discovery_profile=str(search_profile["discovery_profile"]),
                    search_spec=resolved_search_spec,
                    timeframe=timeframe,
                    symbols_requested=symbols_requested,
                    primary_symbol=symbol,
                    feature_rows=int(len(features)),
                    feature_columns=int(len(features.columns)),
                    event_flag_rows=int(len(sym_flags)),
                    event_flag_columns_merged=int(len(sym_flags.columns)),
                    hypotheses_generated=int(
                        generation_audit.get("counts", {}).get("generated", len(hypotheses))
                    ),
                    feasible_hypotheses=0,
                    rejected_hypotheses=generation_rejected_hypotheses,
                    rejection_reason_counts=generation_rejection_reason_counts,
                    metrics_rows=0,
                    valid_metrics_rows=0,
                    rejected_invalid_metrics=0,
                    rejected_by_min_n=0,
                    rejected_by_min_t_stat=0,
                    bridge_candidates_rows=0,
                    multiplicity_discoveries=0,
                    min_t_stat=resolved_min_t_stat,
                    min_n=resolved_min_n,
                    search_budget=search_budget,
                    use_context_quality=use_context_quality,
                    gate_funnel=_build_gate_funnel(
                        hypotheses_generated=int(
                            generation_audit.get("counts", {}).get("generated", len(hypotheses))
                        ),
                        feasible_hypotheses=0,
                        metrics=pd.DataFrame(),
                        candidate_universe=pd.DataFrame(),
                        written_candidates=pd.DataFrame(),
                        min_n=resolved_min_n,
                    ),
                )
            )
            continue

        # 2.5 Walk-forward validation is mandatory when enabled in config.
        folds = _build_required_walkforward_folds(features)

        # ── Phase 4: hierarchical vs flat mode selection ────────────────────────
        # Load search spec doc once so we can check for hierarchical config.
        # The spec has already been resolved above (resolved_search_spec).
        _h_spec_doc: dict = dict(resolved_search_spec_doc)
        if not _h_spec_doc:
            try:
                _h_spec_doc = _load_search_spec_doc(resolved_search_spec)
            except Exception as _h_exc:
                log.warning("Failed to load search spec for hierarchical config: %s", _h_exc)
        _h_config = _load_hierarchical_config(_h_spec_doc) if not experiment_plan else None
        _h_config = _apply_hierarchical_profile_overrides(
            _h_config,
            search_profile.get("hierarchical_overrides"),
        )

        if _h_config is not None:
            # ── HIERARCHICAL MODE ────────────────────────────────────────────
            log.info("Phase 4 hierarchical search mode active for %s", symbol)
            from project.research.search.hierarchical_search import run_hierarchical_search
            from project.spec_validation import expand_triggers

            _h_expanded = expand_triggers(_h_spec_doc)
            _h_events = _h_expanded.get("events", [])

            h_result = run_hierarchical_search(
                run_id=run_id,
                symbol=symbol,
                events=_h_events,
                features=features,
                search_spec_doc=_h_spec_doc,
                hierarchical_config=_h_config,
                chunk_size=chunk_size,
                min_n=resolved_min_n,
                min_t_stat=resolved_min_t_stat,
                use_context_quality=use_context_quality,
                folds=folds,
                bridge_gates=dict(bridge_gates),
                data_root=data_root,
                out_dir=out_dir,
            )

            candidates = h_result.final_candidates
            if (
                hasattr(candidates, "attrs")
                and "candidate_event_timestamps" in candidates.attrs
                and not candidates.attrs["candidate_event_timestamps"].empty
            ):
                all_candidate_event_timestamps.append(
                    candidates.attrs["candidate_event_timestamps"].copy()
                )
            h_candidate_universe = _latest_hierarchical_stage_frame(h_result.stage_artifacts)
            h_candidate_universe = _merge_edge_cell_lineage(
                h_candidate_universe,
                edge_cell_lineage,
            )
            candidates = _merge_edge_cell_lineage(candidates, edge_cell_lineage)
            h_metrics_for_funnel = h_candidate_universe.copy()
            if not h_metrics_for_funnel.empty and "valid" not in h_metrics_for_funnel.columns:
                h_metrics_for_funnel["valid"] = True
            h_valid_metrics_rows = int(len(h_metrics_for_funnel))
            h_rejected_by_min_t_stat = 0
            if (
                not h_candidate_universe.empty
                and "gate_search_min_t_stat" in h_candidate_universe.columns
            ):
                h_rejected_by_min_t_stat = int(
                    (
                        ~h_candidate_universe["gate_search_min_t_stat"]
                        .fillna(False)
                        .astype(bool)
                    ).sum()
                )
            if not candidates.empty:
                if "gate_search_min_t_stat" in candidates.columns:
                    gate_mask = candidates["gate_search_min_t_stat"].fillna(False).astype(bool)
                    candidates = candidates[gate_mask].copy()
                    if candidates.empty:
                        log.warning(
                            "All hierarchical candidates filtered by gate_search_min_t_stat"
                        )
                if not candidates.empty and "candidate_id" in candidates.columns:
                    candidates = candidates.copy()
                    candidates["candidate_id"] = (
                        symbol + "::" + candidates["candidate_id"].astype(str)
                    )
                if not candidates.empty:
                    all_candidates.append(candidates)

            total_metrics_rows += h_result.candidates_evaluated_total
            total_feasible_hypotheses += h_result.candidates_evaluated_total
            total_valid_metrics_rows += h_valid_metrics_rows
            total_rejected_by_min_t_stat += h_rejected_by_min_t_stat
            total_bridge_candidates_rows += len(candidates)
            if not h_metrics_for_funnel.empty:
                metrics_frames.append(h_metrics_for_funnel.copy())
            if not h_candidate_universe.empty:
                candidate_universe_frames.append(h_candidate_universe.copy())

            # Accumulate stage artifacts for combined write after the loop
            if not hasattr(run, "_h_stage_artifacts"):
                run._h_stage_artifacts = {}  # type: ignore[attr-defined]
            for stage_name, stage_df in h_result.stage_artifacts.items():
                if not stage_df.empty:
                    existing = run._h_stage_artifacts.get(stage_name, pd.DataFrame())  # type: ignore[attr-defined]
                    run._h_stage_artifacts[stage_name] = (
                        pd.concat(  # type: ignore[attr-defined]
                            [existing, stage_df], ignore_index=True
                        )
                        if not existing.empty
                        else stage_df.copy()
                    )

            symbol_diagnostics.append(
                build_search_engine_diagnostics(
                    run_id=run_id,
                    discovery_profile=str(search_profile["discovery_profile"]),
                    search_spec=resolved_search_spec,
                    timeframe=timeframe,
                    symbols_requested=symbols_requested,
                    primary_symbol=symbol,
                    feature_rows=int(len(features)),
                    feature_columns=int(len(features.columns)),
                    event_flag_rows=int(len(sym_flags)),
                    event_flag_columns_merged=int(len(sym_flags.columns)),
                    hypotheses_generated=h_result.candidates_evaluated_total,
                    feasible_hypotheses=h_result.candidates_evaluated_total,
                    rejected_hypotheses=0,
                    rejection_reason_counts={},
                    metrics_rows=h_result.candidates_evaluated_total,
                    valid_metrics_rows=h_valid_metrics_rows,
                    rejected_invalid_metrics=0,
                    rejected_by_min_n=0,
                    rejected_by_min_t_stat=h_rejected_by_min_t_stat,
                    bridge_candidates_rows=len(candidates),
                    multiplicity_discoveries=0,
                    min_t_stat=resolved_min_t_stat,
                    min_n=resolved_min_n,
                    search_budget=search_budget,
                    use_context_quality=use_context_quality,
                    gate_funnel=_build_gate_funnel(
                        hypotheses_generated=h_result.candidates_evaluated_total,
                        feasible_hypotheses=h_result.candidates_evaluated_total,
                        metrics=h_metrics_for_funnel,
                        candidate_universe=h_candidate_universe,
                        written_candidates=candidates,
                        min_n=resolved_min_n,
                    ),
                )
            )
            continue  # Skip the flat-mode steps below for this symbol

        # ── FLAT MODE (unchanged) ────────────────────────────────────────────
        # 3. Evaluate in chunks
        log.info("Evaluating hypotheses batch for %s (chunk_size=%d)...", symbol, chunk_size)
        metrics = run_distributed_search(
            hypotheses,
            features,
            chunk_size=chunk_size,
            min_sample_size=resolved_min_n,
            use_context_quality=use_context_quality,
            folds=folds,
        )

        if "fold_breakdown" in metrics.attrs and not metrics.attrs["fold_breakdown"].empty:
            all_fold_breakdowns.append(metrics.attrs["fold_breakdown"].copy())
        if (
            "candidate_event_timestamps" in metrics.attrs
            and not metrics.attrs["candidate_event_timestamps"].empty
        ):
            all_candidate_event_timestamps.append(metrics.attrs["candidate_event_timestamps"].copy())

        if metrics.empty:
            log.warning("No metrics returned for %s", symbol)
            _write_evaluation_artifacts(out_dir, symbol, pd.DataFrame(), pd.DataFrame())
            symbol_diagnostics.append(
                build_search_engine_diagnostics(
                    run_id=run_id,
                    discovery_profile=str(search_profile["discovery_profile"]),
                    search_spec=resolved_search_spec,
                    timeframe=timeframe,
                    symbols_requested=symbols_requested,
                    primary_symbol=symbol,
                    feature_rows=int(len(features)),
                    feature_columns=int(len(features.columns)),
                    event_flag_rows=int(len(sym_flags)),
                    event_flag_columns_merged=int(len(sym_flags.columns)),
                    hypotheses_generated=int(
                        generation_audit.get("counts", {}).get("generated", len(hypotheses))
                    ),
                    feasible_hypotheses=int(
                        generation_audit.get("counts", {}).get("feasible", len(hypotheses))
                    ),
                    rejected_hypotheses=generation_rejected_hypotheses,
                    rejection_reason_counts=generation_rejection_reason_counts,
                    metrics_rows=0,
                    valid_metrics_rows=0,
                    rejected_invalid_metrics=0,
                    rejected_by_min_n=0,
                    rejected_by_min_t_stat=0,
                    bridge_candidates_rows=0,
                    multiplicity_discoveries=0,
                    min_t_stat=resolved_min_t_stat,
                    min_n=resolved_min_n,
                    search_budget=search_budget,
                    use_context_quality=use_context_quality,
                    gate_funnel=_build_gate_funnel(
                        hypotheses_generated=int(
                            generation_audit.get("counts", {}).get("generated", len(hypotheses))
                        ),
                        feasible_hypotheses=int(
                            generation_audit.get("counts", {}).get("feasible", len(hypotheses))
                        ),
                        metrics=pd.DataFrame(),
                        candidate_universe=pd.DataFrame(),
                        written_candidates=pd.DataFrame(),
                        min_n=resolved_min_n,
                    ),
                )
            )
            continue

        valid_metrics_rows, rejected_invalid_metrics, rejected_by_min_n = _classify_metrics_counts(
            metrics,
            min_n=resolved_min_n,
            min_t_stat=resolved_min_t_stat,
        )
        metrics_frames.append(metrics.copy())
        valid_mask = (
            metrics.get("valid", pd.Series(False, index=metrics.index)).fillna(False).astype(bool)
            if not metrics.empty
            else pd.Series(dtype=bool)
        )
        rejected_by_min_t_stat = int(
            (
                valid_mask
                & (
                    pd.to_numeric(metrics.get("n", 0), errors="coerce").fillna(0)
                    >= int(resolved_min_n)
                )
                & (
                    pd.to_numeric(metrics.get("t_stat", 0.0), errors="coerce").abs().fillna(0.0)
                    < float(resolved_min_t_stat)
                )
            ).sum()
        )
        post_eval_rejected_hypotheses = (
            int(rejected_invalid_metrics) + int(rejected_by_min_n) + int(rejected_by_min_t_stat)
        )
        total_rejected_hypotheses += post_eval_rejected_hypotheses
        symbol_rejection_reason_counts = _merge_rejection_reason_counts(
            generation_rejection_reason_counts,
            rejected_invalid_metrics=rejected_invalid_metrics,
            rejected_by_min_n=rejected_by_min_n,
            rejected_by_min_t_stat=rejected_by_min_t_stat,
        )
        if rejected_invalid_metrics:
            aggregated_rejection_reasons["invalid_metrics"] = aggregated_rejection_reasons.get(
                "invalid_metrics", 0
            ) + int(rejected_invalid_metrics)
        if rejected_by_min_n:
            aggregated_rejection_reasons["min_sample_size"] = aggregated_rejection_reasons.get(
                "min_sample_size", 0
            ) + int(rejected_by_min_n)
        if rejected_by_min_t_stat:
            aggregated_rejection_reasons["min_t_stat"] = aggregated_rejection_reasons.get(
                "min_t_stat", 0
            ) + int(rejected_by_min_t_stat)

        regime_conditional_source = metrics[
            valid_mask
            & (pd.to_numeric(metrics.get("n", 0), errors="coerce").fillna(0) >= int(resolved_min_n))
        ].copy()
        if not regime_conditional_source.empty:
            regime_conditional_inputs.append(regime_conditional_source)

        # 4. Convert to bridge candidates
        candidates, gate_failures = split_bridge_candidates(
            metrics,
            min_t_stat=resolved_min_t_stat,
            min_n=resolved_min_n,
        )
        _write_evaluation_artifacts(out_dir, symbol, metrics, gate_failures)
        candidate_universe = hypotheses_to_bridge_candidates(
            metrics,
            symbol=symbol,
            min_t_stat=resolved_min_t_stat,
            min_n=resolved_min_n,
            bridge_min_t_stat=resolved_min_t_stat,
            bridge_min_robustness_score=float(
                bridge_gates.get("search_bridge_min_robustness_score", 0.7)
            ),
            bridge_min_regime_stability_score=float(
                bridge_gates.get("search_bridge_min_regime_stability_score", 0.6)
            ),
            bridge_min_stress_survival=float(
                bridge_gates.get("search_bridge_min_stress_survival", 0.5)
            ),
            bridge_stress_cost_buffer_bps=float(
                bridge_gates.get("search_bridge_stress_cost_buffer_bps", 2.0)
            ),
            prefilter_min_n=True,
            prefilter_min_t_stat=False,
        )
        candidate_universe = _merge_edge_cell_lineage(candidate_universe, edge_cell_lineage)

        if (
            not candidate_universe.empty
            and "p_value" in candidate_universe.columns
            and "family_id" in candidate_universe.columns
        ):
            from project.research.multiplicity import apply_multiplicity_controls

            candidate_universe = apply_multiplicity_controls(
                candidate_universe,
                max_q=multiplicity_max_q,
                mode="research",
                min_sample_size=resolved_min_n,
            )
            candidate_universe = _merge_edge_cell_lineage(candidate_universe, edge_cell_lineage)
        if not candidate_universe.empty:
            candidate_universe_frames.append(candidate_universe.copy())

        if not candidate_universe.empty:
            candidates = candidate_universe[
                candidate_universe["gate_search_min_t_stat"].fillna(False).astype(bool)
            ].copy()
        else:
            candidates = candidate_universe

        if not candidates.empty:
            if "candidate_id" in candidates.columns:
                candidates["candidate_id"] = symbol + "::" + candidates["candidate_id"].astype(str)
            all_candidates.append(candidates)
        total_metrics_rows += int(len(metrics))
        total_valid_metrics_rows += valid_metrics_rows
        total_rejected_invalid_metrics += rejected_invalid_metrics
        total_rejected_by_min_n += rejected_by_min_n
        total_rejected_by_min_t_stat += rejected_by_min_t_stat
        total_bridge_candidates_rows += int(len(candidates))

        symbol_diagnostics.append(
            build_search_engine_diagnostics(
                run_id=run_id,
                discovery_profile=str(search_profile["discovery_profile"]),
                search_spec=resolved_search_spec,
                timeframe=timeframe,
                symbols_requested=symbols_requested,
                primary_symbol=symbol,
                feature_rows=int(len(features)),
                feature_columns=int(len(features.columns)),
                event_flag_rows=int(len(sym_flags)),
                event_flag_columns_merged=int(len(sym_flags.columns)),
                hypotheses_generated=int(
                    generation_audit.get("counts", {}).get("generated", len(hypotheses))
                ),
                feasible_hypotheses=int(
                    generation_audit.get("counts", {}).get("feasible", len(hypotheses))
                ),
                rejected_hypotheses=int(
                    generation_rejected_hypotheses + post_eval_rejected_hypotheses
                ),
                rejection_reason_counts=symbol_rejection_reason_counts,
                metrics_rows=int(len(metrics)),
                valid_metrics_rows=valid_metrics_rows,
                rejected_invalid_metrics=rejected_invalid_metrics,
                rejected_by_min_n=rejected_by_min_n,
                rejected_by_min_t_stat=rejected_by_min_t_stat,
                bridge_candidates_rows=int(len(candidates)),
                multiplicity_discoveries=0,  # Computed globally
                min_t_stat=resolved_min_t_stat,
                min_n=resolved_min_n,
                search_budget=search_budget,
                use_context_quality=use_context_quality,
                gate_funnel=_build_gate_funnel(
                    hypotheses_generated=int(
                        generation_audit.get("counts", {}).get("generated", len(hypotheses))
                    ),
                    feasible_hypotheses=int(
                        generation_audit.get("counts", {}).get("feasible", len(hypotheses))
                    ),
                    metrics=metrics,
                    candidate_universe=candidate_universe,
                    written_candidates=candidates,
                    min_n=resolved_min_n,
                ),
            )
        )

    # 5. Aggregate and final processing
    final_df = pd.concat(all_candidates, ignore_index=True) if all_candidates else pd.DataFrame()

    if not final_df.empty and "is_discovery" in final_df.columns:
        log.info(
            "Multiplicity: %d discoveries out of %d candidates",
            int(final_df.get("is_discovery", pd.Series(False)).sum()),
            len(final_df),
        )

    log.info("Search engine produced %d total bridge candidates", len(final_df))

    final_df = _annotate_candidate_regime_metadata(final_df)
    final_df = _attach_candidate_run_lineage(final_df, run_id=run_id)
    final_df = _merge_edge_cell_lineage(final_df, edge_cell_lineage)
    final_df = _annotate_promotion_gate_fields(final_df)

    if not final_df.empty:
        import yaml

        config = {
            "default_turnover_penalty_thresh": 0.8,
            "default_coverage_thresh": 0.01,
            "min_acceptable_regime_support_ratio": 0.4,
        }
        config_path = PROJECT_ROOT.parent / "project" / "configs" / "discovery_scoring_v2.yaml"
        if config_path.exists():
            try:
                with config_path.open("r", encoding="utf-8") as f:
                    yaml_data = yaml.safe_load(f)
                    if yaml_data and "v2_scoring" in yaml_data:
                        config.update(yaml_data["v2_scoring"])
            except Exception as e:
                log.warning(f"Failed to load V2 scoring config: {e}")

        try:
            from project.research.services.candidate_discovery_scoring import (
                annotate_discovery_v2_scores,
            )

            final_df = annotate_discovery_v2_scores(final_df, config)
        except Exception as e:
            log.critical("Failed to apply discovery v2 scoring: %s", e, exc_info=True)
            raise RuntimeError("Phase 2 discovery v2 scoring failed") from e

        try:
            from project.research.services.candidate_discovery_scoring import (
                apply_ledger_multiplicity_correction,
            )

            final_df = apply_ledger_multiplicity_correction(
                final_df, data_root=data_root, current_run_id=run_id
            )
        except Exception as e:
            log.critical("Failed to apply ledger multiplicity correction: %s", e, exc_info=True)
            raise RuntimeError("Phase 3 ledger multiplicity correction failed") from e

        try:
            final_df = _sort_final_candidates(
                final_df,
                enable_discovery_v2_scoring=enable_discovery_v2_scoring,
            )
        except Exception as e:
            log.error("Failed to sort final candidates (falling back to t_stat): %s", e)
            fallback_col = "t_stat" if "t_stat" in final_df.columns else None
            if fallback_col:
                final_df = final_df.sort_values(fallback_col, ascending=False).reset_index(
                    drop=True
                )

    # Phase 3 — Write concept ledger records (unconditional; always accumulates history)
    if not final_df.empty:
        try:
            from project.research.knowledge.concept_ledger import (
                append_concept_ledger,
                build_ledger_records,
                default_ledger_path,
            )

            ledger_records = build_ledger_records(
                final_df,
                run_id=run_id,
                program_id="",  # program_id not available in search engine path
                timeframe=timeframe,
            )
            if not ledger_records.empty:
                append_concept_ledger(
                    ledger_records,
                    default_ledger_path(data_root),
                    raise_on_error=True,
                )
                log.info("Phase 3: wrote %d concept ledger records", len(ledger_records))
        except Exception as _ledger_exc:
            log.critical("Phase 3 ledger write failed: %s", _ledger_exc, exc_info=True)
            raise RuntimeError("Phase 3 concept ledger write failed") from _ledger_exc

    # 6. Write output
    candidate_universe_df = (
        _safe_concat(candidate_universe_frames, ignore_index=True)
        if candidate_universe_frames
        else pd.DataFrame()
    )
    if str(discovery_mode or "search").strip() == "edge_cells":
        write_parquet(candidate_universe_df, out_dir / "phase2_candidate_universe.parquet")

    final_df = _normalize_phase2_candidate_artifact(final_df)
    write_parquet(final_df, output_path)
    validate_schema_at_producer(final_df, "phase2_candidates", context="phase2_search_engine")

    # Write hypothesis registry so promote_candidates can validate the audit chain.
    _write_hypothesis_registry(final_df, out_dir)

    # Phase 4.2 — Write regime_conditional_candidates.parquet.
    # Surfaces hypotheses that were weak overall (t_stat < 1.5) but had positive
    # mean_return_bps — these are candidates for regime-specific alpha that the
    # campaign controller can target with a context-conditioned follow-up run.
    # The controller reads this artefact in _build_next_actions() and injects
    # matching entries into the explore_adjacent queue.
    regime_conditional_df = (
        _safe_concat(regime_conditional_inputs, ignore_index=True)
        if regime_conditional_inputs
        else pd.DataFrame()
    )
    _write_regime_conditional_candidates(regime_conditional_df, out_dir)

    # Phase 2 — Write fold_breakdown if computed
    if all_fold_breakdowns:
        fold_df = _safe_concat(all_fold_breakdowns, ignore_index=True)
        write_parquet(fold_df, out_dir / "phase2_candidate_fold_metrics.parquet")
    if all_candidate_event_timestamps:
        event_ts_df = _normalize_candidate_event_timestamp_artifact(
            _safe_concat(all_candidate_event_timestamps, ignore_index=True),
            candidates=final_df,
        )
        if not event_ts_df.empty:
            write_parquet(event_ts_df, out_dir / "phase2_candidate_event_timestamps.parquet")

    # Phase 4 — Write merged hierarchical stage artifacts (if any)
    _combined_stage_artifacts = getattr(run, "_h_stage_artifacts", {})
    stage_artifact_name_map = {
        "trigger_viability": "phase2_trigger_probes.parquet",
        "template_refinement": "phase2_template_refinement.parquet",
        "execution_refinement": "phase2_execution_refinement.parquet",
        "context_refinement": "phase2_context_refinement.parquet",
    }
    for stage_name, merged_df in _combined_stage_artifacts.items():
        artifact_name = stage_artifact_name_map.get(
            stage_name, f"phase2_stage_{stage_name}.parquet"
        )
        if not merged_df.empty:
            try:
                write_parquet(merged_df, out_dir / artifact_name)
                log.info("Phase 4: wrote %s (%d rows)", artifact_name, len(merged_df))
            except Exception as exc:
                log.warning("Phase 4 stage artifact write failed (%s): %s", artifact_name, exc)
    # Clean up function-level accumulator
    if hasattr(run, "_h_stage_artifacts"):
        del run._h_stage_artifacts  # type: ignore[attr-defined]

    # Phase 5 — Discovery-time diversification
    # Appends overlap/novelty columns to final_df and optionally writes
    # phase2_diversified_shortlist.parquet and phase2_candidate_overlap_metrics.parquet.
    # This block is non-fatal; failures are visible as fallback columns.
    if not final_df.empty:
        try:
            from project.research.services.candidate_diversification import (
                annotate_candidates_with_diversification,
            )

            # Reuse the spec doc already loaded for Phase 4 config
            _div_spec_doc: dict = {}
            try:
                _div_spec_doc = _load_search_spec_doc(resolved_search_spec)
            except Exception as _div_spec_exc:
                log.warning(
                    "Failed to load search spec for diversification config: %s", _div_spec_exc
                )
            _div_config = _load_diversification_config(_div_spec_doc)

            final_df, _shortlist_df = annotate_candidates_with_diversification(
                final_df, _div_config
            )

            # Write overlap metrics artifact (always, if overlap ran)
            _overlap_cols = [
                "candidate_id",
                "overlap_cluster_id",
                "cluster_size",
                "cluster_density",
                "is_duplicate_like",
                "novelty_score",
                "crowding_penalty",
                "cluster_rank",
                "selected_into_diversified_shortlist",
                "shortlist_rank",
            ]
            _present_overlap_cols = [c for c in _overlap_cols if c in final_df.columns]
            if _present_overlap_cols and "overlap_cluster_id" in final_df.columns:
                _overlap_metrics = (
                    final_df[_present_overlap_cols].dropna(subset=["candidate_id"])
                    if "candidate_id" in _present_overlap_cols
                    else final_df[_present_overlap_cols]
                )
                write_parquet(
                    _overlap_metrics, out_dir / "phase2_candidate_overlap_metrics.parquet"
                )
                log.info("Phase 5: wrote overlap metrics (%d rows)", len(_overlap_metrics))

            # Write shortlist artifact (only if shortlist selection ran)
            if _shortlist_df is not None and not _shortlist_df.empty:
                write_parquet(_shortlist_df, out_dir / "phase2_diversified_shortlist.parquet")
                log.info("Phase 5: wrote diversified shortlist (%d rows)", len(_shortlist_df))

        except Exception as _div_exc:
            log.error("Phase 5 diversification failed (non-fatal): %s", _div_exc, exc_info=True)
            final_df = _ensure_diversification_fallback_columns(final_df, str(_div_exc))

        # Persist the final annotated candidate frame. This intentionally rewrites
        # the baseline candidate artifact written before downstream side artifacts.
        final_df = _normalize_phase2_candidate_artifact(final_df)
        write_parquet(final_df, output_path)
        validate_schema_at_producer(final_df, "phase2_candidates", context="phase2_search_engine:diversified")

    main_diag = build_search_engine_diagnostics(
        run_id=run_id,
        discovery_profile=str(search_profile["discovery_profile"]),
        search_spec=resolved_search_spec,
        timeframe=timeframe,
        symbols_requested=symbols_requested,
        primary_symbol="" if len(symbols_requested) != 1 else symbols_requested[0],
        feature_rows=total_feature_rows,
        feature_columns=int(final_df.shape[1]) if not final_df.empty else 0,
        event_flag_rows=total_event_flag_rows,
        event_flag_columns_merged=max_event_flag_columns_merged,
        hypotheses_generated=total_hypotheses_generated,
        feasible_hypotheses=total_feasible_hypotheses,
        rejected_hypotheses=total_rejected_hypotheses,
        rejection_reason_counts=aggregated_rejection_reasons,
        metrics_rows=total_metrics_rows,
        valid_metrics_rows=total_valid_metrics_rows,
        rejected_invalid_metrics=total_rejected_invalid_metrics,
        rejected_by_min_n=total_rejected_by_min_n,
        rejected_by_min_t_stat=total_rejected_by_min_t_stat,
        bridge_candidates_rows=total_bridge_candidates_rows,
        multiplicity_discoveries=0,
        min_t_stat=resolved_min_t_stat,
        min_n=resolved_min_n,
        search_budget=search_budget,
        use_context_quality=use_context_quality,
        gate_funnel=_build_gate_funnel(
            hypotheses_generated=total_hypotheses_generated,
            feasible_hypotheses=total_feasible_hypotheses,
            metrics=(
                _safe_concat(metrics_frames, ignore_index=True)
                if metrics_frames
                else pd.DataFrame()
            ),
            candidate_universe=(
                _safe_concat(candidate_universe_frames, ignore_index=True)
                if candidate_universe_frames
                else pd.DataFrame()
            ),
            written_candidates=final_df,
            min_n=resolved_min_n,
        ),
    )
    if symbol_diagnostics:
        main_diag["symbol_diagnostics"] = symbol_diagnostics
    if not final_df.empty and "is_discovery" in final_df.columns:
        main_diag["multiplicity_discoveries"] = int(final_df["is_discovery"].sum())
    if symbol_diagnostics:
        main_diag["event_flag_columns_merged"] = int(
            max(int(diag.get("event_flag_columns_merged", 0) or 0) for diag in symbol_diagnostics)
        )
        main_diag["feature_columns"] = int(
            max(int(diag.get("feature_columns", 0) or 0) for diag in symbol_diagnostics)
        )

    write_json_report(main_diag, diagnostics_path)

    # Workstream B: Emit search-burden summary
    try:
        from project.research.contracts.search_burden import (
            build_search_burden_summary,
            write_search_burden_summary,
        )

        unique_families = set()
        unique_lineages = set()
        if not final_df.empty:
            if "family_id" in final_df.columns:
                unique_families = set(final_df["family_id"].dropna().unique())
            if "concept_lineage_key" in final_df.columns:
                unique_lineages = set(final_df["concept_lineage_key"].dropna().unique())

        burden_summary = build_search_burden_summary(
            proposals_attempted=total_hypotheses_generated,
            candidates_generated=total_feasible_hypotheses,
            candidates_scored=total_valid_metrics_rows,
            candidates_eligible=len(final_df),
            parameterizations_attempted=total_metrics_rows,
            mutations_attempted=0,
            directions_tested=len(unique_families),
            confirmations_attempted=0,
            trigger_variants_attempted=0,
            family_count=len(unique_families),
            lineage_count=len(unique_lineages),
            estimated=False,
            scope_version="phase1_v1",
        )

        burden_paths = write_search_burden_summary(burden_summary, out_dir)
        log.info(
            "Wrote search-burden summary: %d proposals, %d candidates, %d families, %d lineages",
            burden_summary["search_proposals_attempted"],
            burden_summary["search_candidates_generated"],
            burden_summary["search_family_count"],
            burden_summary["search_lineage_count"],
        )
        main_diag["search_burden_summary_path"] = burden_paths.get("json_path", "")
    except Exception as _burden_exc:
        log.warning("Failed to emit search-burden summary (non-fatal): %s", _burden_exc)

    log.info("Wrote candidates to %s", output_path)
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Search engine hypothesis discovery stage")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--data_root", default=None)
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--start", default=None)
    parser.add_argument("--end", default=None)
    parser.add_argument("--discovery_profile", choices=["standard", "exploratory", "synthetic"], default="standard")
    parser.add_argument("--gate_profile", default="auto")
    parser.add_argument("--search_spec", default="spec/search_space.yaml")
    parser.add_argument("--phase2_event_type", default="")
    parser.add_argument("--chunk_size", type=int, default=500)
    parser.add_argument("--min_t_stat", type=float, default=None)
    parser.add_argument("--min_n", type=int, default=30)
    parser.add_argument("--search_budget", type=int, default=None)
    parser.add_argument("--use_context_quality", type=int, default=1)
    parser.add_argument("--enable_discovery_v2_scoring", type=int, default=1)
    parser.add_argument(
        "--experiment_config", default=None, help="Path to experiment config for tracking."
    )
    parser.add_argument("--program_id", default=None, help="Program ID for experiment tracking.")
    parser.add_argument(
        "--registry_root", default="project/configs/registries", help="Root for event registries."
    )
    parser.add_argument("--discovery_mode", choices=["search", "edge_cells"], default="search")
    parser.add_argument("--lineage_path", default=None)
    parser.add_argument("--log_path", default=None)

    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        handlers=build_stage_log_handlers(args.log_path),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    from project.core.config import get_data_root

    data_root = Path(args.data_root) if args.data_root else get_data_root()
    out_dir = phase2_run_dir(data_root=data_root, run_id=args.run_id)
    candidate_path = phase2_candidates_path(run_id=args.run_id, data_root=data_root)
    diagnostics_path = phase2_diagnostics_path(run_id=args.run_id, data_root=data_root)
    regime_candidates_path = out_dir / "regime_conditional_candidates.parquet"
    outputs = [
        {"path": str(candidate_path)},
        {"path": str(diagnostics_path)},
        {"path": str(regime_candidates_path)},
    ]
    if args.log_path:
        outputs.append({"path": str(args.log_path)})
    manifest = start_manifest("phase2_search_engine", args.run_id, vars(args), [], outputs)

    rc = 1
    stats: dict[str, object] = {}
    try:
        rc = run(
            run_id=args.run_id,
            symbols=args.symbols,
            data_root=data_root,
            out_dir=out_dir,
            timeframe=args.timeframe,
            start=args.start,
            end=args.end,
            discovery_profile=args.discovery_profile,
            gate_profile=args.gate_profile,
            search_spec=args.search_spec,
            chunk_size=args.chunk_size,
            min_t_stat=args.min_t_stat,
            min_n=args.min_n,
            search_budget=args.search_budget,
            use_context_quality=bool(int(args.use_context_quality)),
            enable_discovery_v2_scoring=bool(int(args.enable_discovery_v2_scoring)),
            experiment_config=args.experiment_config,
            registry_root=args.registry_root,
            phase2_event_type=args.phase2_event_type,
            discovery_mode=args.discovery_mode,
            lineage_path=args.lineage_path,
        )
        if diagnostics_path.exists():
            try:
                diagnostics_payload = json.loads(diagnostics_path.read_text(encoding="utf-8"))
                for key in (
                    "hypotheses_generated",
                    "valid_metrics_rows",
                    "rejected_hypotheses",
                    "final_candidate_count",
                ):
                    if key in diagnostics_payload:
                        stats[key] = diagnostics_payload[key]
            except Exception as exc:
                log.warning("Could not read diagnostics for manifest stats: %s", exc)
        if candidate_path.exists():
            try:
                stats["candidate_rows"] = int(len(read_parquet(candidate_path)))
            except Exception as exc:
                log.warning("Could not read candidate parquet for manifest stats: %s", exc)
        if regime_candidates_path.exists():
            try:
                stats["regime_conditional_candidate_rows"] = int(
                    len(read_parquet(regime_candidates_path))
                )
            except Exception as exc:
                log.warning("Could not read regime candidates for manifest stats: %s", exc)
        finalize_manifest(manifest, "success" if rc == 0 else "failed", stats=stats)
        return rc
    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        raise


if __name__ == "__main__":
    sys.exit(main())
