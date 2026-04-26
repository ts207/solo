from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pandas as pd

from project.core.feature_registry import list_feature_definitions

PIT_LEAKY_PREFIXES: tuple[str, ...] = (
    "forward_",
    "future_",
    "fwd_",
    "next_",
    "lead_",
    "label",
    "target",
    "outcome",
    "return_",
    "pnl",
    "profit",
    "alpha_after",
    "edge_after",
)

PIT_LEAKY_COLUMNS: tuple[str, ...] = (
    "future_return",
    "forward_return",
    "forward_return_raw",
    "forward_log_return",
    "label",
    "labels",
    "target",
    "target_label",
    "outcome",
    "outcome_label",
    "edge_label",
    "genuine_edge",
    "promoted_edge",
    "is_genuine_edge",
    "is_promoted_edge",
)


@dataclass(frozen=True)
class FeatureSelectionReport:
    allowed_columns: tuple[str, ...]
    excluded_columns: tuple[str, ...]
    registry_columns: tuple[str, ...]


def _registry_feature_names() -> set[str]:
    try:
        return {str(defn.name).strip() for defn in list_feature_definitions() if str(defn.name).strip()}
    except Exception:
        return set()


def is_pit_safe_feature_column(column_name: str) -> bool:
    name = str(column_name).strip()
    if not name:
        return False
    lower = name.lower()
    if name in PIT_LEAKY_COLUMNS:
        return False
    return not any(lower.startswith(prefix) for prefix in PIT_LEAKY_PREFIXES)


def select_pit_safe_feature_columns(
    frame: pd.DataFrame,
    *,
    candidate_columns: Sequence[str] | None = None,
    include_registry_only: bool = True,
) -> FeatureSelectionReport:
    if frame.empty:
        return FeatureSelectionReport((), (), ())

    registry_names = _registry_feature_names()
    cols = list(candidate_columns) if candidate_columns is not None else list(frame.columns)

    excluded: list[str] = []
    allowed: list[str] = []

    for column in cols:
        if column not in frame.columns:
            continue
        if not is_pit_safe_feature_column(column):
            excluded.append(column)
            continue
        if include_registry_only and registry_names and column not in registry_names:
            if column not in {"timestamp", "enter_ts", "event_ts", "symbol", "event_type", "template_id", "split_label"}:
                excluded.append(column)
                continue
        allowed.append(column)

    return FeatureSelectionReport(
        allowed_columns=tuple(dict.fromkeys(allowed)),
        excluded_columns=tuple(dict.fromkeys(excluded)),
        registry_columns=tuple(sorted(registry_names)),
    )


def select_model_feature_frame(
    frame: pd.DataFrame,
    *,
    candidate_columns: Sequence[str] | None = None,
    include_registry_only: bool = True,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    selection = select_pit_safe_feature_columns(
        frame,
        candidate_columns=candidate_columns,
        include_registry_only=include_registry_only,
    )
    selected = list(selection.allowed_columns)
    if not selected:
        return pd.DataFrame(index=frame.index)
    return frame.loc[:, selected].copy()


def split_feature_columns(
    frame: pd.DataFrame,
    *,
    candidate_columns: Sequence[str] | None = None,
    include_registry_only: bool = True,
) -> dict[str, list[str]]:
    selected = select_model_feature_frame(
        frame,
        candidate_columns=candidate_columns,
        include_registry_only=include_registry_only,
    )
    numeric: list[str] = []
    categorical: list[str] = []
    for column in selected.columns:
        dtype = selected[column].dtype
        if pd.api.types.is_numeric_dtype(dtype):
            numeric.append(column)
        else:
            categorical.append(column)
    return {"numeric": numeric, "categorical": categorical, "all": list(selected.columns)}
