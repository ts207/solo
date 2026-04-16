from __future__ import annotations

from collections.abc import Callable, Iterable

import numpy as np
import pandas as pd


def _optional_metric(df: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(
        df.get(column, pd.Series(np.nan, index=df.index)),
        errors="coerce",
    ).astype(float)


def _paired_context_metric_name(column: str, suffix: str) -> str | None:
    if not column.endswith("_state"):
        return None
    return f"{column[:-6]}_{suffix}"


def optional_state(
    df: pd.DataFrame,
    column: str,
    *,
    min_confidence: float | None = None,
    max_entropy: float | None = None,
) -> pd.Series:
    state = _optional_metric(df, column)
    confidence_col = _paired_context_metric_name(column, "confidence")
    entropy_col = _paired_context_metric_name(column, "entropy")

    valid = pd.Series(True, index=state.index, dtype=bool)
    if min_confidence is not None and confidence_col is not None:
        confidence = _optional_metric(df, confidence_col)
        if confidence.isna().all():
            valid &= False
        else:
            valid &= (confidence >= float(min_confidence)).fillna(False)
    if max_entropy is not None and entropy_col is not None:
        entropy = _optional_metric(df, entropy_col)
        if entropy.isna().all():
            valid &= False
        else:
            valid &= (entropy <= float(max_entropy)).fillna(False)

    return state.where(valid, np.nan)


def state_guard(
    state: pd.Series,
    *,
    predicate: Callable[[pd.Series], pd.Series],
    lag: int = 1,
    default_if_absent: bool = False,
) -> pd.Series:
    guard_source = state.shift(lag) if lag else state
    if guard_source.isna().all():
        return pd.Series(default_if_absent, index=state.index, dtype=bool)
    return predicate(guard_source).fillna(False).astype(bool)


def state_at_least(
    df: pd.DataFrame,
    column: str,
    minimum: float,
    *,
    lag: int = 1,
    default_if_absent: bool = False,
    min_confidence: float | None = None,
    max_entropy: float | None = None,
) -> pd.Series:
    raw_state = _optional_metric(df, column)
    filtered_state = optional_state(
        df,
        column,
        min_confidence=min_confidence,
        max_entropy=max_entropy,
    )
    if filtered_state.isna().all() and not raw_state.isna().all():
        return pd.Series(False, index=raw_state.index, dtype=bool)
    return state_guard(
        filtered_state,
        predicate=lambda series: series >= minimum,
        lag=lag,
        default_if_absent=default_if_absent,
    )


def state_at_most(
    df: pd.DataFrame,
    column: str,
    maximum: float,
    *,
    lag: int = 1,
    default_if_absent: bool = False,
    min_confidence: float | None = None,
    max_entropy: float | None = None,
) -> pd.Series:
    raw_state = _optional_metric(df, column)
    filtered_state = optional_state(
        df,
        column,
        min_confidence=min_confidence,
        max_entropy=max_entropy,
    )
    if filtered_state.isna().all() and not raw_state.isna().all():
        return pd.Series(False, index=raw_state.index, dtype=bool)
    return state_guard(
        filtered_state,
        predicate=lambda series: series <= maximum,
        lag=lag,
        default_if_absent=default_if_absent,
    )


def state_in(
    df: pd.DataFrame,
    column: str,
    values: Iterable[float],
    *,
    lag: int = 1,
    default_if_absent: bool = False,
    min_confidence: float | None = None,
    max_entropy: float | None = None,
) -> pd.Series:
    allowed = tuple(values)
    raw_state = _optional_metric(df, column)
    filtered_state = optional_state(
        df,
        column,
        min_confidence=min_confidence,
        max_entropy=max_entropy,
    )
    if filtered_state.isna().all() and not raw_state.isna().all():
        return pd.Series(False, index=raw_state.index, dtype=bool)
    return state_guard(
        filtered_state,
        predicate=lambda series: series.isin(allowed),
        lag=lag,
        default_if_absent=default_if_absent,
    )
