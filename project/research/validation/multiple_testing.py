from __future__ import annotations

from typing import Iterable, Sequence

import numpy as np
import pandas as pd

# The canonical BH implementation lives in project.core.stats.  We route the
# "bh" method through it so there is exactly one implementation of
# Benjamini-Hochberg FDR correction in the codebase.  BY and Holm are not
# present in core/stats, so they continue to use statsmodels.
from project.core.stats import bh_adjust as _canonical_bh_adjust


def assign_test_families(
    df: pd.DataFrame,
    *,
    family_cols: Sequence[str],
    out_col: str = "correction_family_id",
) -> pd.DataFrame:
    out = df.copy()

    def _compose(row: pd.Series) -> str:
        parts = []
        for col in family_cols:
            parts.append(str(row.get(col, "")))
        return "::".join(parts)

    out[out_col] = out.apply(_compose, axis=1)
    return out


def _adjust(p_values: Iterable[float], method: str) -> np.ndarray:
    arr = np.asarray(list(p_values), dtype=float)
    n = len(arr)
    if n == 0:
        return arr
    arr = np.where(np.isfinite(arr), np.clip(arr, 0.0, 1.0), 1.0)

    if method == "bh":
        return _canonical_bh_adjust(arr)

    if method == "by":
        # Benjamini-Yekutieli (BY) adjustment
        # q = p * (m/i) * sum(1/j for j in 1..m)
        cm = np.sum(1.0 / np.arange(1, n + 1))
        idx = np.argsort(arr)
        sorted_p = arr[idx]
        adj = np.zeros(n)
        min_p = 1.0
        for i in range(n - 1, -1, -1):
            q = sorted_p[i] * cm * n / (i + 1)
            min_p = min(min_p, q)
            adj[idx[i]] = min_p
        return np.clip(adj, 0.0, 1.0)

    if method == "holm":
        # Holm-Bonferroni adjustment
        # q = p * (m - i + 1)
        idx = np.argsort(arr)
        sorted_p = arr[idx]
        adj = np.zeros(n)
        max_p = 0.0
        for i in range(n):
            q = sorted_p[i] * (n - i)
            max_p = max(max_p, q)
            adj[idx[i]] = max_p
        return np.clip(adj, 0.0, 1.0)

    raise ValueError(f"Unsupported correction method: {method}")


def adjust_pvalues_bh(p_values: Iterable[float]) -> np.ndarray:
    return _adjust(p_values, "bh")


def adjust_pvalues_by(p_values: Iterable[float]) -> np.ndarray:
    return _adjust(p_values, "by")


def adjust_pvalues_holm(p_values: Iterable[float]) -> np.ndarray:
    return _adjust(p_values, "holm")


def apply_multiple_testing(
    df: pd.DataFrame,
    *,
    p_col: str = "p_value_raw",
    family_col: str = "correction_family_id",
    method: str = "bh",
    out_col: str = "p_value_adj",
) -> pd.DataFrame:
    out = df.copy()
    out[out_col] = np.nan
    if out.empty or p_col not in out.columns:
        return out
    if family_col not in out.columns:
        out[out_col] = _adjust(out[p_col].fillna(1.0), method)
        return out
    for _, group in out.groupby(family_col, dropna=False):
        adj = _adjust(group[p_col].fillna(1.0), method)
        out.loc[group.index, out_col] = adj
    return out
