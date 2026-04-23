from __future__ import annotations

import numpy as np
import pandas as pd


def sparsify_mask(mask: pd.Series, *, min_spacing: int = 1) -> list[int]:
    idxs = np.flatnonzero(mask.fillna(False).to_numpy())
    selected: list[int] = []
    last = -(10**9)
    for idx in idxs:
        i = int(idx)
        if i - last >= int(min_spacing):
            selected.append(i)
            last = i
    return selected


def cluster_mask(mask: pd.Series, *, max_gap: int = 0) -> list[tuple[int, int]]:
    idxs = np.flatnonzero(mask.fillna(False).to_numpy())
    if len(idxs) == 0:
        return []
    clusters: list[tuple[int, int]] = []
    start = end = int(idxs[0])
    for idx in idxs[1:]:
        i = int(idx)
        if i - end <= int(max_gap) + 1:
            end = i
            continue
        clusters.append((start, end))
        start = end = i
    clusters.append((start, end))
    return clusters


def select_cluster_representatives(
    mask: pd.Series,
    *,
    score: pd.Series | None = None,
    max_gap: int = 0,
    prefer: str = "first",
) -> list[int]:
    clusters = cluster_mask(mask, max_gap=max_gap)
    if not clusters:
        return []
    if score is None:
        return [start for start, _ in clusters]
    reps: list[int] = []
    numeric_score = pd.to_numeric(score, errors="coerce")
    for start, end in clusters:
        window = numeric_score.iloc[start : end + 1]
        if prefer == "max_score" and window.notna().any():
            reps.append(int(window.idxmax()))
        else:
            reps.append(int(start))
    return reps
