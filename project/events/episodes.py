from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from project.events.sparsify import cluster_mask


@dataclass(frozen=True)
class Episode:
    start_idx: int
    end_idx: int
    peak_idx: int
    duration_bars: int


def build_episodes(
    mask: pd.Series,
    *,
    score: pd.Series | None = None,
    max_gap: int = 0,
) -> list[Episode]:
    clusters = cluster_mask(mask, max_gap=max_gap)
    if not clusters:
        return []
    numeric_score = pd.to_numeric(score, errors="coerce") if score is not None else None
    episodes: list[Episode] = []
    for start, end in clusters:
        if numeric_score is not None and numeric_score.iloc[start : end + 1].notna().any():
            peak_idx = int(numeric_score.iloc[start : end + 1].idxmax())
        else:
            peak_idx = int(start)
        episodes.append(
            Episode(
                start_idx=int(start),
                end_idx=int(end),
                peak_idx=peak_idx,
                duration_bars=int(end - start + 1),
            )
        )
    return episodes


def episodes_to_frame(episodes: list[Episode]) -> pd.DataFrame:
    return pd.DataFrame(
        [ep.__dict__ for ep in episodes],
        columns=["start_idx", "end_idx", "peak_idx", "duration_bars"],
    )
