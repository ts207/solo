"""
Hypothesis clustering for review.

Groups promoted candidates into clusters based on behavior and delay profile similarity.
This helps reviewers analyze representative candidates rather than 20k+ individual ones.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from project.research.promotion.core import behavior_overlap_score, delay_profile_correlation

LOGGER = logging.getLogger(__name__)


def cluster_hypotheses(
    promoted_df: pd.DataFrame,
    behavior_threshold: float = 0.8,
    correlation_threshold: float = 0.9,
) -> pd.DataFrame:
    """
    Assign a 'cluster_id' to each candidate in promoted_df.

    Candidates are in the same cluster if they have high behavior overlap
    OR high delay profile correlation.
    """
    if promoted_df.empty:
        if "cluster_id" not in promoted_df.columns:
            promoted_df["cluster_id"] = pd.Series(dtype=str)
        return promoted_df

    df = promoted_df.copy()
    candidates = df.to_dict(orient="records")
    n = len(candidates)

    # Simple connected components clustering
    # Each candidate starts in its own cluster
    parent = list(range(n))

    def find(i: int) -> int:
        if parent[i] == i:
            return i
        parent[i] = find(parent[i])
        return parent[i]

    def union(i: int, j: int) -> None:
        root_i = find(i)
        root_j = find(j)
        if root_i != root_j:
            parent[root_i] = root_j

    for i in range(n):
        for j in range(i + 1, n):
            # 1. Behavior similarity
            b_score = behavior_overlap_score(candidates[i], candidates[j])
            if np.isfinite(b_score) and b_score >= behavior_threshold:
                union(i, j)
                continue

            # 2. Delay profile correlation
            d_corr = delay_profile_correlation(candidates[i], candidates[j])
            if np.isfinite(d_corr) and d_corr >= correlation_threshold:
                union(i, j)

    # Map roots to cluster IDs
    clusters: Dict[int, str] = {}
    cluster_list = []
    for i in range(n):
        root = find(i)
        if root not in clusters:
            clusters[root] = f"cluster_{len(clusters):03d}"
        cluster_list.append(clusters[root])

    df["cluster_id"] = cluster_list
    return df


def build_cluster_summary(clustered_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a summary report with one row per cluster, picking a representative.
    """
    if clustered_df.empty:
        return pd.DataFrame()

    # Sort within cluster to pick the "best" representative
    sort_cols = [
        c
        for c in ["selection_score", "promotion_score", "n_events", "candidate_id"]
        if c in clustered_df.columns
    ]

    summary_rows = []
    for cid, group in clustered_df.groupby("cluster_id"):
        # Pick the top ranked as representative
        rep = group.sort_values(sort_cols, ascending=[False] * len(sort_cols)).iloc[0]

        summary_rows.append(
            {
                "cluster_id": cid,
                "cluster_size": len(group),
                "representative_id": rep["candidate_id"],
                "event_type": rep.get("event_type", rep.get("event", "unknown")),
                "template_id": rep.get("template_id", rep.get("template_verb", "unknown")),
                "horizon": rep.get("horizon", "unknown"),
                "avg_selection_score": group["selection_score"].mean()
                if "selection_score" in group
                else np.nan,
                "member_ids": ",".join(group["candidate_id"].tolist()[:10])
                + ("..." if len(group) > 10 else ""),
            }
        )

    return pd.DataFrame(summary_rows).sort_values("cluster_id")
