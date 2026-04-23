import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
from typing import Dict, List, Tuple


def cluster_hypotheses(
    pnl_df: pd.DataFrame, 
    eps: float = 0.3, 
    min_samples: int = 1,
    metric: str = "correlation",
    trigger_df: pd.DataFrame | None = None,
) -> Dict[int, List[str]]:
    """
    Group redundant hypotheses using DBSCAN.

    pnl_df: hypothesis_ids as columns, signal_ts as index
    eps: max distance (1-sim) to be considered in same cluster
    metric: 'correlation' or 'jaccard'
    trigger_df: optional binary frame for jaccard overlap
    """
    if pnl_df.empty:
        return {}

    # Calculate distance matrix
    # We use (1 - correlation) as the distance metric
    # Transpose so we cluster hypotheses (columns)
    data = pnl_df.T.fillna(0)

    # Actually, using a correlation-based distance is better
    # DBSCAN on the precomputed distance matrix
    from project.research.clustering.pnl_similarity import (
        calculate_similarity_matrix,
        calculate_trigger_overlap,
        compute_distance_matrix,
    )

    if metric == "jaccard" and trigger_df is not None:
        sim = calculate_trigger_overlap(trigger_df)
    else:
        sim = calculate_similarity_matrix(pnl_df)
        
    dist = compute_distance_matrix(sim)

    clustering = DBSCAN(eps=eps, min_samples=min_samples, metric="precomputed")
    labels = clustering.fit_predict(dist)

    clusters = {}
    for i, label in enumerate(labels):
        if label not in clusters:
            clusters[int(label)] = []
        clusters[int(label)].append(pnl_df.columns[i])

    return clusters


def select_cluster_representatives(
    clusters: Dict[int, List[str]], sharpe_ratios: Dict[str, float]
) -> List[str]:
    """
    Select the best hypothesis from each cluster based on Sharpe ratio.
    """
    representatives = []
    for label, member_ids in clusters.items():
        if label == -1:
            # Noise in DBSCAN - keep all or none?
            # Usually noise are unique strategies, so keep them.
            representatives.extend(member_ids)
            continue

        # Select best by Sharpe
        best_id = max(member_ids, key=lambda hid: sharpe_ratios.get(hid, -1.0))
        representatives.append(best_id)

    return representatives
