from __future__ import annotations

import logging
from typing import Dict, List, Any

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


def extract_excursions(
    features: pd.DataFrame,
    target_columns: List[str],
    threshold_z: float = 2.5,
    min_persistence: int = 1
) -> pd.DataFrame:
    """
    Extract anomalous segments from continuous feature columns.
    Returns a DataFrame where each row is an active excursion frame, tracking
    which features spiked beyond threshold_z.
    """
    if features.empty or not target_columns:
        return pd.DataFrame()
        
    excursion_masks = {}
    
    for col in target_columns:
        if col not in features.columns:
            continue
            
        series = pd.to_numeric(features[col], errors="coerce").fillna(0.0)
        # Assuming these are pre-normalized, but compute a rolling/expanding Z just in case
        roll_mean = series.rolling(288, min_periods=20).mean()
        roll_std = series.rolling(288, min_periods=20).std().replace(0, 1e-9)
        z_scores = (series - roll_mean) / roll_std
        
        mask = (z_scores >= threshold_z).fillna(False)
        
        if min_persistence > 1:
            # Shift windows forward to check consecutive bars
            persisted = mask.copy()
            for shift in range(1, min_persistence):
                persisted = persisted & mask.shift(shift, fill_value=False)
            # Re-align back to the onset
            mask = persisted.shift(-min_persistence + 1, fill_value=False)

        excursion_masks[col] = mask

    if not excursion_masks:
        return pd.DataFrame()

    out = pd.DataFrame(excursion_masks)
    out["any_excursion"] = out.any(axis=1)
    return out


def cluster_excursions(
    excursions_df: pd.DataFrame,
    target_columns: List[str],
    min_support: int = 5
) -> List[Dict[str, Any]]:
    """
    Cluster contiguous or frequent multi-feature excursions.
    Assigns an arbitrary signature based on which features crossed the threshold.
    """
    if excursions_df.empty or "any_excursion" not in excursions_df.columns:
        return []

    active_rows = excursions_df[excursions_df["any_excursion"]].copy()
    if active_rows.empty:
        return []

    # Simple group: exact boolean tuple of which features are active
    active_rows["signature"] = active_rows[target_columns].apply(
        lambda row: tuple(sorted([col for col, val in row.items() if val])), axis=1
    )

    counts = active_rows["signature"].value_counts()
    valid_clusters = counts[counts >= min_support]

    clusters = []
    for sig_idx, (sig_tuple, count) in enumerate(valid_clusters.items(), start=1):
        if not sig_tuple:
            continue
        
        cluster_id = f"CLUSTER_{sig_idx:03d}_{'_'.join([f.split('_')[0] for f in sig_tuple])}"
        clusters.append({
            "candidate_cluster_id": cluster_id,
            "dominant_features": list(sig_tuple),
            "support_count": int(count),
            "suggested_trigger_family": cluster_id.upper(),
        })

    return clusters
