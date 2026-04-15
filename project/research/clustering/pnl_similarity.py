import pandas as pd
import numpy as np
from typing import Dict, List


def calculate_similarity_matrix(pnl_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate similarity between hypotheses based on PnL correlation and overlap.
    pnl_df: columns are hypothesis_ids, index is signal_ts
    """
    # 1. Return correlation
    corr_matrix = pnl_df.corr().fillna(0)

def calculate_trigger_overlap(trigger_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate binary trigger overlap between hypotheses.
    trigger_df: binary frame (1 = signal active, 0 = inactive)
    Result: Jaccard similarity matrix (0.0 to 1.0)
    """
    if trigger_df.empty:
        return pd.DataFrame()
        
    cols = trigger_df.columns
    n = len(cols)
    overlap = pd.DataFrame(np.eye(n), index=cols, columns=cols)
    
    triggers = trigger_df.fillna(0).astype(bool).values
    
    for i in range(n):
        for j in range(i + 1, n):
            intersection = np.logical_and(triggers[:, i], triggers[:, j]).sum()
            union = np.logical_or(triggers[:, i], triggers[:, j]).sum()
            sim = intersection / union if union > 0 else 0.0
            overlap.iloc[i, j] = sim
            overlap.iloc[j, i] = sim
            
    return overlap


def compute_distance_matrix(similarity_matrix: pd.DataFrame) -> pd.DataFrame:
    """Convert similarity to distance: d = 1 - sim"""
    return 1 - similarity_matrix
