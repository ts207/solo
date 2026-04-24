from typing import Any, Dict

import numpy as np
import pandas as pd


def train_local_symbolic_filter(
    features_train: pd.DataFrame,
    labels_train: pd.Series,
    features_val: pd.DataFrame,
    labels_val: pd.Series,
    candidate_id: str,
    max_grid_points: int = 25,
) -> Dict[str, Any]:
    """
    Discovers the optimal continuous filter for an event.
    Enforces deterministic optimization and train/val splitting to prevent overfitting.
    """
    if len(features_train) < 100 or len(features_val) < 50:
        return {}

    best_feat = None
    best_corr = 0
    # 1. Deterministic feature selection on TRAIN only
    for col in sorted(features_train.columns):  # Sort for determinism
        if features_train[col].dtype.kind in "biufc":
            corr = features_train[col].corr(labels_train)
            if pd.notna(corr) and abs(corr) > abs(best_corr):
                best_corr = corr
                best_feat = col

    if not best_feat:
        return {}

    op = "<" if best_corr < 0 else ">"

    # 2. Bounded grid search on TRAIN only
    feature_series = features_train[best_feat].dropna()
    if feature_series.empty:
        return {}

    # Create deterministic quantiles to test
    quantiles = np.linspace(0.1, 0.9, max_grid_points)
    candidate_thetas = feature_series.quantile(quantiles).unique()

    best_train_score = -np.inf

    top_k_thetas = []

    for theta in candidate_thetas:
        if op == "<":
            mask = features_train[best_feat] < theta
        else:
            mask = features_train[best_feat] > theta

        if mask.sum() < 30:  # Min samples
            continue

        score = labels_train[mask].mean()
        if score > best_train_score:
            best_train_score = score

        top_k_thetas.append((theta, score))

    # Take top 3 from train
    top_k_thetas = sorted(top_k_thetas, key=lambda x: x[1], reverse=True)[:3]

    # 3. Validate on VAL
    best_val_theta = None
    best_val_score = -np.inf

    for theta, _ in top_k_thetas:
        if op == "<":
            mask_val = features_val[best_feat] < theta
        else:
            mask_val = features_val[best_feat] > theta

        if mask_val.sum() < 10:
            continue

        val_score = labels_val[mask_val].mean()
        # Tie-breaker: use smallest absolute theta for stability if scores match exactly
        if val_score > best_val_score or (
            val_score == best_val_score
            and (best_val_theta is None or abs(theta) < abs(best_val_theta))
        ):
            best_val_score = val_score
            best_val_theta = theta

    if best_val_theta is not None:
        return {
            "type": "FilterNodeSpec",
            "feature": best_feat,
            "operator": op,
            "theta": float(best_val_theta),
            "theta_min": float(candidate_thetas[0]),
            "theta_max": float(candidate_thetas[-1]),
            "optimized": True,
        }

    return {}
