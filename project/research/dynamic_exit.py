import numpy as np
import pandas as pd


def compute_dynamic_exit_heuristics(
    returns_matrix_train: pd.DataFrame, time_horizons: list[int]
) -> dict:
    """
    Analyzes the survival/decay curve of an event's forward returns
    on a TRAIN-ONLY dataset to recommend a bounded set of optimal dynamic exits.
    """
    heuristics = {}

    if returns_matrix_train.empty:
        return {"recommended_horizons": []}

    # Simple drift decay analysis
    means = returns_matrix_train.mean()
    stds = returns_matrix_train.std()
    counts = returns_matrix_train.count()

    # Require minimum samples for statistical confidence
    valid_horizons = counts[counts >= 30].index.tolist()
    if not valid_horizons:
        return {"recommended_horizons": []}

    means = means.loc[valid_horizons]
    stds = stds.loc[valid_horizons]

    # Use t-stat instead of raw mean to account for variance
    t_stats = means / (stds / np.sqrt(counts.loc[valid_horizons]))

    # 1) Peak t-stat horizon
    best_horizon_idx = t_stats.argmax()
    best_horizon = valid_horizons[best_horizon_idx]

    # 2) Stability horizon (first horizon to reach 90% of max cumulative expectancy)
    max_mean = means.max()
    threshold = 0.90 * max_mean
    stability_horizon = valid_horizons[-1]  # Default to max
    for h in valid_horizons:
        if means.loc[h] >= threshold:
            stability_horizon = h
            break

    # 3) Max safe horizon (where downside risk crosses threshold)
    # E.g., when the 25th percentile crosses 0 (meaning > 25% of trades are losers)
    q25 = returns_matrix_train[valid_horizons].quantile(0.25)
    negative_crossings = q25[q25 < 0]
    if not negative_crossings.empty:
        max_safe = int(negative_crossings.index[0])
    else:
        max_safe = int(valid_horizons[-1])

    # Output a small bounded set to control multiplicity
    recos = list(set([int(best_horizon), int(stability_horizon), int(max_safe)]))

    heuristics["recommended_horizons"] = sorted(recos)
    heuristics["peak_t_stat"] = float(t_stats.loc[best_horizon])
    heuristics["peak_expectancy"] = float(means.loc[best_horizon])

    return heuristics
