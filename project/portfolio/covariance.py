from __future__ import annotations

from collections.abc import Mapping, Sequence

import numpy as np
import pandas as pd


def estimate_return_covariance(
    returns_by_thesis: Mapping[str, Sequence[float] | pd.Series],
    *,
    min_observations: int = 2,
) -> pd.DataFrame:
    """Estimate a thesis-return covariance matrix from aligned return samples."""

    if not returns_by_thesis:
        return pd.DataFrame(dtype=float)
    frame = pd.DataFrame(
        {
            str(thesis_id): pd.to_numeric(pd.Series(values), errors="coerce")
            for thesis_id, values in returns_by_thesis.items()
        }
    )
    frame = frame.dropna(how="all").fillna(0.0)
    if len(frame.index) < int(min_observations):
        return pd.DataFrame(0.0, index=frame.columns, columns=frame.columns, dtype=float)
    return frame.cov().fillna(0.0).astype(float)


def covariance_to_correlation(covariance: pd.DataFrame) -> pd.DataFrame:
    if covariance.empty:
        return pd.DataFrame(dtype=float)
    cov = covariance.astype(float).fillna(0.0)
    diagonal = np.sqrt(np.clip(np.diag(cov.to_numpy(dtype=float)), 0.0, None))
    denom = np.outer(diagonal, diagonal)
    with np.errstate(divide="ignore", invalid="ignore"):
        corr_values = np.divide(cov.to_numpy(dtype=float), denom)
    corr_values[~np.isfinite(corr_values)] = 0.0
    corr = pd.DataFrame(corr_values, index=cov.index, columns=cov.columns)
    return corr.clip(lower=-1.0, upper=1.0).astype(float)


def weighted_active_correlation(
    thesis_id: str,
    covariance: pd.DataFrame,
    active_notional_by_thesis: Mapping[str, float],
) -> float:
    """Return exposure-weighted absolute correlation to active thesis exposure."""

    thesis_key = str(thesis_id)
    if covariance.empty or thesis_key not in covariance.index:
        return 0.0
    corr = covariance_to_correlation(covariance)
    weighted_sum = 0.0
    total_weight = 0.0
    for other_id, notional in active_notional_by_thesis.items():
        other_key = str(other_id)
        weight = abs(float(notional))
        if other_key == thesis_key or weight <= 0.0 or other_key not in corr.columns:
            continue
        weighted_sum += abs(float(corr.loc[thesis_key, other_key])) * weight
        total_weight += weight
    if total_weight <= 0.0:
        return 0.0
    return float(np.clip(weighted_sum / total_weight, 0.0, 1.0))


def covariance_exposure_multiplier(
    thesis_id: str,
    covariance: pd.DataFrame,
    active_notional_by_thesis: Mapping[str, float],
    *,
    correlation_limit: float = 0.65,
    min_multiplier: float = 0.20,
) -> float:
    """Scale a new allocation down when it adds highly correlated active exposure."""

    active_corr = weighted_active_correlation(thesis_id, covariance, active_notional_by_thesis)
    limit = float(np.clip(correlation_limit, 0.01, 1.0))
    if active_corr <= limit:
        return 1.0
    multiplier = limit / max(active_corr, 1e-9)
    return float(np.clip(multiplier, min_multiplier, 1.0))
