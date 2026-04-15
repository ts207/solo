"""
Statistical Gating Logic: Redundancy, Curvature, Delay Robustness, and Stability gates.
Extracted from pipeline scripts to improve testability and separate concerns.
"""

from __future__ import annotations

import json
import re
from typing import Dict, List, Tuple, Optional, Any

import numpy as np
import pandas as pd

try:
    from scipy import stats
except ModuleNotFoundError:
    from project.core.stats import stats

from project.core.coercion import safe_float, safe_int, as_bool
from project.core.stats import subsample_non_overlapping_positions as subsample_non_overlapping_timestamps
from project.research.gating import one_sided_p_from_t

NUMERIC_CONDITION_PATTERN = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(>=|<=|==|>|<)\s*(-?\d+(?:\.\d+)?)\s*$"
)


def gate_redundancy_correlation(
    candidates_df: pd.DataFrame, redundancy_threshold: float = 0.85
) -> pd.DataFrame:
    """Marks candidates as redundant based on return profile similarity.

    Uses cosine similarity on the delay-expectancy profile vectors rather than
    Kendall's tau, which is unreliable on n=5 points (the number of delay
    checkpoints).  Cosine similarity measures the alignment of the return
    profile *shape* regardless of magnitude, which is the correct criterion
    for identifying strategies that would contribute overlapping alpha.

    Two candidates are redundant when their delay profiles point in essentially
    the same direction (cosine similarity >= ``redundancy_threshold``).
    """
    if candidates_df.empty or "expectancy_after_multiplicity" not in candidates_df.columns:
        return candidates_df

    df = candidates_df.copy()
    df = df.sort_values(by="expectancy_after_multiplicity", ascending=False).reset_index(drop=True)
    is_redundant = np.zeros(len(df), dtype=bool)
    accepted_profiles: list[np.ndarray] = []

    def _cosine_sim(a: np.ndarray, b: np.ndarray) -> float:
        """Cosine similarity; returns 0.0 when either vector is zero."""
        na = np.linalg.norm(a)
        nb = np.linalg.norm(b)
        if na < 1e-10 or nb < 1e-10:
            return 0.0
        return float(np.dot(a, b) / (na * nb))

    for i, row in df.iterrows():
        try:
            delay_map = json.loads(str(row.get("delay_expectancy_map", "{}")))
            profile_vec = np.array([float(delay_map.get(str(k), 0.0)) for k in [0, 4, 8, 16, 30]])
        except Exception:
            profile_vec = np.zeros(5)

        redundant = False
        if np.any(profile_vec):
            for acc_vec in accepted_profiles:
                if _cosine_sim(profile_vec, acc_vec) >= redundancy_threshold:
                    redundant = True
                    break
        is_redundant[i] = redundant
        if not redundant:
            accepted_profiles.append(profile_vec)

    df["gate_redundancy"] = ~is_redundant
    return df


def parse_numeric_condition_expr(condition: str) -> Tuple[str, str, float] | None:
    match = NUMERIC_CONDITION_PATTERN.match(str(condition or "").strip())
    if not match:
        return None
    feature, operator, raw_value = match.groups()
    try:
        return feature, operator, float(raw_value)
    except ValueError:
        return None


def condition_mask_for_numeric_expr(
    frame: pd.DataFrame, feature: str, operator: str, threshold: float
) -> pd.Series:
    values = pd.to_numeric(frame.get(feature), errors="coerce")
    if operator == ">=":
        return values >= threshold
    if operator == "<=":
        return values <= threshold
    if operator == ">":
        return values > threshold
    if operator == "<":
        return values < threshold
    if operator == "==":
        return values == threshold
    return pd.Series(False, index=frame.index)


def delay_robustness_fields(
    delay_expectancies_adjusted: List[float],
    *,
    min_delay_positive_ratio: float,
    min_delay_robustness_score: float,
) -> Dict[str, Any]:
    if not delay_expectancies_adjusted:
        return {
            "delay_positive_ratio": 0.0,
            "delay_dispersion": 0.0,
            "delay_robustness_score": 0.0,
            "gate_delay_robustness": False,
        }
    arr = np.asarray(delay_expectancies_adjusted, dtype=float)
    delay_positive_ratio = float(np.mean(arr > 0.0))
    delay_dispersion = float(np.std(arr))
    mean_delay = float(np.mean(arr))
    stability = float(max(0.0, 1.0 - (delay_dispersion / max(abs(mean_delay), 1e-9))))
    score = float((0.7 * delay_positive_ratio) + (0.3 * stability))
    gate = bool(
        delay_positive_ratio >= min_delay_positive_ratio and score >= min_delay_robustness_score
    )
    return {
        "delay_positive_ratio": delay_positive_ratio,
        "delay_dispersion": delay_dispersion,
        "delay_robustness_score": score,
        "gate_delay_robustness": gate,
    }


def effective_sample_size(values: np.ndarray, max_lag: int) -> Tuple[float, int]:
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    n = len(arr)
    if n <= 1:
        return float(n), 0
    lag_cap = min(int(max_lag), n // 10)
    if lag_cap <= 0:
        return float(n), 0
    rho_sum = sum(
        pd.Series(arr).autocorr(lag=lag)
        for lag in range(1, lag_cap + 1)
        if np.isfinite(pd.Series(arr).autocorr(lag=lag))
    )
    ess = float(n / max(1e-6, 1.0 + (2.0 * rho_sum)))
    return float(np.clip(ess, 0.0, float(n))), lag_cap


def multiplicity_penalty(
    *,
    multiplicity_k: float,
    num_tests_primary_event_id: int | None = None,
    num_tests_event_family: int | None = None,
    ess_effective: float,
) -> float:
    num_tests = (
        int(num_tests_primary_event_id)
        if num_tests_primary_event_id is not None
        else int(num_tests_event_family or 0)
    )
    return float(
        float(multiplicity_k)
        * np.sqrt(np.log(max(1.0, float(num_tests))) / max(1.0, float(ess_effective)))
    )


def apply_multiplicity_adjustments(
    candidates: pd.DataFrame,
    *,
    multiplicity_k: float,
    min_delay_positive_ratio: float = 0.60,
    min_delay_robustness_score: float = 0.60,
) -> pd.DataFrame:
    if candidates.empty:
        return candidates
    out = candidates.copy()
    out["multiplicity_penalty"] = out.apply(
        lambda r: multiplicity_penalty(
            multiplicity_k=multiplicity_k,
            num_tests_primary_event_id=int(r.get("num_tests_primary_event_id", 0)),
            num_tests_event_family=int(r.get("num_tests_event_family", 0)),
            ess_effective=float(r.get("ess_effective", 0.0)),
        ),
        axis=1,
    )
    out["expectancy_after_multiplicity"] = (
        pd.to_numeric(out.get("expectancy_per_trade", 0.0)) - out["multiplicity_penalty"]
    )

    # Delay maps adjustment logic
    new_maps, pos_ratios, scores, gates = [], [], [], []
    for _, row in out.iterrows():
        try:
            raw_map = json.loads(str(row.get("delay_expectancy_map", "{}")))
        except:
            raw_map = {}
        adj_map = {
            str(k): float(safe_float(v, 0.0) - safe_float(row.get("multiplicity_penalty"), 0.0))
            for k, v in raw_map.items()
        }
        fields = delay_robustness_fields(
            list(adj_map.values()),
            min_delay_positive_ratio=min_delay_positive_ratio,
            min_delay_robustness_score=min_delay_robustness_score,
        )
        new_maps.append(json.dumps(adj_map, sort_keys=True))
        pos_ratios.append(fields["delay_positive_ratio"])
        scores.append(fields["delay_robustness_score"])
        gates.append(fields["gate_delay_robustness"])

    out["delay_expectancy_map"] = new_maps
    out["delay_positive_ratio"] = pos_ratios
    out["delay_robustness_score"] = scores
    out["gate_delay_robustness"] = gates
    return out
