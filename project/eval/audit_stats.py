from typing import Any, Dict, List

import numpy as np
import pandas as pd


def permutation_test(
    returns: pd.Series,
    n_permutations: int = 10000,
    random_seed: int | None = None,
) -> float:
    """
    Calculate the p-value of the mean return using a permutation test.
    It randomly flips the signs of the returns to create a null distribution.
    """
    arr = returns.dropna().values
    n = len(arr)
    if n == 0 or n_permutations <= 0:
        return 1.0

    observed_mean = np.mean(arr)
    if random_seed is None:
        signs = np.random.choice([-1, 1], size=(int(n_permutations), n))
    else:
        rng = np.random.default_rng(random_seed)
        signs = rng.choice([-1, 1], size=(int(n_permutations), n))
    null_means = (signs * arr).mean(axis=1)

    # Two-sided Monte Carlo p-value with +1 correction to avoid impossible zero p-values.
    exceedances = int(np.count_nonzero(np.abs(null_means) >= abs(observed_mean)))
    p_value = (exceedances + 1.0) / (float(n_permutations) + 1.0)
    return float(p_value)


def detect_selection_bias(p_values: List[float], n_hypotheses: int) -> Dict[str, Any]:
    """
    Check if the best p-value in a set is better than what would be
    expected by chance given the total number of hypotheses tested.
    """
    if not p_values:
        return {"is_biased": False, "reason": "No p-values provided"}

    best_p = min(p_values)
    # Expected best p-value from n_hypotheses independent uniform [0,1] tests is 1/(n+1)
    expected_best_p = 1.0 / (n_hypotheses + 1)

    # Sidak correction for the best p-value
    # alpha_corrected = 1 - (1 - alpha_global)^(1/n)
    # If best_p > alpha_corrected, we fail to reject the global null

    is_biased = bool(best_p > expected_best_p * 2.0)  # Heuristic: if best is 2x expected, it's weak

    return {
        "best_observed_p": best_p,
        "expected_best_p": expected_best_p,
        "n_hypotheses": n_hypotheses,
        "is_biased": is_biased,
        "is_suspicious": bool(best_p > expected_best_p * 5.0),  # Very weak best p
    }
