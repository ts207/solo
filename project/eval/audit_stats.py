from typing import Any

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


def detect_selection_bias(p_values: list[float], n_hypotheses: int) -> dict[str, Any]:
    """
    Diagnose whether the best p-value is suspicious after a multiple-search campaign.

    Under the global null with ``m`` independent tests, the expected minimum p-value
    is ``1 / (m + 1)`` and the probability of observing a minimum at least this
    small is ``1 - (1 - best_p) ** m``. Selection/mining risk is therefore driven
    by p-values that are *too small* relative to the search burden, not by p-values
    that are weaker than the expected null minimum. Weak best p-values are reported
    separately as underpowered evidence.
    """
    clean = [float(p) for p in p_values if p is not None and np.isfinite(float(p))]
    if not clean:
        return {"is_biased": False, "reason": "No finite p-values provided"}

    m = max(1, int(n_hypotheses))
    best_p = float(min(max(p, 0.0) for p in clean))
    best_p = min(best_p, 1.0)
    expected_best_p = 1.0 / (m + 1.0)
    global_null_p_value = float(1.0 - (1.0 - best_p) ** m)

    too_good_for_trials = bool(best_p < expected_best_p * 0.10 and global_null_p_value < 0.05)
    weak_best_p = bool(best_p > expected_best_p * 2.0)

    return {
        "best_observed_p": best_p,
        "expected_best_p": float(expected_best_p),
        "global_null_p_value": global_null_p_value,
        "n_hypotheses": m,
        "is_biased": too_good_for_trials,
        "is_suspicious": too_good_for_trials,
        "too_good_for_trials": too_good_for_trials,
        "weak_best_p": weak_best_p,
        "is_underpowered": weak_best_p,
    }
