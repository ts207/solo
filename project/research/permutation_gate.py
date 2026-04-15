"""Permutation test gate for candidate expectancy.

Shuffles trade returns to verify the observed mean exceeds random chance
at a configurable percentile threshold.
"""

from __future__ import annotations

import numpy as np


def permutation_test_expectancy(
    returns: np.ndarray,
    *,
    n_permutations: int = 1000,
    threshold_pct: float = 95.0,
    rng_seed: int = 42,
) -> dict:
    """Permutation test for the observed mean return.

    Shuffles the sign of each return ``n_permutations`` times and computes
    the fraction of shuffled means that are below the observed mean.

    Args:
        returns: 1-D array of per-trade returns.
        n_permutations: Number of random permutations.
        threshold_pct: Percentile threshold for the gate (0-100).
        rng_seed: Seed for reproducibility.

    Returns:
        Dictionary with ``observed_rank_pct`` (float 0-100) and
        ``gate_permutation`` (bool).
    """
    arr = np.asarray(returns, dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.size < 5:
        return {
            "observed_rank_pct": 0.0,
            "gate_permutation": False,
            "permutation_n_returns": int(arr.size),
            "permutation_n_shuffles": 0,
        }

    observed_mean = float(np.mean(arr))
    rng = np.random.default_rng(rng_seed)

    # Vectorised: generate all shuffled signs at once (n_permutations × n)
    signs = rng.choice([-1, 1], size=(int(n_permutations), arr.size))
    shuffled_means = (signs * arr).mean(axis=1)

    rank_pct = float(100.0 * np.mean(shuffled_means <= observed_mean))
    return {
        "observed_rank_pct": float(rank_pct),
        "gate_permutation": bool(rank_pct >= float(threshold_pct)),
        "permutation_n_returns": int(arr.size),
        "permutation_n_shuffles": int(n_permutations),
    }
