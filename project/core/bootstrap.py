from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class BootstrapMeanResult:
    p_value: float
    ci_low: float
    ci_high: float
    mean: float
    n: int
    n_boot: int


def stationary_block_bootstrap_mean(
    values: np.ndarray,
    *,
    mean_block_len: int,
    n_boot: int = 1000,
    seed: int | None = 1337,
    two_sided: bool = True,
) -> BootstrapMeanResult:
    """Stationary block bootstrap for the mean.

    - Samples blocks with geometrically distributed lengths (Politis & Romano).
    - Preserves local dependence without requiring explicit timestamps.
    - Designed to be dependency-light (numpy only).
    """
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    n = int(arr.size)
    if n == 0:
        return BootstrapMeanResult(p_value=1.0, ci_low=0.0, ci_high=0.0, mean=0.0, n=0, n_boot=0)
    if n == 1:
        m = float(arr.mean())
        return BootstrapMeanResult(p_value=1.0, ci_low=m, ci_high=m, mean=m, n=1, n_boot=0)

    L = max(1, int(mean_block_len))
    # Geometric parameter so E[length] = L.
    p = min(1.0, max(1e-6, 1.0 / float(L)))

    rng = np.random.default_rng(seed)
    boot_means = np.empty(int(n_boot), dtype=float)
    idx = np.arange(n, dtype=int)

    for b in range(int(n_boot)):
        out = np.empty(n, dtype=float)
        filled = 0
        while filled < n:
            start = int(rng.integers(0, n))
            # geometric returns number of failures before first success; add 1 for length
            blen = int(rng.geometric(p))
            blen = max(1, min(blen, n - filled))
            block_idx = (start + idx[:blen]) % n
            out[filled : filled + blen] = arr[block_idx]
            filled += blen
        boot_means[b] = float(out.mean())

    mu = float(arr.mean())
    # Percentile CI
    ci_low, ci_high = np.quantile(boot_means, [0.025, 0.975]).astype(float).tolist()

    # Empirical p-value under null mean == 0 using a centered bootstrap.
    # This approximates the sampling distribution of (mean - 0).
    centered = boot_means - mu
    if two_sided:
        p_val = float(np.mean(np.abs(centered) >= abs(mu)))
    else:
        # One-sided: mean > 0
        p_val = float(np.mean(centered <= -mu))
    p_val = float(np.clip(p_val, 0.0, 1.0))

    return BootstrapMeanResult(
        p_value=p_val,
        ci_low=float(ci_low),
        ci_high=float(ci_high),
        mean=mu,
        n=n,
        n_boot=int(n_boot),
    )
