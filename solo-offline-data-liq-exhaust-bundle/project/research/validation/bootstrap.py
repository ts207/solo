from __future__ import annotations

from typing import Optional, Tuple

import numpy as np
import pandas as pd


def bootstrap_mean_ci(
    values: pd.Series,
    *,
    clusters: Optional[pd.Series] = None,
    n_boot: int = 1000,
    ci: float = 0.95,
    random_state: int = 0,
) -> Tuple[float, float]:
    vals = pd.to_numeric(values, errors="coerce")
    mask = vals.notna()
    vals = vals.loc[mask]
    if vals.empty:
        return 0.0, 0.0
    rng = np.random.default_rng(int(random_state))
    boot_means = []

    if clusters is None:
        arr = vals.to_numpy(dtype=float)
        n = len(arr)
        for _ in range(max(100, int(n_boot))):
            sample = rng.choice(arr, size=n, replace=True)
            boot_means.append(float(np.mean(sample)))
    else:
        raw_clusters = clusters.loc[mask]
        valid_clusters = raw_clusters.notna()
        cl = raw_clusters.loc[valid_clusters].astype(str)
        vals = vals.loc[valid_clusters]
        cl = cl[cl.str.lower() != "nan"]
        vals = vals.loc[cl.index]
        if vals.empty:
            return 0.0, 0.0
        grouped = {k: vals.loc[cl == k].to_numpy(dtype=float) for k in cl.unique()}
        keys = list(grouped)
        if len(keys) <= 1:
            arr = vals.to_numpy(dtype=float)
            n = len(arr)
            for _ in range(max(100, int(n_boot))):
                sample = rng.choice(arr, size=n, replace=True)
                boot_means.append(float(np.mean(sample)))
        else:
            for _ in range(max(100, int(n_boot))):
                sampled_keys = rng.choice(keys, size=len(keys), replace=True)
                sample = np.concatenate([grouped[k] for k in sampled_keys if len(grouped[k]) > 0])
                if sample.size:
                    boot_means.append(float(np.mean(sample)))

    if not boot_means:
        m = float(vals.mean())
        return m, m
    alpha = (1.0 - float(ci)) / 2.0
    low = float(np.quantile(boot_means, alpha))
    high = float(np.quantile(boot_means, 1.0 - alpha))
    return low, high
