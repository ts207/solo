import numpy as np
import pandas as pd

from project.research.dynamic_exit import compute_dynamic_exit_heuristics


def test_dynamic_exit_bounded_output():
    # Simulate a returns matrix for different horizons
    np.random.seed(42)
    horizons = [5, 10, 15, 30, 60, 120]

    # Create fake data that peaks at 30, stays stable, then decays at 120
    data = {}
    for h in horizons:
        if h <= 30:
            data[h] = np.random.normal(loc=0.001 * h, scale=0.01, size=100)
        elif h <= 60:
            data[h] = np.random.normal(loc=0.030, scale=0.02, size=100)
        else:
            data[h] = np.random.normal(loc=-0.010, scale=0.05, size=100)

    df_train = pd.DataFrame(data)

    heuristics = compute_dynamic_exit_heuristics(df_train, horizons)

    assert "recommended_horizons" in heuristics
    recos = heuristics["recommended_horizons"]

    # Must output a bounded set (no more than 3)
    assert len(recos) <= 3
    assert isinstance(recos, list)
    assert all(r in horizons for r in recos)

    # Check that it avoided the decayed horizon (120)
    assert 120 not in recos, "Dynamic exit recommended a decayed horizon"


def test_dynamic_exit_insufficient_samples():
    df_train = pd.DataFrame({5: [0.01] * 10, 10: [0.02] * 10})  # Only 10 samples
    heuristics = compute_dynamic_exit_heuristics(df_train, [5, 10])

    assert heuristics["recommended_horizons"] == [], "Should return empty if insufficient samples"
