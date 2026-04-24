import numpy as np
import pandas as pd

from project.research.local_symbolic_overlay import train_local_symbolic_filter


def test_filter_optimization_determinism():
    np.random.seed(42)
    # Create fake data
    n_train = 200
    n_val = 100

    # Feature 1 is noise, Feature 2 is highly correlated with label, Feature 3 is anti-correlated
    df_train = pd.DataFrame(
        {
            "feat1": np.random.normal(size=n_train),
            "feat2": np.random.normal(size=n_train),
            "feat3": np.random.normal(size=n_train),
        }
    )

    labels_train = pd.Series(
        df_train["feat2"] * 2.0 - df_train["feat3"] * 1.5 + np.random.normal(size=n_train) * 0.1
    )

    df_val = pd.DataFrame(
        {
            "feat1": np.random.normal(size=n_val),
            "feat2": np.random.normal(size=n_val),
            "feat3": np.random.normal(size=n_val),
        }
    )
    labels_val = pd.Series(
        df_val["feat2"] * 2.0 - df_val["feat3"] * 1.5 + np.random.normal(size=n_val) * 0.1
    )

    # Run optimization twice
    res1 = train_local_symbolic_filter(df_train, labels_train, df_val, labels_val, "C1")
    res2 = train_local_symbolic_filter(df_train, labels_train, df_val, labels_val, "C1")

    # Assert determinism
    assert res1 == res2, "Filter optimization is non-deterministic"

    # Assert correctness
    assert res1["feature"] in ["feat2", "feat3"], "Failed to pick an informative feature"
    if res1["feature"] == "feat2":
        assert res1["operator"] == ">", "Positive correlation should result in > operator"
    else:
        assert res1["operator"] == "<", "Negative correlation should result in < operator"

    assert res1["optimized"] is True
    assert "theta" in res1
