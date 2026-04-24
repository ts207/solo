import pandas as pd

from project.core.causal_primitives import trailing_percentile_rank, trailing_quantile


def test_trailing_quantile_pit():
    series = pd.Series([1, 2, 3, 4, 5, 100], name="val")
    # window=3, q=0.5 (median)
    # t=2: [1, 2, 3] -> median 2
    # t=3: [2, 3, 4] -> median 3
    # t=4: [3, 4, 5] -> median 4
    # t=5: [4, 5, 100] -> median 5

    # With lag=1:
    # t=3 should see value from t=2 (which is 2)
    # t=4 should see value from t=3 (which is 3)
    # t=5 should see value from t=4 (which is 4)
    # t=6 (if existed) would see 5

    res = trailing_quantile(series, window=3, q=0.5, lag=1)

    assert pd.isna(res[0])
    assert pd.isna(res[1])
    assert pd.isna(res[2])
    assert res[3] == 2.0
    assert res[4] == 3.0
    assert res[5] == 4.0


def test_trailing_percentile_rank_pit():
    series = pd.Series([1, 10, 2, 11, 3, 12, 4, 13], name="val")
    # window=2, lag=1
    # First valid rank is at t=3.
    # At t=2 in unshifted rolling: x=[1, 10, 2], rank of 2 in [1, 10] is 0.5.
    # With lag=1, this 0.5 value appears at t=3.
    # At t=3 in unshifted rolling: x=[10, 2, 11], rank of 11 in [10, 2] is 1.0.
    # With lag=1, this 1.0 value appears at t=4.

    res = trailing_percentile_rank(series, window=2, lag=1)

    assert pd.isna(res[0])
    assert pd.isna(res[1])
    assert pd.isna(res[2])
    assert res[3] == 0.5
    assert res[4] == 1.0
    assert res[5] == 0.5
    assert res[6] == 1.0
    assert res[7] == 0.5
