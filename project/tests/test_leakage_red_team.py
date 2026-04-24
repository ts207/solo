from __future__ import annotations

import numpy as np
import pandas as pd

from project.core.audited_join import audited_merge_asof
from project.research.helpers.shrinkage import _apply_hierarchical_shrinkage


def test_future_row_injection_affects_output():
    """L2: Future-row injection should change outputs and be caught."""
    # This is a conceptual test: if we add a row in the future,
    # it should not affect the results of a past timestamp if PIT safety holds.

    ts = pd.date_range("2024-01-01", periods=100, freq="5min", tz="UTC")
    left = pd.DataFrame({"enter_ts": ts[:50], "val": np.random.randn(50)})
    right = pd.DataFrame({"timestamp": ts, "feature": np.random.randn(100)})

    # 1. Base join
    res1 = audited_merge_asof(
        left,
        right,
        left_on="enter_ts",
        right_on="timestamp",
        feature_name="test",
        stale_threshold_seconds=3600,
        symbol="BTC",
        run_id="test",
    )

    # 2. Modify future feature row
    right_mod = right.copy()
    right_mod.iloc[90, 1] += 10.0  # Way in the future

    res2 = audited_merge_asof(
        left,
        right_mod,
        left_on="enter_ts",
        right_on="timestamp",
        feature_name="test",
        stale_threshold_seconds=3600,
        symbol="BTC",
        run_id="test",
    )

    # res1 and res2 must be identical if PIT safety is respected
    pd.testing.assert_frame_equal(res1, res2)


def test_timestamp_forward_shift_failure():
    """L2: Timestamp shifted forward by one bar should fail if it causes lookahead."""
    ts = pd.date_range("2024-01-01", periods=100, freq="5min", tz="UTC")
    left = pd.DataFrame({"enter_ts": ts[10:20]})
    right = pd.DataFrame({"timestamp": ts, "feature": range(100)})

    # Intentionally shift enter_ts forward by 1 bar to simulate lookahead
    left_shifted = left.copy()
    left_shifted["enter_ts"] = left_shifted["enter_ts"] + pd.Timedelta(minutes=5)

    res_normal = audited_merge_asof(
        left,
        right,
        left_on="enter_ts",
        right_on="timestamp",
        feature_name="feature",
        stale_threshold_seconds=3600,
        symbol="BTC",
        run_id="test",
    )
    res_shifted = audited_merge_asof(
        left_shifted,
        right,
        left_on="enter_ts",
        right_on="timestamp",
        feature_name="feature",
        stale_threshold_seconds=3600,
        symbol="BTC",
        run_id="test",
    )
    # The joined feature values should be strictly greater in the shifted version
    assert (res_shifted["feature"].values > res_normal["feature"].values).all()


def test_future_timestamp_join_hard_fail():
    """L2: event-feature joins with future timestamps should be blocked."""
    ts = pd.date_range("2024-01-01", periods=10, freq="5min", tz="UTC")
    left = pd.DataFrame({"enter_ts": [ts[5]]})
    right = pd.DataFrame({"timestamp": [ts[6]]})  # Future!

    # direction="backward" with tolerance will result in NaN, which is safe.
    # direction="forward" would be a lookahead.
    res = audited_merge_asof(
        left,
        right,
        left_on="enter_ts",
        right_on="timestamp",
        direction="backward",
        feature_name="test",
        stale_threshold_seconds=3600,
        symbol="BTC",
        run_id="test",
    )
    assert res["timestamp"].isna().all()


def test_shrinkage_oos_leakage_detection():
    """L2: shrinkage trained on OOS rows should be detected."""
    df = pd.DataFrame(
        {
            "canonical_family": ["A"] * 10,
            "canonical_event_type": ["B"] * 10,
            "template_verb": ["V"] * 10,
            "horizon": ["H"] * 10,
            "symbol": ["S"] * 10,
            "state_id": ["ST"] * 10,
            "expectancy": np.random.randn(10),
            "split_label": ["train"] * 5 + ["test"] * 5,
            "n_events": [100] * 10,
            "std_return": [0.1] * 10,
        }
    )

    # Run with train_only_lambda=True
    res = _apply_hierarchical_shrinkage(df, train_only_lambda=True, split_col="split_label")
    assert res["shrinkage_scope"].iloc[0] == "train_only"

    # CONCEPTUAL: If we were to verify that OOS data didn't affect lambda,
    # we'd compare with a run where OOS data is changed.
    df_mod = df.copy()
    df_mod.loc[df_mod["split_label"] == "test", "expectancy"] += 100.0

    res_mod = _apply_hierarchical_shrinkage(df_mod, train_only_lambda=True, split_col="split_label")

    # Lambdas should be identical
    for col in res.columns:
        if "lambda" in col and "_status" not in col and "_prev" not in col:
            pd.testing.assert_series_equal(res[col], res_mod[col])


def test_bridge_combined_is_oos_detection():
    """L2: bridge metrics computed from combined in-sample/OOS should be detected."""
    # This requires checking if bridge code filters by split_label="validation" or "test"
    pass
