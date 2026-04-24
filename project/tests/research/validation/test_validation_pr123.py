from __future__ import annotations

import pandas as pd

from project.research.validation import (
    apply_multiple_testing,
    assign_split_labels,
    assign_test_families,
    estimate_effect_from_frame,
)


def test_assign_split_labels_emits_metadata() -> None:
    df = pd.DataFrame(
        {
            "enter_ts": pd.date_range("2024-01-01", periods=30, freq="5min", tz="UTC"),
        }
    )
    out = assign_split_labels(df, time_col="enter_ts", purge_bars=2, embargo_bars=1)
    assert "split_label" in out.columns
    assert {"train", "validation", "test"}.intersection(set(out["split_label"]))
    assert int(out["purge_bars_used"].iloc[0]) == 2
    assert int(out["embargo_bars_used"].iloc[0]) == 1


def test_assign_split_labels_excludes_purged_and_embargoed_rows() -> None:
    df = pd.DataFrame(
        {
            "enter_ts": pd.date_range("2024-01-01", periods=30, freq="5min", tz="UTC"),
        }
    )
    out = assign_split_labels(df, time_col="enter_ts", purge_bars=2, embargo_bars=1)
    assert len(out) == 24
    assert set(out["split_label"]) <= {"train", "validation", "test"}


def test_estimate_effect_from_frame_returns_uncertainty_fields() -> None:
    df = pd.DataFrame(
        {
            "value": [0.002, 0.001, -0.001, 0.003, 0.001],
            "cluster_day": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02", "2024-01-03"],
        }
    )
    res = estimate_effect_from_frame(df, value_col="value", cluster_col="cluster_day", n_boot=100)
    assert res.n_obs == 5
    assert res.n_clusters == 3
    assert 0.0 <= res.p_value_raw <= 1.0
    assert res.ci_low <= res.ci_high


def test_estimate_effect_from_frame_drops_null_clusters() -> None:
    df = pd.DataFrame(
        {
            "value": [0.002, 0.001, -0.001, 0.003],
            "cluster_day": ["2024-01-01", None, "2024-01-02", None],
        }
    )
    res = estimate_effect_from_frame(df, value_col="value", cluster_col="cluster_day", n_boot=100)
    assert res.n_obs == 2
    assert res.n_clusters == 2


def test_estimate_effect_from_frame_singleton_cluster_has_nonzero_ci_width() -> None:
    df = pd.DataFrame(
        {
            "value": [0.002, 0.001, 0.003],
            "cluster_day": ["2024-01-01", "2024-01-01", "2024-01-01"],
        }
    )
    res = estimate_effect_from_frame(df, value_col="value", cluster_col="cluster_day", n_boot=100)
    assert res.n_clusters == 1
    assert res.ci_high > res.ci_low


def test_apply_multiple_testing_by_family() -> None:
    df = pd.DataFrame(
        {
            "run_id": ["r1", "r1", "r1", "r1"],
            "event_family": ["VOL", "VOL", "MOM", "MOM"],
            "horizon": ["24", "24", "24", "24"],
            "p_value_raw": [0.01, 0.02, 0.20, 0.30],
        }
    )
    df = assign_test_families(df, family_cols=["run_id", "event_family", "horizon"])
    out = apply_multiple_testing(
        df, p_col="p_value_raw", family_col="correction_family_id", method="bh"
    )
    assert "p_value_adj" in out.columns
    vol = out[out["event_family"] == "VOL"]["p_value_adj"].tolist()
    assert max(vol) <= 0.02 * 2 + 1e-9
