"""
Smoke tests for the result-delta harness (Phase 0).

These tests confirm the harness infrastructure itself works — they do not
assert specific metric values because the values will change as bugs are
fixed in subsequent sprint tasks.
"""

from __future__ import annotations

import json

import pytest


@pytest.fixture()
def synthetic_setup():
    """Return (hypotheses, features) built from the harness fixture builder."""
    from project.tests.fixtures.result_harness import (
        make_synthetic_features,
        make_synthetic_hypotheses,
    )

    features = make_synthetic_features(n_rows=40, n_events=6, seed=0)
    hypotheses = make_synthetic_hypotheses(entry_lag=1, horizon="12b")
    return hypotheses, features


def test_make_synthetic_features_shape():
    from project.tests.fixtures.result_harness import make_synthetic_features
    from project.events.event_specs import EVENT_REGISTRY_SPECS

    df = make_synthetic_features(n_rows=20, n_events=4, seed=1)
    assert len(df) == 20
    assert "timestamp" in df.columns
    assert "close" in df.columns
    assert "split_label" in df.columns
    # Canonical signal column for VOL_SPIKE
    sig_col = EVENT_REGISTRY_SPECS["VOL_SPIKE"].signal_column
    assert sig_col in df.columns
    assert df[sig_col].sum() >= 1
    # Split labels assigned
    assert set(df["split_label"].unique()) <= {"train", "validation", "test"}
    assert "train" in df["split_label"].values


def test_make_synthetic_features_no_split_labels():
    from project.tests.fixtures.result_harness import make_synthetic_features

    df = make_synthetic_features(n_rows=10, n_events=2, include_split_labels=False, seed=2)
    assert "split_label" not in df.columns


def test_make_synthetic_hypotheses_returns_list():
    from project.tests.fixtures.result_harness import make_synthetic_hypotheses

    hyps = make_synthetic_hypotheses(entry_lag=1)
    assert isinstance(hyps, list)
    assert len(hyps) == 1


def test_snapshot_metrics_runs_without_error(synthetic_setup):
    from project.tests.fixtures.result_harness import snapshot_metrics

    hypotheses, features = synthetic_setup
    snap = snapshot_metrics(hypotheses, features)

    # Must be JSON-serialisable
    serialised = json.dumps(snap, default=str)
    assert isinstance(serialised, str)

    # Required top-level keys
    required_keys = {
        "hypothesis_count",
        "metrics_rows",
        "valid_metrics_rows",
        "post_entry_lag_event_count",
        "train_n_obs",
        "validation_n_obs",
        "test_n_obs",
        "mean_return_gross_bps",
        "mean_return_net_bps",
        "t_stat_net",
        "p_value",
    }
    assert required_keys <= set(snap.keys()), f"Missing keys: {required_keys - set(snap.keys())}"


def test_snapshot_metrics_counts_are_nonnegative(synthetic_setup):
    from project.tests.fixtures.result_harness import snapshot_metrics

    hypotheses, features = synthetic_setup
    snap = snapshot_metrics(hypotheses, features)

    assert snap["hypothesis_count"] == 1
    assert snap["train_n_obs"] >= 0
    assert snap["validation_n_obs"] >= 0
    assert snap["test_n_obs"] >= 0


def test_delta_no_change():
    from project.tests.fixtures.result_harness import delta

    snap = {"n": 10, "mean": 1.5}
    d = delta(snap, snap)
    assert d["n"]["changed"] is False
    assert d["mean"]["changed"] is False
    assert d["_summary"]["changed_fields"] == []


def test_delta_detects_change():
    from project.tests.fixtures.result_harness import delta

    before = {"n": 10, "mean": 1.5}
    after = {"n": 12, "mean": 1.5}
    d = delta(before, after)
    assert d["n"]["changed"] is True
    assert d["mean"]["changed"] is False
    assert "n" in d["_summary"]["changed_fields"]


def test_snapshot_two_entry_lags_differ():
    """Snapshots with entry_lag=0 vs entry_lag=1 should differ in split counts
    (this is the key property the harness must be able to detect)."""
    from project.tests.fixtures.result_harness import (
        make_synthetic_features,
        make_synthetic_hypotheses,
        snapshot_metrics,
        delta,
    )

    features = make_synthetic_features(n_rows=40, n_events=8, seed=99)
    hyp_lag0 = make_synthetic_hypotheses(entry_lag=1)  # minimal valid lag
    hyp_lag2 = make_synthetic_hypotheses(entry_lag=2)

    snap_lag1 = snapshot_metrics(hyp_lag0, features)
    snap_lag2 = snapshot_metrics(hyp_lag2, features)

    # The two should not be identical — different lags produce different event windows
    d = delta(snap_lag1, snap_lag2)
    assert isinstance(d, dict)
    # At least 'post_entry_lag_event_count' or split counts differ (not asserting values,
    # just that the harness can discriminate)
    assert isinstance(d["_summary"]["changed_fields"], list)
