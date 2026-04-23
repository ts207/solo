"""
Tests for hypothesis clustering.
"""

from __future__ import annotations

import pandas as pd
import pytest
from project.research.promotion.clustering import cluster_hypotheses, build_cluster_summary


def test_cluster_identical_behavior():
    """Identical behavior fields must result in the same cluster."""
    df = pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "event_type": "E1",
                "horizon": "15m",
                "template_verb": "V1",
                "selection_score": 10.0,
            },
            {
                "candidate_id": "c2",
                "event_type": "E1",
                "horizon": "15m",
                "template_verb": "V1",
                "selection_score": 5.0,
            },
            {
                "candidate_id": "c3",
                "event_type": "E2",
                "horizon": "60m",
                "template_verb": "V2",
                "selection_score": 8.0,
            },
        ]
    )

    clustered = cluster_hypotheses(df)

    # c1 and c2 should be in the same cluster, c3 separate
    c1_cluster = clustered.loc[clustered["candidate_id"] == "c1", "cluster_id"].iloc[0]
    c2_cluster = clustered.loc[clustered["candidate_id"] == "c2", "cluster_id"].iloc[0]
    c3_cluster = clustered.loc[clustered["candidate_id"] == "c3", "cluster_id"].iloc[0]

    assert c1_cluster == c2_cluster
    assert c1_cluster != c3_cluster


def test_cluster_delay_correlation():
    """High delay profile correlation must result in the same cluster."""
    # c1 and c2 have perfectly correlated delay expectancies (offset by 2x)
    # c3 has unrelated delay expectancy
    df = pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "event_type": "E1",
                "horizon": "5m",
                "delay_expectancy_map": {"0": 1.0, "1": 0.5, "2": 0.2},
                "selection_score": 1.0,
            },
            {
                "candidate_id": "c2",
                "event_type": "E2",
                "horizon": "15m",
                "delay_expectancy_map": {"0": 2.0, "1": 1.0, "2": 0.4},
                "selection_score": 2.0,
            },
            {
                "candidate_id": "c3",
                "event_type": "E3",
                "horizon": "30m",
                "delay_expectancy_map": {"0": -1.0, "1": 1.0, "2": -1.0},
                "selection_score": 3.0,
            },
        ]
    )

    # behavior_threshold=1.0 ensures different event_types don't cluster by behavior
    clustered = cluster_hypotheses(df, behavior_threshold=1.0, correlation_threshold=0.9)

    c1_cluster = clustered.loc[clustered["candidate_id"] == "c1", "cluster_id"].iloc[0]
    c2_cluster = clustered.loc[clustered["candidate_id"] == "c2", "cluster_id"].iloc[0]
    c3_cluster = clustered.loc[clustered["candidate_id"] == "c3", "cluster_id"].iloc[0]

    assert c1_cluster == c2_cluster
    assert c1_cluster != c3_cluster


def test_build_cluster_summary():
    """Summary report must pick the top representative per cluster."""
    df = pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "cluster_id": "clus1",
                "selection_score": 10.0,
                "event_type": "E1",
            },
            {
                "candidate_id": "c2",
                "cluster_id": "clus1",
                "selection_score": 50.0,
                "event_type": "E1",
            },  # better
            {
                "candidate_id": "c3",
                "cluster_id": "clus2",
                "selection_score": 8.0,
                "event_type": "E2",
            },
        ]
    )

    summary = build_cluster_summary(df)
    assert len(summary) == 2

    clus1_row = summary[summary["cluster_id"] == "clus1"].iloc[0]
    assert clus1_row["representative_id"] == "c2"
    assert clus1_row["cluster_size"] == 2

    clus2_row = summary[summary["cluster_id"] == "clus2"].iloc[0]
    assert clus2_row["representative_id"] == "c3"
    assert clus2_row["cluster_size"] == 1
