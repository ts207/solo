from __future__ import annotations

import pandas as pd

from project.core.feature_quality import summarize_feature_quality


def test_summarize_feature_quality_reports_nulls_constants_and_outliers():
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=5, freq="5min", tz="UTC"),
            "basis_zscore": [0.1, None, 0.3, 0.4, 0.5],
            "spread_zscore": [1.0, 1.0, 1.0, 1.0, 1.0],
            "rv_96": [0.01, 0.02, 0.03, 0.04, 100.0],
        }
    )

    summary = summarize_feature_quality(frame, z_threshold=1.5)

    assert summary["feature_count"] == 3
    assert summary["features_with_nulls"] == 1
    assert summary["constant_features"] == 1
    assert summary["features_with_outliers"] == 1
    assert summary["per_feature"]["basis_zscore"]["null_rate"] > 0.0
    assert summary["per_feature"]["spread_zscore"]["constant_rate"] == 1.0
    assert summary["per_feature"]["rv_96"]["outlier_rate"] > 0.0


def test_summarize_feature_quality_reports_baseline_drift_fields():
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=12, freq="5min", tz="UTC"),
            "basis_zscore": [2.0 + 0.1 * i for i in range(12)],
            "spread_zscore": [1.0] * 12,
        }
    )
    baseline = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-12-01", periods=12, freq="5min", tz="UTC"),
            "basis_zscore": [0.1 * i for i in range(12)],
            "spread_zscore": [1.0] * 12,
        }
    )

    summary = summarize_feature_quality(
        frame,
        baseline_frame=baseline,
        baseline_label="baseline_run",
    )

    assert summary["baseline"]["label"] == "baseline_run"
    assert summary["baseline"]["feature_count_compared"] == 2
    assert "baseline_ks_statistic" in summary["per_feature"]["basis_zscore"]
    assert summary["per_feature"]["basis_zscore"]["baseline_median_delta"] > 0.0
