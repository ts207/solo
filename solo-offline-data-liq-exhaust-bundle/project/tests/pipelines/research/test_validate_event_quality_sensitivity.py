from __future__ import annotations

import pandas as pd

from project.research.validate_event_quality import (
    _compute_rerun_proxy_metrics,
    _compute_sensitivity,
)


def test_compute_sensitivity_without_severity_marks_metric_unavailable():
    events_df = pd.DataFrame(
        {
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "enter_ts": pd.date_range("2024-01-01", periods=2, freq="5min", tz="UTC"),
        }
    )

    result = _compute_sensitivity(events_df, severity_cols=["severity"])

    assert pd.isna(result["prevalence_elasticity"])
    assert pd.isna(result["prevalence_stability_index"])
    assert result["sensitivity_method"] == "unavailable_no_severity"


def test_rerun_proxy_metrics_without_severity_marks_metric_unavailable():
    events_df = pd.DataFrame(
        {
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "enter_ts": pd.date_range("2024-01-01", periods=2, freq="5min", tz="UTC"),
        }
    )

    result = _compute_rerun_proxy_metrics(events_df, severity_cols=["severity"])

    assert pd.isna(result["prevalence_elasticity"])
    assert pd.isna(result["candidate_identity_stability"])
    assert pd.isna(result["sign_stability"])
    assert result["proxy_method"] == "unavailable_no_severity"
