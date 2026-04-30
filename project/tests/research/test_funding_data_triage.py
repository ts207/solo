from __future__ import annotations

import pandas as pd

from project.research.funding_data_triage import classify_funding_field


def _stepwise_frame(*, days: int = 90, include_abs: bool = True) -> pd.DataFrame:
    timestamps = pd.date_range(
        pd.Timestamp("2023-01-01", tz="UTC"),
        pd.Timestamp("2023-01-01", tz="UTC") + pd.Timedelta(days=days),
        freq="5min",
    )
    update_index = (pd.Series(range(len(timestamps))) // 96).astype(float)
    rate = ((update_index % 7) - 3) / 10_000.0
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "funding_rate_scaled": rate,
            "funding_rate": rate,
        }
    )
    if include_abs:
        frame["funding_abs_pct"] = rate.abs()
    return frame


def test_classifies_forward_filled_funding_rate_as_valid_stepwise():
    row = classify_funding_field(
        _stepwise_frame(),
        run_id="triage",
        source_run_id="source",
        symbol="BTCUSDT",
        timeframe="5m",
        field="funding_rate_scaled",
    )

    assert row["classification"] == "valid_stepwise"
    assert row["forward_fill_detected"] is True
    assert row["stale_ratio_raw"] > 0.90
    assert row["stale_ratio_funding_adjusted"] == 0.0
    assert row["median_update_gap_hours"] == 8.0


def test_classifies_missing_funding_field():
    row = classify_funding_field(
        pd.DataFrame({"timestamp": pd.date_range("2023-01-01", periods=10, tz="UTC")}),
        run_id="triage",
        source_run_id="source",
        symbol="BTCUSDT",
        timeframe="5m",
        field="funding_rate_scaled",
    )

    assert row["classification"] == "missing"
    assert row["present"] is False


def test_classifies_true_stale_when_last_update_far_before_dataset_end():
    frame = _stepwise_frame(days=90)
    stale_start = frame["timestamp"].max() - pd.Timedelta(days=10)
    frame.loc[frame["timestamp"] >= stale_start, "funding_rate_scaled"] = 0.001

    row = classify_funding_field(
        frame,
        run_id="triage",
        source_run_id="source",
        symbol="BTCUSDT",
        timeframe="5m",
        field="funding_rate_scaled",
    )

    assert row["classification"] == "true_stale"
    assert row["last_update_lag_hours"] > 24.0


def test_classifies_constant_multimonth_funding_as_invalid():
    frame = _stepwise_frame(days=90)
    frame["funding_rate_scaled"] = 0.001

    row = classify_funding_field(
        frame,
        run_id="triage",
        source_run_id="source",
        symbol="BTCUSDT",
        timeframe="5m",
        field="funding_rate_scaled",
    )

    assert row["classification"] == "invalid"


def test_funding_abs_pct_is_recomputable_from_valid_rate_when_missing():
    frame = _stepwise_frame(include_abs=False)
    rate_row = classify_funding_field(
        frame,
        run_id="triage",
        source_run_id="source",
        symbol="BTCUSDT",
        timeframe="5m",
        field="funding_rate_scaled",
    )
    abs_row = classify_funding_field(
        frame,
        run_id="triage",
        source_run_id="source",
        symbol="BTCUSDT",
        timeframe="5m",
        field="funding_abs_pct",
        companion_rate_classification=rate_row["classification"],
    )

    assert rate_row["classification"] == "valid_stepwise"
    assert abs_row["classification"] == "recomputable"
    assert abs_row["recomputable_from"] == "funding_rate_scaled"


def test_reports_market_context_funding_fields():
    row = classify_funding_field(
        _stepwise_frame(),
        run_id="triage",
        source_run_id="source",
        symbol="BTCUSDT",
        timeframe="5m",
        field="funding_rate_scaled",
    )

    assert "funding_rate_scaled" in row["market_context_funding_fields"]
    assert "funding_abs_pct" in row["market_context_funding_fields"]
