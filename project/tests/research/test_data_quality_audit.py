from __future__ import annotations

import pandas as pd

from project.research.data_quality_audit import (
    FIELD_COLUMNS,
    build_mechanism_data_quality,
    classify_field,
)


def _frame(field: str, values, *, days: int = 181) -> pd.DataFrame:
    timestamps = pd.date_range(
        pd.Timestamp("2023-01-01", tz="UTC"),
        pd.Timestamp("2023-01-01", tz="UTC") + pd.Timedelta(days=days),
        periods=len(values),
    )
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            field: values,
        }
    )


def _row(field: str, classification: str) -> dict:
    return {
        "schema_version": "data_quality_audit_v1",
        "run_id": "audit",
        "source_run_id": "source",
        "symbol": "BTCUSDT",
        "timeframe": "5m",
        "field": field,
        "present": classification != "missing",
        "non_null_count": 1001 if classification != "missing" else 0,
        "row_count": 1001 if classification != "missing" else 0,
        "coverage_ratio": 1.0 if classification != "missing" else 0.0,
        "distinct_count": 1001 if classification == "real" else 1,
        "zero_ratio": 0.0,
        "stale_ratio": 0.0,
        "first_timestamp": "2023-01-01T00:00:00+00:00",
        "last_timestamp": "2023-07-01T00:00:00+00:00",
        "history_days": 181.0,
        "classification": classification,
        "reason": classification,
    }


def test_classifies_missing_field():
    row = classify_field(
        pd.DataFrame({"timestamp": pd.date_range("2023-01-01", periods=10, tz="UTC")}),
        run_id="audit",
        source_run_id="source",
        symbol="BTCUSDT",
        timeframe="5m",
        field="liquidation_notional",
    )

    assert row["classification"] == "missing"
    assert row["present"] is False
    assert row["reason"] == "field is absent from market_context"


def test_classifies_insufficient_history():
    row = classify_field(
        _frame("rv_96", range(1001), days=30),
        run_id="audit",
        source_run_id="source",
        symbol="BTCUSDT",
        timeframe="5m",
        field="rv_96",
    )

    assert row["classification"] == "insufficient_history"


def test_classifies_constant_continuous_field_as_synthetic():
    row = classify_field(
        _frame("rv_96", [1.0] * 1001),
        run_id="audit",
        source_run_id="source",
        symbol="BTCUSDT",
        timeframe="5m",
        field="rv_96",
    )

    assert row["classification"] == "synthetic"


def test_allows_zero_heavy_liquidation_notional_without_synthetic_flag():
    values = [0.0] * 1000 + [25.0]
    row = classify_field(
        _frame("liquidation_notional", values),
        run_id="audit",
        source_run_id="source",
        symbol="BTCUSDT",
        timeframe="5m",
        field="liquidation_notional",
    )

    assert row["zero_ratio"] > 0.95
    assert row["classification"] == "real"


def test_classifies_real_field_with_good_coverage():
    row = classify_field(
        _frame("volume", [float(i + 1) for i in range(1001)]),
        run_id="audit",
        source_run_id="source",
        symbol="BTCUSDT",
        timeframe="5m",
        field="volume",
    )

    assert row["classification"] == "real"
    assert row["coverage_ratio"] == 1.0


def test_mechanism_data_blocked_when_core_observable_missing():
    rows = [
        _row(field, "real")
        for field in [
            "funding_rate_scaled",
            "funding_abs_pct",
            "oi_notional",
            "rv_96",
            "basis_zscore",
            "volume",
        ]
    ]
    rows[0] = _row("funding_rate_scaled", "missing")
    payload = build_mechanism_data_quality(pd.DataFrame(rows, columns=FIELD_COLUMNS), run_id="audit")

    funding = next(item for item in payload["mechanisms"] if item["mechanism_id"] == "funding_squeeze")
    assert funding["data_quality_decision"] == "data_blocked"
    assert funding["blocked_fields"] == ["funding_rate_scaled"]


def test_mechanism_paper_blocked_when_core_observable_proxy():
    rows = [
        _row(field, "real")
        for field in [
            "funding_rate_scaled",
            "funding_abs_pct",
            "oi_notional",
            "rv_96",
            "basis_zscore",
            "volume",
        ]
    ]
    rows[-2] = _row("basis_zscore", "proxy")
    payload = build_mechanism_data_quality(pd.DataFrame(rows, columns=FIELD_COLUMNS), run_id="audit")

    funding = next(item for item in payload["mechanisms"] if item["mechanism_id"] == "funding_squeeze")
    assert funding["data_quality_decision"] == "paper_blocked"
    assert funding["proxy_fields"] == ["basis_zscore"]
