import pytest

from project.specs.manifest import validate_feature_schema_columns


def test_feature_schema_v2_enforcement():
    baseline_cols = [
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "taker_base_volume",
        "funding_rate_scaled",
        "funding_rate",
        "oi_notional",
        "oi_delta_1h",
        "liquidation_notional",
        "liquidation_count",
        "basis_bps",
        "basis_zscore",
        "cross_exchange_spread_z",
        "spread_zscore",
        "revision_lag_bars",
        "revision_lag_minutes",
        "logret_1",
        "rv_96",
        "rv_pct_17280",
        "high_96",
        "low_96",
        "range_96",
        "range_med_2880",
        "ms_vpin_24",
        "ms_roll_24",
        "ms_amihud_24",
        "ms_kyle_24",
    ]

    with pytest.raises(
        ValueError, match="Feature schema contract violated for features_v2_5m; missing columns"
    ):
        validate_feature_schema_columns(dataset_key="features_v2_5m", columns=baseline_cols)

    try:
        validate_feature_schema_columns(dataset_key="features_v2_5m", columns=baseline_cols)
    except ValueError as e:
        error_msg = str(e)
        assert "funding_rate_realized" in error_msg
        assert "is_gap" in error_msg
        assert "spread_bps" not in error_msg

    v2_cols = baseline_cols + ["funding_rate_realized", "is_gap"]
    validate_feature_schema_columns(dataset_key="features_v2_5m", columns=v2_cols)


if __name__ == "__main__":
    pytest.main([__file__])
