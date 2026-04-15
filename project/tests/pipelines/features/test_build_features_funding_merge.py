from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from project.pipelines.features import build_features


def _bars() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z", "2026-01-01T00:10:00Z"],
                utc=True,
            ),
            "open": [1.0, 1.0, 1.0],
            "high": [1.0, 1.0, 1.0],
            "low": [1.0, 1.0, 1.0],
            "close": [1.0, 1.0, 1.0],
        }
    )


def test_revision_lag_minutes_uses_active_timeframe_size():
    assert build_features._revision_lag_minutes(0, timeframe="5m") == 0
    assert build_features._revision_lag_minutes(1, timeframe="5m") == 5
    assert build_features._revision_lag_minutes(3, timeframe="5m") == 15
    assert build_features._revision_lag_minutes(3, timeframe="1m") == 3


def test_duration_windows_translate_to_timeframe_specific_bar_counts():
    assert build_features._duration_to_bars(minutes=60, timeframe="5m") == 12
    assert build_features._duration_to_bars(minutes=60, timeframe="1m") == 60


def test_merge_funding_rates_deduplicates_timestamps():
    funding = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"], utc=True),
            "funding_rate_scaled": [0.01, 0.02],
        }
    )

    out = build_features._merge_funding_rates(_bars(), funding, symbol="BTCUSDT")
    # Should keep the last one (0.02)
    assert out.iloc[0]["funding_rate_scaled"] == 0.02


def test_merge_funding_rates_preserves_row_count():
    funding = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:10:00Z"], utc=True),
            "funding_rate_scaled": [0.01, 0.03],
        }
    )

    out = build_features._merge_funding_rates(_bars(), funding, symbol="BTCUSDT")

    assert len(out) == 3
    assert "funding_rate_scaled" in out.columns


def test_merge_funding_rates_uses_backward_asof_for_sparse_updates():
    bars = _bars()
    bars = (
        pd.concat(
            [
                bars,
                pd.DataFrame(
                    {
                        "timestamp": pd.to_datetime(
                            ["2026-01-01T00:15:00Z", "2026-01-01T00:20:00Z"], utc=True
                        ),
                        "open": [1.0, 1.0],
                        "high": [1.0, 1.0],
                        "low": [1.0, 1.0],
                        "close": [1.0, 1.0],
                    }
                ),
            ],
            ignore_index=True,
        )
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    funding = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
            "funding_rate_scaled": [0.01],
        }
    )

    out = build_features._merge_funding_rates(bars, funding, symbol="BTCUSDT")

    assert out["funding_rate_scaled"].notna().all()
    assert set(out["funding_rate_scaled"].round(8).tolist()) == {0.01}


def test_merge_funding_rates_respects_max_staleness():
    bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T12:00:00Z"], utc=True),
            "open": [1.0, 1.0],
            "high": [1.0, 1.0],
            "low": [1.0, 1.0],
            "close": [1.0, 1.0],
        }
    )
    funding = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
            "funding_rate_scaled": [0.01],
        }
    )

    out = build_features._merge_funding_rates(bars, funding, symbol="BTCUSDT")

    assert out.loc[0, "funding_rate_scaled"] == 0.01
    assert pd.isna(out.loc[1, "funding_rate_scaled"])


def test_merge_funding_rates_normalizes_timestamp_units():
    bars = _bars().copy()
    bars["timestamp"] = bars["timestamp"].astype("datetime64[us, UTC]")
    funding = pd.DataFrame(
        {
            "timestamp": build_features.ts_ns_utc(
                pd.Series(pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True))
            ),
            "funding_rate_scaled": [0.01],
        }
    )

    out = build_features._merge_funding_rates(bars, funding, symbol="BTCUSDT")
    assert len(out) == len(bars)
    assert out["funding_rate_scaled"].notna().all()


def test_merge_funding_rates_preserves_funding_rate_feature_fallback_column():
    funding = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:10:00Z"], utc=True),
            "funding_rate_feature": [0.0015, -0.0025],
        }
    )

    out = build_features._merge_funding_rates(_bars(), funding, symbol="BTCUSDT")

    assert "funding_rate_feature" in out.columns
    assert out["funding_rate_feature"].tolist() == pytest.approx([0.0015, 0.0015, -0.0025])


def test_merge_funding_rates_prefers_existing_cleaned_bar_funding_columns():
    bars = _bars().copy()
    bars["funding_rate_feature"] = [0.001, 0.001, -0.002]
    bars["funding_rate_scaled"] = [0.001, 0.001, np.nan]
    funding = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
            "funding_rate_scaled": [0.02],
        }
    )

    out = build_features._merge_funding_rates(bars, funding, symbol="BTCUSDT")

    assert "funding_rate_scaled_x" not in out.columns
    assert "funding_rate_scaled_y" not in out.columns
    assert out["funding_rate_scaled"].tolist() == pytest.approx([0.001, 0.001, -0.002])


def test_safe_logret_1_avoids_inf_for_non_positive_prices():
    close = pd.Series([100.0, 101.0, 0.0, 102.0, 103.0])
    out = build_features._safe_logret_1(close)
    assert pd.notna(out.iloc[1])
    assert pd.isna(out.iloc[2])
    assert pd.isna(out.iloc[3])
    assert pd.notna(out.iloc[4])
    assert np.isfinite(out.dropna()).all()


def test_ensure_feature_contract_columns_materializes_schema_columns():
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z"], utc=True),
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.5, 101.5],
            "volume": [10.0, 12.0],
            "basis_bps": [1.0, 2.0],
            "basis_zscore": [0.1, 0.2],
            "spread_zscore": [0.3, 0.4],
            "funding_rate_scaled": [0.0001, 0.0002],
        }
    )

    out = build_features._ensure_feature_contract_columns(frame, timeframe="5m")

    required = {
        "quote_volume",
        "taker_base_volume",
        "funding_rate",
        "funding_rate_realized",
        "is_gap",
        "cross_exchange_spread_z",
        "revision_lag_bars",
        "revision_lag_minutes",
    }
    assert required.issubset(set(out.columns))


def test_merge_optional_microstructure_inputs_derives_proxy_columns_without_tob():
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:05:00Z",
                    "2026-01-01T00:10:00Z",
                    "2026-01-01T00:15:00Z",
                ],
                utc=True,
            ),
            "open": [100.0, 101.0, 102.0, 103.0],
            "high": [101.0, 102.0, 103.0, 104.0],
            "low": [99.0, 100.0, 101.0, 102.0],
            "close": [100.5, 101.5, 102.5, 103.5],
            "volume": [10.0, 12.0, 11.0, 13.0],
            "taker_base_volume": [6.0, 7.0, 6.5, 8.0],
            "ms_imbalance_24": [0.0, 0.1, -0.2, 0.3],
        }
    )

    out = build_features._merge_optional_microstructure_inputs(
        frame,
        symbol="BTCUSDT",
        market="perp",
        run_id="unit_test",
        data_root=build_features.Path("/tmp"),
        timeframe="5m",
    )

    required = {
        "spread_bps",
        "depth_usd",
        "bid_depth_usd",
        "ask_depth_usd",
        "imbalance",
        "micro_depth_depletion",
    }
    assert required.issubset(out.columns)
    assert out["imbalance"].round(6).tolist() == [0.0, 0.1, -0.2, 0.3]
    assert out["spread_bps"].notna().sum() >= 2
    assert out["depth_usd"].notna().sum() >= 2


def test_blank_feature_schema_version_defaults_to_canonical():
    assert build_features.normalize_feature_schema_version("") == "feature_schema_v2"


def test_build_features_derives_funding_magnitude_from_funding_rate_feature_fallback():
    timestamps = pd.date_range("2026-01-01T00:00:00Z", periods=10, freq="5min", tz="UTC")
    bars = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": np.linspace(100.0, 109.0, num=10),
            "high": np.linspace(101.0, 110.0, num=10),
            "low": np.linspace(99.0, 108.0, num=10),
            "close": np.linspace(100.5, 109.5, num=10),
            "volume": np.linspace(10.0, 19.0, num=10),
        }
    )
    funding = pd.DataFrame(
        {
            "timestamp": bars["timestamp"],
            "funding_rate_feature": [
                0.0,
                0.0015,
                -0.0025,
                0.0035,
                -0.001,
                0.002,
                -0.003,
                0.001,
                -0.0005,
                0.0025,
            ],
        }
    )

    out = build_features.build_features(
        bars,
        funding,
        symbol="BTCUSDT",
        run_id="unit_test",
        data_root=build_features.Path("/tmp"),
        timeframe="5m",
    )

    assert out["funding_rate_scaled"].tolist() == pytest.approx(funding["funding_rate_feature"].tolist())
    assert out["funding_abs"].iloc[2] == pytest.approx(0.0015)
    assert out["funding_abs"].iloc[3] == pytest.approx(0.0025)
    assert out["funding_abs_pct"].max() > 0.0
