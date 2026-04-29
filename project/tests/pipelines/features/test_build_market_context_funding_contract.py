from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

from project.pipelines.features import build_market_context


def _feature_frame() -> pd.DataFrame:
    return pd.DataFrame(
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
            "close": [100.0, 101.0, 100.5, 101.5],
            "rv_96": [0.1, 0.2, 0.15, 0.22],
            "rv_pct_17280": [0.2, 0.4, 0.6, 0.8],
            "range_96": [2.0, 2.0, 2.0, 2.0],
            "range_med_2880": [4.0, 4.0, 4.0, 4.0],
            "spot_close": [99.9, 100.9, 100.4, 101.4],
        }
    )


def test_build_market_context_uses_canonical_funding_rate_scaled():
    features = _feature_frame()
    features["funding_rate_scaled"] = [0.0002, -0.0002, 0.0003, -0.0003]
    features["funding_rate"] = [999.0, 999.0, 999.0, 999.0]

    out = build_market_context._build_market_context(symbol="BTCUSDT", features=features)

    assert out["funding_rate_bps"].tolist() == pytest.approx([2.0, -2.0, 3.0, -3.0])
    assert set(out["carry_state_code"].tolist()) == {1.0, -1.0}
    assert out["carry_state"].tolist() == ["funding_pos", "funding_neg", "funding_pos", "funding_neg"]


def test_build_market_context_requires_funding_rate_scaled_column():
    features = _feature_frame()
    features["funding_rate"] = [0.0002, -0.0002, 0.0003, -0.0003]

    with pytest.raises(ValueError, match="missing funding_rate_scaled"):
        build_market_context._build_market_context(symbol="BTCUSDT", features=features)


def test_build_market_context_handles_funding_gaps(caplog):
    features = _feature_frame()
    features["funding_rate_scaled"] = [0.0002, None, 0.0003, -0.0003]

    with caplog.at_level("WARNING"):
        out = build_market_context._build_market_context(symbol="BTCUSDT", features=features)

    assert "funding_rate_scaled contains 1/4 missing rows (25.00%) for BTCUSDT" in caplog.text
    # Should be filled with 0.0
    assert out.iloc[1]["funding_rate_scaled"] == 0.0
    assert out.iloc[1]["funding_rate_bps"] == 0.0


def test_build_market_context_handles_fully_missing_funding(caplog):
    features = _feature_frame()
    features["funding_rate_scaled"] = [None, None, None, None]

    with caplog.at_level("WARNING"):
        out = build_market_context._build_market_context(symbol="BTCUSDT", features=features)

    assert (
        "funding_rate_scaled unavailable for BTCUSDT; defaulting all 4/4 rows to 0.0" in caplog.text
    )
    assert out["funding_rate_scaled"].tolist() == [0.0, 0.0, 0.0, 0.0]
    assert out["carry_state_code"].tolist() == [0.0, 0.0, 0.0, 0.0]
    assert out["carry_state"].tolist() == ["neutral", "neutral", "neutral", "neutral"]


def test_build_market_context_materializes_canonical_state_columns():
    features = _feature_frame()
    features["funding_rate_scaled"] = [0.0002, -0.0002, 0.0003, -0.0003]
    features["spread_zscore"] = [0.5, 2.0, 2.5, 1.0]
    features["oi_notional"] = [100.0, 150.0, 200.0, 250.0]
    features["oi_delta_1h"] = [-10.0, -20.0, -5.0, -40.0]
    features["quote_volume"] = [1000.0, 1100.0, 1200.0, 1300.0]

    out = build_market_context._build_market_context(symbol="BTCUSDT", features=features)

    expected_state_cols = {
        "low_liquidity_state",
        "spread_elevated_state",
        "refill_lag_state",
        "aftershock_state",
        "compression_state_flag",
        "vol_regime",
        "vol_regime_code",
        "carry_state",
        "high_vol_regime",
        "low_vol_regime",
        "crowding_state",
        "funding_persistence_state",
        "deleveraging_state",
        "ms_vol_state",
        "ms_liq_state",
        "ms_oi_state",
        "ms_funding_state",
        "ms_trend_state",
        "ms_spread_state",
        "ms_context_state_code",
        "fp_active",
        "fp_age_bars",
        "fp_severity",
        "prob_vol_low",
        "prob_vol_mid",
        "prob_vol_high",
        "prob_vol_shock",
        "ms_vol_confidence",
        "ms_vol_entropy",
        "prob_liq_thin",
        "prob_liq_normal",
        "prob_liq_flush",
        "ms_liq_confidence",
        "ms_liq_entropy",
        "prob_oi_decel",
        "prob_oi_stable",
        "prob_oi_accel",
        "ms_oi_confidence",
        "ms_oi_entropy",
        "prob_funding_neutral",
        "prob_funding_persistent",
        "prob_funding_extreme",
        "ms_funding_confidence",
        "ms_funding_entropy",
        "prob_trend_chop",
        "prob_trend_bull",
        "prob_trend_bear",
        "ms_trend_confidence",
        "ms_trend_entropy",
        "prob_spread_tight",
        "prob_spread_wide",
        "ms_spread_confidence",
        "ms_spread_entropy",
        "close_perp",
        "close_spot",
        "forced_flow_phase",
        "funding_phase",
        "funding_regime",
        "liquidity_phase",
        "liquidity_regime",
        "oi_phase",
        "price_oi_quadrant",
    }
    assert expected_state_cols.issubset(set(out.columns))
    assert out["ms_context_state_code"].notna().all()
    assert out["vol_regime_code"].equals(out["ms_vol_state"])
    assert out["carry_state"].tolist() == ["funding_pos", "funding_neg", "funding_pos", "funding_neg"]
    assert out["close_perp"].tolist() == pytest.approx(features["close"].tolist())
    assert out["close_spot"].tolist() == pytest.approx(features["spot_close"].tolist())
    assert "price_down_oi_down" in set(out["price_oi_quadrant"])
    assert set(out["forced_flow_phase"]).issubset({"none", "cascade", "cooldown", "refill"})
    assert set(out["funding_phase"]).issubset(
        {"neutral", "positive_onset", "negative_onset", "positive_persistent", "negative_persistent"}
    )
    assert set(out["liquidity_phase"]).issubset({"normal", "thin", "collapse", "refill", "recovered"})
    assert set(out["oi_phase"]).issubset({"neutral", "expansion", "flush"})

    probability_columns = [
        ["prob_vol_low", "prob_vol_mid", "prob_vol_high", "prob_vol_shock"],
        ["prob_liq_thin", "prob_liq_normal", "prob_liq_flush"],
        ["prob_oi_decel", "prob_oi_stable", "prob_oi_accel"],
        ["prob_funding_neutral", "prob_funding_persistent", "prob_funding_extreme"],
        ["prob_trend_chop", "prob_trend_bull", "prob_trend_bear"],
        ["prob_spread_tight", "prob_spread_wide"],
    ]
    for cols in probability_columns:
        valid = out[cols].dropna()
        assert not valid.empty
        assert (valid.sum(axis=1) - 1.0).abs().max() < 1e-6


def test_build_market_context_normalizes_timestamp_dtypes_before_merge():
    bars = _feature_frame()
    funding = pd.DataFrame(
        {
            "timestamp": [
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:05:00Z",
                "2026-01-01T00:10:00Z",
                "2026-01-01T00:15:00Z",
            ],
            "funding_rate_scaled": [0.0002, -0.0002, 0.0003, -0.0003],
        }
    )

    out = build_market_context.build_market_context(bars, funding, symbol="BTCUSDT")

    assert str(out["timestamp"].dtype) == "datetime64[ns, UTC]"
    assert out["funding_rate_scaled"].tolist() == pytest.approx([0.0002, -0.0002, 0.0003, -0.0003])


def test_build_market_context_rejects_missing_or_all_null_timestamps():
    features = _feature_frame().drop(columns=["timestamp"])
    features["funding_rate_scaled"] = [0.0002, -0.0002, 0.0003, -0.0003]

    with pytest.raises(ValueError, match="missing timestamp column"):
        build_market_context._build_market_context(symbol="BTCUSDT", features=features)

    bad = _feature_frame()
    bad["timestamp"] = [None, None, None, None]
    bad["funding_rate_scaled"] = [0.0002, -0.0002, 0.0003, -0.0003]

    with pytest.raises(ValueError, match="normalized to all-null timestamps"):
        build_market_context._build_market_context(symbol="BTCUSDT", features=bad)


def test_main_writes_context_quality_report(monkeypatch, tmp_path):
    features = _feature_frame()
    features["funding_rate_scaled"] = [0.0002, -0.0002, 0.0003, -0.0003]
    features["spread_zscore"] = [0.5, 2.0, 2.5, 1.0]
    features["oi_notional"] = [100.0, 150.0, 200.0, 250.0]
    features["oi_delta_1h"] = [-10.0, -20.0, -5.0, -40.0]
    features["quote_volume"] = [1000.0, 1100.0, 1200.0, 1300.0]
    finalized: dict[str, object] = {}

    monkeypatch.setattr(build_market_context, "get_data_root", lambda: tmp_path / "data")
    monkeypatch.setattr(build_market_context, "start_manifest", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        build_market_context,
        "finalize_manifest",
        lambda *args, **kwargs: finalized.setdefault("stats", kwargs.get("stats")),
    )

    def fake_choose_partition_dir(paths):
        for path in paths:
            if "features/perp/BTCUSDT/5m" in str(path):
                return path
        return None

    monkeypatch.setattr(build_market_context, "choose_partition_dir", fake_choose_partition_dir)
    monkeypatch.setattr(
        build_market_context, "list_parquet_files", lambda _path: [Path("dummy.parquet")]
    )
    monkeypatch.setattr(build_market_context, "read_parquet", lambda _files: features.copy())
    monkeypatch.setattr(build_market_context, "write_parquet", lambda _df, path: path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_market_context.py",
            "--run_id",
            "r_context_quality",
            "--symbols",
            "BTCUSDT",
            "--timeframe",
            "5m",
        ],
    )

    rc = build_market_context.main()

    assert rc == 0
    report_path = Path(finalized["stats"]["symbols"]["BTCUSDT"]["context_quality_report_path"])
    assert report_path.exists()
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "context_quality_report_v1"
    assert payload["symbol"] == "BTCUSDT"
    assert payload["quality"]["dimension_count"] == 6
    assert "vol" in payload["quality"]["dimensions"]


def test_main_records_market_context_outputs_in_manifest(monkeypatch, tmp_path):
    features = _feature_frame()
    features["funding_rate_scaled"] = [0.0002, -0.0002, 0.0003, -0.0003]
    captured_manifest: dict[str, object] = {}

    monkeypatch.setattr(build_market_context, "get_data_root", lambda: tmp_path / "data")
    monkeypatch.setattr(build_market_context, "start_manifest", lambda *args, **kwargs: captured_manifest)
    monkeypatch.setattr(build_market_context, "finalize_manifest", lambda *args, **kwargs: None)
    monkeypatch.setattr(build_market_context, "choose_partition_dir", lambda paths: paths[0])
    monkeypatch.setattr(
        build_market_context, "list_parquet_files", lambda _path: [Path("dummy.parquet")]
    )
    monkeypatch.setattr(build_market_context, "read_parquet", lambda _files: features.copy())
    monkeypatch.setattr(build_market_context, "write_parquet", lambda _df, path: path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_market_context.py",
            "--run_id",
            "r_outputs",
            "--symbols",
            "BTCUSDT",
            "--timeframe",
            "5m",
        ],
    )

    rc = build_market_context.main()

    assert rc == 0
    outputs = captured_manifest.get("outputs")
    assert isinstance(outputs, list)
    assert any("market_context_BTCUSDT_2026-01.parquet" in item["path"] for item in outputs)
    assert any("context_quality_report_v1.json" in item["path"] for item in outputs)
