from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from project.research.regime_baselines import (
    CORE_V1_REGIMES,
    RegimeBaselineRequest,
    build_search_burden,
    core_v1_matrix,
    evaluate_regime_baseline,
    regime_id,
    run_regime_baselines,
    validate_regime_filters,
)


def _feature_frame(n: int = 180, *, drift: float = 1.0) -> pd.DataFrame:
    ts = pd.date_range("2022-01-01", periods=n, freq="5min", tz="UTC")
    close = [100.0 + drift * i for i in range(n)]
    return pd.DataFrame(
        {
            "timestamp": ts,
            "close": close,
            "vol_regime": ["high"] * n,
            "carry_state": ["funding_neg"] * n,
            "ms_trend_state": [1.0] * n,
            "spread_bps": [1.0] * n,
        }
    )


def test_core_v1_matrix_is_predeclared_and_registry_native():
    matrix = core_v1_matrix()

    assert len(matrix) == 9
    assert matrix == [dict(item) for item in CORE_V1_REGIMES]
    assert regime_id({"vol_regime": "high", "carry_state": "funding_neg"}) == (
        "vol_regime=high+carry_state=funding_neg"
    )
    for filters in matrix:
        validate_regime_filters(filters)


def test_validate_regime_filters_rejects_display_or_lookahead_labels():
    with pytest.raises(ValueError, match="registry-native lowercase"):
        validate_regime_filters({"VOL_REGIME": "HIGH"})
    with pytest.raises(ValueError, match="lookahead-prone"):
        validate_regime_filters({"forced_flow_phase": "rebound_confirmed"})


def test_evaluate_regime_baseline_computes_stable_positive_metrics():
    request = RegimeBaselineRequest(
        run_id="test_run",
        matrix_id="core_v1",
        symbols=("BTCUSDT",),
        horizons=(1,),
        data_root=Path("data"),
    )
    features = _feature_frame()

    row = evaluate_regime_baseline(
        request,
        features=features,
        filters={"vol_regime": "high"},
        symbol="BTCUSDT",
        direction="long",
        horizon_bars=1,
    )

    assert row["schema_version"] == "regime_baseline_v1"
    assert row["n"] >= 100
    assert row["effective_n"] >= 50
    assert row["mean_gross_bps"] > 0
    assert row["mean_net_bps_2x_cost"] is not None
    assert row["classification"] in {"stable_positive", "year_conditional", "unstable"}
    assert row["decision"] in {"advance_to_event_lift", "park", "monitor"}


def test_evaluate_regime_baseline_marks_missing_context_as_data_repair():
    request = RegimeBaselineRequest(
        run_id="test_run",
        matrix_id="core_v1",
        symbols=("BTCUSDT",),
        horizons=(24,),
        data_root=Path("data"),
    )
    features = _feature_frame().drop(columns=["carry_state"])

    row = evaluate_regime_baseline(
        request,
        features=features,
        filters={"carry_state": "funding_neg"},
        symbol="BTCUSDT",
        direction="long",
        horizon_bars=24,
    )

    assert row["classification"] == "insufficient_support"
    assert row["decision"] == "data_repair"
    assert row["reason"] == "missing context columns: carry_state"


def test_build_search_burden_counts_full_matrix():
    request = RegimeBaselineRequest(
        run_id="test_run",
        matrix_id="core_v1",
        symbols=("BTCUSDT", "ETHUSDT"),
        horizons=(12, 24, 48),
    )

    burden = build_search_burden(request, num_regimes=9)

    assert burden == {
        "schema_version": "regime_search_burden_v1",
        "run_id": "test_run",
        "matrix_id": "core_v1",
        "predeclared": True,
        "num_regimes": 9,
        "num_symbols": 2,
        "num_directions": 2,
        "num_horizons": 3,
        "num_tests": 108,
    }


def test_run_regime_baselines_emits_full_grid_without_data(tmp_path):
    request = RegimeBaselineRequest(
        run_id="test_run",
        matrix_id="core_v1",
        symbols=("BTCUSDT", "ETHUSDT"),
        horizons=(12, 24, 48),
        data_root=tmp_path,
    )

    df, burden, source_run_id = run_regime_baselines(request)

    assert source_run_id is None
    assert len(df) == 108
    assert burden["num_tests"] == 108
    assert set(df["classification"]) == {"insufficient_support"}
