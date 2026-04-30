from __future__ import annotations

import pandas as pd

from project.research.regime_scorecard import (
    build_regime_scorecard,
    classify_regime_scorecard,
    select_baseline_result_paths,
)


def _row(
    *,
    run_id: str = "run_a",
    matrix_id: str = "core_v1",
    regime_id: str = "vol_regime=high",
    symbol: str = "BTCUSDT",
    direction: str = "long",
    horizon_bars: int = 24,
    classification: str = "insufficient_support",
    mean_net_bps: float | None = None,
    t_stat_net: float | None = None,
    max_year_pnl_share: float | None = None,
    effective_n: int = 0,
    proposal_path_eligible: bool = True,
) -> dict:
    return {
        "run_id": run_id,
        "matrix_id": matrix_id,
        "regime_id": regime_id,
        "symbol": symbol,
        "direction": direction,
        "horizon_bars": horizon_bars,
        "classification": classification,
        "mean_net_bps": mean_net_bps,
        "t_stat_net": t_stat_net,
        "max_year_pnl_share": max_year_pnl_share,
        "effective_n": effective_n,
        "proposal_path_eligible": proposal_path_eligible,
    }


def test_classify_regime_scorecard_uses_strict_propagation():
    assert classify_regime_scorecard({"stable_positive": 1}) == (
        "stable_positive",
        "allow_event_lift",
        "run_event_lift_for_best_tuple",
    )
    assert classify_regime_scorecard({"year_conditional": 1})[0] == "year_conditional"
    assert classify_regime_scorecard({"unstable": 1})[1] == "monitor_only"
    assert classify_regime_scorecard({"negative": 2, "insufficient_support": 1}) == (
        "insufficient_support",
        "data_repair",
        "repair_context_or_price_cost_data",
    )
    assert classify_regime_scorecard({"negative": 2, "insufficient_support": 0}) == (
        "negative",
        "reject_directional",
        "reject_directional_event_search_for_regime",
    )


def test_build_regime_scorecard_counts_and_selects_best_row():
    df = pd.DataFrame(
        [
            _row(classification="negative", mean_net_bps=-1.0, t_stat_net=-2.0, effective_n=200),
            _row(
                symbol="ETHUSDT",
                direction="short",
                horizon_bars=48,
                classification="stable_positive",
                mean_net_bps=2.0,
                t_stat_net=1.6,
                max_year_pnl_share=0.4,
                effective_n=80,
            ),
            _row(
                symbol="BTCUSDT",
                direction="short",
                horizon_bars=12,
                classification="year_conditional",
                mean_net_bps=5.0,
                t_stat_net=3.0,
                max_year_pnl_share=0.7,
                effective_n=90,
            ),
        ]
    )

    scorecard = build_regime_scorecard(df)
    row = scorecard.iloc[0].to_dict()

    assert row["schema_version"] == "regime_scorecard_v1"
    assert row["candidate_baseline_count"] == 3
    assert row["stable_positive_count"] == 1
    assert row["year_conditional_count"] == 1
    assert row["negative_count"] == 1
    assert row["classification"] == "stable_positive"
    assert row["decision"] == "allow_event_lift"
    assert row["proposal_path_eligible"] is True
    assert row["best_symbol"] == "ETHUSDT"
    assert row["best_direction"] == "short"
    assert row["best_horizon_bars"] == 48


def test_best_row_numeric_tiebreaks_are_deterministic():
    df = pd.DataFrame(
        [
            _row(
                symbol="ETHUSDT",
                direction="short",
                horizon_bars=48,
                classification="unstable",
                mean_net_bps=1.0,
                t_stat_net=1.0,
                max_year_pnl_share=None,
                effective_n=50,
            ),
            _row(
                symbol="BTCUSDT",
                direction="long",
                horizon_bars=24,
                classification="unstable",
                mean_net_bps=1.0,
                t_stat_net=1.0,
                max_year_pnl_share=0.4,
                effective_n=50,
            ),
        ]
    )

    row = build_regime_scorecard(df).iloc[0].to_dict()

    assert row["best_symbol"] == "BTCUSDT"
    assert row["best_max_year_pnl_share"] == 0.4


def test_diagnostic_regime_cannot_allow_event_lift_even_if_stable_positive():
    df = pd.DataFrame(
        [
            _row(
                matrix_id="funding_squeeze_positioning_v1",
                regime_id="funding_phase=negative_persistent+oi_phase=expansion",
                classification="stable_positive",
                mean_net_bps=5.0,
                t_stat_net=2.0,
                max_year_pnl_share=0.3,
                effective_n=100,
                proposal_path_eligible=False,
            )
        ]
    )

    row = build_regime_scorecard(df).iloc[0].to_dict()

    assert row["classification"] == "stable_positive"
    assert row["decision"] == "diagnostic_only"
    assert row["next_action"] == "primary_regime_must_pass_before_event_lift"
    assert row["proposal_path_eligible"] is False


def test_select_baseline_result_paths_prefers_latest_per_matrix(tmp_path):
    base = tmp_path / "reports" / "regime_baselines"
    for run_id in ["run_20240101", "run_20240201"]:
        out = base / run_id
        out.mkdir(parents=True)
        pd.DataFrame([_row(run_id=run_id)]).to_parquet(out / "regime_baselines.parquet")

    selected = select_baseline_result_paths(data_root=tmp_path)

    assert [path.parent.name for path in selected] == ["run_20240201"]
    assert {path.parent.name for path in select_baseline_result_paths(data_root=tmp_path, all_runs=True)} == {
        "run_20240101",
        "run_20240201",
    }
