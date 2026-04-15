from __future__ import annotations

import pandas as pd

from project.research.search.bridge_adapter import hypotheses_to_bridge_candidates
from project.research.multiplicity import make_family_id


def test_hypotheses_to_bridge_candidates_respects_configured_bridge_thresholds():
    metrics = pd.DataFrame(
        [
            {
                "hypothesis_id": "hyp_1",
                "trigger_type": "event",
                "trigger_key": "event:VOL_SHOCK",
                "direction": "short",
                "horizon": "60m",
                "template_id": "continuation",
                "entry_lag": 2,
                "entry_lag_bars": 2,
                "n": 64,
                "train_n_obs": 30,
                "validation_n_obs": 16,
                "test_n_obs": 18,
                "validation_samples": 16,
                "test_samples": 18,
                "mean_return_bps": 85_000.0,
                "t_stat": 1.9,
                "sharpe": 1.2,
                "hit_rate": 0.55,
                "cost_adjusted_return_bps": 9.0,
                "p_value_raw": 0.04,
                "p_value_for_fdr": 0.04,
                "mae_mean_bps": -12.0,
                "mfe_mean_bps": 23.0,
                "robustness_score": 0.65,
                "stress_score": 0.4,
                "kill_switch_count": 0,
                "capacity_proxy": 1.0,
                "valid": True,
                "invalid_reason": None,
            }
        ]
    )

    default = hypotheses_to_bridge_candidates(metrics)
    relaxed = hypotheses_to_bridge_candidates(
        metrics,
        bridge_min_t_stat=1.8,
        bridge_min_robustness_score=0.65,
        bridge_min_regime_stability_score=0.6,
        bridge_min_stress_survival=0.4,
        bridge_stress_cost_buffer_bps=1.0,
    )

    assert bool(default.iloc[0]["gate_bridge_tradable"]) is False
    assert bool(relaxed.iloc[0]["gate_bridge_tradable"]) is True
    assert int(relaxed.iloc[0]["entry_lag"]) == 2
    assert int(relaxed.iloc[0]["entry_lag_bars"]) == 2
    assert bool(default.iloc[0]["gate_oos_validation"]) is False
    assert bool(relaxed.iloc[0]["gate_oos_validation"]) is True
    assert float(relaxed.iloc[0]["p_value"]) == 0.04
    assert relaxed.iloc[0]["candidate_id"] != relaxed.iloc[0]["hypothesis_id"]
    assert relaxed.iloc[0]["research_family"] == relaxed.iloc[0]["canonical_family"]
    assert relaxed.iloc[0]["family_id"] == make_family_id(
        "ALL",
        relaxed.iloc[0]["canonical_event_type"],
        "continuation",
        "60m",
        "",
        canonical_family=relaxed.iloc[0]["canonical_family"],
    )


def test_hypotheses_to_bridge_candidates_can_keep_min_t_failures_for_multiplicity_universe():
    metrics = pd.DataFrame(
        [
            {
                "hypothesis_id": "hyp_low_t",
                "trigger_type": "event",
                "trigger_key": "event:VOL_SHOCK",
                "direction": "short",
                "horizon": "60m",
                "template_id": "continuation",
                "n": 64,
                "train_n_obs": 30,
                "validation_n_obs": 16,
                "test_n_obs": 18,
                "validation_samples": 16,
                "test_samples": 18,
                "mean_return_bps": 8.5,
                "t_stat": 1.0,
                "sharpe": 1.2,
                "hit_rate": 0.55,
                "cost_adjusted_return_bps": 6.5,
                "p_value_raw": 0.2,
                "p_value_for_fdr": 0.2,
                "mae_mean_bps": -12.0,
                "mfe_mean_bps": 23.0,
                "robustness_score": 0.8,
                "stress_score": 0.8,
                "kill_switch_count": 0,
                "capacity_proxy": 1.0,
                "valid": True,
                "invalid_reason": None,
            }
        ]
    )

    prefiltered = hypotheses_to_bridge_candidates(metrics)
    universe = hypotheses_to_bridge_candidates(metrics, prefilter_min_t_stat=False)

    assert prefiltered.empty
    assert len(universe) == 1
    assert bool(universe.iloc[0]["gate_search_min_sample_size"]) is True
    assert bool(universe.iloc[0]["gate_search_min_t_stat"]) is False


def test_hypotheses_to_bridge_candidates_preserves_event_labels_with_nonconsecutive_indices():
    metrics = pd.DataFrame(
        [
            {
                "hypothesis_id": "gap_mean_12",
                "trigger_type": "event",
                "trigger_key": "event:LIQUIDITY_GAP_PRINT",
                "direction": "long",
                "horizon": "12b",
                "template_id": "mean_reversion",
                "n": 857,
                "train_n_obs": 300,
                "validation_n_obs": 250,
                "test_n_obs": 307,
                "validation_samples": 250,
                "test_samples": 307,
                "mean_return_bps": -10.6634,
                "t_stat": -1.6605,
                "cost_adjusted_return_bps": -8.6634,
                "p_value_raw": 0.096814,
                "p_value_for_fdr": 0.096814,
                "mae_mean_bps": -12.0,
                "mfe_mean_bps": 23.0,
                "robustness_score": 0.8,
                "stress_score": 0.8,
                "kill_switch_count": 0,
                "capacity_proxy": 1.0,
                "valid": True,
                "invalid_reason": None,
            },
            {
                "hypothesis_id": "gap_cont_12",
                "trigger_type": "event",
                "trigger_key": "event:LIQUIDITY_GAP_PRINT",
                "direction": "long",
                "horizon": "12b",
                "template_id": "continuation",
                "n": 857,
                "train_n_obs": 300,
                "validation_n_obs": 250,
                "test_n_obs": 307,
                "validation_samples": 250,
                "test_samples": 307,
                "mean_return_bps": 10.6634,
                "t_stat": 1.6605,
                "cost_adjusted_return_bps": 8.6634,
                "p_value_raw": 0.096814,
                "p_value_for_fdr": 0.096814,
                "mae_mean_bps": -12.0,
                "mfe_mean_bps": 23.0,
                "robustness_score": 0.8,
                "stress_score": 0.8,
                "kill_switch_count": 0,
                "capacity_proxy": 1.0,
                "valid": True,
                "invalid_reason": None,
            },
            {
                "hypothesis_id": "spread_mean_12",
                "trigger_type": "event",
                "trigger_key": "event:SPREAD_BLOWOUT",
                "direction": "long",
                "horizon": "12b",
                "template_id": "mean_reversion",
                "n": 2139,
                "train_n_obs": 700,
                "validation_n_obs": 700,
                "test_n_obs": 739,
                "validation_samples": 700,
                "test_samples": 739,
                "mean_return_bps": 3.3104,
                "t_stat": 1.0483,
                "cost_adjusted_return_bps": 1.3104,
                "p_value_raw": 0.294,
                "p_value_for_fdr": 0.294,
                "mae_mean_bps": -8.0,
                "mfe_mean_bps": 12.0,
                "robustness_score": 0.8,
                "stress_score": 0.8,
                "kill_switch_count": 0,
                "capacity_proxy": 1.0,
                "valid": True,
                "invalid_reason": None,
            },
            {
                "hypothesis_id": "spread_cont_12",
                "trigger_type": "event",
                "trigger_key": "event:SPREAD_BLOWOUT",
                "direction": "long",
                "horizon": "12b",
                "template_id": "continuation",
                "n": 2139,
                "train_n_obs": 700,
                "validation_n_obs": 700,
                "test_n_obs": 739,
                "validation_samples": 700,
                "test_samples": 739,
                "mean_return_bps": -3.3104,
                "t_stat": -1.0483,
                "cost_adjusted_return_bps": -5.3104,
                "p_value_raw": 0.294,
                "p_value_for_fdr": 0.294,
                "mae_mean_bps": -8.0,
                "mfe_mean_bps": 12.0,
                "robustness_score": 0.8,
                "stress_score": 0.8,
                "kill_switch_count": 0,
                "capacity_proxy": 1.0,
                "valid": True,
                "invalid_reason": None,
            },
        ],
        index=[24, 28, 72, 76],
    )

    universe = hypotheses_to_bridge_candidates(metrics, symbol="ETHUSDT", prefilter_min_t_stat=False)
    by_hypothesis = universe.set_index("hypothesis_id")

    assert by_hypothesis.loc["gap_cont_12", "event_type"] == "LIQUIDITY_GAP_PRINT"
    assert by_hypothesis.loc["gap_cont_12", "canonical_event_type"] == "LIQUIDITY_GAP_PRINT"
    assert by_hypothesis.loc["gap_cont_12", "mean_return_bps"] == 10.6634
    assert by_hypothesis.loc["spread_cont_12", "event_type"] == "SPREAD_BLOWOUT"
