import pandas as pd

from project.research.services.candidate_discovery_scoring import (
    annotate_discovery_v2_scores,
    build_discovery_quality_score,
    score_novelty_precheck,
    score_tradability_precheck,
)


def test_falsification_scoring():
    # Strong placebo should generate penalty
    row = pd.Series({
        "mean_return_bps": 10.0,
        "placebo_shift_effect": 12.0,
        "null_strength_ratio": 0.8,
        "t_stat": 2.0
    })

    score = build_discovery_quality_score(row, {}, {})
    assert score["falsification_component"] >= 2.0  # 2.0 for placebo exceeds main
    assert "placebo_exceeds_main" in score["falsification_reason"]


def test_build_discovery_quality_score_accepts_mapping_input():
    score = build_discovery_quality_score(
        {
            "mean_return_bps": 10.0,
            "placebo_shift_effect": 12.0,
            "null_strength_ratio": 0.8,
            "t_stat": 2.0,
        },
        {},
        {},
    )
    assert score["falsification_component"] >= 2.0

def test_tradability_scoring():
    row_poor = pd.Series({"cost_survival_ratio": 0.4, "turnover_proxy": 0.9, "coverage_ratio": 0.005})
    config = {"default_turnover_penalty_thresh": 0.8, "default_coverage_thresh": 0.01}

    t_score, flags = score_tradability_precheck(row_poor, config)
    assert t_score <= -2.5
    assert "poor_cost_survival" in flags
    assert "high_turnover_penalty" in flags

def test_novelty_overlap_scoring():
    overlap_context = {
        "FAMILY|TEMPLATE|LONG|12b": 4,
        "FAMILY|TEMPLATE|SHORT|12b": 2
    }

    row_high_overlap = pd.Series({
        "event_family_key": "FAMILY",
        "template_family_key": "TEMPLATE",
        "direction_key": "LONG",
        "horizon_bucket": "12b"
    })

    row_med_overlap = pd.Series({
        "event_family_key": "FAMILY",
        "template_family_key": "TEMPLATE",
        "direction_key": "SHORT",
        "horizon_bucket": "12b"
    })

    n1, p1, id1, f1 = score_novelty_precheck(row_high_overlap, overlap_context)
    assert p1 == 2.0
    assert n1 == 0.0
    assert "high_structural_overlap" in f1

    n2, p2, id2, f2 = score_novelty_precheck(row_med_overlap, overlap_context)
    assert p2 == 0.5
    assert n2 == 0.5
    assert "structural_duplicate_present" in f2

def test_fold_stability_scoring():
    from project.research.services.candidate_discovery_scoring import score_fold_stability_precheck

    row_unstable = pd.Series({
        "fold_valid_count": 4,
        "fold_sign_consistency": 0.25,
        "fold_fail_ratio": 0.75,
        "fold_worst_oos_expectancy": -2.0
    })

    bonus, penalty, flags = score_fold_stability_precheck(row_unstable, {})
    assert penalty > 0.0
    assert "unstable_sign_across_folds" in flags
    assert "high_fold_fail_ratio" in flags

    row_stable = pd.Series({
        "fold_valid_count": 5,
        "fold_sign_consistency": 1.0,
        "fold_fail_ratio": 0.0,
    })

    bonus_s, penalty_s, flags_s = score_fold_stability_precheck(row_stable, {})
    assert penalty_s == 0.0
    assert bonus_s > 0.0
    assert len(flags_s) == 0


def test_fold_stability_penalizes_zero_valid_oos_folds():
    from project.research.services.candidate_discovery_scoring import score_fold_stability_precheck

    row = pd.Series({
        "fold_count": 3,
        "fold_valid_count": 0,
        "fold_sign_consistency": 0.0,
        "fold_fail_ratio": 1.0,
    })

    bonus, penalty, flags = score_fold_stability_precheck(row, {})

    assert bonus == 0.0
    assert penalty >= 2.5
    assert "no_valid_oos_folds" in flags


def test_annotate_discovery_v2_scores_assigns_overlap_cluster_counts():
    candidates = pd.DataFrame(
        [
            {
                "event_family_key": "FAMILY",
                "template_family_key": "TPL",
                "direction_key": "LONG",
                "horizon_bucket": "12b",
                "mean_return_bps": 5.0,
                "placebo_shift_effect": 0.0,
                "null_strength_ratio": 0.0,
                "t_stat": 2.5,
                "cost_survival_ratio": 1.0,
                "turnover_proxy": 0.1,
                "coverage_ratio": 1.0,
                "fold_valid_count": 3,
                "fold_sign_consistency": 1.0,
                "fold_fail_ratio": 0.0,
            },
            {
                "event_family_key": "FAMILY",
                "template_family_key": "TPL",
                "direction_key": "LONG",
                "horizon_bucket": "12b",
                "mean_return_bps": 4.0,
                "placebo_shift_effect": 0.0,
                "null_strength_ratio": 0.0,
                "t_stat": 2.0,
                "cost_survival_ratio": 1.0,
                "turnover_proxy": 0.1,
                "coverage_ratio": 1.0,
                "fold_valid_count": 3,
                "fold_sign_consistency": 1.0,
                "fold_fail_ratio": 0.0,
            },
        ]
    )

    out = annotate_discovery_v2_scores(candidates, {})

    assert list(out["overlap_cluster_id"]) == ["FAMILY|TPL|LONG|12b", "FAMILY|TPL|LONG|12b"]
    assert list(out["overlap_penalty"]) == [0.5, 0.5]
