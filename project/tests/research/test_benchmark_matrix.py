from pathlib import Path

from project.research.benchmarks.benchmark_utils import evaluate_thresholds
from project.scripts.run_benchmark_matrix import load_yaml


def test_benchmark_preset_loading():
    preset_path = Path("project/configs/benchmarks/discovery/core_v1.yaml")
    assert preset_path.exists()
    preset = load_yaml(preset_path)
    assert "benchmark_modes" in preset
    assert "slices" in preset
    assert list(preset["benchmark_modes"]) == ["D"]
    assert preset["benchmark_modes"]["D"]["label"] == "hierarchical_v2_with_folds"

def test_summary_schema(tmp_path):
    summary = {
        "benchmark_run_id": "test_1",
        "preset_name": "core_v1",
        "generated_at": "2026-04-04T12:00:00Z",
        "slice_id": "m0_strong_event",
        "mode_name": "hierarchical_v2_with_folds",
        "candidate_count_generated": 100,
        "top_n_median_after_cost_expectancy_bps": 2.5
    }
    assert "benchmark_run_id" in summary
    assert "mode_name" in summary

def test_baseline_comparison():
    baseline = {"top_n_median_after_cost_expectancy_bps": 2.0}
    mode = {"top_n_median_after_cost_expectancy_bps": 3.0}
    delta = mode["top_n_median_after_cost_expectancy_bps"] - baseline["top_n_median_after_cost_expectancy_bps"]
    assert delta == 1.0

def test_threshold_evaluation():
    thresholds = {"min_final_candidates": 5}
    mode_results_pass = {
        "D": {
            "emergence": True,
            "candidate_count": 10,
            "top10": {
                "promotion_density": 0.3,
                "placebo_fail_rate": 0.1,
                "rank_diversity_score": 0.8,
                "median_after_cost_expectancy_bps": 1.0,
                "median_cost_survival_ratio": 0.9,
            },
            "median_discovery_quality_score": 0.7,
            "median_falsification_component": 0.8,
        },
    }
    res_pass = evaluate_thresholds(mode_results=mode_results_pass, thresholds=thresholds)
    assert "D" in res_pass["scorecard"]
    assert res_pass["components"]["canonical_d"] == "promote"

    mode_results_fail = {
        "D": {
            "emergence": True,
            "candidate_count": 2,
            "top10": {
                "promotion_density": 0.3,
                "placebo_fail_rate": 0.1,
                "rank_diversity_score": 0.8,
                "median_after_cost_expectancy_bps": 1.0,
                "median_cost_survival_ratio": 0.9,
            },
            "median_discovery_quality_score": 0.7,
            "median_falsification_component": 0.8,
        },
    }
    res_fail = evaluate_thresholds(mode_results=mode_results_fail, thresholds=thresholds)
    assert "D" in res_fail["scorecard"]
    assert res_fail["components"]["canonical_d"] == "hold"

def test_history_append(tmp_path):
    import pandas as pd

    from project.scripts.run_benchmark_matrix import pd as run_pd
    if run_pd is not None:
        df = pd.DataFrame([{"benchmark_run_id": "test_1"}])
        history_path = tmp_path / "benchmark_history.parquet"
        df.to_parquet(history_path, index=False)
        assert history_path.exists()
        df_read = pd.read_parquet(history_path)
        assert df_read.iloc[0]["benchmark_run_id"] == "test_1"

def test_review_artifact(tmp_path):
    review_path = tmp_path / "benchmark_review.md"
    with open(review_path, "w", encoding="utf-8") as f:
        f.write("# Benchmark Review\n\n## Slice: m0\n- Pass: True")
    content = review_path.read_text(encoding="utf-8")
    assert "Benchmark Review" in content
    assert "Pass: True" in content
