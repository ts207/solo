import pytest
import json
import pandas as pd
from pathlib import Path
from project.research.services.candidate_discovery_diagnostics import (
    build_false_discovery_diagnostics,
)


def test_diagnostics_json_contract():
    # Mock dataframe with v2 data
    df = pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "symbol": "BTCUSDT",
                "is_discovery": True,
                "family_id": "fam1",
                "t_stat": 2.5,
                "discovery_quality_score": 1.8,
                "discovery_quality_score_v3": 1.5,
                "rank_primary_reason": "high_quality_discovery",
                "demotion_reason_codes": "none",
            }
        ]
    )

    diag = build_false_discovery_diagnostics(df)

    assert "v2_scoring_diagnostics" in diag
    v2 = diag["v2_scoring_diagnostics"]
    assert "rank_movers_v1_v2" in v2
    assert "rank_movers_v2_v3" in v2
    assert "penalty_counts" in v2
    assert "survivor_quality" in diag
    assert "family_concentration" in diag["survivor_quality"]


def test_benchmark_output_contract(tmp_path):
    from project.research.benchmarks.discovery_benchmark import summarize_case_comparison

    legacy_df = pd.DataFrame(
        [
            {
                "event_type": "E1",
                "horizon": "1h",
                "direction": "long",
                "t_stat": 2.0,
                "is_discovery": True,
            }
        ]
    )
    v2_df = pd.DataFrame(
        [
            {
                "event_type": "E1",
                "horizon": "1h",
                "direction": "long",
                "t_stat": 2.0,
                "discovery_quality_score": 1.5,
                "is_discovery": True,
            }
        ]
    )

    case_results = {"A": legacy_df, "B": v2_df}
    summarize_case_comparison("test_case", case_results, tmp_path)

    assert (tmp_path / "rank_comparison.csv").exists()
    assert (tmp_path / "rank_movers.csv").exists()

    merged = pd.read_csv(tmp_path / "rank_comparison.csv")
    assert "comp_key" in merged.columns
    assert "A_rank" in merged.columns
    assert "B_rank" in merged.columns


def test_decomposition_artifact_contract(tmp_path):
    from project.research.benchmarks import discovery_benchmark
    import pandas as pd

    # Mock data with minimal required columns for Patch 2
    merged = pd.DataFrame(
        {
            "candidate_id": ["c1", "c2"],
            "comp_key": ["K1", "K2"],
            "A_rank": [1, 2],
            "B_rank": [2, 1],
            "rank_delta_A_to_B": [-1, 1],
            "discovery_quality_score": [0.5, 0.8],
            "significance_component": [0.6, 0.9],
            "support_component": [1.0, 1.0],
            "falsification_component": [1.0, 1.0],
            "tradability_component": [1.0, 1.0],
            "novelty_component": [1.0, 1.0],
            "fold_stability_component": [1.0, 1.0],
            "rank_primary_reason": ["stable", "stable"],
            "overlap_penalty": [1.0, 1.0],
            "overlap_reason": ["none", "none"],
            "fold_reason": ["none", "none"],
        }
    )

    discovery_benchmark._write_score_decomposition("test_case", merged, tmp_path)

    # TABULAR CONTRACT
    assert (tmp_path / "score_decomposition.parquet").exists()
    assert (tmp_path / "score_decomposition.csv").exists()

    df = pd.read_csv(tmp_path / "score_decomposition.csv")
    cols = df.columns.tolist()
    expected_cols = [
        "A_rank",
        "B_rank",
        "rank_delta_A_to_B",
        "significance_component",
        "support_component",
        "falsification_component",
        "tradability_component",
        "novelty_component",
        "fold_stability_component",
        "discovery_quality_score",
        "rank_primary_reason",
    ]
    for c in expected_cols:
        assert c in cols, f"Tabular artifact missing column: {c}"

    # MARKDOWN CONTRACT
    assert (tmp_path / "score_decomposition.md").exists()
    md_content = (tmp_path / "score_decomposition.md").read_text()

    headers = [
        "# Score Decomposition",
        "## Biggest Positive Movers",
        "## Biggest Negative Movers",
        "## Highest A-to-B Promotions",
        "## Highest A-to-B Demotions",
    ]
    for h in headers:
        assert h in md_content, f"Markdown artifact missing header: {h}"


def test_hierarchical_search_contract():
    # Verify signature alignment
    from project.research.search import hierarchical_search
    import inspect

    sig = inspect.signature(hierarchical_search._apply_v2_scoring)
    assert "data_root" in sig.parameters
    assert "run_id" in sig.parameters

    # Mock a dataframe
    df = pd.DataFrame([{"event_type": "E1", "t_stat": 2.0}])
    # Should not crash even with missing data_root
    out = hierarchical_search._apply_v2_scoring(df)
    assert not out.empty
    assert "discovery_quality_score" in out.columns
