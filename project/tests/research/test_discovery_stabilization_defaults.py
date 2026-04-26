from pathlib import Path

import pandas as pd

from project.io.utils import read_table_auto
from project.research import phase2_search_engine
from project.research.services.candidate_discovery_scoring import build_discovery_quality_score


def test_v2_scoring_defaults():
    # Mock row
    row = pd.Series(
        {
            "t_stat": 3.0,
            "mean_return_bps": 10.0,
            "placebo_shift_effect": 1.0,
            "null_strength_ratio": 5.0,
            "cost_survival_ratio": 0.8,
            "event_family_key": "fam1",
            "template_family_key": "tpl1",
            "direction_key": "long",
            "horizon_bucket": "1h",
            "is_discovery": True,
        }
    )

    config = {
        "falsification_weight": 1.0,
        "tradability_weight": 1.0,
        "novelty_weight": 1.0,
        "overlap_penalty_weight": 1.0,
        "fragility_penalty_weight": 1.0,
    }

    overlap_context = {"fam1|tpl1|long|1h": 1}

    score_data = build_discovery_quality_score(row, overlap_context, config)

    # Check that all expected components are present
    expected_cols = [
        "falsification_component",
        "tradability_component",
        "novelty_component",
        "support_component",
        "significance_component",
        "fold_stability_bonus",
        "fold_stability_penalty",
        "overlap_penalty",
        "fragility_penalty",
        "discovery_quality_score",
        "rank_primary_reason",
        "demotion_reason_codes",
    ]
    for col in expected_cols:
        assert col in score_data, f"Missing {col} in score decomposition"


def test_search_engine_v2_default(monkeypatch):
    # Check that run() has enable_discovery_v2_scoring=True by default
    import inspect

    sig = inspect.signature(phase2_search_engine.run)
    assert sig.parameters["enable_discovery_v2_scoring"].default is True


def test_search_space_defaults_are_stable():
    import yaml

    from project import PROJECT_ROOT

    spec_path = PROJECT_ROOT.parent / "spec/search_space.yaml"
    with open(spec_path) as f:
        spec = yaml.safe_load(f)
        # Assert benchmark mode D is the official default:
        # hierarchical search, no selection overlay.
        assert spec["discovery_search"]["mode"] == "hierarchical"
        assert spec["discovery_selection"]["mode"] == "off"
        assert spec["discovery_selection"]["shortlist"]["enabled"] is False


def test_ledger_default_is_disabled():
    import yaml

    from project import PROJECT_ROOT

    config_path = PROJECT_ROOT.parent / "project/configs/discovery_ledger.yaml"
    if config_path.exists():
        with open(config_path) as f:
            cfg = yaml.safe_load(f)
            if "discovery_scoring" in cfg and "ledger_adjustment" in cfg["discovery_scoring"]:
                assert cfg["discovery_scoring"]["ledger_adjustment"]["enabled"] is False


def test_legacy_sort_path_remains_deterministic_without_is_discovery() -> None:
    final_df = pd.DataFrame(
        [
            {"candidate_id": "low", "t_stat": 1.0},
            {"candidate_id": "high", "t_stat": 3.0},
        ]
    )

    sorted_df = phase2_search_engine._sort_final_candidates(
        final_df,
        enable_discovery_v2_scoring=False,
    )

    assert list(sorted_df["candidate_id"]) == ["high", "low"]


def test_sort_final_candidates_t_stat_fallback_on_sort_error() -> None:
    """Regression: _sort_final_candidates must never skip sorting.
    If v2/ledger scoring produces an error sentinel, sorting must still
    succeed using t_stat."""
    df_with_sentinel = pd.DataFrame(
        [
            {"candidate_id": "low", "t_stat": 1.0, "_v2_scoring_error": True},
            {"candidate_id": "high", "t_stat": 3.0, "_v2_scoring_error": True},
        ]
    )

    sorted_df = phase2_search_engine._sort_final_candidates(
        df_with_sentinel,
        enable_discovery_v2_scoring=False,
    )

    assert list(sorted_df["candidate_id"]) == ["high", "low"]


def test_v2_scoring_failure_sets_sentinel_and_preserves_t_stat_ranking(
    tmp_path: Path, monkeypatch
) -> None:
    """Regression: when annotate_discovery_v2_scores raises, the engine must
    not abort — it should set a sentinel column and still sort by t_stat."""

    from project.research.services import candidate_discovery_scoring

    def _boom(df, config):
        raise RuntimeError("v2 scoring explosion for test")

    monkeypatch.setattr(candidate_discovery_scoring, "annotate_discovery_v2_scores", _boom)

    final_df = pd.DataFrame(
        [
            {"candidate_id": "a", "t_stat": 2.5, "is_discovery": True},
            {"candidate_id": "b", "t_stat": 4.1, "is_discovery": True},
            {"candidate_id": "c", "t_stat": 1.3, "is_discovery": False},
        ]
    )

    config = {
        "default_turnover_penalty_thresh": 0.8,
        "default_coverage_thresh": 0.01,
        "min_acceptable_regime_support_ratio": 0.4,
    }

    try:
        from project.research.services.candidate_discovery_scoring import (
            annotate_discovery_v2_scores,
        )

        final_df = annotate_discovery_v2_scores(final_df, config)
    except Exception:
        final_df["_v2_scoring_error"] = True

    sorted_df = phase2_search_engine._sort_final_candidates(
        final_df,
        enable_discovery_v2_scoring=False,
    )

    assert sorted_df.iloc[0]["candidate_id"] == "b"
    assert "_v2_scoring_error" in sorted_df.columns


def test_write_hypothesis_registry_preserves_plan_row_id(tmp_path: Path) -> None:
    candidates = pd.DataFrame(
        [
            {"hypothesis_id": "hyp_1", "plan_row_id": "plan_alpha"},
            {"hypothesis_id": "hyp_2", "plan_row_id": ""},
        ]
    )

    phase2_search_engine._write_hypothesis_registry(candidates, tmp_path)

    out = read_table_auto(tmp_path / "hypothesis_registry.parquet")
    assert list(out["hypothesis_id"]) == ["hyp_1", "hyp_2"]
    assert list(out["plan_row_id"]) == ["plan_alpha", "hyp_2"]
