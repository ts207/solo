from __future__ import annotations

import pandas as pd
import pytest

from project import PROJECT_ROOT
from project.contracts.artifacts import build_artifact_specs, validate_artifact_registry_definitions
from project.contracts.pipeline_registry import assert_stage_registry_contract
from project.contracts.schemas import normalize_dataframe_for_schema, validate_dataframe_for_schema
from project.contracts.stage_dag import build_stage_specs


def test_phase5_stage_contracts_have_owner_services() -> None:
    specs = build_stage_specs()
    assert specs
    owners = {spec.family: spec.owner_service for spec in specs}
    assert owners["phase2_discovery"].endswith("candidate_discovery_service")
    assert owners["promotion"].endswith("promotion_service")


def test_phase5_artifact_contracts_nonempty() -> None:
    specs = build_artifact_specs()
    assert specs
    assert validate_artifact_registry_definitions() == []


def test_canonicalize_event_episodes_contract_fields_are_tuple_shaped() -> None:
    specs = {spec.stage_patterns: spec for spec in build_artifact_specs()}
    spec = specs[("canonicalize_event_episodes*",)]
    assert spec.inputs == ("phase2.event_registry.{event_type}",)
    assert spec.outputs == ("phase2.event_episodes.{event_type}",)
    assert spec.external_inputs == ("phase2.event_registry.{event_type}",)


def test_stage_registry_contract_rejects_missing_script_paths() -> None:
    stages = [
        (
            "compile_strategy_blueprints",
            PROJECT_ROOT / "pipelines" / "research" / "compile_strategy_blueprints.py",
            [],
        )
    ]
    with pytest.raises(ValueError, match="does not exist"):
        assert_stage_registry_contract(stages, PROJECT_ROOT)


def test_stage_registry_contract_accepts_real_strategy_packaging_paths() -> None:
    assert_stage_registry_contract(
        [
            (
                "analyze_conditional_expectancy",
                PROJECT_ROOT / "research" / "analyze_conditional_expectancy.py",
                [],
            ),
            (
                "validate_expectancy_traps",
                PROJECT_ROOT / "research" / "validate_expectancy_traps.py",
                [],
            ),
            (
                "generate_recommendations_checklist",
                PROJECT_ROOT / "research" / "generate_recommendations_checklist.py",
                [],
            ),
            (
                "compile_strategy_blueprints",
                PROJECT_ROOT / "research" / "compile_strategy_blueprints.py",
                [],
            ),
            (
                "select_profitable_strategies",
                PROJECT_ROOT / "research" / "select_profitable_strategies.py",
                [],
            ),
        ],
        PROJECT_ROOT,
    )


def test_schema_normalization_and_validation() -> None:
    df = pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "hypothesis_id": "hyp_1",
                "event_type": "VOL_SHOCK",
                "symbol": "BTCUSDT",
                "run_id": "r1",
            }
        ]
    )
    out = validate_dataframe_for_schema(df, "phase2_candidates")
    assert "estimate_bps" in out.columns
    assert out.iloc[0]["candidate_id"] == "cand_1"
    empty = normalize_dataframe_for_schema(pd.DataFrame(), "promotion_decisions")
    assert {"candidate_id", "event_type", "promotion_decision", "promotion_track"}.issubset(
        set(empty.columns)
    )
