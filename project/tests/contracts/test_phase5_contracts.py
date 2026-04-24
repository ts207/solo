from __future__ import annotations

import pandas as pd
import pytest

from project import PROJECT_ROOT
from project.contracts.artifacts import (
    build_artifact_specs,
    get_artifact_contract,
    list_artifact_contracts,
    resolve_artifact_contract_for_path,
    validate_artifact_registry_definitions,
)
from project.contracts.pipeline_registry import assert_stage_registry_contract
from project.contracts.schemas import (
    get_any_schema_contract,
    normalize_dataframe_for_schema,
    validate_dataframe_for_schema,
    validate_payload_for_schema,
)
from project.contracts.stage_dag import build_stage_specs


def test_phase5_stage_contracts_have_owner_services() -> None:
    specs = build_stage_specs()
    assert specs
    owners = {spec.family: spec.owner_service for spec in specs}
    assert owners["phase2_discovery"].endswith("phase2_search_engine")
    assert owners["promotion"].endswith("promotion_service")


def test_phase5_artifact_contracts_nonempty() -> None:
    specs = build_artifact_specs()
    assert specs
    assert validate_artifact_registry_definitions() == []


def test_typed_artifact_contracts_bind_to_known_schemas() -> None:
    contracts = list_artifact_contracts()
    assert contracts
    for contract in contracts:
        schema = get_any_schema_contract(contract.schema_id)
        assert schema.schema_version == contract.schema_version


def test_typed_artifact_contracts_cover_core_runtime_boundary_artifacts() -> None:
    expected = {
        "discovery_phase2_candidates",
        "validation_bundle",
        "promotion_ready_candidates",
        "promoted_theses",
        "live_thesis_index",
        "run_manifest",
    }
    actual = {contract.contract_id for contract in list_artifact_contracts()}
    assert expected.issubset(actual)


def test_resolve_artifact_contract_for_canonical_paths() -> None:
    assert (
        resolve_artifact_contract_for_path("reports/validation/run_1/validation_bundle.json")
        == get_artifact_contract("validation_bundle")
    )
    assert (
        resolve_artifact_contract_for_path("live/theses/run_1/promoted_theses.json")
        == get_artifact_contract("promoted_theses")
    )
    assert (
        resolve_artifact_contract_for_path("runs/run_1/manifest.json")
        == get_artifact_contract("run_manifest")
    )


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


def test_payload_schema_validation_for_promoted_theses() -> None:
    payload = {
        "schema_version": "promoted_theses_v1",
        "run_id": "run_1",
        "generated_at_utc": "2026-04-18T00:00:00Z",
        "thesis_count": 0,
        "active_thesis_count": 0,
        "pending_thesis_count": 0,
        "theses": [],
    }
    out = validate_payload_for_schema(payload, "promoted_theses_payload")
    assert out["schema_version"] == "promoted_theses_v1"


# --- Phase 2: strictness ---

def test_stage_artifact_contract_default_strictness_is_strict() -> None:
    from project.contracts.pipeline_registry import StageArtifactContract
    c = StageArtifactContract(stage_patterns=("some_stage",), outputs=("out.parquet",))
    assert c.strictness == "strict"


def test_stage_artifact_contract_rejects_invalid_strictness() -> None:
    import pytest

    from project.contracts.pipeline_registry import StageArtifactContract
    with pytest.raises(ValueError, match="strictness"):
        StageArtifactContract(stage_patterns=("s",), strictness="unknown")


def test_dataframe_schema_contract_default_strictness_is_strict() -> None:
    from project.contracts.schemas import DataFrameSchemaContract
    c = DataFrameSchemaContract(name="x", required_columns=("a",))
    assert c.strictness == "strict"


def test_validate_schema_at_producer_strict_raises() -> None:
    import pandas as pd
    import pytest

    from project.contracts.schemas import (
        _SCHEMA_REGISTRY,
        DataFrameSchemaContract,
        validate_schema_at_producer,
    )
    _SCHEMA_REGISTRY["_test_strict"] = DataFrameSchemaContract(
        name="_test_strict", required_columns=("req_col",), strictness="strict"
    )
    try:
        with pytest.raises(Exception, match="missing required columns"):
            validate_schema_at_producer(pd.DataFrame({"other": [1]}), "_test_strict")
    finally:
        del _SCHEMA_REGISTRY["_test_strict"]


def test_validate_schema_at_producer_advisory_does_not_raise() -> None:
    import pandas as pd

    from project.contracts.schemas import (
        _SCHEMA_REGISTRY,
        DataFrameSchemaContract,
        validate_schema_at_producer,
    )
    _SCHEMA_REGISTRY["_test_advisory"] = DataFrameSchemaContract(
        name="_test_advisory", required_columns=("req_col",), strictness="advisory"
    )
    try:
        issues = validate_schema_at_producer(pd.DataFrame({"other": [1]}), "_test_advisory")
        assert issues
        assert "missing required columns" in issues[0]
    finally:
        del _SCHEMA_REGISTRY["_test_advisory"]


def test_contracts_init_exposes_validate_schema_at_producer() -> None:
    import project.contracts as contracts
    assert callable(contracts.validate_schema_at_producer)
    assert callable(contracts.validate_schema_at_producer)


# --- Phase 3: registry delegation to compiled domain ---

def test_get_event_definition_delegates_to_compiled_domain() -> None:
    from project.domain.compiled_registry import get_domain_registry
    from project.events.registry import get_event_definition
    row = get_event_definition("VOL_SPIKE")
    assert row is not None
    assert row["signal_column"] == get_domain_registry().get_event("VOL_SPIKE").signal_column


def test_get_event_definition_returns_none_for_unknown() -> None:
    from project.events.registry import get_event_definition
    assert get_event_definition("NONEXISTENT_XYZ_999") is None


def test_list_events_by_family_delegates_to_compiled_domain() -> None:
    from project.events.registry import list_events_by_family
    rows = list_events_by_family("VOLATILITY_TRANSITION")
    assert any(r.get("event_type") == "VOL_SPIKE" for r in rows)


def test_policy_domain_parity() -> None:
    from project.events.policy import assert_policy_domain_parity
    issues = assert_policy_domain_parity()
    assert issues == [], f"DEPLOYABLE_CORE_EVENT_TYPES diverges from compiled domain: {issues}"
