from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from project.contracts.artifacts import validate_artifact_registry_definitions
from project.contracts.stage_dag import (
    validate_stage_plan_contract as _validate_stage_plan_contract,
)
from project.contracts.stage_dag import validate_stage_registry_definitions
from project.pipelines.stage_bootstrap import assert_stage_registry_contract
from project.pipelines.stage_definitions import StageSpec
from project.pipelines.stage_dependencies import validate_stage_dataflow_dag


def validate_stage_artifact_registry_definitions() -> list[str]:
    return validate_artifact_registry_definitions()


def assert_registry_contract() -> None:
    assert_stage_registry_contract()


def validate_stage_plan_contract(stages: Sequence[StageSpec], project_root: Path) -> list[str]:
    return _validate_stage_plan_contract(stages, project_root)


__all__ = [
    "assert_registry_contract",
    "validate_stage_artifact_registry_definitions",
    "validate_stage_dataflow_dag",
    "validate_stage_plan_contract",
    "validate_stage_registry_definitions",
]
