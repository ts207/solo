from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Sequence

from project.contracts.pipeline_registry import (
    STAGE_FAMILY_REGISTRY,
    StageSpec,
    validate_stage_plan_contract as _validate_stage_plan_contract,
)


@dataclass(frozen=True)
class StageSpecContract:
    family: str
    stage_patterns: tuple[str, ...]
    script_patterns: tuple[str, ...]
    owner_service: str
    schema_version: str = "phase5_stage_contract_v1"
    is_legacy: bool = False

def build_stage_specs() -> tuple[StageSpecContract, ...]:
    return tuple(
        StageSpecContract(
            family=contract.family,
            stage_patterns=contract.stage_patterns,
            script_patterns=contract.script_patterns,
            owner_service=contract.owner_service,
        )
        for contract in STAGE_FAMILY_REGISTRY
    )


def validate_stage_registry_definitions(project_root: Path) -> List[str]:
    issues: List[str] = []
    seen_families: set[str] = set()
    for spec in build_stage_specs():
        if not spec.family:
            issues.append("stage family contract missing 'family' name")
            continue
        if spec.family in seen_families:
            issues.append(f"duplicate family name: '{spec.family}'")
        seen_families.add(spec.family)
        if not spec.stage_patterns:
            issues.append(f"family '{spec.family}' has no stage_patterns")
        if not spec.script_patterns:
            issues.append(f"family '{spec.family}' has no script_patterns")
        for pattern in spec.script_patterns:
            if not pattern.strip():
                issues.append(f"family '{spec.family}' has blank script pattern")
                continue
            if any(token in pattern for token in ("*", "?", "[")):
                continue
            candidate = project_root / pattern
            if not candidate.exists():
                issues.append(
                    f"family '{spec.family}' script pattern '{pattern}' does not resolve under {project_root}"
                )
    return issues


def validate_stage_plan_contract(stages: Sequence[StageSpec], project_root: Path) -> List[str]:
    return _validate_stage_plan_contract(stages, project_root)
