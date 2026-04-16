from __future__ import annotations

from dataclasses import dataclass
from typing import List

from project.contracts.pipeline_registry import (
    STAGE_ARTIFACT_REGISTRY,
    resolve_stage_artifact_contract,
)


@dataclass(frozen=True)
class ArtifactSpecContract:
    stage_patterns: tuple[str, ...]
    inputs: tuple[str, ...]
    optional_inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    external_inputs: tuple[str, ...]
    version: str = "phase5_artifact_contract_v1"


def build_artifact_specs() -> tuple[ArtifactSpecContract, ...]:
    return tuple(
        ArtifactSpecContract(
            stage_patterns=contract.stage_patterns,
            inputs=contract.inputs,
            optional_inputs=contract.optional_inputs,
            outputs=contract.outputs,
            external_inputs=contract.external_inputs,
        )
        for contract in STAGE_ARTIFACT_REGISTRY
    )


def validate_artifact_registry_definitions() -> List[str]:
    issues: List[str] = []
    seen: set[tuple[str, ...]] = set()
    for spec in build_artifact_specs():
        if not spec.stage_patterns:
            issues.append(f"artifact contract has no stage_patterns: {spec}")
            continue
        if spec.stage_patterns in seen:
            issues.append(f"duplicate artifact contract patterns: {spec.stage_patterns}")
        seen.add(spec.stage_patterns)
        for field_name in (
            "stage_patterns",
            "inputs",
            "optional_inputs",
            "outputs",
            "external_inputs",
        ):
            value = getattr(spec, field_name)
            if not isinstance(value, tuple):
                issues.append(
                    f"artifact contract field {field_name} must be a tuple for {spec.stage_patterns}"
                )
                continue
            invalid = [item for item in value if not isinstance(item, str) or not item.strip()]
            if invalid:
                issues.append(
                    f"artifact contract field {field_name} has non-string or blank entries in {spec.stage_patterns}: {invalid}"
                )
        for pattern in spec.stage_patterns:
            if not str(pattern).strip():
                issues.append(f"artifact contract has blank stage_pattern in {spec.stage_patterns}")
    return issues


def resolve_artifact_specs_for_stage(
    stage_name: str, base_args: list[str]
) -> ArtifactSpecContract | None:
    resolved, issues = resolve_stage_artifact_contract(stage_name, base_args)
    if issues or resolved is None:
        return None
    return ArtifactSpecContract(
        stage_patterns=(stage_name,),
        inputs=resolved.inputs,
        optional_inputs=resolved.optional_inputs,
        outputs=resolved.outputs,
        external_inputs=resolved.external_inputs,
    )
