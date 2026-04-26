from __future__ import annotations

from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path

from project.contracts.pipeline_registry import (
    STAGE_ARTIFACT_REGISTRY,
    resolve_stage_artifact_contract,
    validate_artifact_stage_family_names,
)
from project.contracts.schemas import get_any_schema_contract, schema_contract_exists


@dataclass(frozen=True)
class ArtifactSpecContract:
    stage_patterns: tuple[str, ...]
    inputs: tuple[str, ...]
    optional_inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    external_inputs: tuple[str, ...]
    version: str = "phase5_artifact_contract_v1"


@dataclass(frozen=True)
class ArtifactContract:
    contract_id: str
    producer_stage_family: str
    consumer_stage_families: tuple[str, ...]
    required: bool
    schema_id: str
    schema_version: str
    path_pattern: str
    strictness: str = "strict"
    legacy_aliases: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        from project.contracts.pipeline_registry import _VALID_STRICTNESS

        if self.strictness not in _VALID_STRICTNESS:
            raise ValueError(
                f"ArtifactContract strictness {self.strictness!r} must be one of {sorted(_VALID_STRICTNESS)}"
            )
        family_issues = validate_artifact_stage_family_names(
            (self.producer_stage_family, *self.consumer_stage_families)
        )
        if family_issues:
            raise ValueError("; ".join(family_issues))
        if not schema_contract_exists(self.schema_id):
            raise ValueError(f"ArtifactContract {self.contract_id!r} references unknown schema {self.schema_id!r}")


def _schema_version(name: str) -> str:
    return str(get_any_schema_contract(name).schema_version)


CORE_ARTIFACT_CONTRACT_REGISTRY: tuple[ArtifactContract, ...] = (
    ArtifactContract(
        contract_id="discovery_phase2_candidates",
        producer_stage_family="phase2_discovery",
        consumer_stage_families=("validation", "operator"),
        required=True,
        schema_id="phase2_candidates",
        schema_version=_schema_version("phase2_candidates"),
        path_pattern="reports/phase2/{run_id}/phase2_candidates.parquet",
    ),
    ArtifactContract(
        contract_id="validation_bundle",
        producer_stage_family="validation",
        consumer_stage_families=("promotion", "operator"),
        required=True,
        schema_id="validation_bundle",
        schema_version=_schema_version("validation_bundle"),
        path_pattern="reports/validation/{run_id}/validation_bundle.json",
    ),
    ArtifactContract(
        contract_id="promotion_ready_candidates",
        producer_stage_family="validation",
        consumer_stage_families=("promotion", "operator"),
        required=True,
        schema_id="promotion_ready_candidates",
        schema_version=_schema_version("promotion_ready_candidates"),
        path_pattern="reports/validation/{run_id}/promotion_ready_candidates.parquet",
        legacy_aliases=("reports/validation/{run_id}/promotion_ready_candidates.csv",),
    ),
    ArtifactContract(
        contract_id="promoted_theses",
        producer_stage_family="promotion",
        consumer_stage_families=("deploy", "operator"),
        required=True,
        schema_id="promoted_theses_payload",
        schema_version=_schema_version("promoted_theses_payload"),
        path_pattern="live/theses/{run_id}/promoted_theses.json",
    ),
    ArtifactContract(
        contract_id="live_thesis_index",
        producer_stage_family="promotion",
        consumer_stage_families=("deploy", "operator"),
        required=True,
        schema_id="live_thesis_index",
        schema_version=_schema_version("live_thesis_index"),
        path_pattern="live/theses/index.json",
    ),
    ArtifactContract(
        contract_id="run_manifest",
        producer_stage_family="run_orchestration",
        consumer_stage_families=("validation", "promotion", "deploy", "operator"),
        required=True,
        schema_id="run_manifest",
        schema_version=_schema_version("run_manifest"),
        path_pattern="runs/{run_id}/run_manifest.json",
        legacy_aliases=("runs/{run_id}/manifest.json",),
    ),
)


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


def list_artifact_contracts() -> tuple[ArtifactContract, ...]:
    return CORE_ARTIFACT_CONTRACT_REGISTRY


def get_artifact_contract(contract_id: str) -> ArtifactContract:
    token = str(contract_id).strip()
    for contract in CORE_ARTIFACT_CONTRACT_REGISTRY:
        if contract.contract_id == token:
            return contract
    raise KeyError(f"unknown artifact contract: {contract_id}")


def resolve_artifact_contract_for_path(path: str | Path) -> ArtifactContract | None:
    rel = str(Path(path)).replace("\\", "/").lstrip("./")
    for contract in CORE_ARTIFACT_CONTRACT_REGISTRY:
        canonical = contract.path_pattern.replace("{run_id}", "*")
        if fnmatch(rel, canonical):
            return contract
        for alias in contract.legacy_aliases:
            if fnmatch(rel, alias.replace("{run_id}", "*")):
                return contract
    return None


def validate_artifact_registry_definitions() -> list[str]:
    issues: list[str] = []
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
    contract_ids: set[str] = set()
    path_patterns: set[str] = set()
    for contract in CORE_ARTIFACT_CONTRACT_REGISTRY:
        if contract.contract_id in contract_ids:
            issues.append(f"duplicate typed artifact contract id: {contract.contract_id}")
        contract_ids.add(contract.contract_id)
        if contract.path_pattern in path_patterns:
            issues.append(f"duplicate typed artifact path_pattern: {contract.path_pattern}")
        path_patterns.add(contract.path_pattern)
        if not contract.consumer_stage_families:
            issues.append(f"typed artifact contract has no consumers: {contract.contract_id}")
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
