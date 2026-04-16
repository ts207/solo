from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List

from project import PROJECT_ROOT
from project.contracts.artifacts import build_artifact_specs
from project.contracts.stage_dag import build_stage_specs

SYSTEM_MAP_SCHEMA_VERSION = "system_map_v1"


@dataclass(frozen=True)
class SystemSurface:
    name: str
    kind: str
    module: str
    status: str
    description: str
    replacement_module: str = ""
    removal_target: str = ""


def build_canonical_entrypoints() -> tuple[SystemSurface, ...]:
    return (
        SystemSurface(
            name="run_all_cli",
            kind="orchestration_entrypoint",
            module="project.pipelines.run_all",
            status="canonical",
            description="Primary orchestration CLI entrypoint.",
        ),
        SystemSurface(
            name="candidate_discovery_service",
            kind="service",
            module="project.research.services.candidate_discovery_service",
            status="canonical",
            description="Canonical phase-2 discovery service.",
        ),
        SystemSurface(
            name="promotion_service",
            kind="service",
            module="project.research.services.promotion_service",
            status="canonical",
            description="Canonical promotion service.",
        ),
        SystemSurface(
            name="reporting_service",
            kind="service",
            module="project.research.services.reporting_service",
            status="canonical",
            description="Schema-aware reporting service for discovery and promotion outputs.",
        ),
    )


def build_compatibility_surfaces() -> tuple[SystemSurface, ...]:
    return ()


def _surface_exists(module: str) -> bool:
    parts = module.split(".")
    module_path = PROJECT_ROOT.parent.joinpath(*parts).with_suffix(".py")
    if module_path.exists():
        return True
    package_path = PROJECT_ROOT.parent.joinpath(*parts)
    return package_path.is_dir() and (package_path / "__init__.py").exists()


def validate_system_map_surfaces() -> List[str]:
    issues: List[str] = []
    stage_specs = build_stage_specs()
    for spec in stage_specs:
        if not str(spec.owner_service).strip():
            issues.append(f"stage family '{spec.family}' missing owner service")
    for surface in (*build_canonical_entrypoints(), *build_compatibility_surfaces()):
        if not _surface_exists(surface.module):
            issues.append(f"documented system surface missing module: {surface.module}")
        if surface.status == "compatibility":
            if not surface.replacement_module:
                issues.append(f"compatibility surface missing replacement module: {surface.module}")
            elif not _surface_exists(surface.replacement_module):
                issues.append(
                    f"compatibility surface replacement missing module: {surface.replacement_module}"
                )
            if not surface.removal_target:
                issues.append(f"compatibility surface missing removal target: {surface.module}")
    return issues


def build_system_map_payload() -> Dict[str, Any]:
    def _json_ready(value: Any) -> Any:
        if isinstance(value, dict):
            return {str(k): _json_ready(v) for k, v in value.items()}
        if isinstance(value, tuple):
            return [_json_ready(v) for v in value]
        if isinstance(value, list):
            return [_json_ready(v) for v in value]
        return value

    stage_families = [_json_ready(asdict(spec)) for spec in build_stage_specs()]
    artifact_contracts = [_json_ready(asdict(spec)) for spec in build_artifact_specs()]
    canonical_entrypoints = [
        _json_ready(asdict(surface)) for surface in build_canonical_entrypoints()
    ]
    compatibility_surfaces = [
        _json_ready(asdict(surface)) for surface in build_compatibility_surfaces()
    ]
    payload = {
        "schema_version": SYSTEM_MAP_SCHEMA_VERSION,
        "canonical_entrypoints": canonical_entrypoints,
        "stage_families": stage_families,
        "artifact_contracts": artifact_contracts,
        "compatibility_surfaces": compatibility_surfaces,
    }
    return payload


def render_system_map_markdown(payload: Dict[str, Any]) -> str:
    lines: List[str] = [
        "# System Map",
        "",
        "Generated from stage and artifact contract registries.",
        "",
        "## Canonical Entrypoints",
        "",
        "| Name | Kind | Module | Status | Description |",
        "| --- | --- | --- | --- | --- |",
    ]
    for item in payload["canonical_entrypoints"]:
        lines.append(
            f"| {item['name']} | {item['kind']} | `{item['module']}` | {item['status']} | {item['description']} |"
        )

    lines.extend(
        ["", "## Compatibility Surfaces", "", "Legacy wrapper surfaces have been removed.", ""]
    )

    lines.extend(["", "## Stage Families", ""])
    for item in payload["stage_families"]:
        lines.extend(
            [
                f"### `{item['family']}`",
                "",
                f"- Owner service: `{item['owner_service']}`",
                f"- Stage patterns: {', '.join(f'`{v}`' for v in item['stage_patterns'])}",
                f"- Script patterns: {', '.join(f'`{v}`' for v in item['script_patterns'])}",
                "",
            ]
        )

    lines.extend(["## Artifact Contracts", ""])
    for item in payload["artifact_contracts"]:
        stage_patterns = ", ".join(f"`{v}`" for v in item["stage_patterns"])
        inputs = ", ".join(f"`{v}`" for v in item["inputs"]) or "_none_"
        optional_inputs = ", ".join(f"`{v}`" for v in item["optional_inputs"]) or "_none_"
        outputs = ", ".join(f"`{v}`" for v in item["outputs"]) or "_none_"
        external_inputs = ", ".join(f"`{v}`" for v in item["external_inputs"]) or "_none_"
        lines.extend(
            [
                f"### {stage_patterns}",
                "",
                f"- Inputs: {inputs}",
                f"- Optional inputs: {optional_inputs}",
                f"- Outputs: {outputs}",
                f"- External inputs: {external_inputs}",
                "",
            ]
        )

    return "\n".join(lines).strip() + "\n"


def render_system_map_json(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"
