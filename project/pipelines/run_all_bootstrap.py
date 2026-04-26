from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from project import PROJECT_ROOT
from project.pipelines.effective_config import (
    build_effective_config_payload,
    write_effective_config,
)
from project.pipelines.pipeline_defaults import utc_now_iso
from project.pipelines.pipeline_planning import collect_startup_non_production_overrides
from project.pipelines.pipeline_provenance import (
    build_initial_run_manifest,
    claim_map_hash,
    config_digest,
    feature_schema_metadata,
    resolve_existing_manifest_state,
)
from project.specs.invariants import runtime_component_hash_fields, runtime_component_hashes
from project.specs.objective import resolve_objective_profile_contract
from project.specs.ontology import ontology_component_hash_fields, ontology_component_hashes
from project.specs.utils import get_spec_hashes


@dataclass(frozen=True)
class RunBootstrapState:
    feature_schema_version: str
    feature_schema_hash: str
    objective_contract: Any
    ontology_component_fields: dict[str, str]
    runtime_component_fields: dict[str, str]
    effective_config_payload: dict[str, Any]
    effective_config_path: Path
    effective_config_hash: str
    data_hash: str
    data_checksum_lineage: dict[str, Any]
    existing_manifest_path: Path
    existing_manifest: dict[str, Any]
    existing_ontology_hash: str
    resume_from_index: int
    non_production_overrides: list[str]
    run_manifest: dict[str, Any]


def build_run_bootstrap_state(
    *,
    args: Any,
    preflight: dict[str, Any],
    resolved_config: dict[str, Any],
    run_id: str,
    stages: Any,
    planned_stage_instances: list[str],
    pipeline_session_id: str,
    data_root: Path,
    data_fingerprint_fn: Callable[..., tuple[str, dict[str, Any]]],
    git_commit_fn: Callable[[Path], str],
) -> RunBootstrapState:
    feature_schema_version, feature_schema_hash = feature_schema_metadata()
    objective_contract = resolve_objective_profile_contract(
        project_root=PROJECT_ROOT,
        data_root=data_root,
        run_id=run_id,
        objective_name=preflight["objective_name"],
        objective_spec_path=getattr(args, "objective_spec", None),
        retail_profile_name=preflight["retail_profile_name"],
        retail_profiles_spec_path=getattr(args, "retail_profiles_spec", None),
    )

    ontology_component_fields = ontology_component_hash_fields(
        ontology_component_hashes(PROJECT_ROOT.parent)
    )
    runtime_component_fields = runtime_component_hash_fields(
        runtime_component_hashes(PROJECT_ROOT.parent)
    )

    effective_config_payload = build_effective_config_payload(
        run_id=run_id,
        resolution=resolved_config,
        preflight=preflight,
    )
    effective_config_path, effective_config_hash = write_effective_config(
        data_root=data_root,
        run_id=run_id,
        payload=effective_config_payload,
    )

    data_hash, data_checksum_lineage = data_fingerprint_fn(
        preflight["parsed_symbols"],
        run_id,
        runtime_invariants={
            "runtime_invariants_mode": str(preflight["runtime_invariants_mode"]),
            "runtime_component_hashes": runtime_component_fields,
            "ontology_component_hashes": ontology_component_fields,
        },
        objective_profile={
            "objective_name": preflight["objective_name"],
            "objective_spec_hash": preflight.get("objective_spec_hash", ""),
            "retail_profile_name": preflight["retail_profile_name"],
            "retail_profile_spec_hash": preflight.get("retail_profile_spec_hash", ""),
        },
        effective_config_hash=effective_config_hash,
    )

    existing_manifest_path = data_root / "runs" / run_id / "run_manifest.json"
    existing_manifest, existing_ontology_hash, resume_from_index = resolve_existing_manifest_state(
        existing_manifest_path=existing_manifest_path,
        ontology_hash=preflight["ontology_hash"],
        effective_config_hash=effective_config_hash,
        allow_ontology_hash_mismatch=bool(int(args.allow_ontology_hash_mismatch)),
        planned_stage_instances=planned_stage_instances,
        resume_from_failed_stage=bool(int(args.resume_from_failed_stage)),
    )

    non_production_overrides = collect_startup_non_production_overrides(
        args=args,
        existing_manifest_path=existing_manifest_path,
        allow_ontology_hash_mismatch=bool(int(args.allow_ontology_hash_mismatch)),
        existing_ontology_hash=existing_ontology_hash,
        ontology_hash=preflight["ontology_hash"],
    )

    run_manifest = build_initial_run_manifest(
        run_id=run_id,
        started_at=utc_now_iso(),
        status="running",
        run_mode=args.mode,
        claim_map_hash=claim_map_hash(PROJECT_ROOT.parent),
        symbols=preflight["parsed_symbols"],
        start=preflight["start"],
        end=preflight["end"],
        git_commit=git_commit_fn(PROJECT_ROOT),
        data_hash=data_hash,
        data_checksum_manifest_path=str(data_checksum_lineage.get("manifest_path", "")),
        data_checksum_manifest_hash=str(data_checksum_lineage.get("manifest_hash", "")),
        spec_hashes=get_spec_hashes(PROJECT_ROOT.parent),
        ontology_spec_hash=preflight["ontology_hash"],
        feature_schema_version=feature_schema_version,
        feature_schema_hash=feature_schema_hash,
        objective_name=preflight["objective_name"],
        objective_id=(
            getattr(objective_contract, "objective_id", preflight["objective_name"])
            if objective_contract is not None
            else preflight["objective_name"]
        ),
        objective_spec_path=preflight["objective_spec_path"],
        objective_spec_hash=preflight["objective_spec_hash"],
        objective_hard_gates=(
            dict(getattr(objective_contract, "objective_hard_gates", {}))
            if objective_contract is not None
            else {}
        ),
        retail_profile_name=preflight["retail_profile_name"],
        retail_profile_spec_path=preflight["retail_profile_spec_path"],
        retail_profile_spec_hash=preflight["retail_profile_spec_hash"],
        retail_profile_config=(
            dict(getattr(objective_contract, "retail_profile_config", {}))
            if objective_contract is not None
            else dict(preflight.get("retail_profile", {}))
        ),
        runtime_invariants_mode=str(preflight["runtime_invariants_mode"]),
        runtime_invariants_status="disabled"
        if str(preflight["runtime_invariants_mode"]) == "off"
        else preflight["runtime_invariants_status"],
        runtime_invariants_validation_ok=True,
        emit_run_hash=preflight["emit_run_hash_requested"],
        run_hash_status="disabled" if not preflight["emit_run_hash_requested"] else "pending",
        hash_schema_version="runtime_hash_v1",
        pipeline_session_id=pipeline_session_id,
        config_digest=config_digest([str(x) for x in args.config]),
        effective_config_path=str(effective_config_path),
        effective_config_hash=effective_config_hash,
        effective_config_schema_version="effective_run_config_v1",
        config_resolution=dict(effective_config_payload.get("config_resolution", {})),
        normalized_symbols=list(
            effective_config_payload.get("config_resolution", {}).get("normalized_symbols", [])
        ),
        normalized_timeframes=list(
            effective_config_payload.get("config_resolution", {}).get("normalized_timeframes", [])
        ),
        planned_stages=list(stages.keys()) if isinstance(stages, dict) else [s[0] for s in stages],
        planned_stage_instances=planned_stage_instances,
        non_production_overrides=non_production_overrides,
        ontology_component_hashes=ontology_component_fields,
        runtime_component_hashes=runtime_component_fields,
        stage_cache_meta={},
        performance_mode=bool(int(getattr(args, "performance_mode", 0) or 0)),
        stage_cache_enabled_global=bool(int(getattr(args, "enable_event_stage_cache", 1) or 0))
        or bool(int(getattr(args, "performance_mode", 0) or 0)),
        phase2_parallel_workers=int(getattr(args, "phase2_parallel_workers", 1) or 1),
        strict_run_scoped_reads=str(args.mode).strip().lower() == "certification",
        require_stage_manifests=str(args.mode).strip().lower() == "certification",
        experiment_type=str(getattr(args, "experiment_type", "discovery") or "discovery"),
        baseline_run_id=str(getattr(args, "research_compare_baseline_run_id", "") or "").strip(),
        allowed_change_field=str(getattr(args, "allowed_change_field", "") or "").strip(),
        resume_recommended=False,
    )

    return RunBootstrapState(
        feature_schema_version=feature_schema_version,
        feature_schema_hash=feature_schema_hash,
        objective_contract=objective_contract,
        ontology_component_fields=ontology_component_fields,
        runtime_component_fields=runtime_component_fields,
        effective_config_payload=effective_config_payload,
        effective_config_path=effective_config_path,
        effective_config_hash=effective_config_hash,
        data_hash=data_hash,
        data_checksum_lineage=data_checksum_lineage,
        existing_manifest_path=existing_manifest_path,
        existing_manifest=existing_manifest,
        existing_ontology_hash=existing_ontology_hash,
        resume_from_index=resume_from_index,
        non_production_overrides=non_production_overrides,
        run_manifest=run_manifest,
    )
