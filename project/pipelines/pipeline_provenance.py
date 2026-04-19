from __future__ import annotations

import hashlib
import json
import os
import subprocess
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from project import PROJECT_ROOT
from project.core.exceptions import DataIntegrityError
from project.io.utils import atomic_write_json, atomic_write_text
from project.pipelines.execution_plan import ExecutionPlan, ExecutionVerificationReport
from project.pipelines.pipeline_defaults import DATA_ROOT, utc_now_iso
from project.pipelines.execution_engine_support import _manifest_declared_outputs_exist


def _sha256_bytes(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _sha256_text(payload: str) -> str:
    return _sha256_bytes(payload.encode("utf-8"))


def _iter_hashable_files(root: Path, *, suffixes: Iterable[str] | None = None) -> List[Path]:
    root = Path(root)
    if not root.exists():
        return []
    allowed = {str(s).lower() for s in (suffixes or [])}
    files: List[Path] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if allowed and path.suffix.lower() not in allowed:
            continue
        files.append(path)
    return sorted(files)


def _hash_file(path: Path) -> str:
    try:
        return _sha256_bytes(path.read_bytes())
    except OSError:
        return "sha256:unreadable"


def _digest_path_listing(path: Path, *, suffixes: Iterable[str] | None = None) -> Dict[str, str]:
    out: Dict[str, str] = {}
    base = Path(path)
    for file_path in _iter_hashable_files(base, suffixes=suffixes):
        try:
            rel = str(file_path.relative_to(base))
        except ValueError:
            rel = str(file_path)
        out[rel] = _hash_file(file_path)
    return dict(sorted(out.items()))


def _lake_fingerprint(symbols: List[str], data_root: Path) -> Dict[str, object]:
    lake_root = Path(data_root) / "lake" / "raw" / "binance"
    symbol_entries: Dict[str, Dict[str, List[str]]] = {}
    for symbol in sorted({str(s).strip().upper() for s in symbols if str(s).strip()}):
        market_digests: Dict[str, List[str]] = {}
        for market in ("perp", "spot"):
            market_root = lake_root / market / symbol
            if not market_root.exists():
                continue
            digests: List[str] = []
            for file_path in _iter_hashable_files(market_root, suffixes=(".csv", ".parquet")):
                digests.append(_hash_file(file_path))
            market_digests[market] = sorted(digests)
        symbol_entries[symbol] = dict(sorted(market_digests.items()))
    payload = {"symbols": sorted(symbol_entries.keys()), "files": symbol_entries}
    return {
        "digest": _sha256_text(json.dumps(payload, sort_keys=True)),
        "file_count": int(
            sum(len(files) for markets in symbol_entries.values() for files in markets.values())
        ),
        "by_symbol": symbol_entries,
    }


def _feature_schema_metadata_payload() -> Dict[str, str]:
    version, registry_hash = feature_schema_metadata()
    return {"version": str(version), "registry_hash": str(registry_hash)}


def _spec_component_digests(project_root: Path) -> Dict[str, object]:
    repo_root = Path(project_root).parent
    spec_root = repo_root / "spec"
    project_cfg_root = Path(project_root) / "configs"
    components = {
        "spec": _digest_path_listing(spec_root, suffixes=(".yaml", ".yml", ".json", ".csv")),
        "project_configs": _digest_path_listing(
            project_cfg_root, suffixes=(".yaml", ".yml", ".json")
        ),
        "event_registry": _digest_path_listing(
            spec_root / "events", suffixes=(".yaml", ".yml", ".json")
        ),
        "gate_specs": _digest_path_listing(spec_root, suffixes=(".yaml", ".yml")),
        "objective_specs": _digest_path_listing(
            spec_root / "objectives", suffixes=(".yaml", ".yml", ".json")
        ),
        "runtime_invariants": _digest_path_listing(
            spec_root / "runtime", suffixes=(".yaml", ".yml", ".json")
        ),
        "feature_schemas": _digest_path_listing(
            spec_root / "features", suffixes=(".yaml", ".yml", ".json")
        ),
        "state_ontology": _digest_path_listing(
            spec_root / "states", suffixes=(".yaml", ".yml", ".json")
        ),
        "hypotheses": _digest_path_listing(
            spec_root / "hypotheses", suffixes=(".yaml", ".yml", ".json")
        ),
    }
    component_hashes = {
        k: _sha256_text(json.dumps(v, sort_keys=True)) for k, v in components.items()
    }
    return {"components": components, "component_hashes": component_hashes}


def data_fingerprint(
    symbols: List[str],
    run_id: str,
    *,
    project_root: Path | None = None,
    data_root: Path | None = None,
    runtime_invariants: Dict[str, object] | None = None,
    objective_profile: Dict[str, object] | None = None,
    effective_config_hash: str | None = None,
) -> Tuple[str, Dict[str, object]]:
    """Generate a reproducibility digest over data, specs, and runtime invariants."""
    project_root = Path(project_root or PROJECT_ROOT)
    data_root = Path(data_root or _get_data_root())
    lake = _lake_fingerprint(symbols, data_root)
    spec_payload = _spec_component_digests(project_root)
    feature_payload = _feature_schema_metadata_payload()
    runtime_payload = dict(sorted((runtime_invariants or {}).items()))
    objective_payload = dict(sorted((objective_profile or {}).items()))
    manifest_payload = {
        "symbols": sorted({str(s).strip().upper() for s in symbols if str(s).strip()}),
        "lake_digest": lake["digest"],
        "lake_file_count": lake["file_count"],
        "feature_schema": feature_payload,
        "spec_component_hashes": spec_payload["component_hashes"],
        "runtime_invariants": runtime_payload,
        "objective_profile": objective_payload,
        "effective_config_hash": str(effective_config_hash or ""),
    }
    manifest_hash = _sha256_text(json.dumps(manifest_payload, sort_keys=True))
    return manifest_hash, {
        "manifest_path": "in_memory",
        "manifest_hash": manifest_hash,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "lake": lake,
        "feature_schema": feature_payload,
        "spec_component_hashes": spec_payload["component_hashes"],
        "runtime_invariants": runtime_payload,
        "objective_profile": objective_payload,
        "effective_config_hash": str(effective_config_hash or ""),
        "spec_components": spec_payload["components"],
    }


def feature_schema_metadata() -> Tuple[str, str]:
    """Retrieves feature schema version and its registry hash."""
    # Importing from project.specs.manifest to reuse existing logic
    try:
        from project.specs.manifest import feature_schema_identity

        return feature_schema_identity()
    except ImportError:
        return "v2", "unknown"


def git_commit(project_root: Path) -> str:
    """Gets the current git commit hash."""
    try:
        return subprocess.check_output(
            ["git", "-C", str(project_root), "rev-parse", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return "unknown"


def _get_data_root() -> Path:
    from project import PROJECT_ROOT

    return Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))


def write_run_manifest(
    run_id: str,
    manifest: Dict[str, object],
    *,
    data_root: Path | None = None,
) -> None:
    """Writes the run manifest to disk."""
    path = Path(data_root or _get_data_root()) / "runs" / run_id / "run_manifest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)


def execution_report_dir(
    run_id: str,
    *,
    data_root: Path | None = None,
) -> Path:
    return Path(data_root or _get_data_root()) / "runs" / run_id / "execution"


def render_execution_plan_markdown(plan: ExecutionPlan) -> str:
    lines = [
        "# Explain Plan",
        "",
        f"- Run ID: `{plan.run_id}`",
        f"- Planned at: `{plan.planned_at}`",
        f"- Mode: `{plan.run_mode}`",
        f"- Symbols: `{', '.join(plan.symbols) or '(none)'}`",
        f"- Timeframe: `{plan.timeframe}`",
        "",
        "## Selected Stages",
        "",
    ]
    for stage in plan.stages:
        reason = stage.reason_code if stage.reason_code != "selected" else "selected"
        lines.append(
            f"- `{stage.stage_name}` [{reason}] family=`{stage.stage_family or 'unknown'}` "
            f"owner=`{stage.owner_service or 'unknown'}`"
        )
        if stage.artifact_outputs:
            lines.append(f"  outputs: {', '.join(f'`{item}`' for item in stage.artifact_outputs)}")
        if stage.artifact_inputs:
            lines.append(f"  inputs: {', '.join(f'`{item}`' for item in stage.artifact_inputs)}")
    if plan.artifact_obligations:
        lines.extend(["", "## Artifact Obligations", ""])
        for obligation in plan.artifact_obligations:
            lines.append(
                f"- `{obligation.contract_id}` -> `{obligation.expected_path}` "
                f"(producer=`{obligation.producer_stage_family}`, schema=`{obligation.schema_id}`)"
            )
    return "\n".join(lines).strip() + "\n"


def render_execution_verification_markdown(report: ExecutionVerificationReport) -> str:
    lines = [
        "# Contract Conformance",
        "",
        f"- Run ID: `{report.run_id}`",
        f"- Verified at: `{report.verified_at}`",
        f"- Final status: `{report.final_status}`",
        f"- Stage mismatches: `{len(report.mismatches)}`",
        f"- Artifact mismatches: `{len(report.artifact_mismatches)}`",
        "",
        "## Stage Verification",
        "",
    ]
    for result in report.results:
        lines.append(
            f"- `{result.stage_name}` planned=`{result.planned_reason_code}` actual=`{result.actual_outcome}`"
        )
    if report.artifact_results:
        lines.extend(["", "## Artifact Verification", ""])
        for result in report.artifact_results:
            suffix = f" actual=`{result.actual_path}`" if result.actual_path else ""
            lines.append(
                f"- `{result.contract_id}` status=`{result.status}` expected=`{result.expected_path}`{suffix}"
            )
    return "\n".join(lines).strip() + "\n"


def write_execution_reports(
    *,
    run_id: str,
    plan: ExecutionPlan,
    verification_report: ExecutionVerificationReport,
    data_root: Path | None = None,
) -> Dict[str, str]:
    out_dir = execution_report_dir(run_id, data_root=data_root)
    explain_plan_json = out_dir / "explain_plan.json"
    explain_plan_md = out_dir / "explain_plan.md"
    conformance_json = out_dir / "contract_conformance.json"
    conformance_md = out_dir / "contract_conformance.md"

    atomic_write_json(explain_plan_json, asdict(plan))
    atomic_write_text(explain_plan_md, render_execution_plan_markdown(plan))
    atomic_write_json(conformance_json, asdict(verification_report))
    atomic_write_text(conformance_md, render_execution_verification_markdown(verification_report))

    return {
        "explain_plan_json": str(explain_plan_json),
        "explain_plan_markdown": str(explain_plan_md),
        "contract_conformance_json": str(conformance_json),
        "contract_conformance_markdown": str(conformance_md),
    }


def read_run_manifest(
    run_id: str,
    *,
    data_root: Path | None = None,
) -> Dict[str, object]:
    """Reads the run manifest from disk."""
    root = Path(data_root or _get_data_root())
    path = root / "runs" / run_id / "run_manifest.json"
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        raise DataIntegrityError(f"Failed to read run manifest from {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise DataIntegrityError(f"Run manifest {path} did not contain an object payload")
    return dict(data)


def reconcile_run_manifest_from_stage_manifests(
    run_id: str,
    *,
    data_root: Path | None = None,
) -> Dict[str, object]:
    root = Path(data_root or _get_data_root())
    manifest_path = root / "runs" / run_id / "run_manifest.json"
    if manifest_path.exists():
        try:
            run_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise DataIntegrityError(
                f"Failed to reconcile malformed run manifest at {manifest_path}: {exc}"
            ) from exc
        if not isinstance(run_manifest, dict):
            raise DataIntegrityError(
                f"Run manifest {manifest_path} did not contain an object payload"
            )
    else:
        run_manifest = read_run_manifest(run_id, data_root=root)
    if not run_manifest:
        return {}

    planned_stage_instances = [
        str(item).strip()
        for item in run_manifest.get("planned_stage_instances", [])
        if str(item).strip()
    ]
    if not planned_stage_instances:
        return run_manifest

    stage_timings_sec = dict(run_manifest.get("stage_timings_sec", {}))
    stage_instance_timings_sec = dict(run_manifest.get("stage_instance_timings_sec", {}))
    stage_finished_times: List[str] = []
    all_completed = True
    checklist_path = root / "runs" / run_id / "research_checklist" / "checklist.json"

    for stage_instance in planned_stage_instances:
        stage_manifest_path = root / "runs" / run_id / f"{stage_instance}.json"
        if not stage_manifest_path.exists():
            all_completed = False
            continue
        try:
            stage_manifest = json.loads(stage_manifest_path.read_text(encoding="utf-8"))
        except Exception:
            all_completed = False
            continue

        status = str(stage_manifest.get("status", "")).strip().lower()
        if status not in {"success", "skipped", "warning"}:
            all_completed = False
            continue
        if status == "success" and not _manifest_declared_outputs_exist(
            stage_manifest_path, stage_manifest
        ):
            all_completed = False
            continue

        stage_name = str(stage_manifest.get("stage", stage_instance)).strip() or stage_instance
        started_at = stage_manifest.get("started_at")
        finished_at = stage_manifest.get("finished_at") or stage_manifest.get("ended_at")
        if isinstance(finished_at, str) and finished_at.strip():
            stage_finished_times.append(finished_at)

        if (
            stage_instance not in stage_instance_timings_sec
            and isinstance(started_at, str)
            and isinstance(finished_at, str)
        ):
            try:
                started_ts = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
                finished_ts = datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
                elapsed = max(0.0, float((finished_ts - started_ts).total_seconds()))
                stage_instance_timings_sec[stage_instance] = elapsed
                if stage_name not in stage_timings_sec:
                    stage_timings_sec[stage_name] = elapsed
            except ValueError:
                pass

    run_manifest["stage_timings_sec"] = stage_timings_sec
    run_manifest["stage_instance_timings_sec"] = stage_instance_timings_sec
    if checklist_path.exists():
        try:
            checklist_payload = json.loads(checklist_path.read_text(encoding="utf-8"))
            if isinstance(checklist_payload, dict):
                decision = str(checklist_payload.get("decision", "")).strip()
                if decision:
                    run_manifest["checklist_decision"] = decision
        except Exception:
            pass
    if all_completed:
        run_manifest["status"] = "success"
        run_manifest["failed_stage"] = None
        run_manifest["failed_stage_instance"] = None
        if stage_finished_times:
            run_manifest["finished_at"] = max(stage_finished_times)
    write_run_manifest(run_id, run_manifest, data_root=root)
    return run_manifest


def maybe_emit_run_hash(manifest: Dict[str, object]) -> None:
    """Emits a run hash for tracking if requested."""
    if manifest.get("emit_run_hash"):
        run_id = str(manifest.get("run_id", ""))
        run_hash = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()
        print(f"Run Hash [{run_id}]: {run_hash}")


def refresh_runtime_lineage_fields(manifest: Dict[str, object], **kwargs) -> None:
    """Updates manifest with runtime lineage information."""
    manifest["runtime_lineage_refreshed_at"] = utc_now_iso()
    if (
        kwargs.get("determinism_replay_checks_requested")
        and not str(manifest.get("determinism_status", "")).strip()
    ):
        manifest["determinism_status"] = "requested"
    if (
        kwargs.get("oms_replay_checks_requested")
        and not str(manifest.get("oms_replay_status", "")).strip()
    ):
        manifest["oms_replay_status"] = "requested"


def config_digest(configs: List[str]) -> str:
    """Generates a digest for a set of configuration files."""
    hasher = hashlib.sha256()
    for config_path in sorted(configs):
        p = Path(config_path)
        if p.exists():
            hasher.update(p.read_bytes())
        else:
            hasher.update(config_path.encode())
    return hasher.hexdigest()


def effective_config_digest(path: Path) -> str:
    if not Path(path).exists():
        return _sha256_text("")
    return _sha256_bytes(Path(path).read_bytes())


def claim_map_hash(project_root: Path) -> str:
    """Generates a hash for the claim test map if it exists."""
    path = project_root / "claim_test_map.csv"
    if not path.exists():
        return hashlib.sha256(b"").hexdigest()
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_initial_run_manifest(**fields) -> Dict[str, object]:
    """Constructs the initial run manifest dictionary."""
    return dict(fields)


def _validate_manifest_path_within_root(
    raw_path: object,
    *,
    field_name: str,
    base_root: Path,
    require_exists: bool,
) -> None:
    value = str(raw_path or "").strip()
    if not value or value == "in_memory":
        return

    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = (base_root / candidate).resolve()
    else:
        candidate = candidate.resolve()

    root = base_root.resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError(
            f"Resume manifest {field_name} points outside active root {root}: {candidate}"
        ) from exc

    if require_exists and not candidate.exists():
        raise FileNotFoundError(f"Resume manifest {field_name} missing: {candidate}")


def _validate_resume_manifest_provenance(
    existing_manifest: Dict[str, object],
    *,
    existing_manifest_path: Path,
) -> None:
    if not existing_manifest:
        return

    data_root = existing_manifest_path.parents[2]
    repo_root = PROJECT_ROOT.parent

    _validate_manifest_path_within_root(
        existing_manifest.get("effective_config_path", ""),
        field_name="effective_config_path",
        base_root=data_root,
        require_exists=True,
    )
    _validate_manifest_path_within_root(
        existing_manifest.get("objective_spec_path", ""),
        field_name="objective_spec_path",
        base_root=repo_root,
        require_exists=True,
    )
    _validate_manifest_path_within_root(
        existing_manifest.get("retail_profile_spec_path", ""),
        field_name="retail_profile_spec_path",
        base_root=repo_root,
        require_exists=True,
    )


def resolve_existing_manifest_state(
    *,
    existing_manifest_path: Path,
    ontology_hash: str,
    effective_config_hash: str,
    allow_ontology_hash_mismatch: bool,
    planned_stage_instances: List[str],
    resume_from_failed_stage: bool,
) -> Tuple[Dict[str, object], str, int]:
    """Resolves state from an existing manifest for resumes."""
    existing_ontology_hash = ""
    existing_manifest: Dict[str, object] = {}
    if existing_manifest_path.exists():
        try:
            with existing_manifest_path.open("r", encoding="utf-8") as f:
                existing_manifest = json.load(f)
        except Exception:
            existing_manifest = {}

        existing_ontology_hash = str(existing_manifest.get("ontology_spec_hash", "")).strip()
        if existing_ontology_hash and existing_ontology_hash != ontology_hash:
            if not allow_ontology_hash_mismatch:
                raise ValueError(
                    f"Ontology hash mismatch for resume. Existing: {existing_ontology_hash}, Current: {ontology_hash}. "
                    "Use --allow_ontology_hash_mismatch 1 to override."
                )
        existing_effective_config_hash = str(
            existing_manifest.get("effective_config_hash", "")
        ).strip()
        if (
            resume_from_failed_stage
            and existing_effective_config_hash
            and effective_config_hash
            and existing_effective_config_hash != effective_config_hash
        ):
            existing_manifest = {}
        if resume_from_failed_stage and existing_manifest:
            _validate_resume_manifest_provenance(
                existing_manifest,
                existing_manifest_path=existing_manifest_path,
            )

    resume_from_index = 0
    if resume_from_failed_stage and existing_manifest:
        failed_instance = str(existing_manifest.get("failed_stage_instance", "")).strip()
        if failed_instance in planned_stage_instances:
            resume_from_index = planned_stage_instances.index(failed_instance)

    return existing_manifest, existing_ontology_hash, resume_from_index


def resolve_objective_name(name: str) -> str:
    """Resolves the objective name, defaulting to retail_profitability."""
    return name or "retail_profitability"


def objective_spec_metadata(
    objective_name: str, explicit_path: str | None
) -> Tuple[Dict[str, Any], str, str]:
    """Retrieves metadata and hash for an objective specification."""
    if explicit_path:
        path = Path(explicit_path)
    else:
        path = PROJECT_ROOT.parent / "spec" / "objectives" / f"{objective_name}.yaml"

    if not path.exists():
        return {}, "unknown_hash", str(path)

    try:
        import yaml

        content = path.read_text(encoding="utf-8")
        spec = yaml.safe_load(content)
        if isinstance(spec, dict) and isinstance(spec.get("objective"), dict):
            spec = dict(spec["objective"])
        spec_hash = hashlib.sha256(content.encode()).hexdigest()
        return spec, spec_hash, str(path)
    except Exception as exc:
        raise DataIntegrityError(f"Failed to load objective spec metadata from {path}: {exc}") from exc


def resolve_retail_profile_name(name: str) -> str:
    """Resolves the retail profile name, defaulting to capital_constrained."""
    return name or "capital_constrained"


def retail_profile_metadata(
    profile_name: str, explicit_path: str | None
) -> Tuple[Dict[str, Any], str, str]:
    """Retrieves metadata and hash for a retail profile."""
    if explicit_path:
        path = Path(explicit_path)
    else:
        path = PROJECT_ROOT / "configs" / "retail_profiles.yaml"

    if not path.exists():
        return {}, "unknown_hash", str(path)

    try:
        import yaml

        content = path.read_text(encoding="utf-8")
        registry = yaml.safe_load(content)
        profile = (
            registry.get("profiles", {}).get(profile_name, {}) if isinstance(registry, dict) else {}
        )
        if isinstance(profile, dict) and "id" not in profile:
            profile = {"id": str(profile_name), **profile}
        # We hash the whole file for the registry hash
        file_hash = hashlib.sha256(content.encode()).hexdigest()
        return profile, file_hash, str(path)
    except Exception as exc:
        raise DataIntegrityError(f"Failed to load retail profile metadata from {path}: {exc}") from exc
