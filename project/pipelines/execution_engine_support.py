from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from functools import lru_cache
from json import JSONDecodeError
from pathlib import Path
from typing import Dict, List, Mapping

from project import PROJECT_ROOT
from project.specs.manifest import validate_stage_manifest_contract


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _base_args_to_parameters(base_args: List[str]) -> Dict[str, object]:
    """Best-effort CLI arg decoding for synthesized stage manifests."""
    params: Dict[str, object] = {}
    idx = 0
    while idx < len(base_args):
        token = str(base_args[idx])
        if token.startswith("--"):
            key = token[2:]
            value: object = True
            if idx + 1 < len(base_args) and not str(base_args[idx + 1]).startswith("--"):
                value = str(base_args[idx + 1])
                idx += 1
            params[key] = value
        idx += 1
    return params


def _required_stage_manifest_enabled() -> bool:
    return str(os.environ.get("BACKTEST_REQUIRE_STAGE_MANIFEST", "0")).strip() in {
        "1",
        "true",
        "TRUE",
        "yes",
        "YES",
    }


def _allow_synthesized_manifest() -> bool:
    return str(os.environ.get("BACKTEST_ALLOW_SYNTHESIZED_STAGE_MANIFEST", "0")).strip() in {
        "1",
        "true",
        "TRUE",
        "yes",
        "YES",
    }


def _validate_stage_manifest_on_disk(
    manifest_path: Path,
    *,
    allow_failed_minimal: bool,
) -> tuple[bool, str]:
    if not manifest_path.exists():
        return False, f"missing stage manifest: {manifest_path}"
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, JSONDecodeError) as exc:
        return False, f"invalid manifest JSON ({manifest_path}): {exc}"
    if not isinstance(payload, dict):
        return False, f"manifest payload must be an object: {manifest_path}"
    try:
        validate_stage_manifest_contract(payload, allow_failed_minimal=allow_failed_minimal)
    except ValueError as exc:
        return False, f"manifest schema validation failed ({manifest_path}): {exc}"
    return True, ""


def _synthesize_stage_manifest_if_missing(
    *,
    manifest_path: Path,
    stage: str,
    stage_instance_id: str,
    run_id: str,
    script_path: Path,
    base_args: List[str],
    log_path: Path,
    status: str,
    error: str | None = None,
    input_hash: str | None = None,
) -> None:
    if manifest_path.exists():
        return
    payload: Dict[str, object] = {
        "run_id": run_id,
        "stage": stage,
        "stage_name": stage,
        "stage_instance_id": stage_instance_id,
        "pipeline_session_id": str(os.environ.get("BACKTEST_PIPELINE_SESSION_ID", "")).strip()
        or None,
        "started_at": _utc_now_iso(),
        "finished_at": _utc_now_iso(),
        "ended_at": _utc_now_iso(),
        "status": status,
        "error": error,
        "parameters": {
            "script_path": str(script_path),
            "argv": list(base_args),
            **_base_args_to_parameters(base_args),
        },
        "inputs": [],
        "outputs": [{"path": str(log_path)}],
        "stats": {"synthesized_manifest": True},
        "input_parquet_hashes": {"files": {}, "truncated": False, "max_files": 32},
        "input_artifact_hashes": {"files": {}, "truncated": False, "max_files": 256},
        "output_artifact_hashes": {"files": {}, "truncated": False, "max_files": 256},
        "spec_hashes": {},
        "ontology_spec_hash": "",
    }
    if input_hash:
        payload["input_hash"] = input_hash
    tmp = manifest_path.with_suffix(manifest_path.suffix + ".tmp")
    try:
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(manifest_path)
    except OSError:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass


def _script_supports_log_path(script_path: Path) -> bool:
    try:
        return _script_supports_log_path_cached(script_path, script_path.stat().st_mtime)
    except OSError:
        return False


@lru_cache(maxsize=2048)
def _script_supports_log_path_cached(script_path: Path, mtime: float) -> bool:
    try:
        return "--log_path" in script_path.read_text(encoding="utf-8")
    except OSError:
        return False


@lru_cache(maxsize=2048)
def _script_supports_flag_cached(script_path: Path, flag: str, mtime: float) -> bool:
    import re

    try:
        content = script_path.read_text(encoding="utf-8")
        pattern = rf"^(?!.*#).*(?<![\w-]){re.escape(flag)}(?![\w-])"
        return bool(re.search(pattern, content, re.MULTILINE))
    except OSError:
        return False


_DANGEROUS_GLOBAL_FLAGS = {"--config", "--experiment_config", "--override"}


def _filter_unsupported_flags(script_path: Path, base_args: List[str]) -> List[str]:
    """Filter CLI flags the script does not explicitly support."""
    try:
        mtime = script_path.stat().st_mtime
    except OSError:
        return base_args

    out = []
    idx = 0
    while idx < len(base_args):
        token = str(base_args[idx])
        if token.startswith("--"):
            if token in _DANGEROUS_GLOBAL_FLAGS and not _script_supports_flag_cached(
                script_path, token, mtime
            ):
                if idx + 1 < len(base_args) and not str(base_args[idx + 1]).startswith("--"):
                    idx += 1
            else:
                out.append(token)
                if idx + 1 < len(base_args) and not str(base_args[idx + 1]).startswith("--"):
                    out.append(base_args[idx + 1])
                    idx += 1
        else:
            out.append(token)
        idx += 1
    return out


def _flag_value(args: List[str], flag: str) -> str | None:
    try:
        idx = args.index(flag)
    except ValueError:
        return None
    if idx + 1 >= len(args):
        return None
    return str(args[idx + 1]).strip()


def stage_instance_base(stage: str, base_args: List[str]) -> str:
    event_type = _flag_value(base_args, "--event_type")
    if event_type and stage in {
        "build_event_registry",
        "phase2_conditional_hypotheses",
        "bridge_evaluate_phase2",
    }:
        return f"{stage}_{event_type}"
    return stage


def _collect_project_module_hashes(script_path: Path) -> str:
    """
    Parse ``script_path`` for direct ``project.*`` imports and hash their source files.
    """
    import ast as _ast

    try:
        source = script_path.read_text(encoding="utf-8", errors="replace")
        tree = _ast.parse(source)
    except (OSError, SyntaxError):
        return "parse_error"

    project_modules: set[str] = set()
    for node in _ast.walk(tree):
        if isinstance(node, _ast.Import):
            for alias in node.names:
                if alias.name.startswith("project."):
                    project_modules.add(alias.name)
        elif isinstance(node, _ast.ImportFrom):
            if node.module and node.module.startswith("project."):
                project_modules.add(node.module)

    if not project_modules:
        return "no_project_imports"

    repo_root = script_path.resolve().parent
    for _ in range(8):
        if (repo_root / "project").is_dir():
            break
        repo_root = repo_root.parent
    else:
        return "repo_root_not_found"

    hashes: list[str] = []
    for module in sorted(project_modules):
        rel_path = module.replace(".", "/") + ".py"
        abs_path = repo_root / rel_path
        try:
            content = abs_path.read_bytes()
            hashes.append(hashlib.sha256(content).hexdigest())
        except OSError:
            hashes.append(f"missing:{module}")

    combined = "|".join(hashes)
    return hashlib.sha256(combined.encode()).hexdigest()


def _manifest_declared_outputs_exist(
    manifest_path: Path,
    payload: Mapping[str, object],
) -> bool:
    def _path_has_payload(path: Path) -> bool:
        if not path.exists():
            return False
        if path.is_dir():
            for child in path.rglob("*"):
                if child.is_file():
                    return True
            return False
        return path.is_file()

    outputs = payload.get("outputs")
    if not isinstance(outputs, list):
        return False
    if not outputs:
        stage_name = str(payload.get("stage", "")).strip()
        return _stage_allows_zero_outputs(stage_name)
    for row in outputs:
        if not isinstance(row, dict):
            return False
        raw_path = str(row.get("path", "")).strip()
        if not raw_path:
            return False
        candidate = Path(raw_path)
        if candidate.is_absolute():
            if not _path_has_payload(candidate):
                return False
            continue
        if _path_has_payload(manifest_path.parent / candidate):
            continue
        if _path_has_payload(PROJECT_ROOT.parent / candidate):
            continue
        return False
    return True


def compute_stage_input_hash(
    script_path: Path,
    base_args: List[str],
    run_id: str,
    *,
    cache_context: Mapping[str, object] | None = None,
) -> str:
    """Hash the stage command + script content + directly-imported module content."""
    try:
        script_hash = hashlib.sha256(script_path.read_bytes()).hexdigest()
    except OSError:
        script_hash = "unknown"
    module_hash = _collect_project_module_hashes(script_path)
    context_payload = json.dumps(dict(cache_context or {}), sort_keys=True, default=str)
    payload = f"{script_path}:{script_hash}:{module_hash}:{' '.join(base_args)}:{run_id}:{context_payload}"
    return hashlib.sha256(payload.encode()).hexdigest()


def is_phase2_stage(stage_name: str) -> bool:
    return (
        stage_name == "phase2_search_engine"
        or stage_name == "phase2_conditional_hypotheses"
        or stage_name.startswith("phase2_conditional_hypotheses_")
    )


def _stage_allows_zero_outputs(stage_name: str) -> bool:
    zero_output_allowed_stages = frozenset({
        "ingest",
        "build_strategy_candidates",
        "compile_strategy_blueprints",
        "select_profitable_strategies",
    })
    zero_output_allowed_stages_containing = (
        "build_event_registry",
        "canonicalize_event_episodes",
        "analyze_events",
        "phase1_correlation_clustering",
        "phase2_conditional_hypotheses",
        "bridge_evaluate_phase2",
        "analyze_interaction_lift",
        "export_edge_candidates",
        "generate_negative_control_summary",
        "promote_candidates",
        "update_edge_registry",
        "update_campaign_memory",
        "analyze_conditional_expectancy",
        "validate_expectancy_traps",
        "generate_recommendations_checklist",
        "summarize_discovery_quality",
        "evaluate_naive_entry",
        "finalize_experiment",
    )
    if stage_name in zero_output_allowed_stages:
        return True
    if any(s in stage_name for s in zero_output_allowed_stages_containing):
        return True
    zero_output_prefixes = (
        "ingest_",
        "validate_",
        "build_normalized_replay",
        "run_causal_lane",
        "build_microstructure_rollup",
    )
    return stage_name.startswith(zero_output_prefixes)
