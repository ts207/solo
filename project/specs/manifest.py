from __future__ import annotations

import functools
import hashlib
import json
import os
import platform
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from project import PROJECT_ROOT
from project.core.config import get_data_root
from project.spec_registry import (
    feature_schema_registry_path as registry_feature_schema_registry_path,
)
from project.spec_registry import (
    load_feature_schema_registry as registry_load_feature_schema_registry,
)
from project.specs.ontology import (
    ontology_component_hash_fields,
    ontology_component_hashes,
    ontology_spec_hash,
)
from project.specs.utils import get_spec_hashes


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _manifest_path(run_id: str, stage: str, stage_instance_id: str | None = None) -> Path:
    data_root = get_data_root()
    out_dir = data_root / "runs" / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    resolved_stage_instance_id = str(stage_instance_id or "").strip()
    if not resolved_stage_instance_id:
        resolved_stage_instance_id = str(os.getenv("BACKTEST_STAGE_INSTANCE_ID", "")).strip()
    out_name = resolved_stage_instance_id or stage
    return out_dir / f"{out_name}.json"


def _run_manifest_path(run_id: str) -> Path:
    data_root = get_data_root()
    return data_root / "runs" / run_id / "run_manifest.json"


def _is_stale_pipeline_session(manifest: dict[str, Any]) -> bool:
    session_id = str(os.getenv("BACKTEST_PIPELINE_SESSION_ID", "")).strip()
    if not session_id:
        return False
    run_manifest_path = _run_manifest_path(str(manifest.get("run_id", "")))
    if not run_manifest_path.exists():
        return False
    try:
        payload = json.loads(run_manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if not isinstance(payload, dict):
        return False
    current = str(payload.get("pipeline_session_id", "")).strip()
    return bool(current) and current != session_id


def _project_root() -> Path:
    return PROJECT_ROOT


@functools.lru_cache(maxsize=1)
def _git_commit_cached(project_root_str: str) -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", project_root_str, "rev-parse", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return "unknown"


def _git_commit(project_root: Path) -> str:
    return _git_commit_cached(str(project_root.resolve()))


def _file_fingerprint(path: Path) -> str:
    """Content hash for manifest lineage comparisons."""
    try:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()
    except OSError:
        return "error"


def _normalize_manifest_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(k): _normalize_manifest_value(v)
            for k, v in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, list):
        return [_normalize_manifest_value(v) for v in value]
    if isinstance(value, tuple):
        return [_normalize_manifest_value(v) for v in value]
    return value


def _normalize_manifest_dict(value: dict[str, Any]) -> dict[str, Any]:
    return {
        str(k): _normalize_manifest_value(v)
        for k, v in sorted(dict(value).items(), key=lambda item: str(item[0]))
    }


def _normalize_artifact_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized.append(_normalize_manifest_dict(row))
    return normalized


def _input_parquet_hashes(inputs: list[dict[str, Any]], *, max_files: int = 32) -> dict[str, Any]:
    files: list[Path] = []
    seen: set[str] = set()
    for item in inputs:
        raw_path = item.get("path")
        if not raw_path:
            continue
        path = Path(str(raw_path))
        if path.is_file() and path.suffix.lower() == ".parquet":
            key = str(path.resolve())
            if key not in seen:
                files.append(path)
                seen.add(key)
            continue
        if path.is_dir():
            for child in sorted(path.rglob("*.parquet")):
                key = str(child.resolve())
                if key in seen:
                    continue
                files.append(child)
                seen.add(key)
                if len(files) >= max_files:
                    break
        if len(files) >= max_files:
            break

    hashes: dict[str, str] = {}
    for path in files:
        if not path.exists() or not path.is_file():
            continue
        hashes[str(path)] = _file_fingerprint(path)

    return {
        "files": hashes,
        "truncated": len(files) >= max_files,
        "max_files": int(max_files),
    }


def _artifact_hashes(rows: list[dict[str, Any]], *, max_files: int = 256) -> dict[str, Any]:
    files: list[Path] = []
    seen: set[str] = set()
    for item in rows:
        raw_path = item.get("path")
        if not raw_path:
            continue
        path = Path(str(raw_path))
        if path.is_file():
            key = str(path.resolve())
            if key not in seen:
                files.append(path)
                seen.add(key)
            continue
        if path.is_dir():
            for child in sorted(p for p in path.rglob("*") if p.is_file()):
                key = str(child.resolve())
                if key in seen:
                    continue
                files.append(child)
                seen.add(key)
                if len(files) >= max_files:
                    break
        if len(files) >= max_files:
            break

    hashes: dict[str, str] = {}
    for path in files:
        hashes[str(path)] = _file_fingerprint(path)
    return {
        "files": hashes,
        "truncated": len(files) >= max_files,
        "max_files": int(max_files),
    }


def validate_stage_manifest_contract(
    manifest: dict[str, Any],
    *,
    allow_failed_minimal: bool = False,
) -> None:
    required = (
        "run_id",
        "stage",
        "stage_instance_id",
        "started_at",
        "status",
        "parameters",
        "inputs",
        "outputs",
        "spec_hashes",
        "ontology_spec_hash",
    )
    missing = [key for key in required if key not in manifest]
    if missing:
        raise ValueError(f"stage manifest missing required fields: {missing}")

    status = str(manifest.get("status", "")).strip().lower()
    if status not in {"running", "success", "failed", "warning", "skipped", "aborted_stale_run"}:raise ValueError(f"stage manifest has invalid status: {manifest.get('status')}")

    if not isinstance(manifest.get("parameters"), dict):
        raise ValueError("stage manifest.parameters must be an object")
    if not isinstance(manifest.get("inputs"), list):
        raise ValueError("stage manifest.inputs must be a list")
    if not isinstance(manifest.get("outputs"), list):
        raise ValueError("stage manifest.outputs must be a list")

    if status == "failed" and allow_failed_minimal:
        return

    finished = str(manifest.get("finished_at", "") or "").strip()
    if status in {"success", "failed", "warning", "skipped", "aborted_stale_run"} and not finished:
        raise ValueError("stage manifest.finished_at must be set for terminal statuses")

    if "ended_at" in manifest and manifest.get("ended_at") is not None:
        ended = str(manifest.get("ended_at", "")).strip()
        if status in {"success", "failed", "warning", "skipped", "aborted_stale_run"} and not ended:
            raise ValueError("stage manifest.ended_at must be a non-empty timestamp when present")


REQUIRED_INPUT_PROVENANCE_KEYS = (
    "vendor",
    "exchange",
    "schema_version",
    "schema_hash",
    "extraction_start",
    "extraction_end",
)


def schema_hash_from_columns(columns: list[str]) -> str:
    normalized = [str(col) for col in columns]
    payload = "|".join(normalized)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def validate_input_provenance(inputs: list[dict[str, Any]]) -> None:
    for idx, item in enumerate(inputs):
        provenance = item.get("provenance")
        if not isinstance(provenance, dict):
            raise ValueError(f"Input index {idx} missing provenance block")
        missing = [k for k in REQUIRED_INPUT_PROVENANCE_KEYS if not provenance.get(k)]
        if missing:
            path = item.get("path", f"input[{idx}]")
            raise ValueError(f"Input {path} missing required provenance keys: {missing}")


def feature_schema_registry_path() -> Path:
    return registry_feature_schema_registry_path()


def load_feature_schema_registry() -> dict[str, Any]:
    return registry_load_feature_schema_registry()


def feature_schema_identity() -> tuple[str, str]:
    schema_path = feature_schema_registry_path()
    payload = load_feature_schema_registry()
    version = str(payload.get("version", "feature_schema_v2"))
    schema_hash = hashlib.sha256(schema_path.read_bytes()).hexdigest()
    return version, schema_hash


def validate_feature_schema_columns(*, dataset_key: str, columns: list[str]) -> tuple[str, str]:
    registry = load_feature_schema_registry()
    datasets = registry.get("datasets", {})
    if not isinstance(datasets, dict):
        raise ValueError("Feature schema registry missing `datasets` object")
    contract = datasets.get(dataset_key, {})
    if not isinstance(contract, dict):
        raise ValueError(f"Feature schema registry missing dataset contract: {dataset_key}")
    required_columns = contract.get("required_columns", [])
    if not isinstance(required_columns, list):
        raise ValueError(
            f"Feature schema required_columns must be a list for dataset: {dataset_key}"
        )
    missing = [col for col in required_columns if col not in columns]
    if missing:
        raise ValueError(
            f"Feature schema contract violated for {dataset_key}; missing columns: {missing}"
        )
    return feature_schema_identity()


def start_manifest(
    stage_name: str,
    run_id: str,
    params: dict[str, Any],
    inputs: list[dict[str, Any]],
    outputs: list[dict[str, Any]],
    stage_instance_id: str | None = None,
) -> dict[str, Any]:
    project_root = _project_root()
    ontology_hash = ontology_spec_hash(project_root.parent)
    ontology_component_fields = ontology_component_hash_fields(
        ontology_component_hashes(project_root.parent)
    )
    manifest = {
        "run_id": run_id,
        "stage": stage_name,
        "stage_name": stage_name,
        "stage_instance_id": str(stage_instance_id or "").strip()
        or str(os.getenv("BACKTEST_STAGE_INSTANCE_ID", "")).strip()
        or stage_name,
        "pipeline_session_id": str(os.getenv("BACKTEST_PIPELINE_SESSION_ID", "")).strip() or None,
        "started_at": _utc_now_iso(),
        "finished_at": None,
        "ended_at": None,
        "status": "running",
        "git_commit": _git_commit(project_root),
        "spec_hashes": get_spec_hashes(project_root.parent),
        "ontology_spec_hash": ontology_hash,
        "taxonomy_hash": ontology_component_fields.get("taxonomy_hash"),
        "canonical_event_registry_hash": ontology_component_fields.get(
            "canonical_event_registry_hash"
        ),
        "state_registry_hash": ontology_component_fields.get("state_registry_hash"),
        "verb_lexicon_hash": ontology_component_fields.get("verb_lexicon_hash"),
        "parameters": _normalize_manifest_dict(dict(params)),
        "inputs": _normalize_artifact_rows(list(inputs)),
        "outputs": _normalize_artifact_rows(list(outputs)),
        "input_parquet_hashes": {"files": {}, "truncated": False, "max_files": 32},
        "input_artifact_hashes": {"files": {}, "truncated": False, "max_files": 256},
        "output_artifact_hashes": {"files": {}, "truncated": False, "max_files": 256},
        "error": None,
        "stats": None,
    }
    return enrich_manifest_with_env(manifest)


def finalize_manifest(
    manifest: dict[str, Any],
    status: str,
    error: str | None = None,
    stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if _is_stale_pipeline_session(manifest):
        status = "aborted_stale_run"
        stale_msg = "stale pipeline_session_id detected; refusing to finalize stage as current run"
        error = f"{error}; {stale_msg}" if error else stale_msg
    finished_at = _utc_now_iso()
    manifest["finished_at"] = finished_at
    manifest["ended_at"] = finished_at
    manifest["status"] = status
    manifest["error"] = error
    manifest["stats"] = _normalize_manifest_dict(dict(stats or {}))
    manifest["input_parquet_hashes"] = _input_parquet_hashes(manifest.get("inputs", []))
    manifest["input_artifact_hashes"] = _artifact_hashes(manifest.get("inputs", []))
    manifest["output_artifact_hashes"] = _artifact_hashes(manifest.get("outputs", []))
    validate_stage_manifest_contract(
        manifest,
        allow_failed_minimal=str(status).strip().lower() in {"failed"},
    )

    out_path = _manifest_path(
        manifest["run_id"],
        manifest["stage"],
        stage_instance_id=str(manifest.get("stage_instance_id", "")).strip() or None,
    )
    temp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    temp_path.replace(out_path)
    pipeline_session_id = str(manifest.get("pipeline_session_id", "") or "").strip()
    if not pipeline_session_id and str(status).strip().lower() in {"success", "warning"}:
        import importlib

        run_id = str(manifest.get("run_id", ""))
        try:
            mod = importlib.import_module("project.pipelines.pipeline_provenance")
            mod.reconcile_run_manifest_from_stage_manifests(run_id)
        except Exception as exc:
            raise RuntimeError(
                f"Failed to reconcile run manifest for standalone stage rerun {run_id!r}"
            ) from exc
    return manifest


def load_run_manifest(run_id: str) -> dict[str, Any]:
    path = _run_manifest_path(run_id)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def enrich_manifest_with_env(manifest: dict):
    manifest["python_version"] = sys.version
    manifest["platform"] = platform.platform()
    manifest["env_snapshot"] = {k: os.environ.get(k) for k in ["BACKTEST_DATA_ROOT"]}
    return manifest
