from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from json import JSONDecodeError
from pathlib import Path
from typing import Dict, List, Mapping

from project.core.exceptions import DataIntegrityError
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


def validate_stage_manifest_on_disk(
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
        raise DataIntegrityError(
            f"manifest schema validation failed ({manifest_path}): {exc}"
        ) from exc
    return True, ""


def synthesize_stage_manifest_if_missing(
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


def manifest_declared_outputs_exist(
    manifest_path: Path,
    payload: Mapping[str, object],
) -> bool:
    from project import PROJECT_ROOT

    outputs = payload.get("outputs")
    if not isinstance(outputs, list) or not outputs:
        return False
    for row in outputs:
        if not isinstance(row, dict):
            return False
        raw_path = str(row.get("path", "")).strip()
        if not raw_path:
            return False
        candidate = Path(raw_path)
        if candidate.is_absolute():
            if not candidate.exists():
                return False
            continue
        if (
            not (manifest_path.parent / candidate).exists()
            and not (PROJECT_ROOT.parent / candidate).exists()
        ):
            return False
    return True
