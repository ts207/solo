from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable

from project.reliability.schemas import (
    ENGINE_MANIFEST_SCHEMA,
    STAGE_MANIFEST_SCHEMA,
    ManifestSchemaSpec,
)


def load_manifest(manifest_or_path: Dict[str, Any] | Path | str) -> Dict[str, Any]:
    if isinstance(manifest_or_path, dict):
        return dict(manifest_or_path)
    path = Path(manifest_or_path)
    return json.loads(path.read_text(encoding="utf-8"))


def _select_schema(manifest: Dict[str, Any]) -> ManifestSchemaSpec:
    if str(manifest.get("manifest_type", "")).strip() == ENGINE_MANIFEST_SCHEMA.manifest_type:
        return ENGINE_MANIFEST_SCHEMA
    return STAGE_MANIFEST_SCHEMA


def validate_manifest_core(manifest_or_path: Dict[str, Any] | Path | str) -> Dict[str, Any]:
    manifest = load_manifest(manifest_or_path)
    schema = _select_schema(manifest)
    missing = [key for key in schema.required_keys if key not in manifest]
    if missing:
        raise ValueError(f"manifest missing required keys: {missing}")
    inventory_key = schema.artifact_inventory_key
    if inventory_key is not None and not isinstance(manifest.get(inventory_key), list):
        raise ValueError(f"manifest field {inventory_key} must be a list")
    return manifest


def validate_manifest_artifacts_exist(
    manifest_or_path: Dict[str, Any] | Path | str,
) -> Dict[str, Any]:
    manifest = validate_manifest_core(manifest_or_path)
    inventory_key = _select_schema(manifest).artifact_inventory_key
    if not inventory_key:
        return manifest
    for item in manifest.get(inventory_key, []):
        if not isinstance(item, dict):
            raise ValueError(f"manifest artifact entry is not an object: {item!r}")
        raw_path = item.get("path")
        if not raw_path:
            raise ValueError("manifest artifact missing path")
        path = Path(str(raw_path))
        if not path.exists():
            raise FileNotFoundError(f"manifest artifact missing on disk: {path}")
    return manifest


def summarize_manifest_environment(
    *,
    git_sha: str,
    python_version: str,
    storage_mode: str,
    smoke_dataset_version: str,
    config_hash: str,
) -> Dict[str, Any]:
    return {
        "git_sha": str(git_sha),
        "python_version": str(python_version),
        "storage_mode": str(storage_mode),
        "smoke_dataset_version": str(smoke_dataset_version),
        "config_hash": str(config_hash),
    }
