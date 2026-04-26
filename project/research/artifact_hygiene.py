from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from project.spec_registry.loaders import repo_root

ARTIFACT_METADATA_SCHEMA_VERSION = "artifact_metadata_v1"


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _resolve_root(path: str | Path | None) -> Path | None:
    if path is None:
        return None
    return Path(path).resolve()


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def infer_workspace_root(*paths: str | Path | None) -> Path:
    resolved = [_resolve_root(path) for path in paths if path is not None]
    resolved = [path for path in resolved if path is not None]
    repo = repo_root().resolve()
    if any(_is_relative_to(path, repo) or path == repo for path in resolved):
        return repo
    if not resolved:
        return repo
    common = Path(os.path.commonpath([str(path) for path in resolved]))
    return common.resolve()


def ensure_workspace_path(
    path: str | Path,
    *,
    workspace_root: str | Path | None = None,
    label: str = "artifact",
    must_exist: bool = False,
) -> Path:
    resolved = Path(path).resolve()
    root = Path(workspace_root).resolve() if workspace_root is not None else infer_workspace_root(resolved)
    if not _is_relative_to(resolved, root) and resolved != root:
        raise ValueError(f"{label} path must stay within workspace: {resolved}")
    if must_exist and not resolved.exists():
        raise FileNotFoundError(f"{label} path does not exist in current workspace: {resolved}")
    return resolved


def render_workspace_path(path: str | Path, *, workspace_root: str | Path | None = None) -> str:
    resolved = Path(path).resolve()
    root = Path(workspace_root).resolve() if workspace_root is not None else repo_root().resolve()
    if _is_relative_to(resolved, root) or resolved == root:
        relative = resolved.relative_to(root)
        return relative.as_posix() or "."
    return resolved.as_posix()


def build_artifact_refs(
    refs: Mapping[str, str | Path],
    *,
    workspace_root: str | Path | None = None,
) -> tuple[dict[str, dict[str, object]], list[str]]:
    root = Path(workspace_root).resolve() if workspace_root is not None else repo_root().resolve()
    payload: dict[str, dict[str, object]] = {}
    invalid: list[str] = []
    for key, raw_path in refs.items():
        path = Path(raw_path).resolve()
        relative = render_workspace_path(path, workspace_root=root)
        exists = path.exists()
        within_workspace = _is_relative_to(path, root) or path == root
        payload[str(key)] = {
            "path": relative,
            "exists": exists,
            "within_workspace": within_workspace,
        }
        if not exists or not within_workspace:
            invalid.append(str(key))
    return payload, invalid


def build_summary_metadata(
    *,
    schema_version: str,
    artifact_root: str | Path,
    source_run_id: str | None,
    workspace_root: str | Path | None,
    invalid_artifact_refs: list[str],
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    root = Path(workspace_root).resolve() if workspace_root is not None else repo_root().resolve()
    artifact_root_path = ensure_workspace_path(artifact_root, workspace_root=root, label="artifact_root")
    return {
        "schema_version": str(schema_version),
        "workspace_root": ".",
        "artifact_root": render_workspace_path(artifact_root_path, workspace_root=root),
        "source_run_id": str(source_run_id or "").strip(),
        "generated_at_utc": str(generated_at_utc or utc_now_iso()),
        "all_referenced_files_exist": not invalid_artifact_refs,
    }


def metadata_markdown_lines(metadata: Mapping[str, Any]) -> list[str]:
    return [
        "## Artifact metadata",
        "",
        f"- schema_version: `{metadata.get('schema_version', '')}`",
        f"- workspace_root: `{metadata.get('workspace_root', '.')}`",
        f"- artifact_root: `{metadata.get('artifact_root', '')}`",
        f"- source_run_id: `{metadata.get('source_run_id', '')}`",
        f"- generated_at_utc: `{metadata.get('generated_at_utc', '')}`",
        f"- all_referenced_files_exist: `{metadata.get('all_referenced_files_exist', False)}`",
        "",
    ]


def invalid_artifact_header(invalid_keys: list[str]) -> list[str]:
    if not invalid_keys:
        return []
    joined = ", ".join(sorted(invalid_keys))
    return [
        "> INVALID ARTIFACT SUMMARY",
        "> Referenced path does not exist in current workspace or falls outside it.",
        f"> Invalid references: {joined}",
        "",
    ]
