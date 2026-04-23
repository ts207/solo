from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

from project import PROJECT_ROOT
from project.apps.chatgpt.schemas import ToolDefinition, ToolHints

DEFAULT_FILE_BYTE_LIMIT = 250_000
DEFAULT_DIFF_BYTE_LIMIT = 400_000
DEFAULT_SEARCH_LIMIT = 100
DEFAULT_RANGE_LINE_LIMIT = 500
DEFAULT_GIT_TIMEOUT_SEC = 10
DEFAULT_SEARCH_TIMEOUT_SEC = 15


class RepoToolBase(BaseModel):
    repo_root: str | None = Field(
        default=None,
        description=(
            "Optional absolute or repo-relative root path. Defaults to the Edge checkout root "
            "when the module runs inside the repo."
        ),
    )


class RepoStatusInput(RepoToolBase):
    pass


class RepoListChangedFilesInput(RepoToolBase):
    staged: bool | None = Field(
        default=None,
        description="Optional staged filter. True = staged only, False = unstaged only, None = both.",
    )


class RepoDiffInput(RepoToolBase):
    path: str | None = Field(
        default=None,
        description="Optional relative file path. When omitted, returns the aggregate diff.",
    )
    staged: bool = Field(
        default=False,
        description="Whether to read the staged diff (`git diff --cached`) instead of the working-tree diff.",
    )
    context_lines: int = Field(default=3, ge=0, le=20)
    byte_limit: int = Field(default=DEFAULT_DIFF_BYTE_LIMIT, ge=10_000, le=2_000_000)


class RepoReadFileInput(RepoToolBase):
    path: str = Field(description="Repo-relative file path.")
    byte_limit: int = Field(default=DEFAULT_FILE_BYTE_LIMIT, ge=1_024, le=2_000_000)


class RepoReadFileRangeInput(RepoToolBase):
    path: str = Field(description="Repo-relative file path.")
    start_line: int = Field(default=1, ge=1)
    end_line: int = Field(description="Inclusive 1-indexed end line.", ge=1)
    byte_limit: int = Field(default=DEFAULT_FILE_BYTE_LIMIT, ge=1_024, le=2_000_000)


class RepoSearchInput(RepoToolBase):
    query: str = Field(min_length=1, description="Literal or regex search query.")
    glob: str | None = Field(default=None, description="Optional ripgrep glob restriction, e.g. '*.py'.")
    limit: int = Field(default=25, ge=1, le=DEFAULT_SEARCH_LIMIT)
    literal: bool = Field(default=True, description="Use literal matching instead of regex when true.")


class RepoToolError(RuntimeError):
    """Raised when a repo-only tool request is invalid or unsafe."""


def _schema(model: type[Any]) -> str:
    return f"{model.__module__}.{model.__name__}"


def _json_schema(model: type[Any]) -> dict[str, Any]:
    return model.model_json_schema()


def _default_repo_root() -> Path:
    return PROJECT_ROOT.parent


def _resolve_repo_root(repo_root: str | None) -> Path:
    base = _default_repo_root()
    if repo_root is None or not str(repo_root).strip():
        resolved = base.resolve()
    else:
        candidate = Path(str(repo_root).strip())
        if not candidate.is_absolute():
            candidate = (base / candidate).resolve()
        else:
            candidate = candidate.resolve()
        resolved = candidate
    if not resolved.exists() or not resolved.is_dir():
        raise RepoToolError(f"Repo root does not exist or is not a directory: {resolved}")
    return resolved


def _resolve_repo_path(repo_root: Path, path: str) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        resolved = candidate.resolve()
    else:
        resolved = (repo_root / candidate).resolve()
    try:
        resolved.relative_to(repo_root)
    except ValueError as exc:
        raise RepoToolError(f"Path escapes repo root: {path}") from exc
    return resolved


def _is_binary_bytes(sample: bytes) -> bool:
    if b"\x00" in sample:
        return True
    try:
        sample.decode("utf-8")
        return False
    except UnicodeDecodeError:
        return True


def _read_text_with_limit(path: Path, byte_limit: int) -> tuple[str, bool, int]:
    if not path.exists() or not path.is_file():
        raise RepoToolError(f"File does not exist: {path}")
    size = path.stat().st_size
    with path.open("rb") as handle:
        payload = handle.read(byte_limit + 1)
    if _is_binary_bytes(payload[: min(len(payload), 4096)]):
        raise RepoToolError(f"Binary files are not supported: {path}")
    truncated = len(payload) > byte_limit
    if truncated:
        payload = payload[:byte_limit]
    return payload.decode("utf-8", errors="replace"), truncated, size


def _git_command(repo_root: Path, args: list[str], *, timeout_sec: int = DEFAULT_GIT_TIMEOUT_SEC) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        timeout=timeout_sec,
        check=False,
    )


def _git_available(repo_root: Path) -> bool:
    if shutil.which("git") is None:
        return False
    result = _git_command(repo_root, ["rev-parse", "--is-inside-work-tree"])
    return result.returncode == 0 and result.stdout.strip() == "true"


def _parse_porcelain_lines(lines: list[str]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for raw in lines:
        if not raw:
            continue
        status = raw[:2]
        path = raw[3:] if len(raw) > 3 else ""
        original_path = None
        if " -> " in path:
            original_path, path = path.split(" -> ", 1)
        entries.append(
            {
                "path": path,
                "status": status,
                "staged_status": status[:1],
                "unstaged_status": status[1:2],
                "original_path": original_path,
            }
        )
    return entries


def _truncate_text(text: str, byte_limit: int) -> tuple[str, bool]:
    encoded = text.encode("utf-8")
    if len(encoded) <= byte_limit:
        return text, False
    clipped = encoded[:byte_limit]
    return clipped.decode("utf-8", errors="replace"), True


def _search_with_rg(repo_root: Path, query: str, *, glob: str | None, limit: int, literal: bool) -> list[dict[str, Any]]:
    rg = shutil.which("rg")
    if rg is None:
        raise FileNotFoundError("rg not installed")
    cmd = [
        rg,
        "--line-number",
        "--column",
        "--color=never",
        "--no-heading",
        "--max-count",
        str(limit),
    ]
    if literal:
        cmd.append("--fixed-strings")
    if glob:
        cmd.extend(["-g", glob])
    cmd.append(query)
    cmd.append(str(repo_root))
    result = subprocess.run(
        cmd,
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        timeout=DEFAULT_SEARCH_TIMEOUT_SEC,
        check=False,
    )
    if result.returncode not in (0, 1):
        stderr = result.stderr.strip() or result.stdout.strip()
        raise RepoToolError(f"search failed: {stderr or 'unknown error'}")
    hits: list[dict[str, Any]] = []
    for line in result.stdout.splitlines():
        parts = line.split(":", 3)
        if len(parts) != 4:
            continue
        path_text, line_no, column_no, snippet = parts
        path = Path(path_text)
        try:
            relative_path = path.resolve().relative_to(repo_root)
        except Exception:
            relative_path = Path(path_text)
        hits.append(
            {
                "path": relative_path.as_posix(),
                "line": int(line_no),
                "column": int(column_no),
                "text": snippet,
            }
        )
    return hits


def _search_with_python(repo_root: Path, query: str, *, glob: str | None, limit: int, literal: bool) -> list[dict[str, Any]]:
    hits: list[dict[str, Any]] = []
    pattern = query if literal else None
    for root, dirnames, filenames in os.walk(repo_root):
        dirnames[:] = [name for name in dirnames if name not in {".git", ".venv", "venv", "node_modules", "__pycache__"}]
        for filename in filenames:
            rel_path = Path(root, filename).resolve().relative_to(repo_root)
            if glob and not rel_path.match(glob):
                continue
            path = repo_root / rel_path
            try:
                text, _, _ = _read_text_with_limit(path, DEFAULT_FILE_BYTE_LIMIT)
            except RepoToolError:
                continue
            for line_no, line in enumerate(text.splitlines(), start=1):
                matched = pattern in line if literal else False
                if not literal:
                    import re

                    matched = re.search(query, line) is not None
                if matched:
                    hits.append(
                        {
                            "path": rel_path.as_posix(),
                            "line": line_no,
                            "column": (line.find(pattern) + 1) if literal else 1,
                            "text": line,
                        }
                    )
                    if len(hits) >= limit:
                        return hits
    return hits


def repo_get_status(*, repo_root: str | None = None) -> dict[str, Any]:
    root = _resolve_repo_root(repo_root)
    git_available = _git_available(root)
    payload: dict[str, Any] = {
        "repo_root": str(root),
        "git_available": git_available,
    }
    if not git_available:
        payload.update(
            {
                "branch": None,
                "ahead_behind": None,
                "staged_count": 0,
                "unstaged_count": 0,
                "untracked_count": 0,
                "changed_file_count": 0,
                "note": "git metadata unavailable for this repo root",
            }
        )
        return payload

    branch_result = _git_command(root, ["branch", "--show-current"])
    status_result = _git_command(root, ["status", "--porcelain=v1", "--branch"])
    lines = status_result.stdout.splitlines()
    entries = _parse_porcelain_lines([line for line in lines if not line.startswith("##")])
    payload.update(
        {
            "branch": branch_result.stdout.strip() or None,
            "ahead_behind": lines[0][3:] if lines and lines[0].startswith("## ") else None,
            "staged_count": sum(1 for row in entries if row["staged_status"] not in {"", " ", "?"}),
            "unstaged_count": sum(1 for row in entries if row["unstaged_status"] not in {"", " "}),
            "untracked_count": sum(1 for row in entries if row["status"] == "??"),
            "changed_file_count": len(entries),
        }
    )
    return payload


def repo_list_changed_files(*, repo_root: str | None = None, staged: bool | None = None) -> dict[str, Any]:
    root = _resolve_repo_root(repo_root)
    if not _git_available(root):
        return {
            "repo_root": str(root),
            "git_available": False,
            "files": [],
            "note": "git metadata unavailable for this repo root",
        }
    args = ["status", "--porcelain=v1"]
    result = _git_command(root, args)
    entries = _parse_porcelain_lines(result.stdout.splitlines())
    if staged is True:
        entries = [row for row in entries if row["staged_status"] not in {"", " ", "?"}]
    elif staged is False:
        entries = [row for row in entries if row["unstaged_status"] not in {"", " "}]
    return {
        "repo_root": str(root),
        "git_available": True,
        "files": entries,
        "count": len(entries),
    }


def repo_git_diff(
    *,
    repo_root: str | None = None,
    path: str | None = None,
    staged: bool = False,
    context_lines: int = 3,
    byte_limit: int = DEFAULT_DIFF_BYTE_LIMIT,
) -> dict[str, Any]:
    root = _resolve_repo_root(repo_root)
    if not _git_available(root):
        return {
            "repo_root": str(root),
            "git_available": False,
            "diff": "",
            "truncated": False,
            "note": "git metadata unavailable for this repo root",
        }
    args = ["diff", f"--unified={context_lines}"]
    if staged:
        args.append("--cached")
    if path:
        resolved = _resolve_repo_path(root, path)
        if resolved.exists() and resolved.is_file():
            with resolved.open("rb") as handle:
                if _is_binary_bytes(handle.read(4096)):
                    raise RepoToolError(f"Binary files are not supported in diff view: {path}")
        args.extend(["--", str(resolved.relative_to(root))])
    result = _git_command(root, args, timeout_sec=30)
    if result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip()
        raise RepoToolError(f"git diff failed: {stderr or 'unknown error'}")
    diff_text, truncated = _truncate_text(result.stdout, byte_limit)
    return {
        "repo_root": str(root),
        "git_available": True,
        "path": path,
        "staged": staged,
        "context_lines": context_lines,
        "diff": diff_text,
        "truncated": truncated,
        "byte_limit": byte_limit,
    }


def repo_read_file(*, repo_root: str | None = None, path: str, byte_limit: int = DEFAULT_FILE_BYTE_LIMIT) -> dict[str, Any]:
    root = _resolve_repo_root(repo_root)
    resolved = _resolve_repo_path(root, path)
    text, truncated, size = _read_text_with_limit(resolved, byte_limit)
    return {
        "repo_root": str(root),
        "path": resolved.relative_to(root).as_posix(),
        "content": text,
        "truncated": truncated,
        "file_size_bytes": size,
        "byte_limit": byte_limit,
    }


def repo_read_file_range(
    *,
    repo_root: str | None = None,
    path: str,
    start_line: int = 1,
    end_line: int,
    byte_limit: int = DEFAULT_FILE_BYTE_LIMIT,
) -> dict[str, Any]:
    if end_line < start_line:
        raise RepoToolError("end_line must be greater than or equal to start_line")
    if (end_line - start_line + 1) > DEFAULT_RANGE_LINE_LIMIT:
        raise RepoToolError(f"Requested line range exceeds limit of {DEFAULT_RANGE_LINE_LIMIT} lines")
    root = _resolve_repo_root(repo_root)
    resolved = _resolve_repo_path(root, path)
    text, _, size = _read_text_with_limit(resolved, byte_limit)
    lines = text.splitlines()
    selected = lines[start_line - 1 : end_line]
    content = "\n".join(selected)
    return {
        "repo_root": str(root),
        "path": resolved.relative_to(root).as_posix(),
        "start_line": start_line,
        "end_line": min(end_line, len(lines)),
        "content": content,
        "line_count": len(selected),
        "file_size_bytes": size,
    }


def repo_search(
    *,
    repo_root: str | None = None,
    query: str,
    glob: str | None = None,
    limit: int = 25,
    literal: bool = True,
) -> dict[str, Any]:
    root = _resolve_repo_root(repo_root)
    try:
        hits = _search_with_rg(root, query, glob=glob, limit=limit, literal=literal)
        backend = "rg"
    except FileNotFoundError:
        hits = _search_with_python(root, query, glob=glob, limit=limit, literal=literal)
        backend = "python"
    return {
        "repo_root": str(root),
        "query": query,
        "glob": glob,
        "limit": limit,
        "literal": literal,
        "backend": backend,
        "hits": hits,
        "count": len(hits),
    }


REPO_TOOL_CATALOG: tuple[ToolDefinition, ...] = (
    ToolDefinition(
        name="repo_get_status",
        title="Get repository status",
        description="Inspect the current repository status, branch, and change counts in read-only mode.",
        handler=f"{__name__}.repo_get_status",
        input_model=_schema(RepoStatusInput),
        input_schema=_json_schema(RepoStatusInput),
        invoking_text="Inspecting repository status",
        invoked_text="Repository status loaded",
        hints=ToolHints(
            read_only=True,
            destructive=False,
            open_world=False,
            justification="Reads git metadata and returns a structured status summary without mutating repository state.",
        ),
        category="data",
    ),
    ToolDefinition(
        name="repo_list_changed_files",
        title="List changed files",
        description="List changed files from git porcelain output in read-only mode.",
        handler=f"{__name__}.repo_list_changed_files",
        input_model=_schema(RepoListChangedFilesInput),
        input_schema=_json_schema(RepoListChangedFilesInput),
        invoking_text="Listing changed files",
        invoked_text="Changed files loaded",
        hints=ToolHints(
            read_only=True,
            destructive=False,
            open_world=False,
            justification="Reads git status output only.",
        ),
        category="data",
    ),
    ToolDefinition(
        name="repo_git_diff",
        title="Read repository diff",
        description="Return a bounded unified diff for the current working tree or staged index.",
        handler=f"{__name__}.repo_git_diff",
        input_model=_schema(RepoDiffInput),
        input_schema=_json_schema(RepoDiffInput),
        invoking_text="Reading repository diff",
        invoked_text="Repository diff loaded",
        hints=ToolHints(
            read_only=True,
            destructive=False,
            open_world=False,
            justification="Runs git diff in read-only mode and bounds response size.",
        ),
        category="data",
    ),
    ToolDefinition(
        name="repo_read_file",
        title="Read file",
        description="Read a text file inside the repository root with path-sandboxing and size caps.",
        handler=f"{__name__}.repo_read_file",
        input_model=_schema(RepoReadFileInput),
        input_schema=_json_schema(RepoReadFileInput),
        invoking_text="Reading file",
        invoked_text="File loaded",
        hints=ToolHints(
            read_only=True,
            destructive=False,
            open_world=False,
            justification="Reads an existing text file inside the configured repo root.",
        ),
        category="data",
    ),
    ToolDefinition(
        name="repo_read_file_range",
        title="Read file range",
        description="Read a bounded inclusive line range from a text file inside the repository root.",
        handler=f"{__name__}.repo_read_file_range",
        input_model=_schema(RepoReadFileRangeInput),
        input_schema=_json_schema(RepoReadFileRangeInput),
        invoking_text="Reading file range",
        invoked_text="File range loaded",
        hints=ToolHints(
            read_only=True,
            destructive=False,
            open_world=False,
            justification="Reads a bounded line range from an existing text file inside the configured repo root.",
        ),
        category="data",
    ),
    ToolDefinition(
        name="repo_search",
        title="Search repository",
        description="Search repository text content using ripgrep when available, otherwise a bounded Python fallback.",
        handler=f"{__name__}.repo_search",
        input_model=_schema(RepoSearchInput),
        input_schema=_json_schema(RepoSearchInput),
        invoking_text="Searching repository",
        invoked_text="Repository search loaded",
        hints=ToolHints(
            read_only=True,
            destructive=False,
            open_world=False,
            justification="Performs bounded text search within the configured repo root only.",
        ),
        category="data",
    ),
)


__all__ = [
    "REPO_TOOL_CATALOG",
    "RepoDiffInput",
    "RepoListChangedFilesInput",
    "RepoReadFileInput",
    "RepoReadFileRangeInput",
    "RepoSearchInput",
    "RepoStatusInput",
    "RepoToolError",
    "repo_get_status",
    "repo_git_diff",
    "repo_list_changed_files",
    "repo_read_file",
    "repo_read_file_range",
    "repo_search",
]
