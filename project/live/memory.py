from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from project.io.utils import ensure_dir

_LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class LiveMemoryPaths:
    root: Path
    episodic_path: Path
    semantic_path: Path
    procedural_path: Path


def resolve_live_memory_paths(root: str | Path) -> LiveMemoryPaths:
    base = Path(root)
    return LiveMemoryPaths(
        root=base,
        episodic_path=base / "episodic_trades.jsonl",
        semantic_path=base / "semantic_theses.json",
        procedural_path=base / "procedural_rules.json",
    )


def append_live_episode(root: str | Path, payload: Mapping[str, Any]) -> Path:
    paths = resolve_live_memory_paths(root)
    ensure_dir(paths.root)
    with paths.episodic_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(dict(payload), sort_keys=True) + "\n")
    return paths.episodic_path


def write_live_semantic_memory(root: str | Path, payload: Mapping[str, Any]) -> Path:
    paths = resolve_live_memory_paths(root)
    ensure_dir(paths.root)
    paths.semantic_path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True), encoding="utf-8")
    return paths.semantic_path


def write_live_procedural_memory(root: str | Path, payload: Mapping[str, Any]) -> Path:
    paths = resolve_live_memory_paths(root)
    ensure_dir(paths.root)
    paths.procedural_path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True), encoding="utf-8")
    return paths.procedural_path


def load_live_episodes(root: str | Path) -> list[dict[str, Any]]:
    paths = resolve_live_memory_paths(root)
    if not paths.episodic_path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line_no, line in enumerate(paths.episodic_path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            _LOG.warning(
                "Skipping malformed live episodic memory line %s in %s",
                line_no,
                paths.episodic_path,
            )
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows
