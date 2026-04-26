from __future__ import annotations

import functools
import hashlib
from pathlib import Path

from project.spec_registry import (
    canonical_yaml_hash,
    iter_spec_yaml_files,
    resolve_relative_spec_path,
)


@functools.lru_cache(maxsize=1)
def _get_spec_hashes_cached(project_root_str: str) -> dict[str, str]:
    repo_root = Path(project_root_str)
    hashes: dict[str, str] = {}
    spec_dir = resolve_relative_spec_path("spec", repo_root=repo_root)
    for f in iter_spec_yaml_files(repo_root=repo_root):
        rel = str(f.relative_to(spec_dir))
        hashes[rel] = hashlib.sha256(canonical_yaml_hash(f).encode("utf-8")).hexdigest()
    return dict(sorted(hashes.items()))


def get_spec_hashes(project_root: Path) -> dict[str, str]:
    return _get_spec_hashes_cached(str(project_root.resolve()))
