from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Dict

from project.spec_registry import (
    canonical_yaml_hash,
    iter_spec_yaml_files,
    resolve_relative_spec_path,
)


def get_spec_hashes(project_root: Path) -> Dict[str, str]:
    repo_root = Path(project_root).resolve()
    hashes: Dict[str, str] = {}
    spec_dir = resolve_relative_spec_path("spec", repo_root=repo_root)
    for f in iter_spec_yaml_files(repo_root=repo_root):
        rel = str(f.relative_to(spec_dir))
        hashes[rel] = hashlib.sha256(canonical_yaml_hash(f).encode("utf-8")).hexdigest()
    return dict(sorted(hashes.items()))
