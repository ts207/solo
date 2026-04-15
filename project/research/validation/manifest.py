from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

@dataclass
class RunArtifactManifest:
    run_id: str
    stage: str  # discover, validate, promote, deploy
    created_at: str
    schema_version: str = "v1"
    upstream_run_ids: List[str] = field(default_factory=list)
    artifacts: Dict[str, str] = field(default_factory=dict) # name -> path
    config_hash: Optional[str] = None
    git_sha: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    def persist(self, base_dir: Path):
        base_dir.mkdir(parents=True, exist_ok=True)
        path = base_dir / "artifact_manifest.json"
        with path.open("w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2, sort_keys=True)
        return path

def load_manifest(path: Path) -> RunArtifactManifest:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return RunArtifactManifest(**data)
