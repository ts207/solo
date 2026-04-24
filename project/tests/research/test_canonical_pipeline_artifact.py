from __future__ import annotations

import json
from pathlib import Path

from project.research.CANONICAL_PIPELINE import persist_canonical_pipeline_artifact


def test_persist_canonical_pipeline_artifact(tmp_path: Path):
    path = persist_canonical_pipeline_artifact(
        tmp_path,
        run_id="run_1",
        stage="discover",
        used_module="project.research.services.candidate_discovery_service",
        extra={"foo": "bar"},
    )
    payload = json.loads(path.read_text())
    assert payload["run_id"] == "run_1"
    assert payload["stage"] == "discover"
    assert payload["foo"] == "bar"
    assert "canonical_stage_sequence" in payload
