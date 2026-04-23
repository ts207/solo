"""
Canonical research pipeline declaration.

This module exists to make the intended flow explicit and machine-readable.
It is the authoritative answer to "which path is canonical?" — imported by
 tests, system_map checks, and documentation generators.

Canonical flow (proposal → promoted thesis):

  spec/proposals/*.yaml
    └─ project/research/agent_io/issue_proposal.py        ← CLI front door
         └─ proposal_to_experiment.py                     ← translate + validate
              └─ execute_proposal.py                      ← shell into run_all
                   └─ project/pipelines/run_all.py        ← stage DAG
                        ├─ ingest / clean / features / market_context
                        ├─ analyze_events                 ← event detection
                        ├─ phase2_search_engine           ← canonical discovery stage
                        ├─ promote_candidates
                        └─ finalize_experiment
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

CANONICAL_STAGE_SEQUENCE: tuple[str, ...] = (
    "ingest",
    "clean",
    "features",
    "build_market_context",
    "analyze_events",
    "phase2_search_engine",
    "promote_candidates",
    "finalize_experiment",
)

CANONICAL_DISCOVERY_STAGE = "phase2_search_engine"
CANONICAL_DISCOVERY_MODULE = "project.research.phase2_search_engine"
CANONICAL_RESEARCH_PATH_VERSION = "v1"

LEGACY_COMPAT_MODULES: tuple[str, ...] = (
    "project.research.candidate_discovery",
    "project.research.discovery",
)


def canonical_pipeline_payload(
    *,
    run_id: str = "",
    stage: str = "",
    used_module: str = "",
    extra: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "canonical_research_path_version": CANONICAL_RESEARCH_PATH_VERSION,
        "run_id": str(run_id or ""),
        "stage": str(stage or ""),
        "canonical_stage_sequence": list(CANONICAL_STAGE_SEQUENCE),
        "canonical_discovery_stage": CANONICAL_DISCOVERY_STAGE,
        "canonical_discovery_module": CANONICAL_DISCOVERY_MODULE,
        "legacy_compat_modules": list(LEGACY_COMPAT_MODULES),
        "used_module": str(used_module or ""),
        "legacy_compat_used": bool(used_module and used_module in LEGACY_COMPAT_MODULES),
    }
    if extra:
        payload.update(dict(extra))
    return payload


def persist_canonical_pipeline_artifact(
    out_dir: Path,
    *,
    run_id: str = "",
    stage: str = "",
    used_module: str = "",
    extra: Dict[str, Any] | None = None,
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = canonical_pipeline_payload(
        run_id=run_id,
        stage=stage,
        used_module=used_module,
        extra=extra,
    )
    path = out_dir / "canonical_research_path.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path
