"""
Canonical research pipeline declaration.

This module exists to make the intended flow explicit and machine-readable.
It is the authoritative answer to "which path is canonical?" — imported by
tests, system_map checks, and documentation generators.

Canonical flow (proposal → promoted thesis):

  spec/proposals/*.yaml
    └─ project/research/agent_io/issue_proposal.py        ← CLI front door
         └─ proposal_to_experiment.py                     ← translate + validate
              └─ execute_proposal.py                       ← shell into run_all
                   └─ project/pipelines/run_all.py         ← stage DAG
                        ├─ ingest / clean / features / market_context
                        ├─ analyze_events                  ← event detection
                        ├─ phase2_search_engine            ← CANONICAL discovery stage
                        ├─ promotion / finalize
                        └─ artifacts written to data/artifacts/experiments/<program_id>/<run_id>/

Deploy flow (promoted thesis → paper runtime):

  promoted_theses.json
    └─ project/cli.py deploy bind-config                  ← generate runtime config
         └─ project/cli.py deploy certify                 ← startup certification
              └─ project/scripts/run_live_engine.py        ← actual runtime launcher
"""
from __future__ import annotations

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

LEGACY_COMPAT_MODULES: tuple[str, ...] = (
    "project.research.candidate_discovery",
    "project.research.services.candidate_discovery_service",
    "project.research.discovery",
)
