# Research layer

`project/research/` owns bounded search, candidate evaluation, promotion policy, reporting, knowledge, and thesis packaging support.

## What this layer owns

- proposal translation and issuance support under `agent_io/`
- candidate discovery and phase-2 evaluation
- promotion policy and promotion reporting
- research diagnostics and run comparison
- campaign memory and knowledge tables
- packaging and thesis-export support
- service surfaces under `project/research/services/`

## Canonical public surfaces

Read and depend on these first:

- `project.research.services.candidate_discovery_service`
- `project.research.services.promotion_service`
- `project.research.services.reporting_service`
- `project.research.services.run_comparison_service`
- `project.research.agent_io.proposal_schema`
- `project.research.agent_io.proposal_to_experiment`
- `project.research.agent_io.execute_proposal`
- `project.research.agent_io.issue_proposal`

## What this layer does not own

- raw data ingestion and cleaning
- top-level DAG coordination
- live execution behavior
- OMS implementation

## Design rule

Keep policy in services and contract-aware modules. Do not spread research policy across wrappers or convenience scripts.

## Relationship to repo surfaces

Use the root `README.md`, package READMEs, and `edge` CLI commands as the current operator-facing surfaces.
