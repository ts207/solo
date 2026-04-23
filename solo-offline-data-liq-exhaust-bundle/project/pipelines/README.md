# Pipelines layer

`project/pipelines/` owns orchestration, stage planning, execution coordination, provenance, and run-level bookkeeping.

## What this layer owns

- `run_all.py` and its support modules
- stage planning and dependency resolution
- stage-family assembly through `project/pipelines/stages/`
- execution/provenance helpers
- ingest, clean, feature, runtime-invariant, and evaluation stage wrappers

## What this layer does not own

- research policy
- promotion policy
- live/runtime decisioning
- thesis packaging semantics
- domain meaning of events, states, templates, or regimes

Those belong elsewhere.

## Most important files

- `run_all.py`
- `pipeline_planning.py`
- `pipeline_execution.py`
- `pipeline_provenance.py`
- `stage_registry.py`
- `stages/ingest.py`
- `stages/core.py`
- `stages/research.py`
- `stages/evaluation.py`

## How to read this layer

Start from `project.pipelines.run_all`, then the stage builders, then the stage scripts themselves.

Do not start from individual stage scripts unless you already know which stage family you are tracing.

## Relationship to repo surfaces

Use the root `README.md`, package READMEs, and `edge` CLI commands as the current operator-facing surfaces.
