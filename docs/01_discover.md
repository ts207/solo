# Discover stage

## Scope

The discover stage is the bounded research entry point. It begins with a structured proposal and ends with a pipeline run plan or execution plus discovery artifacts and memory updates.

CLI surface from `project/cli.py`:

- `edge discover plan --proposal <path>`
- `edge discover run --proposal <path>`
- `edge discover list-artifacts --run_id <run_id>`
- `edge discover triggers <subcommand>` for the advanced trigger-mining lane

## Core code path

### Structured proposal ingress

`project/research/agent_io/issue_proposal.py` is the canonical front door.

Its responsibilities are:

1. load the proposal through `project/research/agent_io/proposal_schema.py`
2. validate bounded constraints with `project/operator/bounded.py`
3. choose or generate a `run_id`
4. create/update the program memory store under `data/artifacts/experiments/<program_id>/memory/`
5. copy the source proposal into memory for lineage
6. translate the proposal into an experiment config and `run_all` overrides
7. invoke the pipeline orchestrator

### Proposal translation

`project/research/agent_io/proposal_to_experiment.py` performs the structured translation.

Important transformations:

- normalizes horizons, directions, and entry lags
- loads search-limit defaults from `project/configs/registries/search_limits.yaml`
- renders an experiment config YAML
- validates the plan by calling `project.research.experiment_engine.build_experiment_plan`
- emits `run_all` override flags such as discovery profile, gate profile, symbols, and promotion toggles

### Orchestration

`project/research/agent_io/execute_proposal.py` shells into `project/pipelines/run_all.py` with the validated plan.

`project/pipelines/run_all.py` owns:

- preflight and effective config resolution
- stage-instance planning
- plan-only output
- bootstrap and resume logic
- stage execution
- manifest/provenance updates
- runtime postflight and audit hooks
- final run summaries

## Discover inputs

### Proposal inputs

The repo ships example proposals under `spec/proposals/`, including:

- `canonical_event_hypothesis.yaml`
- `canonical_event_hypothesis_h24.yaml` — bounded follow-on example; requires an existing baseline proposal in program memory

`uirun.yaml` is not part of the current structured execution path. It uses a legacy payload shape and should not be treated as a canonical `edge discover` example.

### Registry/spec inputs

The discover path depends on registry and ontology material loaded via `project/spec_registry`, including:

- event registry and event ontology
- search limits
- search specs
- blueprint policy
- global defaults and objective/retail profile specs

### Data inputs

The pipeline reads market data from the resolved data root. The default root is `<repo>/data`, unless overridden by `EDGE_DATA_ROOT` or `BACKTEST_DATA_ROOT`.

## Authoring references

Use these docs together when working on discover inputs:

- `11_proposal_authoring_and_campaigns.md` for proposal fields outside `hypothesis`, bounded follow-ups, experiment compilation, and campaign boundaries
- the structured proposal examples in `spec/proposals/` for canonical cold-start and bounded follow-up patterns
- `09_operator_runbook.md` for the end-to-end operator sequence after discover succeeds

## Discover outputs

Outputs are spread across four scopes.

### 1. Program memory

`project/research/knowledge/memory.py` creates and maintains:

- `tested_regions.parquet`
- `region_statistics.parquet`
- `event_statistics.parquet`
- `template_statistics.parquet`
- `context_statistics.parquet`
- `failures.parquet`
- `proposals.parquet`
- `reflections.parquet`
- `evidence_ledger.parquet`
- `belief_state.json`
- `next_actions.json`
- copied proposal files under `proposals/`

Root:

- `data/artifacts/experiments/<program_id>/memory/`

### 2. Run manifests

The pipeline writes run-scoped manifests under:

- `data/runs/<run_id>/run_manifest.json`
- stage-instance manifest JSON files in the same directory
- `kpi_scorecard.json`
- research checklist/signoff artifacts when applicable

### 3. Discovery reports

The exact stage outputs depend on the selected run plan, but the repo treats phase-2 discovery outputs as canonical search artifacts under:

- `data/reports/phase2/<run_id>/phase2_candidates.parquet`
- `data/reports/phase2/<run_id>/phase2_diagnostics.json`
- symbol/hypothesis subdirectories under the same root

### 4. Comparison and summary side outputs

`run_all.py` also calls run-comparison, summary, and postflight helpers that may write scorecards, audits, and runtime lineage fields.

## Trigger-mining lane

`edge discover triggers` is a separate internal research lane that generates candidate trigger proposals rather than directly mutating runtime behavior.

Current subcommands from `project/cli.py`:

- `parameter-sweep`
- `feature-cluster`
- `report`
- `emit-registry-payload`
- `list`
- `inspect`
- `review`
- `approve`
- `reject`
- `mark-adopted`

Use this lane only for manual-review workflows; it is not the default runtime path.

## Canonical commands

```bash
# cold-start plan without executing
edge discover plan --proposal spec/proposals/canonical_event_hypothesis.yaml

# cold-start execute
edge discover run --proposal spec/proposals/canonical_event_hypothesis.yaml

# bounded follow-up once baseline proposal memory exists
edge discover plan --proposal spec/proposals/canonical_event_hypothesis_h24.yaml

# inspect artifacts
edge discover list-artifacts --run_id <run_id>

# advanced trigger lane
edge discover triggers parameter-sweep --family vol_shock --symbol BTCUSDT
```

## Cold-start vs bounded examples

- `spec/proposals/canonical_event_hypothesis.yaml` is the canonical cold-start example and plans successfully on a clean data root.
- `spec/proposals/canonical_event_hypothesis_h24.yaml` is a bounded confirmation example. It will fail on a clean data root unless the referenced `baseline_run_id` already exists in the proposal memory store for the same `program_id`.
- Treat bounded proposals as follow-on experiments, not as bootstrap examples.

## Operational cautions

- Do not hand-edit generated experiment configs as a substitute for fixing proposal translation or spec defaults.
- Do not reintroduce legacy proposal compatibility into `proposal_schema.py`; the execution path is intentionally structured-only.
- When discover behavior changes, update the stage docs, the command inventory, and any affected artifact-path docs together.
