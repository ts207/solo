# System overview

## What Edge is

Edge is a research-to-runtime trading repository with a canonical four-stage lifecycle exposed by `project/cli.py`:

1. `discover`
2. `validate`
3. `promote`
4. `deploy`

The same lifecycle is mirrored in the Makefile through `make discover`, `make validate`, `make promote`, `make export`, and `make deploy-paper`.

## Current mental model

### 1. Specs define the search and runtime vocabulary

The repo’s authored truth lives under `spec/`:

- event definitions and families in `spec/events/`
- feature/state/runtime/search specs in `spec/features/`, `spec/states/`, `spec/runtime/`, and `spec/search/`
- strategy, ontology, and objective specs in `spec/templates/`, `spec/ontology/`, `spec/objectives/`, and related subtrees
- the compiled domain graph in `spec/domain/domain_graph.yaml`

`project/spec_registry` loads these YAML sources. `project/domain/registry_loader.py` compiles them into a runtime `DomainRegistry` used by research, promotion, and live export.

### 2. Discover turns a structured proposal into an executable experiment

The discover stage is intentionally structured-only:

- `project/research/agent_io/proposal_schema.py` loads and validates the proposal
- `project/research/agent_io/proposal_to_experiment.py` translates it into an experiment config and `run_all` overrides
- `project/research/agent_io/execute_proposal.py` invokes the orchestrator
- `project/pipelines/run_all.py` plans, executes, audits, and finalizes pipeline stages

Proposal issuance also writes memory artifacts under `data/artifacts/experiments/<program_id>/memory/`.

### 3. Validate converts run outputs into formal validation artifacts

`project/validate/__init__.py` delegates to `project/research/services/evaluation_service.py` and `project/research/validation/result_writer.py`.

The validate stage reads candidate outputs from the canonical search/promotion locations, creates a `ValidationBundle`, and writes:

- `validation_bundle.json`
- `validation_report.json`
- `effect_stability_report.json`
- `validated_candidates.parquet`
- `rejection_reasons.parquet`
- `promotion_ready_candidates.parquet`

under `data/reports/validation/<run_id>/`.

### 4. Promote packages validated candidates into governable theses

`project/promote/__init__.py` delegates to `project/research/services/promotion_service.py`. The promotion stage reads validation outputs, applies promotion gates, writes promotion audits, and emits live-facing promoted thesis packages through `project/research/live_export.py`.

Important artifact roots:

- `data/reports/promotions/<run_id>/`
- `data/reports/strategy_blueprints/<run_id>/`
- `data/live/theses/<run_id>/promoted_theses.json`
- `data/live/theses/index.json`

### 5. Deploy runs a live or paper runtime against promoted theses

The deploy surface in `project/cli.py` routes to the live-engine launcher. The runtime entry point is `project/scripts/run_live_engine.py`.

In the current code, CLI `--run_id` on `edge deploy paper|live` gates deployment against an exported thesis batch, but thesis loading inside the runner still comes from `strategy_runtime.thesis_run_id` or `strategy_runtime.thesis_path` in the runtime config.

The launcher enforces:

- runtime mode in `{monitor_only, trading}`
- explicit thesis input via `strategy_runtime.thesis_path` or `strategy_runtime.thesis_run_id`
- environment gating for trading mode
- venue and snapshot configuration validation before start

## Repo-wide invariants that show up in code and tests

### Structured proposals only

The proposal loader and execution path are now structured-only. The compatibility/legacy proposal path has been removed from the runtime path.

### Canonical package DAG

`project/tests/test_architectural_integrity.py` enforces allowed import directions between package families. The repo is not free-form; package placement matters.

### Manifested pipeline runs

The pipeline orchestrator writes stage manifests and a `run_manifest.json`. `project/specs/manifest.py` normalizes, fingerprints, and validates manifest payloads.

### Specs feed generated registries

A large part of the repo is spec-driven. Changing behavior often means updating source YAML or the generator that emits compiled registries and audits.

### Live theses are governed artifacts

Promotion export validates the schema of `promoted_theses.json` and `index.json` and rejects malformed or incomplete lineage.

## High-signal directories

- `project/core` — cross-cutting primitives and configuration
- `project/spec_registry` / `project/specs` / `project/spec_validation` — spec loading and validation
- `project/domain` — compiled registry models
- `project/events` / `project/features` / `project/research` — the research/search/ontology heart of the repo
- `project/pipelines` — orchestrator and stage DAG
- `project/engine` / `project/portfolio` — execution and risk simulation logic
- `project/live` / `project/runtime` — live/paper runtime and replay logic
- `project/reliability` / `project/tests` — assurance, smoke, regression, and contract checks

## What changed relative to older docs

This repo no longer needs a compatibility-layer explanation. The current source tree uses canonical paths, structured proposal ingestion, canonical family/regime metadata, and direct package boundaries enforced by tests. Treat older wrapper-oriented descriptions as obsolete.


## Terminology: Proposal vs Experiment vs Campaign

In the Edge lifecycle architecture, the abstraction boundary is strictly defined:
- **Proposal**: the authored configuration (YAML) hypothesis/input.
- **Experiment**: the compiled executable run config derived specifically from that proposal.
- **Campaign**: a higher-level multi-run controller/container over related proposals/runs.

See `spec/proposals/` for proposal authoring examples.
