# Full repository surface

This file is the top-down map of the entire repository. Use it when you need to answer three questions quickly:

1. what exists,
2. what owns it,
3. where to start reading.

## 1. Top-level directories

| Path | Role | Start reading |
|------|------|---------------|
| `README.md` | top-level orientation and canonical lifecycle | this file, then `docs/README.md` |
| `Makefile` | operator shortcuts, quality gates, workflow bundles | `make help` and `docs/reference/commands.md` |
| `pyproject.toml` | package metadata, dependencies, console entry points | `[project.scripts]` section |
| `project/` | application code | `project/cli.py` then package docs below |
| `spec/` | authored domain model and search vocabulary | `docs/reference/spec_authoring.md` |
| `docs/` | authored docs plus generated audits | `docs/README.md` |
| `dashboard/` | standalone local operator dashboard | `dashboard/README.md` |
| `deploy/` | example env files and systemd service units | `deploy/env/`, `deploy/systemd/` |
| `data/` | default local runtime data root | see artifact map below |
| `CONTRIBUTING.md`, `CLAUDE.md` | workflow and repo conventions | read before structural changes |

## 2. Application packages under `project/`

Counts below reflect the current repository snapshot.

### Lifecycle and entry packages

| Package | Approx. size | What it owns | Read first |
|---------|--------------|--------------|------------|
| `project/cli.py` | single CLI module | canonical `edge` lifecycle surface | `project/cli.py` |
| `project/discover/` | 1 module | discover command wrappers | `project/discover/__init__.py` |
| `project/validate/` | 1 module | validate command wrappers | `project/validate/__init__.py` |
| `project/promote/` | 1 module | promote command wrappers | `project/promote/__init__.py` |
| `project/deploy/` | 1 module | deploy command wrappers/helpers | `project/deploy/__init__.py` |

### Core substrate

| Package | Python files | What it owns | Read first |
|---------|--------------|--------------|------------|
| `project/core/` | 28 | config, coercion, stats, common validation primitives | `project/core/config.py` |
| `project/io/` | 10 | file-system and parquet helpers | `project/io/*` helpers used by artifacts/research/live |
| `project/artifacts/` | 3 | canonical artifact path helpers | `project/artifacts/*` |
| `project/schemas/` | 4 | shared schemas and control spec helpers | `project/schemas/*` |

### Spec and domain model

| Package | Python files | What it owns | Read first |
|---------|--------------|--------------|------------|
| `project/spec_registry/` | 4 | loading authored specs from `spec/` | `project/spec_registry/*` |
| `project/specs/` | 9 | spec-side helpers, manifests, gates, invariants | `project/specs/manifest.py` |
| `project/spec_validation/` | 7 | grammar and spec validation | `project/spec_validation/*` |
| `project/domain/` | 7 | compiled domain registry and loaders | `project/domain/compiled_registry.py` |
| `project/contracts/` | 7 | stage/artifact/system-map contracts | `project/contracts/README.md`, then `pipeline_registry.py` |

### Research and discovery stack

| Package | Python files | What it owns | Read first |
|---------|--------------|--------------|------------|
| `project/research/` | 270 | proposal translation, candidate search, evaluation, reporting, promotion support, thesis export support | `project/research/README.md` |
| `project/operator/` | 9 | bounded proposal semantics, proposal comparison, campaign support | `project/operator/bounded.py` |
| `project/eval/` | 24 | attribution, ablation, benchmark, cost, drift, evaluation helpers | package modules nearest the metric you need |
| `project/experiments/` | 4 | experiment-level orchestration helpers and metadata | package root |
| `project/episodes/` | 2 | episode contracts/registry support | package root |
| `project/compilers/` | 3 | strategy/blueprint compilation utilities | package root |

### Market/event/context model

| Package | Python files | What it owns | Read first |
|---------|--------------|--------------|------------|
| `project/events/` | 82 | event registry, detectors, families, governance, ontology mapping | `project/events/registry.py`, `project/events/detectors/` |
| `project/features/` | 16 | feature/state/context derivation | relevant feature module for the signal family |
| `project/synthetic_truth/` | 10 | synthetic truth generation and support tooling | package root |

### Orchestration and runtime execution

| Package | Python files | What it owns | Read first |
|---------|--------------|--------------|------------|
| `project/pipelines/` | 83 | run planning, stage assembly, provenance, `run_all` execution | `project/pipelines/README.md`, then `run_all.py` |
| `project/engine/` | 17 | execution mechanics, fills, PnL, lower-level execution transitions | `project/engine/README.md` |
| `project/portfolio/` | 8 | shared admission, overlap, sizing, risk budget policy | package modules directly |
| `project/runtime/` | 9 | replay/timebase/runtime invariants | `project/runtime/README.md` |
| `project/live/` | 38 | live runner, thesis store, reconciliation, OMS, venue interfaces, kill-switch | `project/live/runner.py` |
| `project/strategy/` | 35 | strategy spec/runtime components used by engine/live/research | package root |
| `project/reliability/` | 12 | smoke entrypoints, safety checks, operational lint | `project/reliability/cli_smoke.py` |

### Tooling and integration

| Package | Python files | What it owns | Read first |
|---------|--------------|--------------|------------|
| `project/scripts/` | 95 | direct scripts for governance, audits, certification, generation, maintenance | `docs/reference/commands.md` for an inventory |
| `project/apps/` | 11 | ChatGPT/MCP app scaffold | `project/apps/chatgpt/` |
| `project/tests/` | 640 | unit, contract, regression, smoke, architecture, runtime, research tests | see test taxonomy below |

## 3. Authoritative spec tree under `spec/`

The `spec/` tree is the authored source of truth for the domain model. Important subtrees:

| Path | What it contains |
|------|------------------|
| `spec/events/` | event contracts, governance fields, detector/runtime bindings |
| `spec/states/` | regime/state definitions used by context and filtering |
| `spec/features/` | feature definitions and metric vocabulary |
| `spec/templates/` | template registries and event-template compatibility |
| `spec/proposals/` | runnable discover-stage proposals |
| `spec/search/` | search-space and discovery controls |
| `spec/objectives/` | discovery objectives |
| `spec/ontology/` | ontology templates and domain vocabulary (generated output — do not edit by hand) |
| `spec/runtime/` | runtime-related grammar and controls |
| `spec/campaigns/`, `spec/hypotheses/`, `spec/theses/`, `spec/benchmarks/` | campaign, hypothesis, thesis, and benchmark contracts |
| `spec/domain/domain_graph.yaml` | compiled domain graph, generated read model, do not edit by hand |

### Useful counts

- `spec/events/`: 90 YAML files
- `spec/states/`: 80 YAML files
- `spec/features/`: 42 YAML files
- `spec/proposals/`: 67 YAML files
- `spec/ontology/`: generated output (gitignored); regenerated by `build_template_registry_sidecars.py`

## 4. Documentation tree under `docs/`

| Path | Role |
|------|------|
| `docs/lifecycle/` | discover/validate/promote/deploy explanations |
| `docs/reference/` | repo maps, architecture, commands, spec authoring, assurance |
| `docs/operator/` | operator runbook |
| `docs/research/` | current results and narrative |
| `docs/generated/` | generated inventories and audits |

Generated docs worth knowing:

- `docs/generated/system_map.md`
- `docs/generated/detector_coverage.md`
- `docs/generated/event_contract_reference.md`
- `docs/generated/event_ontology_mapping.md`

## 5. Runtime artifacts and data layout

The repo is artifact-driven. Common locations beneath the data root:

| Path | Meaning |
|------|---------|
| `data/artifacts/experiments/<program_id>/` | proposal memory, validated plans, experiment artifacts |
| `data/lake/runs/<run_id>/` | cached run lake for reuse across stages |
| `data/reports/phase2/<run_id>/` | discovery/phase2 reports |
| `data/reports/edge_candidates/<run_id>/` | normalized candidate output |
| `data/reports/validation/<run_id>/` | canonical validation bundle and survivor tables |
| `data/live/theses/<run_id>/` | exported thesis package for runtime |
| `data/live/theses/index.json` | thesis batch index |

Data root resolution order:

1. `EDGE_DATA_ROOT`
2. `BACKTEST_DATA_ROOT`
3. `<repo>/data`

## 6. Tests and assurance surface

`project/tests/` is large and intentionally partitioned. Highest-signal directories:

| Directory | Approx. `test_*.py` count | What it covers |
|-----------|---------------------------|----------------|
| `project/tests/research/` | 117 | proposal flow, discovery logic, promotion-adjacent research behavior |
| `project/tests/pipelines/` | 138 | orchestrator and stage-family behavior |
| `project/tests/live/` | 29 | live engine, thesis runtime, startup/runtime safety |
| `project/tests/eval/` | 26 | evaluation and benchmark helpers |
| `project/tests/engine/` | 21 | execution behavior and lower-level engine correctness |
| `project/tests/events/` | 35 | event contracts, detectors, registry correctness |
| `project/tests/contracts/` | 18 | stage/artifact/schema contracts |
| `project/tests/scripts/` | 33 | CLI and script-level regressions |
| `project/tests/regressions/` | 13 | protection against previously observed failures |
| `project/tests/architecture/` | 4 | package dependency DAG and structural checks |
| `project/tests/smoke/` | 10 | quick operational confidence checks |

Critical repo-wide tests/checks:

- `project/tests/test_architectural_integrity.py`
- `make minimum-green-gate`
- `make governance`

## 7. Entry points and external-facing surfaces

### CLI and scripts

The canonical front door is `project/cli.py`, exposed as `edge`, `backtest`, and `edge-backtest`.

Other important console scripts:

- `edge-run-all`
- `edge-live-engine`
- `edge-phase2-discovery`
- `edge-promote`
- `edge-smoke`
- `edge-chatgpt-app`

### Dashboard

- `dashboard/index.html`
- `dashboard/server.py`
- `dashboard/start.sh`

### Deployment assets

- `deploy/env/*.env.example`
- `deploy/systemd/*.service`

### Runtime configs

`project/configs/` contains the working configuration library for discovery presets, validation/promotion presets, and live paper/production configs.

## 8. How to read the repo efficiently

### To understand one full run

1. `project/cli.py`
2. `project/research/agent_io/issue_proposal.py`
3. `project/research/agent_io/proposal_to_experiment.py`
4. `project/research/agent_io/execute_proposal.py`
5. `project/pipelines/run_all.py`
6. `project/research/services/evaluation_service.py`
7. `project/research/services/promotion_service.py`
8. `project/research/live_export.py`
9. `project/scripts/run_live_engine.py`
10. `project/live/runner.py`

### To add a new event

1. `spec/events/<NEW_EVENT>.yaml`
2. `project/events/detectors/`
3. `project/configs/registries/detectors.yaml` if applicable
4. `spec/search_space.yaml`
5. rebuild domain graph and run governance/assurance

### To modify runtime policy safely

1. `project/live/`
2. `project/portfolio/`
3. `project/runtime/`
4. relevant tests in `project/tests/live/`, `project/tests/runtime/`, and `project/tests/regressions/`

## 9. Generated vs authored rule

Edit by hand:

- `docs/` except `docs/generated/`
- `spec/**/*.yaml` except generated compiled outputs
- `project/**/*.py`
- runtime config files in `project/configs/`
- deployment templates in `deploy/`

Regenerate instead of hand-editing:

- `docs/generated/*`
- `spec/domain/domain_graph.yaml` — rebuild with `project/scripts/build_domain_graph.py`
- `spec/ontology/` — generated by `project/scripts/build_template_registry_sidecars.py`
- `spec/templates/event_template_registry.yaml` — generated by the same script
- other generated inventories or registry mirrors produced by governance scripts
