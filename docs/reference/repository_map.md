# Repository map

This file explains how the repo is shaped logically, not just where directories happen to sit on disk.

## System model in one sentence

`spec/` defines the market vocabulary, `project/research/` and `project/pipelines/` turn that vocabulary into evidence, `project/research/live_export.py` packages evidence into theses, and `project/live/` consumes those theses at runtime.

## Top-level dependency flow

```text
spec/ → spec_registry/spec_validation/specs/domain
     → events/features/research/operator/eval
     → pipelines
     → validation/promotion/live_export
     → live/runtime/engine/portfolio
```

The package DAG is enforced by `project/tests/test_architectural_integrity.py`. Package placement is therefore part of correctness, not just tidiness.

## Reading order for code

### For end-to-end lifecycle understanding

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

### For package-boundary understanding

1. `docs/reference/architecture.md`
2. `project/contracts/`
3. `project/pipelines/`
4. `project/research/`
5. `project/live/`, `project/runtime/`, `project/engine/`, `project/portfolio/`

## Package ownership

### 1. Core substrate

| Package | Ownership | Important files |
|---------|-----------|-----------------|
| `project/core` | shared config, stats, coercion, validation helpers | `config.py`, shared utility modules |
| `project/io` | file and serialization helpers | parquet and file-system helpers |
| `project/artifacts` | canonical artifact paths | artifact path helpers |
| `project/schemas` | reusable schemas/control contracts | schema modules |

These packages should stay low-level and side-effect light.

### 2. Spec and domain model

| Package | Ownership | Important files |
|---------|-----------|-----------------|
| `project/spec_registry` | loading authored specs from `spec/` | package modules |
| `project/specs` | spec-side invariants, manifests, helper models | `manifest.py` |
| `project/spec_validation` | grammar and consistency checks for specs | package modules |
| `project/domain` | compiled registry read model | `compiled_registry.py` |
| `project/contracts` | stage, artifact, and system-map contracts | `pipeline_registry.py`, `artifacts.py`, `system_map.py` |

These packages encode vocabulary and contracts. They should not own research decisions or live orchestration.

### 3. Market interpretation layer

| Package | Ownership | Important files |
|---------|-----------|-----------------|
| `project/events` | event registry, ontology, governance, detectors | `registry.py`, detector modules |
| `project/features` | feature/state/context derivation | feature modules relevant to your event family |
| `project/synthetic_truth` | synthetic data/truth support | package modules |

This layer converts authored vocabulary into detectible and computable market constructs.

### 4. Research and evaluation layer

| Package | Ownership | Important files |
|---------|-----------|-----------------|
| `project/research` | proposal handling, candidate search, evaluation, reporting, promotion support | `agent_io/`, `services/`, `live_export.py` |
| `project/operator` | bounded proposal semantics and operator support | `bounded.py` |
| `project/eval` | analytics, benchmarks, drift, attribution, cost helpers | package modules |
| `project/experiments`, `project/episodes`, `project/compilers` | supporting experiment/episode/compiler utilities | package roots |

This is the broadest layer because it coordinates search and evidence production. It is intentionally rich, but it should not absorb generic utility code that belongs lower.

### 5. Orchestration layer

| Package | Ownership | Important files |
|---------|-----------|-----------------|
| `project/pipelines` | stage planning, run orchestration, provenance, run manifests | `run_all.py`, `pipeline_planning.py`, `stage_registry.py` |

The orchestration layer wires together stages but should not become the home for business semantics.

### 6. Execution and runtime layer

| Package | Ownership | Important files |
|---------|-----------|-----------------|
| `project/engine` | execution mechanics and deterministic trade-state transitions | engine core modules |
| `project/portfolio` | overlap, admission, sizing, risk budget policy | policy modules |
| `project/runtime` | runtime invariants, replay/timebase surfaces | runtime modules |
| `project/live` | live runner, thesis store, reconciliation, OMS, venue wiring, kill-switch | `runner.py`, `thesis_store.py`, `thesis_reconciliation.py` |
| `project/strategy` | strategy runtime/spec support used by research and execution | package modules |
| `project/reliability` | smoke and reliability checks | `cli_smoke.py` |

The key design intent here is that the live runner acts on promoted theses rather than arbitrary raw research outputs.

## The authored spec tree and how it maps to code

| Spec subtree | Main consumers |
|-------------|----------------|
| `spec/events/` | `project/events`, `project/domain`, research planning |
| `spec/states/`, `spec/features/` | `project/features`, research filters, runtime context |
| `spec/templates/`, `spec/search/` | discovery planning and hypothesis expansion |
| `spec/proposals/` | `project/research/agent_io/*`, discover CLI |
| `spec/objectives/`, `spec/ontology/`, `spec/runtime/` | validation, routing, domain compilation, runtime controls |
| `spec/domain/domain_graph.yaml` | `project/domain/compiled_registry.py` |

When changing domain behavior, start from the authored spec files and regenerate compiled outputs rather than patching generated artifacts.

## Non-code surfaces that matter operationally

| Path | What it does |
|------|--------------|
| `project/configs/` | discovery, validation, promotion, and live runtime config presets |
| `project/scripts/` | governance, audits, maintenance, certification, and direct launch scripts |
| `dashboard/` | local repo/operator dashboard |
| `deploy/` | env examples and systemd service templates |
| `docs/generated/` | machine-derived audits used for governance and orientation |

## Where to start for common tasks

### Add or change a proposal

- `spec/proposals/`
- `docs/lifecycle/discover.md`
- `project/research/agent_io/proposal_schema.py`

### Diagnose a run

- `data/artifacts/experiments/<program_id>/`
- `data/reports/phase2/<run_id>/`
- `data/reports/validation/<run_id>/`
- `docs/operator/runbook.md`

### Add an event detector

- `spec/events/`
- `project/events/detectors/`
- `docs/reference/spec_authoring.md`
- `make governance`

### Change runtime behavior

- `project/scripts/run_live_engine.py`
- `project/live/runner.py`
- `project/live/thesis_store.py`
- `project/portfolio/`
- `project/tests/live/` and `project/tests/regressions/`

## Authored vs generated

### Edit directly

- `project/**/*.py`
- `spec/**/*.yaml` except compiled generated outputs
- `docs/**/*.md` except `docs/generated/`
- `project/configs/**/*.yaml`
- `deploy/**/*`

### Regenerate

- `docs/generated/*`
- `spec/domain/domain_graph.yaml`
- generated registry and audit artifacts refreshed by governance scripts
