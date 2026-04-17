# Lifecycle overview

This repository is built around a governed four-stage lifecycle:

```text
discover → validate → promote → deploy
```

That sequence is the core architectural contract of the repo. It is not just the preferred CLI flow. The packages, artifacts, and runtime checks are organized around it.

## What the system is

Edge is a research-to-runtime trading system for event-driven strategies.

Its core unit of authorship is not a Python strategy class. It is a structured market hypothesis expressed through specs and proposals. Its core runtime unit is not a raw candidate row. It is a promoted thesis artifact.

The most important system transition is:

```text
proposal → discovery evidence → validation bundle → promoted thesis package → runtime action
```

## Stage boundaries in one table

| Stage | Purpose | Canonical input | Canonical output | Main code surface |
|------|---------|-----------------|------------------|-------------------|
| Discover | turn a proposal into candidate evidence | `spec/proposals/*.yaml` | experiment artifacts and discovery candidate tables | `project/research/agent_io/*`, `project/pipelines/run_all.py` |
| Validate | decide whether candidate rows remain credible under formal gates | discovery candidate tables | `data/reports/validation/<run_id>/` | `project/research/services/evaluation_service.py` |
| Promote | turn validated candidates into governed thesis contracts | `promotion_ready_candidates.parquet` | `data/live/theses/<run_id>/promoted_theses.json` | `project/research/services/promotion_service.py`, `project/research/live_export.py` |
| Deploy | run paper/live runtime against promoted theses | thesis package + runtime config | runtime actions, audits, snapshots, kill-switch events | `project/scripts/run_live_engine.py`, `project/live/runner.py` |

## The system model

### 1. Specs define the vocabulary

`spec/` is the authored source of truth for the trading vocabulary.

Important authored subtrees:
- `spec/events/` — event definitions and event metadata
- `spec/templates/` — template vocabulary used in research
- `spec/proposals/` — runnable proposal files
- `spec/search/` — search-space presets
- `spec/objectives/` — evaluation and objective semantics
- `spec/states/`, `spec/features/`, `spec/runtime/` — context and runtime vocabulary

`spec/domain/domain_graph.yaml` is a generated compiled read model. Treat it as generated output, not as the primary authoring surface.

### 2. Discovery is bounded research, not free search

A proposal is validated, normalized, and turned into an experiment plan. That plan includes feasibility checks and a bounded hypothesis count. The repo is intentionally structured to constrain search and make run scope explicit.

### 3. Validation is a separate legal stage

Discovery can produce rows that look statistically interesting. Validation decides whether those rows survive formal gates and can become promotion candidates.

### 4. Promotion is the bridge to runtime

The runtime does not consume raw discovery output. It consumes promoted thesis contracts exported from validated candidates. That is one of the strongest safety and governance rules in the codebase.

### 5. Deploy is thesis-driven runtime

The live and paper runtime loads a thesis batch, reconciles it against prior runtime state, and decides trade intent only in the context of active theses, risk policy, and runtime health.

## Canonical run identity and artifact flow

Each lifecycle instance is centered on a `run_id`.

A `run_id` ties together:
- the discovery lake under `data/lake/runs/<run_id>/`
- stage reports under `data/reports/*/<run_id>/`
- validation outputs under `data/reports/validation/<run_id>/`
- thesis export under `data/live/theses/<run_id>/`

A useful simplification is:

```text
run_id = the identity that binds discovery, validation, promotion, and deployment artifacts together
```

## Repo-wide invariants

### Runtime should consume only promoted theses

The codebase is designed so that research and runtime communicate through exported thesis artifacts. Bypassing that contract removes the main evidence-to-runtime safety boundary.

### Specs are authored; compiled outputs are generated

Change the authored specs and rebuild generated views. Do not patch the compiled domain graph or generated audits by hand unless the generators are broken.

### Package boundaries matter

`project/tests/test_architectural_integrity.py` enforces allowed package dependencies. Package placement is part of correctness.

### Shared risk policy should remain shared

Overlap, admission, and sizing logic lives in `project/portfolio/` so both execution-side simulation and live runtime can share policy instead of diverging.

### Artifacts and manifests are first-class

Runs are tracked through manifests, reports, and thesis exports. This is an artifact-driven system, not just a collection of scripts.

## The shortest correct mental model

Use this model when reading the repo:

1. a proposal defines what to test,
2. discovery runs the bounded search and writes evidence,
3. validation decides what survives,
4. promotion turns survivors into runtime-safe thesis contracts,
5. deploy loads those thesis contracts and trades only when runtime and risk gates allow it.

## Where to go next

- For the proposal-to-candidate path: [discover.md](discover.md)
- For the candidate-to-bundle path: [validate.md](validate.md)
- For the bundle-to-thesis path: [promote.md](promote.md)
- For the thesis-to-runtime path: [deploy.md](deploy.md)
- For the full code/package map: [../reference/full_repo_surface.md](../reference/full_repo_surface.md)
