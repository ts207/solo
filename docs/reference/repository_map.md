# Repository map

## Top-level layout

```
project/      application code, pipeline orchestration, runtime, research, tests
spec/         authored YAML/CSV specs — the source of truth for domain model
docs/         documentation (lifecycle, reference, operator, research)
data/         local data root (default; override with EDGE_DATA_ROOT)
deploy/       deployment config roots
```

## `project/` packages

### Core substrate (lowest layer)

| Package | What it owns |
|---------|-------------|
| `project/core` | Config, coercion, stats, validation primitives, feature schema |
| `project/io` | Parquet compat, repository helpers, I/O utilities |
| `project/artifacts` | Canonical artifact path helpers |
| `project/contracts` | Pipeline/stage/system-map contracts |

### Spec and domain model

| Package | What it owns |
|---------|-------------|
| `project/spec_registry` | Load YAML specs from `spec/` |
| `project/specs` | Manifest, ontology, gates, invariants, objective helpers |
| `project/spec_validation` | Grammar, ontology, search validators |
| `project/domain` | Compiled registry models and loaders; `get_domain_registry()` |

### Research stack

| Package | What it owns |
|---------|-------------|
| `project/events` | Event registry, ontology, detectors, arbitration, governance |
| `project/features` | Feature, state, and context derivation |
| `project/research` | Search, candidate generation, evaluation, promotion helpers, reporting |
| `project/operator` | Bounded proposal semantics, campaign/mutation decision support |
| `project/pipelines` | Run planner, execution DAG, stage registry, provenance |

### Execution and runtime

| Package | What it owns |
|---------|-------------|
| `project/engine` | Backtest execution, fills, PnL, slippage, risk allocator |
| `project/portfolio` | Overlap/admission policy, sizing, risk budgets |
| `project/live` | Live runner, OMS, kill-switch, thesis reconciliation, venue clients |
| `project/runtime` | Replay, timebase, firewall, invariant helpers |
| `project/reliability` | Smoke, contracts, promotion gates, temporal lint |

### Supporting packages

| Package | What it owns |
|---------|-------------|
| `project/eval` | Ablation, attribution, benchmarks, cost model, drift detection |
| `project/strategy` | Executable strategy spec and runtime |
| `project/compilers` | Strategy compilation surface |
| `project/episodes` | Episode registry |
| `project/schemas` | Control spec, data contracts, strategy spec |
| `project/scripts` | CLI scripts — audits, generation, benchmarks, governance |
| `project/tests` | All test files (565 files) — unit, contract, integration, regression, smoke |

## `spec/` subtree

| Directory | What it contains |
|-----------|-----------------|
| `spec/events/` | Event definitions: detector, signal_column, family, canonical_regime, templates |
| `spec/templates/` | Expression templates, filter templates, registry, event_template_registry |
| `spec/proposals/` | Runnable YAML proposals (authored examples + project-specific runs) |
| `spec/states/` | Regime/state definitions |
| `spec/features/` | Feature definitions and metrics |
| `spec/ontology/` | Ontology templates, features, states |
| `spec/runtime/` | Runtime lanes, hashing, firewall |
| `spec/search/` | Search specs (benchmark, full, smoke, productive) |
| `spec/domain/domain_graph.yaml` | **Compiled domain graph — generated, do not edit** |
| `spec/campaigns/`, `spec/hypotheses/`, `spec/theses/` | Campaign and hypothesis specs |

## Authored vs generated

### Authored (edit these)
- `spec/**/*.yaml` except `spec/domain/domain_graph.yaml`
- `project/configs/registries/*.yaml`
- `docs/` except `docs/generated/`
- `spec/proposals/*.yaml`

### Generated (do not edit; regenerate with commands)
- `spec/domain/domain_graph.yaml` — `PYTHONPATH=. python3 project/scripts/build_domain_graph.py`
- `docs/generated/*` — `make governance`
- `project/configs/registries/events.yaml` — synced by governance scripts

### Written at runtime (not source-controlled by default)
- `data/lake/runs/<run_id>/` — cleaned bars and features
- `data/artifacts/experiments/` — discovery artifacts and program memory
- `data/reports/` — all stage output reports
- `data/live/theses/` — promoted thesis packages

## `data/` layout

```
data/
  lake/runs/<run_id>/
    cleaned/perp/<SYMBOL>/bars_5m/year=.../month=.../
    features/perp/<SYMBOL>/5m/features_feature_schema_v2/year=.../
    features/perp/<SYMBOL>/5m/market_context/year=.../
    metadata/run_manifest.json
  artifacts/experiments/<program_id>/
    <run_id>/
      evaluation_results.parquet     ← primary discovery result
      validated_plan.json            ← check estimated_hypothesis_count
      expanded_hypotheses.parquet
    memory/
      event_statistics.parquet
      tested_ledger.parquet
      belief_state.json
  reports/
    phase2/<run_id>/
      hypotheses/BTCUSDT/evaluated_hypotheses.parquet
      phase2_diagnostics.json
      phase2_candidates.parquet
    validation/<run_id>/
    promotions/<run_id>/
    edge_candidates/<run_id>/
    expectancy/<run_id>/
  live/theses/
    index.json
    <run_id>/promoted_theses.json
```
