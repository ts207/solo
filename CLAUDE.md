# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## Commands

```bash
# Run all tests
PYTHONPATH=. python3 -m pytest -q

# Run fast tests only (skip slow/integration)
PYTHONPATH=. python3 -m pytest -q -m "not slow"

# Run a single test file
PYTHONPATH=. python3 -m pytest project/tests/path/to/test_file.py -q

# Lint changed files
make lint

# Format changed files
make format

# Minimum green gate (compile + architecture + regression)
make minimum-green-gate

# Rebuild the compiled domain graph after spec changes
PYTHONPATH=. python3 project/scripts/build_domain_graph.py

# CLI entrypoint (also available as `edge` after pip install -e .)
PYTHONPATH=. python3 -m project.cli --help
```

### Research lifecycle

```bash
# Discover (plan only — no data)
PYTHONPATH=. python3 -m project.cli discover plan --proposal spec/proposals/your_proposal.yaml

# Discover (run — builds lake, evaluates, writes artifacts)
PYTHONPATH=. python3 -m project.cli discover run --proposal spec/proposals/your_proposal.yaml

# Reuse cached lake data from a previous run
PYTHONPATH=. python3 -m project.cli discover run --proposal spec/proposals/your_proposal.yaml --run_id <existing_run_id>

# Promote candidates from a run
PYTHONPATH=. python3 -m project.cli promote run --run_id <run_id> --symbols BTCUSDT

# Export promoted theses
PYTHONPATH=. python3 -m project.cli promote export --run_id <run_id>

# Bind a paper config from a promoted run
PYTHONPATH=. python3 -m project.cli deploy bind-config --run_id <run_id>
```

---

## Architecture

### Four-stage lifecycle

`discover → validate → promote → deploy`, all exposed through `project/cli.py` (also: `edge` CLI after install).

1. **Discover** — takes a YAML proposal, translates it to an experiment config, runs the pipeline DAG, writes discovery artifacts and program memory to `data/artifacts/experiments/<program_id>/`
2. **Validate** — reads pipeline outputs, produces `ValidationBundle`, writes to `data/reports/validation/<run_id>/`
3. **Promote** — applies promotion gates, packages candidates into governed theses, writes to `data/live/theses/<run_id>/promoted_theses.json`
4. **Deploy** — launches the live/paper runtime against a bound config, reading theses from `strategy_runtime.thesis_run_id` or `thesis_path`

### Proposal → pipeline flow

```
spec/proposals/*.yaml
  └─ project/research/agent_io/issue_proposal.py       ← front door
       ├─ proposal_schema.py                            ← load + validate
       ├─ proposal_to_experiment.py                     ← translate to experiment config + run_all overrides
       │    └─ experiment_engine.build_experiment_plan  ← expand hypotheses, check feasibility
       └─ execute_proposal.py                           ← shell into run_all.py
            └─ project/pipelines/run_all.py             ← stage DAG orchestrator
```

`project/pipelines/run_all.py` plans and executes the stage DAG: ingest → clean → features → market_context → analyze_events → phase2_search_engine → promotion → finalize.

### Spec and domain model

All authored truth lives under `spec/`. The compiled runtime model is `spec/domain/domain_graph.yaml` (11k lines, generated — do not edit).

- `spec/events/*.yaml` — event definitions: detector class, signal_column, events_file, family, canonical_regime
- `spec/templates/` — expression templates (exhaustion_reversal, continuation, etc.) and filter templates (only_if_regime, only_if_highvol)
- `spec/proposals/` — proposal YAML files; canonical example: `canonical_event_hypothesis.yaml`
- `spec/productive_search.yaml` — default search spec used by most proposals
- `project/domain/compiled_registry.py` — `get_domain_registry()` loads the compiled graph (LRU-cached)

After changing any spec YAML, rebuild the domain graph:
```bash
PYTHONPATH=. python3 project/scripts/build_domain_graph.py
```

### Hypothesis feasibility and template-family compatibility

`project/research/search/feasibility.check_hypothesis_feasibility` filters incompatible event+template pairs at plan time and now records a `feasibility_summary` in `validated_plan.json`. A run fails closed when feasible hypotheses are below `search_control.min_feasible` (default: 1); use the explicit allow-empty/min-feasible escape valve only for diagnostic exploration.

Use `edge discover explain-empty --run_id <run_id>` for grouped drop reasons and `edge discover funnel --run_id <run_id>` for generated → feasible → gated → promoted survival counts.

Compatible templates by event family:
- VOLATILITY_EXPANSION / VOLATILITY_TRANSITION → `mean_reversion`, `continuation`, `impulse_continuation`, `vol_breakout`
- TREND_FAILURE_EXHAUSTION / FORCED_FLOW_AND_EXHAUSTION → `exhaustion_reversal`
- TREND_STRUCTURE → `exhaustion_reversal`, `mean_reversion`, `impulse_continuation`

### Data layout

```
data/
  lake/runs/<run_id>/
    cleaned/perp/<SYMBOL>/bars_5m/year=.../month=.../
    features/perp/<SYMBOL>/5m/features_feature_schema_v2/...
    features/perp/<SYMBOL>/5m/market_context/...
  artifacts/experiments/<program_id>/
    <run_id>/evaluation_results.parquet   ← primary result artifact
    <run_id>/validated_plan.json          ← feasibility_summary + estimated_hypothesis_count
    memory/event_statistics.parquet       ← per-event summary across runs
    memory/tested_ledger.parquet          ← eval_status per hypothesis
  reports/
    phase2/<run_id>/hypotheses/BTCUSDT/evaluated_hypotheses.parquet
    validation/<run_id>/
    edge_candidates/<run_id>/
  live/theses/<run_id>/promoted_theses.json
```

**`--run_id` reuse:** passing an existing run_id reuses that lake (skips data rebuild). When multiple proposals share the same `run_id`, each overwrites `data/reports/phase2/<run_id>/`. Read results from per-experiment `evaluation_results.parquet` or `campaign_summary.json`, not the shared phase2 dir.

### Research gates

- **Promotion bridge gate**: t ≥ 2.0 AND robustness ≥ 0.70 (checked at promotion time via `min_stability_score`)
- **Search bridge gate**: robustness ≥ 0.5 (`spec/gates.yaml` `search_bridge_min_robustness_score`) — intentionally permissive for research recall
- **Phase2 gate**: robustness ≥ 0.60
- `rv_pct_17280` is on a 0–100 percentile scale (mean ≈ 46) — threshold 70 means 70th percentile, not 0.70

### Adding a filter template to an event (3-file sync required)

1. `spec/templates/registry.yaml` — `compatible_families` must use `canonical_regime` (not `canonical_family`); add to event block AND families section
2. `spec/templates/event_template_registry.yaml` — mirror
3. `spec/events/event_registry_unified.yaml` — mirror
4. Rebuild domain graph

### Package dependency DAG (enforced by tests)

`project/tests/test_architectural_integrity.py` enforces allowed import directions. Key rule: `project.core` → `project.research` is one-way. Placing code in the wrong package will fail the architecture test.

### Live / paper runtime

The live engine runs from `project/scripts/run_live_engine.py`. Required env vars:

```bash
export EDGE_ENVIRONMENT=paper         # or live
export EDGE_VENUE=binance             # or bybit
export EDGE_LIVE_CONFIG=<path>        # bound config yaml
export EDGE_LIVE_SNAPSHOT_PATH=<path> # state snapshot json
# Plus venue-specific API key/secret vars
```

Startup certification (no credentials needed): `PYTHONPATH=. python3 project/scripts/certify_paper_startup.py`

## Current research state

### Confirmed green as of 2026-04-27

```bash
make minimum-green-gate                                                        # pass
PYTHONPATH=. python3 project/scripts/run_supported_path_benchmark.py \
    --execute 1 --data_root data                                               # completed, failures: []
```

Benchmark slice verified end-to-end:

```
event:   PRICE_DOWN_OI_DOWN
spec:    spec/discovery/benchmark_eligible_v1
symbol:  BTCUSDT  (2022-01-01 – 2024-12-31)

proposals attempted:      1
multiplicity discoveries: 1
best q-value:             ≈ 0.0095
validation pass rate:     1.0
```

The full supported path is smoke-proven:
`cell discovery → summarize → assemble-theses → discover → validate → promote → export`

### Lake data

`data/lake/cleaned/` and `data/lake/features/` cover BTCUSDT 2021–2024 and ETHUSDT 2022–2024.
`data/lake/features/perp/BTCUSDT/5m/market_context/` covers 2022–2024 (required for context-conditioned cells).
No run-scoped lake — the pipeline falls back to the global lake automatically.

### Starting a new discovery run

```bash
# First run (uses global lake, no --run_id needed)
edge discover run --proposal spec/proposals/<proposal>.yaml

# Subsequent runs over the same data: reuse the lake
edge discover run --proposal spec/proposals/<other>.yaml --run_id <run_id_from_first_run>
```
