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

`project/research/search/feasibility.check_hypothesis_feasibility` filters incompatible event+template pairs at plan time. Incompatible hypotheses are **silently dropped** — `estimated_hypothesis_count` in `validated_plan.json` becomes 0 with no error.

**Always verify `estimated_hypothesis_count > 0` in `data/artifacts/experiments/<prog>/<run_id>/validated_plan.json` before concluding an event has no edge.**

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
    <run_id>/validated_plan.json          ← check estimated_hypothesis_count
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

- **Bridge gate**: t ≥ 2.0 AND robustness ≥ 0.70
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

### Auto-updated docs (do not edit manually)

- `docs/all_results_2026-04-17.md` — every hypothesis result across the project, regenerated by `project/scripts/update_results_index.py`
- `docs/reflections.md` — auto-detected patterns (ceilings, template warnings, regime sensitivity) below the `<!-- AUTO-GENERATED -->` marker; human observations above it
- Both regenerate automatically via `.claude/settings.json` PostToolUse hook after any `project.cli discover/promote run` or `project.pipelines.run_all` command

---

## Current research state (2026-04-17)

### Promoted theses (paper-only)

| Event | Dir | Horizon | Template | t | rob | run_id |
|-------|-----|---------|----------|---|-----|--------|
| VOL_SPIKE | long | 24b | mean_reversion | 3.59 | 0.62 | `broad_vol_spike_20260416T210045Z_68e0020707` |
| OI_SPIKE_NEGATIVE | long | 24b | exhaustion_reversal | 2.28 | 0.85 | `campaign_pe_oi_spike_neg_20260416T092104Z_f6e6885923` |
| LIQUIDATION_CASCADE | long | 24b | exhaustion_reversal | 1.78 | 0.82 | `liquidation_std_gate_2yr_20260416T090207Z_84e1c40190` |

OI_SPIKE_NEGATIVE is running on Bybit testnet. VOL_SPIKE and LIQUIDATION_CASCADE are promoted but not deployed.

Paper config: `project/configs/live_paper_campaign_pe_oi_spike_neg_20260416T092104Z_f6e6885923.yaml`

### Signal boundaries

- Long only, BTC only, high-vol only (rv_pct_17280 > 70), 2023-2024 only (2022 dilutes all signals)
- Full campaign results: `docs/all_results_2026-04-17.md`
- Research reflections: `docs/reflections.md`, `docs/research_reflections_2026-04-17.md`

### Cached lake runs (for `--run_id` reuse)

| Data scope | run_id |
|------------|--------|
| BTC 2023-2024 (FORCED_FLOW events) | `broad_climax_volume_bar_20260416T202235Z_9787da0dd4` |
| BTC 2023-2024 (POSITIONING_EXTREMES) | `broad_oi_spike_positive_20260416T201712Z_2c9510827b` |
| BTC 2023-2024 (VOLATILITY_TRANSITION) | `broad_vol_spike_20260416T210045Z_68e0020707` |
| BTC 2022-2024 (3yr) | `liquidation_std_gate_3yr_20260416T090827Z_91dd43e2f6` |

### Next actions

1. Deploy VOL_SPIKE to paper: `edge deploy bind-config --run_id broad_vol_spike_20260416T210045Z_68e0020707`
2. Fund Bybit testnet USDT for OI_SPIKE_NEGATIVE paper engine
3. Multi-feature regime classifier — only remaining path to unlock below-gate cluster (CLIMAX_VOLUME_BAR, POST_DELEVERAGING_REBOUND, OI_SPIKE_POSITIVE)
