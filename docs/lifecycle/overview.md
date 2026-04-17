# System overview

## What Edge is

Edge is a research-to-runtime trading repository structured around a canonical four-stage lifecycle:

```
discover → validate → promote → deploy
```

Each stage is a distinct executable operation with defined artifact inputs and outputs. The same lifecycle is exposed through the `edge` CLI (`project/cli.py`) and the Makefile.

---

## Four-stage lifecycle

### 1. Discover

Takes a structured YAML proposal and produces discovery candidates.

- **Input:** `spec/proposals/*.yaml`
- **Runs:** pipeline DAG (ingest → clean → features → market_context → analyze_events → phase2_search_engine)
- **Output:** `data/artifacts/experiments/<program_id>/`, `data/reports/phase2/<run_id>/`
- **Details:** [lifecycle/discover.md](discover.md)

### 2. Validate

Converts discovery candidates into formally validated outputs with robustness testing.

- **Input:** `data/reports/phase2/<run_id>/phase2_candidates.parquet` (or edge_candidates, promotions tables)
- **Runs:** evaluation service, stability tests, regime slicing
- **Output:** `data/reports/validation/<run_id>/` — bundle, report, stability report, candidate tables
- **Details:** [lifecycle/validate.md](validate.md)

### 3. Promote

Packages validated candidates into governed thesis artifacts.

- **Input:** `data/reports/validation/<run_id>/promotion_ready_candidates.parquet`
- **Runs:** promotion service, live export
- **Output:** `data/live/theses/<run_id>/promoted_theses.json`, `data/live/theses/index.json`
- **Details:** [lifecycle/promote.md](promote.md)

### 4. Deploy

Runs the live or paper engine against promoted theses.

- **Input:** bound config yaml + promoted thesis package
- **Runs:** `project/scripts/run_live_engine.py`
- **Output:** live orders, positions, audit log, kill-switch events
- **Details:** [lifecycle/deploy.md](deploy.md)

---

## Mental model

### Specs define the vocabulary

All authored truth lives under `spec/`. The compiled runtime model is `spec/domain/domain_graph.yaml` — generated, do not edit.

- `spec/events/` — event definitions: family, detector, signal column, canonical regime
- `spec/templates/` — expression templates (e.g. `exhaustion_reversal`, `continuation`) and filter templates
- `spec/proposals/` — runnable YAML proposal files
- `spec/search/`, `spec/objectives/`, `spec/ontology/` — search spec vocabulary

After any spec change: `PYTHONPATH=. python3 project/scripts/build_domain_graph.py`

### A proposal is structured input only

The proposal schema (`project/research/agent_io/proposal_schema.py`) validates that a proposal is well-formed before anything runs. The pipeline cannot receive free-text instructions — only structured YAML that the schema accepts.

### Each run has a `run_id` and a lake

The `run_id` is the canonical identifier. It names:
- the data lake: `data/lake/runs/<run_id>/`
- all report directories: `data/reports/*/run_id>/`
- the thesis package: `data/live/theses/<run_id>/`

**Passing an existing `run_id` to a new proposal reuses that lake** (skips ingest/clean/features). When multiple proposals share a `run_id`, each overwrites `data/reports/phase2/<run_id>/` — read per-run results from `data/artifacts/experiments/<program_id>/evaluation_results.parquet` instead.

### Template-family compatibility

`check_hypothesis_feasibility` (in `project/research/search/feasibility.py`) silently drops incompatible event+template hypotheses at plan time. **If `estimated_hypothesis_count` in `validated_plan.json` is 0, check template compatibility before concluding no signal exists.**

Compatible templates by event family:
- VOLATILITY_EXPANSION / VOLATILITY_TRANSITION → `mean_reversion`, `continuation`, `impulse_continuation`, `vol_breakout`
- TREND_FAILURE_EXHAUSTION / FORCED_FLOW_AND_EXHAUSTION → `exhaustion_reversal`
- TREND_STRUCTURE → `exhaustion_reversal`, `mean_reversion`, `impulse_continuation`

---

## Research gates

| Gate | Requirement |
|------|-------------|
| Bridge gate | t ≥ 2.0 AND robustness ≥ 0.70 |
| Phase2 gate | robustness ≥ 0.60 |
| FDR gate | q_value < 0.05 (BH-adjusted) |

`rv_pct_17280` is on a 0–100 percentile scale (mean ≈ 46). A threshold of 70 means the 70th percentile, not 0.70.

---

## Repo-wide invariants

### Manifested pipeline runs

The orchestrator writes stage manifests and `run_manifest.json`. `project/specs/manifest.py` fingerprints and validates these. A manifest is the provenance record for a run.

### Specs feed generated registries

Changing event behavior means updating `spec/events/*.yaml` (and related template/ontology files) plus rebuilding the domain graph. The domain graph is the compiled read model consumed by research, promotion, and live export.

### Package DAG is enforced

`project/tests/test_architectural_integrity.py` enforces allowed import directions. Placing code in the wrong package fails the architecture test. See [reference/architecture.md](../reference/architecture.md).

### Live theses are governed artifacts

`promoted_theses.json` and `index.json` are schema-validated on export. `live_export.py` rejects malformed or incomplete lineage rather than silently exporting a partial package.

### Promotion gates (research profile)

Four gates have been removed from `PROMOTION_CONFIG_DEFAULTS` for the research profile:
1. `min_events`: 100 → 0
2. `allow_missing_negative_controls`: False → True
3. `dsr`: removed from `required_for_eligibility`
4. `use_effective_q_value=False` (prevents q inflation from scope degradation)
