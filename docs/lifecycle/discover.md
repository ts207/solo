# Discover stage

The discover stage turns a structured proposal into a governed pipeline run and candidate evidence.

It answers this question:

**Given a bounded market hypothesis, what candidate effects can the system discover, measure, and record?**

## CLI surface

```bash
edge discover plan --proposal spec/proposals/your_proposal.yaml
edge discover run  --proposal spec/proposals/your_proposal.yaml
edge discover run  --proposal spec/proposals/your_proposal.yaml --run_id <existing_run_id>
edge discover list-artifacts --run_id <run_id>
```

Use `plan` when you want the normalized experiment plan and feasibility checks without running the full pipeline.

Use `run` when you want the orchestrated discovery pipeline executed.

## What discover consumes

The canonical input is a proposal file under `spec/proposals/`.

A proposal is structured YAML, not free text. It is validated by the proposal schema before the pipeline is allowed to run.

Typical proposal fields include:
- `program_id`
- `objective_name`
- `promotion_profile`
- `symbols`
- `timeframe`
- `start`, `end`
- `search_spec`
- `hypothesis.anchor`
- `hypothesis.filters`
- `hypothesis.template`
- `hypothesis.direction`
- `hypothesis.horizon_bars`

## Code path

The stage entry is the discover CLI, but the real path is:

```text
project/cli.py
  → project/discover/__init__.py
  → project/research/agent_io/issue_proposal.py
      → proposal_schema.py
      → project/operator/bounded.py
      → proposal_to_experiment.py
          → experiment planning / feasibility checks
      → execute_proposal.py
          → project/pipelines/run_all.py
```

Read these files in that order if you want the clearest proposal-to-pipeline understanding.

## What each step does

### 1. Proposal load and schema validation

`proposal_schema.py` turns raw YAML into a typed proposal object and checks structural correctness.

This is where malformed or semantically incomplete proposals should fail.

### 2. Bounded-change validation

If the proposal includes bounded semantics, `project/operator/bounded.py` checks that the requested change is within allowed bounds relative to the baseline.

This exists to prevent uncontrolled strategy drift during operator-driven research.

### 3. Proposal-to-experiment translation

`proposal_to_experiment.py` converts the proposal into two important outputs:

- a normalized experiment configuration,
- a `run_all` override set used by the orchestrator.

This translation is where event, template, direction, horizon, filters, and discovery profile become concrete run settings.

### 4. Feasibility and plan build

Before execution, the system compiles a plan and checks feasibility.

The resulting validated plan typically surfaces:
- estimated hypothesis count,
- required detectors,
- required features,
- required states.

This step matters because it tells you whether the search you described is actually compatible with the domain registry.

### 5. Pipeline execution through `run_all`

`execute_proposal.py` shells into `project/pipelines/run_all.py`, which executes the stage family DAG required for the run.

A simplified conceptual path is:

```text
ingest → cleaned bars → features → market context → event analysis → event registry / phase2 discovery → candidate reports → manifests
```

The exact set of stage instances depends on config, but `run_all.py` is the canonical orchestrator.

## Important lifecycle rule: discover does not promote by default

The discover path explicitly disables candidate promotion during the normal proposal execution flow.

That means discovery is for finding and recording candidates. Promotion is a later lifecycle step with its own canonical input and rules.

Do not mentally collapse discover and promote into one stage.

## Writing a proposal

A representative proposal shape:

```yaml
program_id: my_event_long_24b
run_mode: research
objective_name: retail_profitability
promotion_profile: research
symbols:
  - BTCUSDT
timeframe: 5m
start: "2023-01-01"
end: "2024-12-31"
instrument_classes:
  - crypto
search_spec:
  path: spec/productive_search.yaml
knobs:
  - name: discovery_profile
    value: synthetic
hypothesis:
  anchor:
    type: event
    event_id: VOL_SPIKE
  filters:
    feature_predicates:
      - feature: rv_pct_17280
        operator: ">"
        threshold: 70
  sampling_policy:
    entry_lag_bars: 1
  template:
    id: mean_reversion
  direction: long
  horizon_bars: 24
```

## Template and event compatibility

A common source of confusion is an apparently valid proposal that yields an empty plan.

One major reason is event-family and template incompatibility. The feasibility layer can reduce the plan to zero hypotheses without an obvious runtime error.

That makes `validated_plan.json` one of the first artifacts to inspect.

If `estimated_hypothesis_count` is `0`, check:
- whether the event exists,
- whether the template exists,
- whether the template is compatible with that event family,
- whether the search spec and proposal overrides leave any legal combinations.

## Key discovery artifacts

| Artifact | Path | Why it matters |
|----------|------|----------------|
| Validated plan | `data/artifacts/experiments/<program_id>/<run_id>/validated_plan.json` | confirms what the system believed you asked it to run |
| Evaluation results | `data/artifacts/experiments/<program_id>/<run_id>/evaluation_results.parquet` | canonical per-run evidence table |
| Program memory | `data/artifacts/experiments/<program_id>/memory/` | proposal memory, ledgers, and campaign-level state |
| Phase2 diagnostics | `data/reports/phase2/<run_id>/phase2_diagnostics.json` | explains funnel counts and candidate outcomes |
| Candidate tables | `data/reports/phase2/<run_id>/...` and/or `data/reports/edge_candidates/<run_id>/...` | handoff surface for validation |
| Run manifest | run-scoped manifest files written by the pipeline | provenance for the orchestrated run |

## How to read discovery results

A good order is:

1. `validated_plan.json` — did the plan compile as expected?
2. `phase2_diagnostics.json` — where were candidates filtered?
3. `evaluation_results.parquet` — what was actually measured?
4. candidate tables in `data/reports/phase2/<run_id>/` or `data/reports/edge_candidates/<run_id>/` — what will validation see?

A practical note: `after_cost_expectancy_per_trade` is stored as a fraction, so convert to basis points if you want trading-friendly units.

## Reusing an existing `run_id`

Passing `--run_id <existing_run_id>` lets the pipeline reuse that run’s lake and skip some expensive upstream build steps.

That is useful when sweeping templates, horizons, or related proposal variants over the same prepared data.

The trade-off is that run-scoped report directories such as `data/reports/phase2/<run_id>/` can be overwritten by later runs sharing that same `run_id`.

When you need proposal-specific evidence, prefer the per-program artifact locations under:

`data/artifacts/experiments/<program_id>/<run_id>/`

## Common failure modes

### Empty or zero-hypothesis plan

Usually caused by proposal/schema errors, event/template incompatibility, or search-space settings that eliminate all legal combinations.

### Pipeline ran, but campaign reporting looks missing

Some ledger-style summaries can mark rows as missing or not executed even when the pipeline itself did run. Read the direct per-run artifacts first.

### Candidate reports exist but look sparse

Check the validated plan and diagnostics before changing thresholds. Sparse outputs often reflect narrow search scope rather than a broken pipeline.

## What discover hands to the next stage

Validation consumes normalized candidate tables produced by discovery. Discover is done when you have:
- a stable `run_id`,
- experiment evidence,
- discovery candidate tables,
- enough diagnostics to explain what the search did.

Next: [validate.md](validate.md)
