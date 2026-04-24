# Discover

Discover translates one structured proposal into an experiment plan, executes the research DAG when requested, and writes evidence artifacts. Discovery output is not production readiness.

## Command Surface

Plan:

```bash
edge discover plan --proposal spec/proposals/canonical_event_hypothesis.yaml
```

Run:

```bash
edge discover run --proposal spec/proposals/canonical_event_hypothesis.yaml
```

Optional run ID reuse:

```bash
edge discover run --proposal spec/proposals/other.yaml --run_id <existing_run_id>
```

The Make wrappers are:

```bash
make discover-plan PROPOSAL=spec/proposals/canonical_event_hypothesis.yaml RUN_ID=<run_id>
make discover PROPOSAL=spec/proposals/canonical_event_hypothesis.yaml RUN_ID=<run_id>
```

## Important Behavior

`discover plan` is not a pure read-only dry inspection. The canonical front door records proposal memory and writes proposal artifacts before invoking `run_all` with `--plan_only`.

The proposal path is copied into:

```text
data/artifacts/experiments/<program_id>/memory/proposals/<run_id>/
```

The generated experiment config and run-all overrides are written beside that copy.

## Structured Proposal Format

The canonical operator format is structured hypothesis YAML:

```yaml
program_id: volshock_btc_long_12b
description: Canonical VOL_SHOCK continuation slice
run_mode: research
objective_name: retail_profitability
promotion_profile: research
symbols:
  - BTCUSDT
timeframe: 5m
start: "2024-01-01"
end: "2024-01-31"
instrument_classes:
  - crypto
hypothesis:
  anchor:
    type: event
    event_id: VOL_SHOCK
  filters: {}
  sampling_policy:
    entry_lag_bars: 1
  template:
    id: continuation
  direction: long
  horizon_bars: 12
```

Canonical proposal loading rejects legacy proposal formats. Legacy and single-hypothesis normalization helpers still exist, but `load_operator_proposal()` accepts only the structured hypothesis path.

## Executable Anchors

Current structured execution supports these primary anchor types:

- `event`
- `transition`
- `sequence`

State anchors are deprecated as primary anchors and are rejected on the structured execution path. Feature-crossing anchors can be normalized but are not currently accepted by strict structured execution.

Use filters for conditioning rather than treating persistent state as the main anchor.

## Proposal Translation

The discover front door is:

```text
project.research.agent_io.issue_proposal.issue_proposal
```

The translation path is:

```text
proposal YAML
  -> load_operator_proposal()
  -> compile_structured_proposal_to_agent_proposal()
  -> proposal_to_experiment_config()
  -> build_experiment_plan()
  -> project.pipelines.run_all
```

The run-all command receives:

- `--experiment_config`
- `--registry_root`
- `--symbols`
- `--start`
- `--end`
- proposal-derived overrides such as `--search_spec`, `--discovery_profile`, `--phase2_gate_profile`, and `--program_id`.

When executing a discover run, internal candidate promotion is disabled. Promotion is an explicit downstream stage.

## Planning Checks

Planning validates:

- Proposal structure.
- Entry lag leakage guard.
- Template existence and trigger-type support.
- Event/state/transition/sequence identifiers.
- Instrument compatibility.
- Context labels.
- Search limits.
- Campaign state and cumulative budget.
- Event/template family compatibility.

Incompatible event/template hypotheses are dropped before evaluation. Always inspect:

```text
data/artifacts/experiments/<program_id>/<run_id>/validated_plan.json
```

The key field is:

```json
{
  "estimated_hypothesis_count": 1
}
```

If `estimated_hypothesis_count` is `0`, do not conclude there is no edge. First check whether the proposal was filtered to zero by feasibility or compatibility.

## Pipeline Stages

Discovery runs through a DAG assembled by `project.pipelines.planner`:

- ingest
- clean
- features
- market context
- event analysis
- event registry build
- episode canonicalization
- phase-2 search engine
- edge candidate export
- campaign memory update
- finalize experiment

For experiment-config runs, strategy packaging stages are skipped. `finalize_experiment` writes experiment-level outputs and updates memory.

## Primary Outputs

Shared run outputs:

```text
data/reports/phase2/<run_id>/phase2_candidates.parquet
data/reports/phase2/<run_id>/phase2_diagnostics.json
data/reports/phase2/<run_id>/hypotheses/<SYMBOL>/evaluated_hypotheses.parquet
data/reports/edge_candidates/<run_id>/edge_candidates_normalized.parquet
```

Experiment memory:

```text
data/artifacts/experiments/<program_id>/<run_id>/validated_plan.json
data/artifacts/experiments/<program_id>/<run_id>/expanded_hypotheses.parquet
data/artifacts/experiments/<program_id>/<run_id>/evaluation_results.parquet
data/artifacts/experiments/<program_id>/<run_id>/summary.json
data/artifacts/experiments/<program_id>/tested_ledger.parquet
data/artifacts/experiments/<program_id>/memory/proposals.parquet
```

## Run ID Reuse

Passing `--run_id <existing_run_id>` reuses the same run ID and can overwrite shared `data/reports/phase2/<run_id>/` artifacts. When multiple proposals share one run ID, prefer per-experiment artifacts under:

```text
data/artifacts/experiments/<program_id>/<run_id>/
```

Do not rely only on the shared phase-2 directory when comparing proposal variants.

## Stop Conditions

Stop and diagnose before running validation when:

- The CLI return code is nonzero.
- `validated_plan.json` has zero hypotheses.
- `phase2_diagnostics.json` has zero feature rows or zero metrics rows.
- The run manifest reports a failed stage.
- The domain graph freshness check fails after spec changes.
