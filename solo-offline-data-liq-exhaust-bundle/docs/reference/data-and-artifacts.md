# Data and Artifacts Reference

This reference maps the main data roots, run outputs, and lifecycle artifacts.

## Data Root Resolution

Canonical resolution in `project/core/config.py`:

1. `EDGE_DATA_ROOT`
2. `BACKTEST_DATA_ROOT`
3. `<repo>/data`

Check the environment before debugging missing artifacts:

```bash
env | sort | rg '^(EDGE|BACKTEST)_'
```

When no environment override is set, local artifacts resolve under:

```text
data/
```

## Lake Layout

Cleaned bars:

```text
data/lake/cleaned/perp/<SYMBOL>/bars_5m/year=<YYYY>/month=<MM>/
```

Features:

```text
data/lake/features/perp/<SYMBOL>/5m/features_feature_schema_v2/year=<YYYY>/month=<MM>/
```

Market context:

```text
data/lake/features/perp/<SYMBOL>/5m/market_context/year=<YYYY>/month=<MM>/
```

Raw inputs:

```text
data/lake/raw/<venue>/perp/<SYMBOL>/
```

The pipeline may reuse global lake data when run-scoped lake data is absent.

## Run Manifests

Pipeline runs write manifests and conformance reports through the pipeline provenance layer. Manifest fields include:

- run ID
- run mode
- planned stages
- stage timings
- failed stage
- effective behavior
- artifact contract conformance status
- runtime invariants status
- research comparison thresholds

When a run fails, inspect the manifest before rerunning.

## Discovery Artifacts

Shared phase-2 outputs:

```text
data/reports/phase2/<run_id>/phase2_candidates.parquet
data/reports/phase2/<run_id>/phase2_diagnostics.json
data/reports/phase2/<run_id>/hypothesis_registry.parquet
data/reports/phase2/<run_id>/search_burden_summary.json
data/reports/phase2/<run_id>/hypotheses/<SYMBOL>/evaluated_hypotheses.parquet
data/reports/phase2/<run_id>/hypotheses/<SYMBOL>/gate_failures.parquet
```

Candidate universe:

```text
data/reports/edge_candidates/<run_id>/edge_candidates_normalized.parquet
```

Experiment-scoped outputs:

```text
data/artifacts/experiments/<program_id>/<run_id>/request.yaml
data/artifacts/experiments/<program_id>/<run_id>/validated_plan.json
data/artifacts/experiments/<program_id>/<run_id>/execution_requirements.json
data/artifacts/experiments/<program_id>/<run_id>/expanded_hypotheses.parquet
data/artifacts/experiments/<program_id>/<run_id>/evaluation_results.parquet
data/artifacts/experiments/<program_id>/<run_id>/summary.json
```

## Experiment Memory

Program memory root:

```text
data/artifacts/experiments/<program_id>/memory/
```

Tables:

```text
tested_regions.parquet
region_statistics.parquet
event_statistics.parquet
template_statistics.parquet
context_statistics.parquet
failures.parquet
proposals.parquet
reflections.parquet
evidence_ledger.parquet
```

JSON state:

```text
belief_state.json
next_actions.json
```

Proposal copies:

```text
proposals/<run_id>/<proposal_name>.yaml
proposals/<run_id>/experiment.yaml
proposals/<run_id>/run_all_overrides.json
```

## Validation Artifacts

```text
data/reports/validation/<run_id>/validation_bundle.json
data/reports/validation/<run_id>/validated_candidates.parquet
data/reports/validation/<run_id>/rejection_reasons.parquet
data/reports/validation/<run_id>/validation_report.json
data/reports/validation/<run_id>/effect_stability_report.json
data/reports/validation/<run_id>/promotion_ready_candidates.parquet
data/reports/validation/<run_id>/artifact_manifest.json
```

Promotion requires validation bundle and promotion-ready candidates.

## Promotion Artifacts

```text
data/reports/promotions/<run_id>/promotion_audit.parquet
data/reports/promotions/<run_id>/promoted_candidates.parquet
data/reports/promotions/<run_id>/promotion_summary.csv
data/reports/promotions/<run_id>/promotion_diagnostics.json
data/reports/promotions/<run_id>/evidence_bundles.jsonl
data/reports/promotions/<run_id>/evidence_bundle_summary.parquet
data/reports/promotions/<run_id>/promotion_decisions.parquet
data/reports/promotions/<run_id>/promoted_thesis_contracts.json
data/reports/promotions/<run_id>/promoted_thesis_contracts.md
```

The promotion diagnostics file is the fastest way to understand why no thesis was exported.

## Live Thesis Artifacts

```text
data/live/theses/<run_id>/promoted_theses.json
data/live/theses/index.json
```

Runtime loads thesis artifacts through `ThesisStore`, which checks schema, trust, and deployment gate state.

## Runtime Artifacts

Generated configs:

```text
project/configs/live_paper_<run_id>.yaml
```

Runtime snapshots and metrics are config-dependent. Generated bind-config defaults include paths such as:

```text
artifacts/live_state_<run_id>.json
artifacts/live_runtime_metrics_<run_id>.json
artifacts/live_runtime_alerts_<run_id>.jsonl
artifacts/live_memory/<run_id>/
```

## Artifact Collision Risk

Shared `data/reports/phase2/<run_id>/` artifacts are keyed only by run ID. If multiple proposals reuse the same run ID, the shared phase-2 directory can be overwritten.

For proposal comparisons, prefer:

```text
data/artifacts/experiments/<program_id>/<run_id>/
```

and campaign memory tables.

## Cleanup

Repo cleanup:

```bash
make clean
make clean-runtime
make clean-hygiene
```

Data cleanup:

```bash
make clean-run-data
make clean-all-data
```

Data cleanup is destructive for local artifacts. Use it only when explicitly resetting local state.
