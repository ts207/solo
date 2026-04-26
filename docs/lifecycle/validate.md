# Validate

Validate converts discovery candidate outputs into canonical validation artifacts. Promotion requires this stage.

## Command Surface

```bash
edge validate run --run_id <run_id>
```

Spec validation is separate:

```bash
edge validate specs
```

## Implementation Surface

The CLI calls:

```text
project.validate.run()
  -> project.research.services.evaluation_service.ValidationService
```

The validation service loads candidate tables in this order of preference:

1. `data/reports/edge_candidates/<run_id>/edge_candidates_normalized.parquet`
2. `data/reports/phase2/<run_id>/phase2_candidates.parquet`

It then writes canonical validation outputs under:

```text
data/reports/validation/<run_id>/
```

## Outputs

Expected validation outputs include:

```text
validation_bundle.json
validated_candidates.parquet
rejection_reasons.parquet
validation_report.json
effect_stability_report.json
promotion_ready_candidates.parquet
artifact_manifest.json
```

The promotion stage checks specifically for:

```text
data/reports/validation/<run_id>/promotion_ready_candidates.parquet
data/reports/validation/<run_id>/validation_bundle.json
```

If either is missing, promotion is rejected.

## Decision Model

Validation maps candidate rows into:

- validated candidates
- rejected candidates
- inconclusive candidates

Common gate-derived rejection reasons include:

- failed out-of-sample validation
- failed cost survival
- failed regime support
- failed multiplicity threshold
- insufficient sample support
- insufficient data

Validation is not the same as promotion. It identifies candidates that can enter promotion; promotion applies a stricter governed policy and lineage checks.

## Forward Confirmation Boundary

Origin-window validation is research validation only. It must not be treated as
release readiness when an unseen forward window is available.

Before deploy promotion/export, a candidate needs a successful forward-window confirmation artifact at `data/reports/validation/<run_id>/forward_confirmation.json`. Produce it with `edge validate forward-confirm --run_id <run_id> --window <ISO8601-start>/<ISO8601-end>`. Research-profile validation may proceed without this artifact; deploy-profile promotion degrades candidates with `forward_confirmation_missing`, `forward_confirmation_drift`, or sign-flip reasons instead of promoting them.

`confirmatory` is a lifecycle role, not currently a `run_all --mode` value. A
forward confirmation run may still execute under `run_mode=research` when
promotion is disabled and the proposal records its forward-falsification role in
artifacts.

The funding-continuation branch is the canonical failure example:
2023-2024 validation passed, but full-2025 confirmation failed on `min_t_stat`
with zero edge candidates. See
[`funding-continuation-2025-postmortem.md`](funding-continuation-2025-postmortem.md).

## Inputs Worth Inspecting

Before validation, inspect:

```text
data/reports/phase2/<run_id>/phase2_candidates.parquet
data/reports/phase2/<run_id>/phase2_diagnostics.json
data/reports/edge_candidates/<run_id>/edge_candidates_normalized.parquet
```

If `edge_candidates_normalized.parquet` exists and is nonempty, validation will prefer it over raw phase-2 candidates.

## Stop Conditions

Stop before promotion when:

- `validation_bundle.json` is missing.
- `promotion_ready_candidates.parquet` is missing.
- All candidates are inconclusive due to missing critical data.
- The source candidate table is empty and that was not expected.
- Validation output candidate IDs do not correspond to source candidate IDs.

## Minimal Verification

```bash
edge validate run --run_id <run_id>
ls data/reports/validation/<run_id>
```

For programmatic checks, read `validation_bundle.json` and confirm `summary_stats.validated` is greater than zero before expecting promotion to produce promoted theses.
