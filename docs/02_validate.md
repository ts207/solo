# Validate stage

## Scope

The validate stage converts discovery or promotion candidate tables into formal validation artifacts. It is the canonical truth-testing and robustness layer.

CLI surface from `project/cli.py`:

- `edge validate run --run_id <run_id>`
- `edge validate report --run_id <run_id>`
- `edge validate diagnose --run_id <run_id> [--program_id ...]`
- `edge validate list-artifacts --run_id <run_id>`

## Core code path

`project/validate/__init__.py` provides the stage façade.

### Candidate table loading

`ValidationService.load_candidate_tables()` looks for candidate sources in this order:

1. `data/reports/promotions/<run_id>/promotion_statistical_audit.parquet`
2. `data/reports/edge_candidates/<run_id>/edge_candidates_normalized.parquet`
3. `data/reports/phase2/<run_id>/phase2_candidates.parquet`

The stage uses the first non-empty table.

### Bundle creation and writing

`project/research/services/evaluation_service.py` builds a `ValidationBundle`.

`project/research/validation/result_writer.py` writes the canonical outputs under `data/reports/validation/<run_id>/`.

Required JSON artifacts:

- `validation_bundle.json`
- `validation_report.json`
- `effect_stability_report.json`

Required tabular artifacts:

- `validated_candidates.parquet`
- `rejection_reasons.parquet`
- `promotion_ready_candidates.parquet`

The writer validates schema shape before finalizing these payloads.

## Reporting and diagnostics

### `edge validate report`

`project/operator/stability.py:write_regime_split_report` is the reporting surface for regime/stability views.

### `edge validate diagnose`

`project/operator/stability.py:write_negative_result_diagnostics` writes negative-result diagnostics, optionally scoped to a specific `program_id`.

## What validate is for

Validate answers a narrower question than discover:

- discover asks whether the search space can produce candidates worth examining
- validate asks whether the selected candidates remain credible under formal gates, regime slicing, and robustness reporting

That distinction matters when you decide where a new rule belongs. Search-space enumeration logic belongs upstream; formal acceptance criteria belong here or in promotion.

## Canonical output contract

The validate stage is the handoff into promotion. The promotion service expects validation artifacts that are already normalized and machine-readable.

In practice, this means:

- validation bundle JSON is the narrative summary + structured contract
- validated/rejected tables are the machine-level handoff
- `promotion_ready_candidates.parquet` is the canonical promotion intake

## Canonical commands

```bash
edge validate run --run_id <run_id>
edge validate report --run_id <run_id>
edge validate diagnose --run_id <run_id>
edge validate list-artifacts --run_id <run_id>
```

## Common failure modes

- No candidate tables exist for the run.
- Candidate table exists but is empty after filtering.
- Validation artifacts are malformed or missing required fields.
- Historical artifacts fail trust or compatibility checks when reused.

When these happen, fix the producing stage or artifact contract; do not patch the validation writer to accept malformed payloads silently.
