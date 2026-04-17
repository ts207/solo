# Validate stage

The validate stage turns discovery candidate rows into a formal validation bundle.

It answers this question:

**Which candidate rows remain credible after formal gates, consistency checks, and canonical validation packaging?**

## CLI surface

```bash
edge validate run            --run_id <run_id>
edge validate report         --run_id <run_id>
edge validate diagnose       --run_id <run_id> [--program_id <program_id>]
edge validate list-artifacts --run_id <run_id>
```

Use `run` for the canonical stage output.

Use `report` and `diagnose` when you need additional interpretation of what happened during validation or why a run produced weak survivors.

## What validate consumes

Validation looks for discovery-stage candidate tables in a priority order. The service prefers the highest-normalized candidate surface available for the run.

Priority order:
1. `data/reports/promotions/<run_id>/promotion_statistical_audit.parquet`
2. `data/reports/edge_candidates/<run_id>/edge_candidates_normalized.parquet`
3. `data/reports/phase2/<run_id>/phase2_candidates.parquet`

This matters because validate is not tightly coupled to only one upstream table shape. It can consume the best available normalized candidate view for the run.

## Code path

```text
project/cli.py
  → project/validate/__init__.py
  → project/research/services/evaluation_service.py
      → project/research/validation/result_writer.py
```

`evaluation_service.py` is the stage’s real center of gravity.

## What the stage does

At a high level, validation:

1. locates the candidate table,
2. maps rows into a canonical candidate representation,
3. applies status logic,
4. writes the validation bundle and machine-readable tables,
5. prepares the promotion intake table.

That last output is crucial. Promotion should not have to reinterpret arbitrary discovery tables. It should receive a canonical, already-normalized survivor table.

## Validation statuses

The service classifies rows into buckets such as:
- validated,
- rejected,
- inconclusive.

The exact logic depends on the row surface, but in practice validation uses gate fields and candidate metrics such as:
- out-of-sample gate status,
- post-cost positivity,
- stressed post-cost positivity,
- regime stability,
- multiplicity controls,
- support counts such as `n_events`.

Rows can also be treated as inconclusive when the required metrics are absent or the supporting evidence is too thin to certify a decision cleanly.

## Canonical outputs

All canonical validation outputs are written under:

`data/reports/validation/<run_id>/`

| File | Role |
|------|------|
| `validation_bundle.json` | canonical summary contract for the run |
| `validation_report.json` | detailed run/candidate report |
| `effect_stability_report.json` | effect concentration and stability details |
| `validated_candidates.parquet` | machine-readable validated set |
| `rejection_reasons.parquet` | machine-readable rejection explanations |
| `promotion_ready_candidates.parquet` | canonical handoff table for promotion |

The single most important output for the next stage is:

`promotion_ready_candidates.parquet`

## Why validation is a separate stage

Discover is allowed to be broad. Validation is where the repo narrows from "interesting" to "credible enough to package."

This separation gives the system a clean boundary between search and certification. It also means you can improve validation rules without rewriting the discover stage’s role.

## Diagnostic commands

### `edge validate report`

Builds regime and stability reporting surfaces useful for manual review of where the effect lives and whether it is concentrated in a suspicious way.

### `edge validate diagnose`

Writes negative-result diagnostics that help answer questions like:
- why did a run produce no validated survivors?
- did candidates disappear at candidate selection, validation gates, or stability checks?
- is the problem likely upstream in discovery rather than in validation?

## How to inspect a validation run

A good reading order is:

1. `validation_bundle.json` — what did validation decide overall?
2. `validated_candidates.parquet` — what survived?
3. `rejection_reasons.parquet` — why did the others fail?
4. `effect_stability_report.json` — are survivors stable or narrowly concentrated?
5. `promotion_ready_candidates.parquet` — what exactly will promotion ingest?

## Common failure modes

### No candidate table found

Discovery did not produce the expected upstream candidate surfaces, or the wrong `run_id` is being inspected.

### Candidate table exists but is empty

Usually an upstream issue. Check discover diagnostics, event/template compatibility, and search-space settings.

### Validation outputs look malformed

Fix the producer or the normalization logic. Do not weaken the canonical writer just to accept bad upstream payloads.

### No promotion-ready survivors

This may be a legitimate validation outcome. Promotion should then produce an empty or no-op packaging result rather than pretending evidence exists.

## What validate hands to the next stage

Promotion should consume:
- canonical validation artifacts,
- especially `promotion_ready_candidates.parquet`,
- plus enough lineage to package promoted theses correctly.

Next: [promote.md](promote.md)
