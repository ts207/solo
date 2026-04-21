---
name: edge-analyst
description: Diagnose completed Edge experiment runs using current run artifacts and operator reporting surfaces. Use when a run has finished or failed and the next step depends on run health, funnel collapse, near-misses, or bounded follow-up directions.
---

# Edge Analyst

Use this after a run has completed or failed with artifacts.

## Read first

1. `docs/lifecycle/discover.md`
2. `docs/lifecycle/validate.md`
3. `docs/reference/assurance.md`

## Required inputs

- `run_id`
- `program_id` when known
- proposal YAML path when available

## Files to inspect

- `data/artifacts/experiments/<program_id>/<run_id>/validated_plan.json`
- `data/artifacts/experiments/<program_id>/<run_id>/evaluation_results.parquet`
- `data/reports/phase2/<run_id>/phase2_diagnostics.json`
- `data/reports/phase2/<run_id>/phase2_candidates.parquet`
- `data/reports/edge_candidates/<run_id>/edge_candidates_normalized.parquet`
- `data/reports/validation/<run_id>/` when validation has run

## Non-negotiable rules

- Diagnose the exact funnel collapse stage when candidates are empty.
- Use exact repo column names such as `effect_raw`, `q_value`, and `selection_score` when present.
- Do not compile proposals here.
- Keep conclusions tied to the bounded loop unless validation and promotion artifacts prove stronger lifecycle progress.

## Minimum result

- Run health
- Funnel summary
- Primary rejection mechanism
- Top near-misses
- Mechanistic meaning
- Whether the run supports keep, modify, or kill
- 1-3 bounded next experiments
