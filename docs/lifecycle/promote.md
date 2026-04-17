# Promote stage

The promote stage turns validated candidates into governed thesis artifacts that runtime can consume.

It answers this question:

**Which validated candidates should become deployable thesis contracts, and how should those contracts be packaged for runtime?**

## CLI surface

```bash
edge promote run            --run_id <run_id> --symbols BTCUSDT
edge promote export         --run_id <run_id>
edge promote list-artifacts --run_id <run_id>
```

`run` performs the promotion decision flow.

`export` writes or refreshes the runtime-facing thesis package.

That distinction matters because promotion decisioning and runtime packaging are related but not identical responsibilities.

## What promote consumes

Promotion is downstream of validation.

Its canonical input is the validation surface under:

`data/reports/validation/<run_id>/`

The most important input file is:

`promotion_ready_candidates.parquet`

Promotion should not be treated as a direct consumer of ad hoc discovery outputs. The validation boundary is part of the repo’s evidence-governance model.

## Code path

```text
project/cli.py
  → project/promote/__init__.py
  → project/research/services/promotion_service.py
      → project/research/live_export.py
```

`promotion_service.py` decides what qualifies and prepares the promoted objects.

`live_export.py` turns those promoted objects into the runtime thesis package and thesis index.

## What the stage does

Promotion has two distinct responsibilities:

### 1. Decide what qualifies

This includes:
- reading validated survivors,
- applying promotion-profile policy,
- assigning promotion class,
- assigning deployment state,
- producing promotion summaries and audits.

### 2. Package for runtime

This includes:
- building thesis payloads,
- validating lineage and schema,
- writing `promoted_theses.json`,
- updating the thesis batch index.

Keep those responsibilities mentally separate. A candidate may be valid enough for promotion evaluation but still fail export if the runtime contract or lineage is incomplete.

## Promotion profiles and gates

The repo distinguishes research-oriented promotion from stricter production-style gating.

For the `research` promotion profile, several defaults are intentionally relaxed relative to stricter promotion rules. This makes the research track usable while preserving the canonical packaging path.

The key idea is:
- research discovery should not be blocked by every production-grade gate,
- but runtime packaging should still require coherent contracts and lineage.

## Promotion classes and deployment states

Promotion assigns a class and a default deployment state.

| Promotion class | Default deployment state |
|----------------|--------------------------|
| `paper_promoted` | `paper_only` |
| `production_promoted` | `live_enabled` |

These states matter later because deploy checks deployment state before it allows paper or live startup.

## Promotion outputs

### Promotion reports and audits

Common outputs live under:
- `data/reports/promotions/<run_id>/`
- `data/reports/strategy_blueprints/<run_id>/`

Typical artifacts include:
- `promotion_summary.json`
- `promotion_report.json`
- `promoted_blueprints.jsonl`
- audit parquet files

### Runtime thesis package

The runtime-facing package is written under:

`data/live/theses/<run_id>/`

with the global thesis index under:

`data/live/theses/index.json`

Main runtime artifact:
- `data/live/theses/<run_id>/promoted_theses.json`

## Export contract

`live_export.py` validates the thesis package instead of writing arbitrary JSON.

The package is treated as a governed runtime contract. That means schema shape, lineage, and thesis details must be coherent enough for the live thesis store to load safely.

The broad design rule is:

**promotion writes deployable contracts, not informal summaries.**

## Empty promotion outcomes

If validation produced no promotable survivors, promotion should still behave deterministically.

A clean empty outcome is better than fabricating a thesis package from weak or absent evidence. Treat zero-thesis output as a valid lifecycle state.

## How to inspect a promotion run

A good reading order is:

1. `promotion_summary.json` — how many candidates were considered and promoted?
2. `promotion_report.json` — what decisions were made and why?
3. `promoted_blueprints.jsonl` — what strategy/thesis blueprint surface was emitted?
4. `data/live/theses/<run_id>/promoted_theses.json` — what will runtime actually load?
5. `data/live/theses/index.json` — how is the batch indexed globally?

## Config binding for paper runtime

After promotion, a common operator step is:

```bash
edge deploy bind-config --run_id <run_id>
```

This creates a paper runtime config bound to the thesis batch so deployment can launch against the correct `thesis_run_id` without manually editing the template.

## Operational rules

- Do not bypass validation and promote directly from arbitrary candidate tables.
- Do not hand-author `promoted_theses.json` or `index.json`.
- If thesis schema or export semantics change, update export code, runtime readers, and tests together.
- If overlap, admission, or runtime eligibility policy changes, review the related logic in `project/portfolio/` and the live runtime tests.

## What promote hands to the next stage

Deploy consumes:
- a runtime config,
- a thesis batch under `data/live/theses/<run_id>/`,
- deployment states compatible with the intended runtime mode.

Next: [deploy.md](deploy.md)
