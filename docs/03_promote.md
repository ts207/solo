# Promote stage

## Scope

The promote stage packages validated candidates into governed theses and related blueprint artifacts. It is the bridge between research outputs and live-consumable runtime payloads.

CLI surface from `project/cli.py`:

- `edge promote run --run_id <run_id> --symbols BTCUSDT`
- `edge promote export --run_id <run_id>`
- `edge promote list-artifacts --run_id <run_id>`

## Core code path

`project/promote/__init__.py` is the façade.

### Promotion run

`project/research/services/promotion_service.py` provides:

- promotion configuration building
- gate application
- diagnostics recording
- promoted-candidate packaging
- blueprint/promotion report writing

The service writes under:

- `data/reports/promotions/<run_id>/`
- `data/reports/strategy_blueprints/<run_id>/`

### Live export

`project/research/live_export.py` exports promoted theses to the live thesis store.

Key responsibilities:

- validate `promoted_theses.json` payload structure
- validate and update the thesis `index.json`
- derive thesis evidence, governance, requirements, and lineage
- resolve authored thesis-definition IDs where possible
- reject malformed or incomplete lineage rather than exporting a partially valid live package

## Canonical outputs

### Promotion outputs

Typical promotion artifacts include:

- `promotion_summary.json`
- `promotion_report.json`
- `promoted_blueprints.jsonl`
- promotion audit parquet/json artifacts owned by the promotion service

### Live thesis outputs

The exported live-facing contract lives under:

- `data/live/theses/<run_id>/promoted_theses.json`
- `data/live/theses/index.json`

The live thesis payload currently enforces `schema_version == promoted_theses_v1`.

## Promotion class and deployment state

`project/research/services/promotion_service.py` defines promotion-class handling and default deployment states.

Current promotion classes:

- `paper_promoted`
- `production_promoted`

Default deployment state mapping:

- `paper_promoted -> paper_only`
- `production_promoted -> live_enabled`

## Why export is separate from promotion

Promotion and export are related but not identical.

- promotion decides what qualifies as a promoted output
- export decides how that output is packaged into the live thesis contract and index

Keeping them separate makes it easier to rerun export rules or thesis packaging without pretending the promotion decision itself changed.

## Canonical commands

```bash
edge promote run --run_id <run_id> --symbols BTCUSDT
edge promote export --run_id <run_id>
edge promote list-artifacts --run_id <run_id>
```

## Operational cautions

- Do not bypass validation artifacts and promote directly from ad hoc tables.
- Do not write live thesis JSON by hand; `live_export.py` owns schema validation and index maintenance.
- If the overlap/admission logic changes, update `project/portfolio/` and the live/runtime docs together.
