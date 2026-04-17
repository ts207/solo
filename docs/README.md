# Edge documentation index

This docset was rewritten from the current codebase, tests, specs, configs, and entry points. It is organized around the repo’s real boundaries rather than the older narrative structure.

## Reading order

1. `00_overview.md` — system model, lifecycle, invariants, and current mental model.
2. `02_REPOSITORY_MAP.md` — where code lives, which package owns what, and which directories are generated versus authored.
3. `operator_command_inventory.md` — current CLI, Make, and script entry points.
4. Stage guides:
   - `01_discover.md`
   - `02_validate.md`
   - `03_promote.md`
   - `04_deploy.md`
5. Operator references:
   - `operator_runbook.md`
6. Repo constraints and extension guidance:
   - `90_architecture.md`
   - `92_assurance_and_benchmarks.md`

## What this docset is trying to answer

- How a structured proposal becomes a pipeline run.
- How proposal fields outside the hypothesis block affect discover, experiments, and campaigns.
- Which files are authoritative versus generated.
- Which artifacts each lifecycle stage reads and writes.
- How promoted theses are exported and consumed by the live stack.
- Which commands and tests should be run after a structural change.
- Where to place new logic without violating the package DAG.

## Current repo shape

- `project/` — 1444 Python files organized as packages with an architecture test enforcing allowed import directions.
- `project/tests/` — 565 test files spanning pipelines, research, events, live runtime, contracts, scripts, regressions, and smoke coverage.
- `spec/` — 443 YAML source specs plus supporting CSV assets.
- `docs/generated/` — generated audits, maps, and references; outputs, not authored guidance.

## Generated reference

- `generated/` — code-derived audits, maps, and inventories
