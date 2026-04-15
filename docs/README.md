# Edge documentation index

This docset was rewritten from the current codebase, tests, specs, configs, and entry points. It is organized around the repo’s real boundaries rather than the older narrative structure.

## Reading order

1. `00_overview.md` — system model, lifecycle, invariants, and current mental model.
2. `02_REPOSITORY_MAP.md` — where code lives, which package owns what, and which directories are generated versus authored.
3. `operator_command_inventory.md` — current CLI, Make, and script entry points.
4. `05_specs_and_domain.md` — how YAML specs become runtime/domain registries.
5. `06_data_and_artifacts.md` — artifact layout, manifests, reports, memory stores, and live thesis packages.
6. Stage guides:
   - `01_discover.md`
   - `02_validate.md`
   - `03_promote.md`
   - `04_deploy.md`
7. Operator references:
   - `08_runtime_config_reference.md`
   - `09_operator_runbook.md`
   - `10_regeneration_and_test_matrix.md`
   - `11_proposal_authoring_and_campaigns.md`
8. Repo constraints and extension guidance:
   - `90_architecture.md`
   - `92_assurance_and_benchmarks.md`
   - `07_extension_guide.md`

## What this docset is trying to answer

- How a structured proposal becomes a pipeline run.
- How proposal fields outside the hypothesis block affect discover, experiments, and campaigns.
- Which files are authoritative versus generated.
- Which artifacts each lifecycle stage reads and writes.
- How promoted theses are exported and consumed by the live stack.
- Which commands and tests should be run after a structural change.
- Where to place new logic without violating the package DAG.

## Current repo shape

- `project/` — 1442 Python files organized as packages with an architecture test enforcing allowed import directions.
- `project/tests/` — 564 test files spanning pipelines, research, events, live runtime, contracts, scripts, regressions, and smoke coverage.
- `spec/` — 397 YAML source specs plus supporting CSV assets.
- `docs/generated/` — generated audits, maps, and references; outputs, not authored guidance.
- `docs/reference/` — code-derived inventories produced during this rewrite.

## Reference appendices

- `reference/package_inventory.md`
- `reference/spec_inventory.md`
- `reference/test_inventory.md`
- `reference/script_inventory.md`
- `reference/command_audit.md`
- `generated/README.md`
