# Assurance, smoke, and benchmarks

## Test surface

The repo currently contains a large and distributed test suite under `project/tests/`.

High-signal test families include:

- `architecture/` and `test_architectural_integrity.py` — package DAG and repo structure
- `contracts/` — manifest, config, and artifact contracts
- `events/`, `features/`, `specs/`, `spec_validation/` — registry/spec correctness
- `research/`, `pipelines/`, `operator/` — lifecycle behavior
- `engine/`, `portfolio/`, `live/`, `runtime/` — execution and runtime correctness
- `regressions/`, `smoke/`, `replays/`, `reliability/` — safety against operational drift
- `scripts/` — command-layer regressions for important tooling

See `docs/reference/test_inventory.md` for a subtree count inventory.

## Minimum green gate

The Makefile defines `make minimum-green-gate` as the stabilization baseline.

It currently runs:

- Python bytecode compilation over `project/` and tests
- architecture tests
- spec QA linting
- detector coverage audit with `--check`
- ontology consistency audit with `--check`
- event contract and ontology artifact checks
- system map and architecture metrics checks
- selected regression tests
- golden regression and golden workflow scripts

This is the best single command to verify repo integrity beyond a small targeted test run.

## Smoke entry point

`edge-smoke` maps to `project.reliability.cli_smoke:main`.

The smoke CLI supports multiple modes such as:

- `engine`
- `research`
- `promotion`
- `full`
- `validate-artifacts`

It writes smoke summaries under a chosen root and is useful for operational sanity checks when you do not want to run the full suite.

## Governance and regeneration

`make governance` runs the governance and registry-sync block:

- `project/scripts/pipeline_governance.py --audit --sync`
- event contract artifact build
- event ontology audit
- event ontology artifact build

This is the normal path after spec-driven structural changes.

## Benchmark and certification surfaces

The repo has a non-trivial benchmarking/certification toolchain exposed through the Makefile and scripts, including:

- `benchmark-maintenance-smoke`
- `benchmark-maintenance`
- `discover-blueprints`
- `discover-edges`
- `discover-edges-from-raw`
- `golden-workflow`
- `golden-certification`
- `golden-synthetic-discovery`
- `synthetic-demo`

Relevant scripts include:

- `project/scripts/run_benchmark_maintenance_cycle.py`
- `project/scripts/run_benchmark_matrix.py`
- `project/scripts/run_certification_workflow.py`
- `project/scripts/run_golden_workflow.py`
- `project/scripts/run_golden_regression.py`
- `project/scripts/run_golden_synthetic_discovery.py`

## Practical guidance

- Use focused pytest targets while developing.
- Use `make minimum-green-gate` before calling a structural change complete.
- Use governance/regeneration commands after spec or registry changes.
- Keep benchmark and golden-workflow changes isolated from unrelated refactors when possible.
