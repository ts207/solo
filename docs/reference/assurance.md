# Assurance, testing, and benchmarks

## Test surface

All tests live under `project/tests/`. Run with:

```bash
PYTHONPATH=. python3 -m pytest -q                     # full suite
PYTHONPATH=. python3 -m pytest -q -m "not slow"       # skip slow tests
PYTHONPATH=. python3 -m pytest project/tests/path/to/test_file.py -q   # single file
```

### High-signal test families

| Directory / file | What it tests |
|---|---|
| `test_architectural_integrity.py` | Package import DAG — fails if a package imports from a forbidden layer |
| `architecture/` | Repo structure, metrics, hygiene |
| `contracts/` | Manifest, config, and artifact schema contracts |
| `events/`, `features/`, `specs/`, `spec_validation/` | Registry and spec correctness |
| `research/`, `pipelines/`, `operator/` | Lifecycle behavior |
| `engine/`, `portfolio/`, `live/`, `runtime/` | Execution and runtime correctness |
| `regressions/`, `smoke/`, `replays/`, `reliability/` | Safety against operational drift |
| `scripts/` | Command-layer regressions for important tooling |

### Test markers

| Marker | Meaning |
|--------|---------|
| `slow` | Long-running tests; skip with `-m "not slow"` |
| `contract` | End-to-end artifact contract tests |
| `audit` | Pipeline hygiene checks |
| `integration` | Requires live data / full pipeline; skip with `-m "not integration"` |

---

## Minimum green gate

The single most important check before calling a structural change complete:

```bash
make minimum-green-gate
```

This runs:
- Python bytecode compilation over `project/` and tests
- Architecture tests (`test_architectural_integrity.py`)
- Spec QA lint
- Detector coverage audit
- Ontology consistency audit
- Event contract and ontology artifact checks
- System map and architecture metrics checks
- Selected regression tests
- Golden regression and golden workflow scripts

---

## Governance and registry sync

After any spec or registry structural change:

```bash
make governance
```

This runs:
- `project/scripts/pipeline_governance.py --audit --sync`
- Event contract artifact build
- Event ontology audit and artifact build

This is the normal regeneration path. The governance script syncs `project/configs/registries/events.yaml` from the spec tree and validates consistency.

---

## Smoke CLI

`edge-smoke` (`project.reliability.cli_smoke:main`) supports modes:

```bash
edge-smoke engine
edge-smoke research
edge-smoke promotion
edge-smoke full
edge-smoke validate-artifacts
```

Useful for operational sanity checks without running the full test suite.

---

## Benchmarks and certification

| Make target | What it runs |
|---|---|
| `make benchmark-maintenance-smoke` | Dry-run benchmark maintenance |
| `make benchmark-maintenance` | Full benchmark maintenance cycle |
| `make benchmark-core` | Core benchmark matrix |
| `make golden-workflow` | End-to-end smoke workflow |
| `make golden-certification` | Golden workflow + certification manifest |
| `make golden-synthetic-discovery` | Synthetic discovery golden path |

Key scripts:
- `project/scripts/run_benchmark_maintenance_cycle.py`
- `project/scripts/run_certification_workflow.py`
- `project/scripts/run_golden_workflow.py`
- `project/scripts/run_golden_regression.py`

---

## After structural changes: checklist

| Change type | Commands to run |
|---|---|
| New/modified Python package | `make minimum-green-gate` |
| New/modified event spec | `build_domain_graph.py` → `make governance` → `make minimum-green-gate` |
| New/modified template | 3-file sync → `build_domain_graph.py` → `make minimum-green-gate` |
| Modified promotion/validation logic | `make minimum-green-gate` + focused test on `research/`, `pipelines/` |
| Modified live runner or OMS | `make minimum-green-gate` + focused test on `live/`, `runtime/` |
| Modified import structure | `make minimum-green-gate` (architecture test will catch violations) |
