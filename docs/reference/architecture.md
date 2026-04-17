# Architecture and package boundaries

## The package dependency DAG

`project/tests/test_architectural_integrity.py` enforces allowed import directions. A package may only import from packages in its allowed set. Violations fail the architecture test.

| Package | May import from |
|---------|----------------|
| `project.core` | `project.spec_registry`, `project.artifacts`, `project.specs`, `project.io` |
| `project.io` | `project.core`, `project.artifacts` |
| `project.specs` | `project.core`, `project.io`, `project.spec_registry`, `project.schemas`, `project.artifacts` |
| `project.domain` | `project.core`, `project.specs`, `project.spec_registry`, `project.events` |
| `project.runtime` | `project.core`, `project.specs` |
| `project.events` | `project.core`, `project.io`, `project.specs`, `project.spec_registry`, `project.research`, `project.features`, `project.artifacts`, `project.contracts`, `project.domain`, `project.spec_validation` |
| `project.features` | `project.core`, `project.io`, `project.events`, `project.spec_registry`, `project.artifacts`, `project.contracts` |
| `project.engine` | `project.core`, `project.io`, `project.events`, `project.features`, `project.strategy`, `project.portfolio` |
| `project.portfolio` | `project.core`, `project.specs`, `project.strategy`, `project.live`, `project.research` |
| `project.research` | `project.core`, `project.io`, `project.specs`, `project.runtime`, `project.events`, `project.features`, `project.strategy`, `project.engine`, `project.eval`, `project.spec_registry`, `project.artifacts`, `project.schemas`, `project.spec_validation`, `project.contracts`, `project.domain`, `project.compilers`, `project.portfolio`, `project.live`, `project.operator`, `project.episodes` |
| `project.live` | `project.core`, `project.events`, `project.features`, `project.strategy`, `project.portfolio`, `project.episodes`, `project.io`, `project.engine`, `project.research`, `project.artifacts`, `project.domain` |
| `project.pipelines` | `project.research`, `project.engine`, `project.events`, `project.core`, `project.io`, `project.specs`, `project.contracts`, `project.domain`, `project.features`, `project.schemas`, `project.eval`, `project.runtime`, `project.spec_registry`, `project.experiments`, `project.operator` |
| `project.operator` | `project.core`, `project.research`, `project.specs`, `project.io` |

---

## Design principles behind the DAG

### `project.core` is the lowest substrate

Shared primitives, config, stats, validation helpers. It does not know about research policy, live trading, or event semantics.

### Spec/domain packages sit below research and runtime

`project.spec_registry`, `project.specs`, `project.spec_validation`, and `project.domain` define and compile the vocabulary consumed by higher layers. They do not orchestrate behavior.

### `project.research` is intentionally broad

Research can see many layers because it orchestrates search, evaluation, promotion support, and reporting. That breadth is by design. It should not become a dumping ground for generic utilities that belong in `core` or `specs`.

### Portfolio is the shared policy layer between engine and live

The previous direct `engine → live` dependency was removed by moving shared admission logic into `project.portfolio`. Use this pattern for any new cross-runtime policy.

### Pipelines orchestrate, not own

`project.pipelines` has broad visibility because it wires together many stages. Business semantics belong in research, specs, events, or live — not in the pipeline wiring.

---

## Safe extension patterns

### Adding a new event

1. Write `spec/events/<NEW_EVENT>.yaml` with `event_type`, `detector`, `signal_column`, `events_file`, `research_family`, `canonical_regime`, `identity`, `governance`, `runtime`
2. Add the detector class in `project/events/detectors/`
3. Register the detector in `project/configs/registries/detectors.yaml`
4. Add the event to `spec/search_space.yaml` (required for `--events` flag to work)
5. Add to `spec/events/event_registry_unified.yaml` and `project/configs/registries/events.yaml`
6. Rebuild domain graph: `PYTHONPATH=. python3 project/scripts/build_domain_graph.py`
7. Run `make governance` to sync registries

### Adding a filter template to an event (3-file sync)

1. `spec/templates/registry.yaml` — `compatible_families` must use `canonical_regime` (not `canonical_family`); add to event block AND families section
2. `spec/templates/event_template_registry.yaml` — mirror
3. `spec/events/event_registry_unified.yaml` — mirror
4. Rebuild domain graph

### Adding a new package

1. Place it at the correct layer — check the DAG above to confirm allowed imports
2. Add `__init__.py`
3. Add the new package's allowed imports to `_PACKAGE_DEPENDENCY_ROWS` in `test_architectural_integrity.py`
4. Run `make minimum-green-gate` to verify

### Changing the dependency matrix

Only as a last resort, after:
1. Moving shared logic downward into an already-allowed package
2. Splitting generic utilities out of the higher-level owner

---

## Notable structural choices

- Compatibility wrapper namespaces have been removed from the canonical path
- Structured proposal execution is the only supported proposal path — legacy free-text proposals are not supported
- Live thesis export is a governed contract (`live_export.py` validates schema), not an ad hoc JSON dump
- Package placement is correctness — the architecture test enforces it
