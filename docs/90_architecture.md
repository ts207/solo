# Architecture and package boundaries

The repository enforces a package dependency DAG in `project/tests/test_architectural_integrity.py`.

## Why this matters

The codebase is large (1442 Python files) and spans research, runtime, specs, and live trading concerns. The architecture test is the main mechanism preventing those concerns from collapsing into a single import graph.

## Current allowed base-package dependencies

| Base package | May import |
|---|---|
| `project.compilers` | `project.core`, `project.specs`, `project.events`, `project.domain`, `project.strategy`, `project.schemas` |
| `project.core` | `project.spec_registry`, `project.artifacts`, `project.specs`, `project.io` |
| `project.domain` | `project.core`, `project.specs`, `project.spec_registry`, `project.events` |
| `project.engine` | `project.core`, `project.io`, `project.events`, `project.features`, `project.strategy.runtime`, `project.strategy`, `project.portfolio` |
| `project.episodes` | `project.core`, `project.specs`, `project.spec_registry` |
| `project.events` | `project.core`, `project.io`, `project.specs`, `project.spec_registry`, `project.research`, `project.features`, `project.artifacts`, `project.contracts`, `project.domain`, `project.spec_validation` |
| `project.features` | `project.core`, `project.io`, `project.events`, `project.spec_registry`, `project.artifacts`, `project.contracts` |
| `project.io` | `project.core`, `project.artifacts` |
| `project.live` | `project.core`, `project.events`, `project.features`, `project.strategy`, `project.strategy.runtime`, `project.portfolio`, `project.episodes`, `project.io`, `project.engine`, `project.research`, `project.artifacts`, `project.domain` |
| `project.operator` | `project.core`, `project.research`, `project.specs`, `project.io` |
| `project.pipelines` | `project.research`, `project.engine`, `project.events`, `project.core`, `project.io`, `project.specs`, `project.contracts`, `project.domain`, `project.features`, `project.schemas`, `project.eval`, `project.runtime`, `project.spec_registry`, `project.experiments`, `project.operator` |
| `project.portfolio` | `project.core`, `project.specs`, `project.strategy`, `project.live`, `project.research` |
| `project.research` | `project.core`, `project.io`, `project.specs`, `project.runtime`, `project.events`, `project.features`, `project.strategy`, `project.strategy.runtime`, `project.engine`, `project.eval`, `project.spec_registry`, `project.artifacts`, `project.schemas`, `project.spec_validation`, `project.contracts`, `project.domain`, `project.compilers`, `project.portfolio`, `project.live`, `project.operator`, `project.episodes` |
| `project.runtime` | `project.core`, `project.specs` |
| `project.specs` | `project.core`, `project.io`, `project.spec_registry`, `project.schemas`, `project.artifacts` |
| `project.strategy` | `project.compilers`, `project.core`, `project.strategy.runtime`, `project.events`, `project.domain`, `project.engine`, `project.schemas` |
| `project.strategy.runtime` | `project.core`, `project.strategy`, `project.events`, `project.compilers` |


## How to read the DAG

### `project.core` is the lowest common substrate

It carries shared primitives, config, stats, registries, and validation helpers. Higher packages can depend on it, but it should not know about research or live-runtime policy.

### Spec/domain packages sit below research/runtime packages

`project.spec_registry`, `project.specs`, `project.spec_validation`, and `project.domain` define or compile the vocabulary consumed by higher layers.

### Research is intentionally broad

`project.research` can see many layers because it orchestrates search, evaluation, promotion support, and reporting. That breadth is by design, but it should not become the dumping ground for generic utilities that belong in `core`, `specs`, or `portfolio`.

### Live and engine share policy through portfolio where possible

The previous direct `engine -> live` dependency was removed by moving shared admission logic into `project.portfolio`. That is the preferred pattern for shared cross-runtime policy.

### Pipelines orchestrate rather than own business rules

`project.pipelines` has broad visibility because it wires together many stages. Resist the temptation to bury business semantics there if they have a clearer home in research, specs, events, or live.

## Current notable structure choices

- compatibility wrapper namespaces have been removed from the canonical path
- structured proposal execution is the only supported proposal path
- live thesis export is a governed contract, not an ad hoc JSON dump
- package placement is part of correctness because the architecture test enforces it

## Safe change pattern

When a new dependency is needed, prefer this order:

1. move the shared logic downward into an already-allowed package
2. split generic utilities out of the higher-level owner
3. only then consider changing the dependency matrix

Changing the matrix should be the last move, not the first.
