# Architecture Reference

Edge is a Python repository with contract-enforced package boundaries. Package ownership matters because tests enforce import direction and because runtime must not absorb research policy implicitly.

## Top-Level Packages

`project.core`:

- shared configuration, exceptions, timeframes, column registry, coercion, and low-level utilities
- no research policy

`project.io`:

- table and file IO helpers
- parquet/csv compatibility surfaces

`project.specs`, `project.spec_registry`, `project.spec_validation`:

- authored and generated spec loading
- ontology/search/spec validation
- compiled registry support

`project.domain`:

- compiled domain registry models
- event, state, template, regime, and thesis definition accessors
- compatibility checks for search and runtime

`project.events`:

- event detector implementations
- event contracts and governance metadata
- runtime event detection support

`project.features`:

- feature construction and feature-related contracts

`project.pipelines`:

- top-level DAG planning and execution
- run manifests
- stage dependencies
- artifact contract-backed execution plans
- pipeline provenance

`project.research`:

- proposal ingestion and translation
- phase-2 search engine
- candidate evaluation
- validation services
- promotion policy
- reporting
- experiment memory
- live thesis export support

`project.validate`, `project.promote`, `project.discover`:

- thin facade packages used by `project.cli`

`project.runtime` and `project.engine`:

- execution-state mechanics
- replay invariants
- live-session state
- order/fill transitions

`project.live`:

- live data ingestion
- live decisioning
- thesis store
- OMS
- risk, kill switches, and deployment status

`project.portfolio`:

- allocation, overlap, incubation, and portfolio decisioning

`project.apps.chatgpt`:

- ChatGPT app and MCP-oriented facade
- should wrap canonical operator surfaces rather than redefining research policy

## Command Entrypoints

`pyproject.toml` exposes:

- `edge = project.cli:main`
- `backtest = project.cli:main`
- `edge-backtest = project.cli:main`
- `edge-chatgpt-app = project.apps.chatgpt.cli:main`
- `edge-run-all = project.pipelines.run_all:main`
- `edge-live-engine = project.scripts.run_live_engine:main`

Treat `edge` as the canonical lifecycle command.

## Dependency DAG

Architecture tests live under:

```text
project/tests/architecture/
```

They enforce:

- allowed cross-package imports
- no domain package upward imports from pipelines
- removed legacy wrapper namespaces stay removed
- shallow package root surfaces remain shallow
- preferred root imports replace deep cross-domain imports
- research pipeline wrappers stay deleted
- decomposed detector modules remain research-free

Run:

```bash
PYTHONPATH=. ./.venv/bin/python -m pytest -s -q project/tests/architecture
```

Use `-s` if the local pytest capture layer fails.

## Guarded Contract Files

Do not edit these without explicit approval and a broader verification loop:

```text
spec/events/event_registry_unified.yaml
spec/events/regime_routing.yaml
project/contracts/pipeline_registry.py
project/contracts/schemas.py
project/engine/schema.py
project/research/experiment_engine_schema.py
project/strategy/dsl/schema.py
project/strategy/models/executable_strategy_spec.py
```

Changes here can affect discovery feasibility, artifact contracts, runtime loading, and promotion policy.

## Contracts Layer

`project/contracts/` defines formal interfaces:

- stage-family registry definitions
- artifact input/output declarations
- schema normalization
- schema validation
- generated system-map payload support

The contract layer should stay side-effect free. Business logic belongs in owning packages, not in contract declarations.

## Pipeline Layer

`project/pipelines/` owns:

- `run_all.py`
- argument parsing and effective config
- stage planning
- DAG execution
- run manifests
- artifact contract conformance
- preflight and postflight audits

It does not own research policy, promotion policy, live decisioning, or thesis packaging semantics.

## Research Layer

`project/research/` owns:

- proposal translation
- experiment planning support
- search engine candidate generation
- candidate diagnostics
- validation services
- promotion policy
- experiment memory
- thesis export support

Policy should stay in services and contract-aware modules. Avoid spreading policy through wrapper scripts.

## Runtime Boundary

The runtime boundary is explicit:

```text
data/live/theses/<run_id>/promoted_theses.json
```

Runtime consumes exported theses and current live state. It should not interpret unpromoted discovery artifacts as deployable instructions.

## Change Strategy

For changes that affect package boundaries:

1. Identify the owning package.
2. Keep logic in the owning package instead of a wrapper.
3. Update contracts if artifact/schema behavior changes.
4. Update docs and regression tests.
5. Run architecture tests and targeted contract checks.

Default validation:

```bash
./plugins/edge-agents/scripts/edge_validate_repo.sh contracts
```

For structural changes:

```bash
./plugins/edge-agents/scripts/edge_validate_repo.sh minimum-green
```
