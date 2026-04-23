# Engine Layer (`project/engine`)

The engine layer owns execution mechanics, artifacts, and lower-level trading-state transitions used by replay and runtime surfaces.

## Ownership

- order and fill state transitions
- execution-side artifact generation
- engine exceptions and low-level execution helpers
- deterministic execution behavior that runtime and replay surfaces depend on

## Non-Ownership

- research policy and signal selection
- data cleaning and feature generation
- top-level run orchestration

## Constraints

- identical inputs should yield identical execution state transitions
- execution code should stay lean and avoid unnecessary research-layer dependencies
- engine modules should describe how to execute, not whether a hypothesis deserves execution
