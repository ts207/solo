# Contracts Layer (`project/contracts`)

The contracts layer defines the formal interfaces between stages, artifacts, and schema-validated outputs.

## Ownership

- pipeline and stage-family contract definitions
- artifact input and output declarations
- schema normalization and validation helpers
- generated system-map payloads and related integrity checks

## Non-Ownership

- business logic or numerical computation
- runtime orchestration
- environment-specific configuration values

## Important Modules

- `pipeline_registry.py`: source of truth for stage-family registry definitions
- `artifacts.py`: artifact contract registry
- `schemas.py`: dataframe normalization and schema validation
- `stage_dag.py`: stage-family contract view used by tests and generated docs
- `system_map.py`: machine-readable and markdown system map generation

## Constraints

- Imports should stay side-effect free.
- Contract declarations should reflect the artifacts implementations actually read and write.
- Generated docs should be derived from these contracts rather than edited by hand.
