# Runtime Layer (`project/runtime`)

The runtime layer owns live-session behavior, replay invariants, and runtime-facing execution state.

## Ownership

- trading session lifecycle and session-scoped state
- deterministic replay support
- venue-facing runtime drivers and runtime invariants
- OMS behavior exposed to live and replay surfaces

## Non-Ownership

- research discovery and promotion policy
- historical data ingestion and cleaning
- phase-2 statistical evaluation

## Constraints

- keep state session-scoped rather than global
- preserve replay determinism for identical inputs
- keep heavy dataframe-oriented dependencies at boundaries where possible
