# Contract Hardening Patch — Continued

This continuation tightens runtime integration points after the initial contract-hardening pass.

## Added / hardened

1. `event_output_schema.normalize_event_output_frame` now derives signed event side correctly from numeric `event_direction` values (`-1`, `0`, `1`) when no side is provided.
2. `event_output_schema.validate_event_output_frame` now validates normalized event direction and severity buckets in addition to required side fields.
3. `live.decision` resolves both legacy thesis/context sides (`long`, `short`) and new ontology sides (`bullish`, `bearish`, `bidirectional`) into order-side decisions.
4. `PromotedThesis` accepts new ontology side values at ingestion time and normalizes them back to the existing live-contract side vocabulary (`long`, `short`, `both`, `conditional`, `unknown`).
5. Cell-discovery lineage now records event-template compatibility verdict fields and uses those verdicts to suppress paper/runtime and promotion eligibility when required contexts are missing or a combination is research-only.

## Validation performed

- Python compilation for patched runtime/research/domain modules.
- Domain graph rebuild.
- Domain graph freshness check.
- Direct smoke checks for event-output normalization, live side resolution, promoted-thesis side normalization, and contrarian direction resolution.

## Known limitation

The full pytest suite was not completed in this execution environment because targeted pytest invocations timed out during collection/runtime. The patch was kept non-invasive and guarded by compatibility normalization to reduce migration risk.
