# Contract Enforcement v10

This patch moves the edge-semantics work from structural contracts into hard enforcement paths.

## Added

- Formal promotion `semantic_pass` gate with `semantic_reason_codes`.
- Data-quality hard veto for promotion and compatibility:
  - `stale`
  - `missing_required_feature`
  - `synthetic_only`
- Runtime manifest enforcement can now be required explicitly; implemented runtime load paths use mandatory manifest admission.
- `shadow` is accepted as a runtime mode in live-engine config validation.
- Expanded explicit event-template compatibility coverage for the top statistical, basis, volatility, breakout, liquidation, liquidity-sweep, and price/OI candidates.
- Runtime manifest validator script.
- Template promotion contract validator script.
- Fixture migration scanner for legacy JSON artifacts.
- `make contract-check` fast contract target.
- Portfolio admission trace builder for deterministic signal admission/rejection audit rows.

## Key invariant

A candidate should not promote unless semantic validity, compatibility, side/direction resolution, data quality, mechanism evidence, and runtime-package requirements are explicit.
