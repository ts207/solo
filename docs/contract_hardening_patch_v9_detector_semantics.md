# Contract Hardening Patch v9 — Detector Semantic Overrides

This patch moves the event layer from structural polarity support to detector-specific semantic polarity for the highest-value edge candidates.

## Changes

- Added explicit basis-spread semantics for `BASIS_DISLOC`, `FND_DISLOC`, and `SPOT_PERP_BASIS_SHOCK` through `BasisDetectorV2Base`.
- Added first-class funding variant adapters:
  - `FUNDING_POS_EXTREME_ONSET`
  - `FUNDING_NEG_EXTREME_ONSET`
  - `FUNDING_POS_PERSISTENCE`
  - `FUNDING_NEG_PERSISTENCE`
  - `FUNDING_POS_NORMALIZATION`
  - `FUNDING_NEG_NORMALIZATION`
  - `FUNDING_FLIP_TO_POSITIVE`
  - `FUNDING_FLIP_TO_NEGATIVE`
- Added first-class price/OI quadrant adapters:
  - `PRICE_UP_OI_UP`
  - `PRICE_DOWN_OI_UP`
  - `PRICE_UP_OI_DOWN`
  - `PRICE_DOWN_OI_DOWN`
- Removed load-time alias collapse for `PRICE_UP_OI_DOWN` and `PRICE_DOWN_OI_DOWN` so quadrant events can resolve to their own detector adapters.
- Added explicit price-direction methods for key trend detectors:
  - `RANGE_BREAKOUT`
  - `PULLBACK_PIVOT`
  - `TREND_ACCELERATION`
  - `TREND_DECELERATION`
- Added explicit neutral/execution-guard semantics for liquidity stress/vacuum/collapse detectors so guard events cannot accidentally become directional alpha via incidental price movement.
- Added liquidation-side explicit polarity for cascade detectors using `cascade_side` / forced-flow semantics.
- Added explicit sweep-side direction for wick/stop-run proxy events and price-direction for price-volume imbalance proxies.
- Adjusted detector contract assembly so `deployable_core` refers to detector runtime availability, not live-thesis trading eligibility.

## Validation

- `compileall` over `project/` passed.
- Domain graph rebuild passed.
- Domain graph freshness check passed.
- Detector polarity audit passed with `--fail-on-runtime-unknown`.
- New direct regression tests in `project/tests/events/test_detector_semantic_overrides_v9.py` passed.

Full repository pytest was not completed in this environment.
