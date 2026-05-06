# Detector Fix Patch Notes

This patch hardens the detector layer without changing research gates or live risk controls.

## Changes

- `BaseDetectorV2.validate_no_lookahead` now performs timestamp/index checks and bounded prefix replay against detected event bars.
- `BaseDetectorV2.detect_events` preserves detector params in `DataFrame.attrs` so the no-lookahead replay uses the same runtime parameters.
- `FundingFlipDetectorV2` and legacy `FundingFlipDetector` no longer use future-row confirmation. They now emit on the causal persistence confirmation bar and preserve the original flip bar in metadata.
- `FundingFlipDetector` now advertises `causal = True` after the lookahead repair.
- Funding flip rolling calibration now clamps `min_periods <= threshold_window`, preventing small-window crashes.
- `LiquidityVacuumDetectorV2` is direct-evidence only by default. Proxy mode must be explicitly enabled with `allow_proxy_vacuum_trigger=True`; proxy events are marked `data_quality_flag=degraded` and `trade_eligible=false`.
- `spec/events/LIQUIDITY_VACUUM.yaml` documents direct live-trade requirements for `depth_usd` and `spread_bps`.
- Detector governance artifacts in `docs/generated/` were regenerated and pass drift check.

## Sanity checks run

- AST parse for patched detector files.
- Targeted funding flip causal confirmation sanity check.
- Targeted liquidity vacuum direct/proxy gating sanity check.
- Detector governance artifact regeneration and `--check` drift validation.
- Targeted `compileall` for patched detector/script files.
