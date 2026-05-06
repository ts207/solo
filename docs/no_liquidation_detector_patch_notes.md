# No-liquidation detector patch notes

This patch pivots the runtime profile away from liquidation-dependent forced-flow detectors.

## Core profile

`spec/governance/data_capabilities.yaml` defines `no_liquidations_v1`, where liquidation feeds are unavailable and liquidation detectors are disabled.

## New/updated detector path

- `OI_EXPANSION_STRESS`: context-only price/OI/funding quadrant classifier.
- `OI_FLUSH`: no-liquidation forced-flow proxy for position unwind; requires pairing.
- `LIQUIDITY_VACUUM`: execution/risk guard, not entry trigger.
- `LIQUIDITY_VACUUM_RECOVERY`: direct-book recovery trigger after vacuum.
- `FUNDING_*_EXTREME_ONSET`: context-only, sign-specific crowding evidence that requires pairing.

## End vision

OHLCV + funding + OI + spread/depth -> causal detectors -> context/trade eligibility -> composite thesis builder -> paper gate -> tiny live.

Primary composite theses: `FUNDING_CROWDING_BREAK`, `OI_FLUSH_REVERSAL`, `SHORT_BUILD_CONTINUATION`, and `SQUEEZE_RISK_REVERSAL`.
