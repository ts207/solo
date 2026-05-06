# No-liquidation detector round 3 patch notes

- Runtime selector now runs profile-declared evidence detectors by default.
- Trade eligibility remains separate from runtime detection.
- Added profile-driven composite thesis router.
- Added funding runtime input bindings and causal funding percentile derivation.
- Restored `make detector-audit`.
- Capped OI z-scores with a denominator floor.
- OI_FLUSH now fires on core OI contraction + price move even when ms_oi_state lags; conflicting state degrades quality instead of blocking.
- Raw LIQUIDITY_VACUUM is never standalone trade eligible.
- LIQUIDITY_VACUUM_RECOVERY now fires on recovery onset only.
- Updated LIQUIDITY_VACUUM and OI_FLUSH specs.
- Renamed funding `event_lookahead` fields to `post_event_horizon_bars`.
