# Contract Hardening Patch

Implemented bounded updates:

1. Event output contracts now carry first-class `event_side`, `event_direction`, `magnitude`, and `severity_bucket` fields.
2. Runtime `NormalizedEvent` carries side, direction, magnitude, severity, and confidence for auditable direction resolution.
3. Event registry rows include staged `eligibility`, `eligibility_reason`, and `lifecycle_stage` fields while preserving legacy booleans.
4. Event-template compatibility now has a structured `CompatibilityVerdict` with reason codes and promotion/paper/live flags.
5. Promotion explicitly rejects abstract/generic templates such as `mean_reversion`, `continuation`, `exhaustion_reversal`, and `reversal_or_squeeze`.
6. Search evaluation emits explicit cost-stress metrics: 1x, 1.5x, 2x, 3x net mean bps, break-even cost multiplier, and cost survival ratio.

These changes preserve existing discovery behavior while hardening the promotion/runtime path.
