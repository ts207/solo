# Reflections

This document has two parts:
1. **Observations** (below) - human-written entries. Add new ones at the top of this section. Never edit the auto-generated section.
2. **Auto-detected patterns** - regenerated automatically after every pipeline run.

To add an observation, insert a new `### [YYYY-MM-DD] Title` block before the AUTO marker.

---

## Observations

### [2026-04-17] Template-family incompatibility: the silent failure

`check_hypothesis_feasibility` drops incompatible hypotheses at plan time with no visible error.
VOL_SPIKE + `exhaustion_reversal` produced 0 hypotheses for the entire broad sweep and all batch4_vol runs.
The t=3.59 result was sitting one template swap away.

**Rule:** Verify `estimated_hypothesis_count > 0` in `validated_plan.json` before concluding an event has no edge.

VOLATILITY_EXPANSION/TRANSITION events require `mean_reversion`, `continuation`, or `impulse_continuation` - never `exhaustion_reversal`.

---

### [2026-04-17] 2022 is a regime break, not noise

Every extension to include 2022 data weakened signals. CVB 24b: t=1.95 (2023-2024) -> t=1.17 (2022-2024).
The bear market actively opposes the effect direction. This is structural, not sample-size noise.

All promoted signals are bull-market conditional. The robustness metric does not capture regime stability across cycles.

---

### [2026-04-17] run_id reuse overwrites phase2 results

When multiple proposals share the same `--run_id`, each sequential run overwrites
`data/reports/phase2/<run_id>/hypotheses/`. Results from earlier proposals in the sequence survive
only in `data/artifacts/experiments/<program_id>/`. Use `campaign_summary.json` or `event_statistics.parquet`
per experiment, not the shared phase2 dir.

---

### [2026-04-17] Mechanistic clarity predicts signal quality

All three promoted signals (VOL_SPIKE, OI_SPIKE_NEGATIVE, LIQUIDATION_CASCADE) have clear
forced-flow mechanisms. Events that fire at the wrong cycle point (VOL_SHOCK = relaxation phase)
or have no consistent directional consequence (FAILED_CONTINUATION) showed no edge.
Mechanistic plausibility is a better prior than statistical fishing.

---

### [2026-04-17] Below-gate cluster may unlock with multi-feature conditioning

CVB, PDR, OI_SPIKE_POS, FFE all show t=1.4-1.95 with rob=0.60-0.79. No single feature
(rv, trend, funding) concentrates the effect to bridge gate. These events tend to
co-occur in time - a learned regime label combining multiple features may unlock them.

<!-- AUTO-GENERATED: do not edit below this line -->

*Last updated: 2026-04-28 10:40 UTC*

---

## Auto-detected patterns

### Current signal rankings

Sorted by t-stat. Gate: bridge = t >= 2.0 AND rob >= 0.70.

| Event | Dir | Horizon | Template | t | rob | q/p | exp (bps) | status |
|-------|-----|---------|----------|---|-----|---|-----------|--------|
| PRICE_DOWN_OI_DOWN | long | 24b | mean_reversion | 2.35 | 0.839 | 0.0095 | 42.0 | bridge gate (research) |
| OVERSHOOT_AFTER_SHOCK | long | 48b | mean_reversion | 2.09 | 0.585 | 0.0182 | 33.1 | t-only pass (rob below gate) |
| ZSCORE_STRETCH | long | 48b | mean_reversion | 1.73 | 0.414 | 0.0417 | 11.8 | discovery p<0.05 |
| BREAKOUT_TRIGGER | short | 24b | continuation | 1.45 | 0.582 | 0.0733 | 15.7 | below gate |
| SUPPORT_RESISTANCE_BREAK | long | 48b | breakout_followthrough | 1.24 | 0.519 | 0.1080 | 12.1 | below gate |

### Ceiling patterns

Events tested >=3 times that are still below bridge gate (t >= 2.0 AND rob >= 0.70):

| Event | Dir | Template | Best t | Best rob | Tests | Horizons | Gap-to-t | Gap-to-rob |
|-------|-----|----------|--------|----------|-------|----------|----------|------------|
| OVERSHOOT_AFTER_SHOCK | long | mean_reversion | 2.09 | 0.585 | 6 | 24b, 48b, 72b | 0.00 | 0.12 |
| ZSCORE_STRETCH | long | mean_reversion | 1.73 | 0.414 | 3 | 12b, 48b | 0.27 | 0.29 |

### Template incompatibility warnings (estimated_hypothesis_count = 0)

*None detected.*

### Regime sensitivity (more data -> lower t)

*None detected.*

