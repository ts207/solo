# Reflections

This document has two parts:
1. **Observations** (below) — human-written entries. Add new ones at the top of this section. Never edit the auto-generated section.
2. **Auto-detected patterns** — regenerated automatically after every pipeline run.

To add an observation, insert a new `## [YYYY-MM-DD] Title` block before the AUTO marker.

---

# Observations

## [2026-04-17] Template-family incompatibility: the silent failure

`check_hypothesis_feasibility` drops incompatible hypotheses at plan time with no visible error.
VOL_SPIKE + `exhaustion_reversal` produced 0 hypotheses for the entire broad sweep and all batch4_vol runs.
The t=3.59 result was sitting one template swap away.

**Rule:** Verify `estimated_hypothesis_count > 0` in `validated_plan.json` before concluding an event has no edge.

VOLATILITY_EXPANSION/TRANSITION events require `mean_reversion`, `continuation`, or `impulse_continuation` — never `exhaustion_reversal`.

---

## [2026-04-17] 2022 is a regime break, not noise

Every extension to include 2022 data weakened signals. CVB 24b: t=1.95 (2023-2024) → t=1.17 (2022-2024).
The bear market actively opposes the effect direction. This is structural, not sample-size noise.

All promoted signals are bull-market conditional. The robustness metric does not capture regime stability across cycles.

---

## [2026-04-17] run_id reuse overwrites phase2 results

When multiple proposals share the same `--run_id`, each sequential run overwrites
`data/reports/phase2/<run_id>/hypotheses/`. Results from earlier proposals in the sequence survive
only in `data/artifacts/experiments/<program_id>/`. Use `campaign_summary.json` or `event_statistics.parquet`
per experiment, not the shared phase2 dir.

---

## [2026-04-17] Mechanistic clarity predicts signal quality

All three promoted signals (VOL_SPIKE, OI_SPIKE_NEGATIVE, LIQUIDATION_CASCADE) have clear
forced-flow mechanisms. Events that fire at the wrong cycle point (VOL_SHOCK = relaxation phase)
or have no consistent directional consequence (FAILED_CONTINUATION) showed no edge.
Mechanistic plausibility is a better prior than statistical fishing.

---

## [2026-04-17] Below-gate cluster may unlock with multi-feature conditioning

CVB, PDR, OI_SPIKE_POS, FFE all show t=1.4–1.95 with rob=0.60–0.79. No single feature
(rv, trend, funding) concentrates the effect to bridge gate. These events tend to
co-occur in time — a learned regime label combining multiple features may unlock them.

<!-- AUTO-GENERATED: do not edit below this line -->

*Last updated: 2026-04-17 20:44 UTC*

---

# Auto-detected patterns

## Current signal rankings

Sorted by t-stat. Gate: bridge = t ≥ 2.0 AND rob ≥ 0.70.

| Event | Dir | Horizon | Template | t | rob | q | exp (bps) | status |
|-------|-----|---------|----------|---|-----|---|-----------|--------|
| VOL_SPIKE | long | 24b | mean_reversion | 3.59 | 0.616 | 0.0002 | 21.8 | **PROMOTED** |
| LIQUIDATION_CASCADE | long | 8b | mean_reversion | 3.07 | 0.611 | 0.0011 | 27.1 | phase2 gate |
| LIQUIDATION_CASCADE | short | 8b | mean_reversion | 3.00 | 0.118 | 0.0013 | 12.9 | t passes |
| OI_SPIKE_NEGATIVE | long | 24b | exhaustion_reversal | 2.74 | 0.933 | 0.0031 | 62.3 | bridge gate |
| LIQUIDATION_CASCADE_PROXY | long | 16b | reversal_or_squeeze | 2.64 | 0.608 | 0.0083 | 57.3 | phase2 gate |
| LIQUIDATION_CASCADE_PROXY | short | 16b | mean_reversion | 2.64 | 0.186 | 0.0042 | 57.3 | t passes |
| CLIMAX_VOLUME_BAR | long | 12b | exhaustion_reversal | 2.09 | 0.521 | 0.0183 | 38.7 | t passes |
| POST_DELEVERAGING_REBOUND | long | 48b | exhaustion_reversal | 1.95 | 0.677 | 0.0254 | 14.6 | discovery |
| LIQUIDATION_EXHAUSTION_REVERSAL | long | 48b | exhaustion_reversal | 1.95 | 0.677 | 0.0254 | 14.6 | discovery |
| OI_SPIKE_POSITIVE | long | 48b | exhaustion_reversal | 1.65 | 0.646 | 0.0499 | 44.1 | discovery |
| FORCED_FLOW_EXHAUSTION | long | 48b | exhaustion_reversal | 1.40 | 0.599 | 0.0807 | 30.4 | below gate |
| OI_FLUSH | long | 24b | exhaustion_reversal | 1.27 | 0.793 | 0.1026 | 12.3 | below gate |
| VOL_SPIKE | short | 48b | continuation | 1.08 | 0.598 | 0.1407 | 8.4 | below gate |
| DELEVERAGING_WAVE | long | 24b | exhaustion_reversal | 1.02 | 0.667 | 1.0000 | 12.7 | below gate |

## Ceiling patterns

Events tested ≥3 times with no path to bridge gate (t ≥ 2.0 AND rob ≥ 0.70):

| Event | Dir | Template | Best t | Best rob | Tests | Horizons | Gap-to-t | Gap-to-rob |
|-------|-----|----------|--------|----------|-------|----------|----------|------------|
| VOL_SPIKE | long | mean_reversion | 3.59 | 0.616 | 6 | 12b, 24b, 48b | 0.00 | 0.08 |
| LIQUIDATION_CASCADE | short | mean_reversion | 3.00 | 0.173 | 4 | 8b | 0.00 | 0.53 |
| LIQUIDATION_CASCADE_PROXY | short | mean_reversion | 2.64 | 0.186 | 4 | 14b, 16b, 18b, 49b | 0.00 | 0.51 |
| POST_DELEVERAGING_REBOUND | long | exhaustion_reversal | 1.95 | 0.677 | 3 | 12b, 24b, 48b | 0.05 | 0.02 |
| OI_SPIKE_POSITIVE | long | exhaustion_reversal | 1.65 | 0.646 | 3 | 24b, 48b | 0.35 | 0.05 |

## Template incompatibility warnings (estimated_hypothesis_count = 0)

These runs produced 0 hypotheses — likely wrong template for the event family:

| program_id | required_detectors |
|------------|-------------------|
| `vol_shock` | none |

## Regime sensitivity (more data → lower t)

| Event | Horizon | Template | Base t | Extended t | Drop |
|-------|---------|----------|--------|------------|------|
| CLIMAX_VOLUME_BAR | 24b | exhaustion_reversal | 1.95 | 1.17 | −0.78 |

