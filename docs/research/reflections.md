# Reflections

This document has two parts:
1. **Observations** (below) - human-written entries. Add new ones at the top of this section. Never edit the auto-generated section.
2. **Auto-detected patterns** - regenerated automatically after every pipeline run.

To add an observation, insert a new `### [YYYY-MM-DD] Title` block before the AUTO marker.

---

## Observations

### [2026-04-28] Proxy engineering: basis_zscore and imbalance now populated

`basis_zscore`, `cross_exchange_spread_z`, and `imbalance` were null/zero everywhere because:
- No spot OHLCV in the lake → `_add_basis_features` fell back to all-null
- `taker_base_volume=0` everywhere in Bybit v5 raw data → `imbalance` fell back to `0.0`

Engineering fix in `project/pipelines/features/build_features.py`:
- **`basis_bps` proxy**: when no spot data, use deviation of close from its 8h EMA (in bps). EMA deviation varies every bar and captures intraday premium vs. trend anchor. Produces `basis_zscore` mean≈0, std≈1.4 — a well-calibrated z-score.
- **`imbalance` proxy**: when `taker_base_volume` is all zeros, substitute a Lee-Ready tick-rule proxy: rolling mean of `sign(close.diff())` over 24 bars. Range: -1 (persistent selling) to +1 (persistent buying). Produces std≈0.19 vs. flat 0.0 before.

`liquidation_notional` remains 0 by design — a synthetic proxy would contaminate the LIQUIDATION_CASCADE detector's threshold calibration (`liq > median * 3`). LIQUIDATION_CASCADE_PROXY is the correct alternative.

Global lake rebuilt (72 files: BTC 2022-2024, ETH 2022-2024). FALSE_BREAKOUT template also fixed (TREND_STRUCTURE family; `false_breakout_reversal` + `continuation` replace the incompatible `mean_reversion` + `exhaustion_reversal`).

**Rule:** `basis_zscore` from this proxy measures intra-session price deviation from 8h EMA trend, not cross-venue perp-spot basis. Signals conditioned on it should be interpreted as "price extended from recent trend" rather than "funding arbitrage".

---

### [2026-04-28] Null features silently kill entire event families

`liquidation_notional`, `basis_zscore`, `cross_exchange_spread_z`, and `imbalance` are all zero or 100% null across the entire lake (all years, both symbols). This is not missing data — the columns exist in the schema, they simply were never populated.

Consequences:
- LIQUIDATION_CASCADE, DELEVERAGING_WAVE, OI_FLUSH, POST_DELEVERAGING_REBOUND: detectors cannot fire (require `liquidation_notional`)
- All INFORMATION_DESYNC events (CROSS_VENUE_DESYNC, SPOT_PERP_BASIS_SHOCK, LEAD_LAG_BREAK): dead (require `cross_exchange_spread_z`)
- `overshoot_repair` and `basis_repair` templates: broken (require `basis_zscore`)
- All order-flow imbalance events: dead (`imbalance` = 0 everywhere)

**Rule:** Before concluding an event has no edge, verify the detector is actually firing (check events.parquet event count > 0). Zero detections with zero hypothesis count is a data gap, not a null result.

---

### [2026-04-28] PRICE_DOWN_OI_DOWN: the conditioning does all the work

The unconditional PRICE_DOWN_OI_DOWN result is t=1.13, rob=0.57, n=341 (2022-2024 BTC). The bridge-gate result (t=2.35, rob=0.84) exists entirely within the vol=high slice: n=79, 23% of total events.

The vol=high filter concentrates the effect by ~5× in t-stat terms. This is aggressive conditioning — 77% of events are discarded. Before treating this as deployable, verify the vol=high effect is not itself a 2022 artifact: 2022 had significantly elevated rv_96 (mean 0.00175 vs 0.00117 in 2023), which inflates the high-vol event count in that year.

**Rule:** When a bridge-gate result lives in a context slice containing <30% of events, run the 2023-2024 sub-period separately before promoting.

---

### [2026-04-28] CLIMAX_VOLUME_BAR / funding_neg: bear-market concentration risk

CLIMAX_VOLUME_BAR / long / 24b / exhaustion_reversal / carry=funding_neg passes the bridge gate (t=2.25-2.34, rob=0.70-0.72, n=309-472 BTC). But 61% of the funding_neg events (189/309) are from 2022 — the bear market year when funding_neg occurred 3× more often (28% of bars vs ~10% in 2023-2024).

ETH cross-validation is weak: t=0.69, rob=0.51 for the same context. The effect does not replicate on ETH.

The mechanism (capitulation bounce when funding is already negative) is plausible, but the bear-market concentration means in-sample robustness overstates true structural robustness. A 2023-2024 only sub-period test is required before promotion.

**Rule:** When >50% of context-conditioned events cluster in a single calendar year, treat the result as year-conditional until split testing confirms otherwise.

---

### [2026-04-28] FALSE_BREAKOUT: passes gate but fails specificity

FALSE_BREAKOUT / long / 48b / exhaustion_reversal / ms_trend=bullish passes the bridge gate (t=2.09, rob=0.70, n=293 BTC, bps=30.4). However the `specificity_lift_pass` flag is False and placebo_shift_effect is extremely large (98,737), indicating the trade's timing is not precisely identified — artificially shifting the entry point produces similar returns.

Additionally, the effect is directionally consistent across horizons (t=1.75 at 24b, t=2.09 at 48b) and contexts, suggesting a real distributional skew, but the timing imprecision is a live execution risk. No ETH cross-validate exists yet.

**Rule:** A bridge-gate result with `specificity_lift_pass=False` and placebo_shift_effect >10,000 should be treated as hypothesis-grade, not promotion-grade, until entry timing is confirmed.

---

### [2026-04-28] Template mismatch produces high-t artifacts

SLIPPAGE_SPIKE_EVENT (EXECUTION_FRICTION family) was run with `mean_reversion` template — incompatible per the registry (EXECUTION_FRICTION only supports `slippage_aware_filter` + `tail_risk_avoid`). The result: t=2.99, rob=0.46, n=289.

High t with low robustness on an incompatible template is the fingerprint of a mismatch artifact. The `mean_reversion` feature (rv_pct_17280 threshold) is orthogonal to execution friction events, so the cell is testing noise against a real outcome variable, which occasionally produces high t from fold variance.

Current spec bugs using this pattern: `tier2_guard_filter_v1` and `tier2_temporal_execution_guard_v1` both changed SPREAD_REGIME_WIDENING_EVENT and SLIPPAGE_SPIKE_EVENT to `mean_reversion`. Results from these runs should be discarded.

**Rule:** Any result from an EXECUTION_FRICTION event with a non-filter template is an artifact. Discard without further review.

---

### [2026-04-28] test_n_obs=0 is a hidden robustness warning

The CLIMAX_VOLUME_BAR / bullish / 48b result (t=2.58, rob=0.60, n=843) has `test_n_obs=0` — all observations landed in train+validation, none in the held-out test set. The robustness score (0.60) is computed from validation folds only, with no truly out-of-sample confirmation.

This can happen when n is small relative to the fold structure or when events cluster in a time period that doesn't produce a test window. A result with test_n=0 should carry a robustness discount — it is structurally equivalent to a shorter out-of-sample window.

**Rule:** Before acting on a result, check `test_n_obs > 0`. If zero, treat robustness_score as indicative only and require ETH cross-validation as a substitute out-of-sample test.

---

### [2026-04-28] ms_trend_state does not track the crypto market cycle

The ms_trend_state regime classifier produces nearly identical year-over-year distributions: chop=56%, bull=22%, bear=22% in every year from 2022 through 2024 — including the 2022 bear market. This classifier is measuring short-term price momentum state, not multi-month market cycle.

Implications: conditioning on ms_trend_state=bullish does not exclude 2022 bear-market data. Signals conditional on bullish trend state may still be 2022-heavy. The only feature that reliably distinguishes the 2022 bear period is funding_neg frequency (28% in 2022 vs ~10% after) and rv_96 level (0.00175 vs 0.00117).

**Rule:** Year-split ablations are more reliable than regime-state conditioning for detecting bear-market artifacts. Never assume ms_trend_state=bullish filters out 2022.

---

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

