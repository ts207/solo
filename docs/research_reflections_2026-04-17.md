# Research Reflections — 2026-04-17

Campaign scope: systematic discovery across all events, BTC 2023-2024, rv_pct_17280 > 70. ~50 proposal runs, 20+ events tested, 4 horizon sweeps, multiple vol filter sweeps.

---

## The silent failure pattern is dangerous

The template incompatibility check drops hypotheses at plan time with no visible error — just a warning buried in logs. If the root cause investigation hadn't been triggered, VOL_SPIKE would have been accepted as a dead signal. The t=3.59 result (strongest in the campaign) was sitting one template swap away the entire time.

The lesson generalises: any "no signal" result from a proposal run needs a sanity check on `estimated_hypothesis_count` in `validated_plan.json` before it can be trusted. The pipeline is silent about a lot. Similarly, FAILED_CONTINUATION's `not_executed_or_missing_data` label in the campaign ledger was a reporting artifact — the pipeline ran and found t=-0.81, but the framework couldn't locate the result parquet. Trust the phase2 diagnostics over the campaign summary when they conflict.

**Rule going forward:** Verify `estimated_hypothesis_count > 0` before concluding an event has no edge.

---

## The signal landscape has a ceiling

After exhaustive tuning — horizon sweeps, vol filter sweeps, date range extensions — the results cluster into three groups:

**Strong (promoted):**
| Event | Horizon | t | rob | Template |
|-------|---------|---|-----|----------|
| VOL_SPIKE long | 24b | 3.59 | 0.62 | mean_reversion |
| OI_SPIKE_NEGATIVE long | 48b | 2.37 | 0.87 | exhaustion_reversal |
| LIQUIDATION_CASCADE long | 24b | 1.78 | 0.82 | exhaustion_reversal |

**Below bridge gate (real but capped):**
| Event | Best horizon | t | rob |
|-------|-------------|---|-----|
| CLIMAX_VOLUME_BAR | 24b | 1.95 | 0.79 |
| POST_DELEVERAGING_REBOUND | 48b | 1.95 | 0.68 |
| OI_SPIKE_POSITIVE | 48b | 1.65 | 0.65 |
| FORCED_FLOW_EXHAUSTION | 48b | 1.40 | 0.60 |

**No edge:** VOL_SHOCK, TREND_EXHAUSTION_TRIGGER, FAILED_CONTINUATION (long), and all batch4_vol events tested with wrong template.

The cluster of below-gate discoveries isn't random noise — these are real effects. But no single-feature conditioning (regime, vol, trend) or horizon adjustment can push them over both gates simultaneously. That boundary appears structural rather than tuning-dependent.

---

## 2022 is a structural break

Every extension to include 2022 data weakened the signal. CLIMAX_VOLUME_BAR 24b dropped from t=1.95 (2023-2024) to t=1.17 (2022-2024). The 2022 bear market doesn't just add noise — it actively opposes the effect direction. This isn't a sample size problem; it's a regime break.

Implication: all discovered signals are bull-market conditional. The robustness metric captures fold stability within the tested window, but not regime stability across cycles. Live trading depends on the 2023-2024 regime persisting. That risk is not fully reflected in the reported statistics.

---

## The strongest signals have clear mechanisms

VOL_SPIKE mean-reversion: a vol spike overshoots, short-covering drives a bounce, price reverts within 2 hours. OI_SPIKE_NEGATIVE: a sudden drop in open interest (forced position closure) creates a directional imbalance that persists for 4 hours. LIQUIDATION_CASCADE: forced liquidation at extremes creates a brief dislocation that reverts at 24b.

The events with no signal tend to fire at the wrong point in the cycle (VOL_SHOCK fires during relaxation, not the shock itself) or have no consistent directional consequence (FAILED_CONTINUATION can fail in either direction). Mechanistic plausibility is a better prior than statistical fishing.

---

## Gate calibration appears correct

The bridge gate (t ≥ 2.0, rob ≥ 0.70) is doing its job. Most below-gate events cluster just under one threshold but clear the other — suggesting the gates are tight but not arbitrary. The few that clear both (OI_SPIKE_NEGATIVE, VOL_SPIKE) are genuinely distinguishable from the pack in both signal strength and fold stability.

VOL_SPIKE's robustness of 0.62 is the weak point — it clears phase2 gate but not bridge gate. The high t=3.59 partially compensates, but the cross-fold instability is real and warrants careful position sizing.

---

## Diminishing returns on further discovery

The 6 remaining batch4_vol events (BREAKOUT_TRIGGER, RANGE_COMPRESSION_END, VOL_CLUSTER_SHIFT, etc.) are queued with correct templates and cached lake. Worth running — they're fast. But VOL_SHOCK (most similar to VOL_SPIKE) showed nothing, which is a weak prior.

The real upside at this stage is probably deployment rather than more discovery:
1. VOL_SPIKE is the strongest signal found and isn't live yet
2. OI_SPIKE_NEGATIVE is running on testnet but needs USDT funding
3. Paper data will be more informative than additional backtests on the same 2023-2024 window

---

## What would change the picture

- **Multi-feature regime classifier**: the below-gate cluster (CVB, PDR, OI_SPIKE_POS) all show real effects that no single feature can concentrate. A learned regime label might unlock them.
- **Post-2024 data**: extending the lake forward as 2025 data accumulates will either confirm or break the 2023-2024 regime signals.
- **Cross-asset**: ETH signals are largely absent (OI_SPIKE_NEGATIVE ETH t=0.94, no others tested) — the BTC-only constraint may be worth revisiting with more data.
