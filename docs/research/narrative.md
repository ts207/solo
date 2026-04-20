# Research Reflections — 2026-04-17

> **Historical record.** This narrative covers the initial discovery campaign. The promoted theses were cleared due to bugs in gate computation and sizing policy that have since been fixed. The observations about signal quality, regime sensitivity, template incompatibility, and the discovery landscape remain valid and are preserved here as research context.

Campaign scope: systematic discovery across all events, BTC 2023-2024, rv_pct_17280 > 70. ~50 proposal runs, 20+ events tested, 4 horizon sweeps, multiple vol filter sweeps.

---

# Part I — Technical / Repository

## The silent failure pattern is dangerous

The template incompatibility check (`check_hypothesis_feasibility`) drops hypotheses at plan time with no visible error — just a warning buried in logs. If the root cause investigation hadn't been triggered, VOL_SPIKE would have been accepted as a dead signal. The t=3.59 result (strongest in the campaign) was sitting one template swap away the entire time.

The lesson generalises: any "no signal" result from a proposal run needs a sanity check on `estimated_hypothesis_count` in `validated_plan.json` before it can be trusted. Similarly, FAILED_CONTINUATION's `not_executed_or_missing_data` label in the campaign ledger was a reporting artifact — the pipeline ran and found t=-0.81, but the framework couldn't locate the result parquet because the same `run_id` was used for multiple sequential proposals, each overwriting the shared phase2 directory.

**Rules going forward:**
- Verify `estimated_hypothesis_count > 0` before concluding an event has no edge.
- When running multiple proposals against the same `run_id`, each overwrites phase2 results — check per-experiment `campaign_summary.json` or `event_statistics.parquet`, not the shared phase2 dir, for individual results.
- Trust phase2 diagnostics over campaign summary when they conflict.

## Template-family contracts need documentation at the event level

The `spec/events/*.yaml` files specify a `detector` but not which templates are compatible. A researcher writing a proposal has no way to know that `exhaustion_reversal` is invalid for VOLATILITY_EXPANSION events without running the code. This caused the entire VOLATILITY_TRANSITION family to go unevaluated across batch4_vol and the broad sweep.

The compatible template groups are:
- VOLATILITY_EXPANSION / VOLATILITY_TRANSITION: `mean_reversion`, `continuation`, `impulse_continuation`, `vol_breakout`
- TREND_FAILURE_EXHAUSTION / FORCED_FLOW_AND_EXHAUSTION: `exhaustion_reversal`
- TREND_STRUCTURE: `exhaustion_reversal`, `mean_reversion`, `impulse_continuation`

Adding this to event specs or a quick-reference table would prevent the same class of error.

## The run_id reuse pattern creates result trapping

Using `--run_id` to reuse a cached lake is efficient, but when multiple proposals share the same run_id, each sequential run overwrites the phase2 outputs. The campaign framework only surfaces the last run's result in the shared directory. Results from earlier runs in the sequence survive only in the per-experiment artifacts. This is not obvious and cost several "no results" false negatives.

## Gate calibration appears correct

The bridge gate (t ≥ 2.0, rob ≥ 0.70) is doing its job. Most below-gate events cluster just under one threshold while clearing the other — suggesting the gates are tight but not arbitrary. The few that clear both (OI_SPIKE_NEGATIVE, VOL_SPIKE) are genuinely distinguishable from the pack in both signal strength and fold stability. The gate has genuine discriminative power.

---

# Part II — Trading Research

## The signal landscape has a hard ceiling

After exhaustive tuning — horizon sweeps, vol filter sweeps, date range extensions — the results cluster into three groups:

**Promoted:**
| Event | Horizon | t | rob | Template | Mechanism |
|-------|---------|---|-----|----------|-----------|
| VOL_SPIKE long | 24b | 3.59 | 0.62 | mean_reversion | Spike overshoots → short-covering bounce over 2h |
| OI_SPIKE_NEGATIVE long | 48b | 2.37 | 0.87 | exhaustion_reversal | Forced OI unwind → directional imbalance over 4h |
| LIQUIDATION_CASCADE long | 24b | 1.78 | 0.82 | exhaustion_reversal | Liquidation dislocation → reversion over 2h |

**Real but capped (below bridge gate):**
| Event | Best horizon | t | rob | Notes |
|-------|-------------|---|-----|-------|
| CLIMAX_VOLUME_BAR | 24b | 1.95 | 0.79 | Structural t ceiling ~1.95 |
| POST_DELEVERAGING_REBOUND | 48b | 1.95 | 0.68 | Rob ceiling 0.68 |
| OI_SPIKE_POSITIVE | 48b | 1.65 | 0.65 | Confirmed live H2-2024 |
| FORCED_FLOW_EXHAUSTION | 48b | 1.40 | 0.60 | Hard floor on both gates |

**No edge:** VOL_SHOCK, TREND_EXHAUSTION_TRIGGER, FAILED_CONTINUATION (long), entire VOLATILITY_TRANSITION batch (BREAKOUT_TRIGGER, RANGE_COMPRESSION_END, VOL_CLUSTER_SHIFT, VOL_REGIME_SHIFT_EVENT, VOL_RELAXATION_START, BETA_SPIKE_EVENT).

The below-gate cluster is not noise — these are real effects. But no single-feature conditioning or horizon adjustment can push them over both gates simultaneously. The ceiling appears structural.

## 2022 is a regime break, not just a different sample

Every extension to include 2022 data weakened the signal. CLIMAX_VOLUME_BAR 24b dropped from t=1.95 (2023-2024) to t=1.17 (2022-2024). The bear market doesn't just add noise — it actively opposes the effect direction. Adding more samples made signals weaker, which means the mechanism itself doesn't operate in bear regimes.

The implication is stark: all three promoted signals are bull-market conditional. The robustness metric measures fold stability within the tested window, not regime stability across market cycles. A regime shift to sustained bearish or choppy conditions could extinguish these effects entirely. That risk is not captured in the reported statistics and needs to inform position sizing and kill-switch design.

## The clearest signals have the simplest mechanisms

VOL_SPIKE: a volatility spike in a high-vol regime overshoots, triggering short-covering, and prices bounce back within two hours. OI_SPIKE_NEGATIVE: forced position closure creates a directional imbalance that the market takes four hours to absorb. LIQUIDATION_CASCADE: forced liquidation at extremes creates a brief dislocation that reverts over two hours.

All three are microstructure-driven. They don't require a view on fundamentals or macro — they exploit the mechanical consequence of forced flows. This is the right hunting ground for event-driven signals on perpetual futures.

The events with no signal mostly fire at the wrong point in the cycle. VOL_SHOCK fires during the relaxation phase, after the directional move is done. VOL_CLUSTER_SHIFT detects a regime change that has already occurred. These are descriptive labels for market states, not precursors to predictable moves.

## The below-gate cluster may unlock with multi-feature conditioning

The four below-gate events (CVB, PDR, OI_SPIKE_POS, FFE) all show t in the range 1.4–1.95 and robustness 0.60–0.79. No single feature (rv, trend, funding) concentrates these effects enough to clear the gate. But they share a common structure: they're all forced-flow or exhaustion events that tend to cluster together in time. A multi-feature regime label combining rv + funding extreme + OI regime might identify the specific market conditions where all four are predictive simultaneously. That's the most plausible path to unlocking further edge from the existing event universe.

## The discovery campaign is effectively exhausted

The full event universe has been tested with correct templates. The signal picture is unlikely to change materially with more single-event proposal runs on the same 2023-2024 data. What would genuinely change the picture:

- **More recent data**: 2025 data will either confirm the 2023-2024 signals or show regime decay. This is the highest-value input at this stage.
- **Multi-feature regime classifier**: the below-gate cluster is the most accessible target.
- **ETH signals**: ETH OI_SPIKE_NEGATIVE was t=0.94 (no signal) — likely needs the liquidation ingest to be done properly before retesting.
- **Cross-event sequences**: the promoted events (OI_SPIKE_NEGATIVE, VOL_SPIKE, LIQUIDATION_CASCADE) sometimes co-occur. Sequence or conjunction hypotheses may yield higher-confidence entries.
