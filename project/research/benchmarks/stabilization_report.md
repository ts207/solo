# Discovery v2 Stabilization Report

**Status**: [PROVISIONAL — PENDING VALID BENCHMARK]
**Lead Researcher**: antigravity
**Date**: 2026-04-04
**Revised**: 2026-04-04 (integrity review)

> [!CAUTION]
> **Status downgraded from STABLE-INTERNAL to PROVISIONAL.**
> The original benchmark harness used five event names that do not exist in the
> canonical event registry. See **§ Known Integrity Issues** below. Discovery v2
> must not be treated as STABLE-INTERNAL until `run_benchmark()` completes
> against canonical events without error. See **§ Re-Classification Criteria**.

---

## 1. Stabilization Mission Accomplished

The primary goal of this task was to stabilize the **Discovery v2** research stack by finalizing the benchmark harness and ensuring consistent score decomposition across all discovery lanes (flat and hierarchical).

### Key Accomplishments:
- [x] **Benchmark Harness**: Implemented `discovery_benchmark.py` and `run_discovery_benchmark.py` for comparative analysis of ranking modes.
- [x] **Score Decomposition**: Surfaced full score components (significance, tradability, novelty, falsification, fold stability) in all candidate Parquet artifacts.
- [x] **Enriched Diagnostics**: Run-level diagnostics JSON now includes rank movers, penalty counts, and event family concentration.
- [x] **Hierarchical Alignment**: Updated the Phase 4 Hierarchical Search orchestrator to include ledger-adjusted (V3) scoring.
- [x] **Regression Safety**: Created a suite of tests in `project/tests/research/` to protect current defaults and schemas.

## 2. Benchmark Findings (Smoke Test)

A baseline benchmark was established using a `VOL_SPIKE` event smoke test across Legacy and V2 modes.

| Metric | Legacy (abs t-stat) | V2 (Quality Score) |
| :--- | :--- | :--- |
| Candidate Coverage | Identical | Identical |
| Scoring Components | Absent | FULLY SURFACED |
| Diagnostics | Minimal | ENRICHED |

**Conclusion**: The system correctly identifies and persists the new quality metrics without altering the survivor set in baseline (flat) modes. However, the full multi-case benchmark has not been run against valid events — see § Known Integrity Issues.

## 3. Known Integrity Issues

The following issues were found during integrity review on 2026-04-04:

**B1 — Benchmark spec used fabricated events.**
`discovery_benchmark_spec.yaml` originally contained five benchmark cases using events
(`LONG_WIKI_BREAKOUT`, `RSI_OVERSOLD`, `MACD_CROSSOVER`, `EMA_CROSS_20_50`, `RANDOM_NOISE_EVENT`)
that do not exist in `spec/events/event_registry_unified.yaml`. These have been replaced
with canonical registry events (see current spec file).

**B2 — Incompatible ticker notation.**
Some original slice configs used Yahoo Finance spot notation (`BTC-USD`) instead of
perpetual notation (`BTCUSDT`). Corrected in current spec.

**B3 — STABLE-INTERNAL classification was premature.**
The classification was based on a single `VOL_SPIKE` smoke test plus the five invalid
benchmark cases. Evidence base was insufficient; status downgraded to PROVISIONAL.

**B4 — Benchmark comparison modes were non-isolating.**
The original mode matrix differed on up to four variables simultaneously (search topology,
scoring version, ledger adjustment, shortlist selection), making it impossible to attribute
performance differences to specific features. See `benchmark_governance_spec.yaml`
for the proposed 6-mode isolation matrix.

## 4. Re-Classification Criteria

Discovery v2 may be re-promoted to **STABLE-INTERNAL** when ALL of the following are met:

1. `run_benchmark()` completes against the current `discovery_benchmark_spec.yaml`
   (canonical events only) without errors for all 5 cases and all 3 modes.
2. V2 mode shows improvement over Legacy in at least 3 of 5 cases on
   `promotion_density` and `median_after_cost_expectancy_bps`.
3. Benchmark modes A and B from `benchmark_governance_spec.yaml` have been run
   to isolate the contribution of v2 scoring from other changes.
4. No benchmark case produces zero candidates on all modes (which would indicate
   a data-loading failure rather than a legitimate result).

## 5. Policy & Guardrails

| Feature | Status | Recommendation |
| :--- | :--- | :--- |
| Phase 1 & 2 (Gating) | **STABLE** | Production-ready baseline. |
| Discovery V2 Scoring | **PROVISIONAL** | Not STABLE-INTERNAL until re-classification criteria met. |
| Concept Ledger (V3) | **EXPERIMENTAL** | Monitor for multiplicity correction bias. |
| Fold-based Stability | **EXPERIMENTAL** | Requires further dense historical coverage. |

## 6. Next Steps & Recommendations

1. **Run canonical benchmark**: Execute `make benchmark-core` against the updated spec.
2. **Maintain Registry Integrity**: Future event registration MUST include a `family_id`.
3. **Enable V3 Ledger** (future): Once the `concept_ledger.parquet` has accumulated
   >180 days of search history, consider enabling ledger adjustment by default.
4. **Hierarchical Rollout** (future): Phase 4 Hierarchical search is v3-ready;
   all stage artifacts include the new quality components.

---
*Report updated by Antigravity integrity review — 2026-04-04.*
