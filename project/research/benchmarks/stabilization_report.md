# Discovery v2 Stabilization Report

**Status**: [STABLE — CANONICAL D PATH]
**Lead Researcher**: antigravity
**Date**: 2026-04-04
**Revised**: 2026-04-18 (canonical D adoption)

> Canonical discovery now has one operator path: mode D
> (`hierarchical_v2_with_folds`). Legacy comparison modes and the ledger /
> shortlist overlays are retired from active benchmark presets.

---

## 1. Stabilization Mission Accomplished

The primary goal of this task was to stabilize the **Discovery v2** research stack and collapse active discovery onto one path.

### Key Accomplishments:
- [x] **Benchmark Harness**: Implemented `discovery_benchmark.py` and `run_benchmark_matrix.py` for canonical path validation.
- [x] **Score Decomposition**: Surfaced full score components (significance, tradability, novelty, falsification, fold stability) in all candidate Parquet artifacts.
- [x] **Enriched Diagnostics**: Run-level diagnostics JSON now includes rank movers, penalty counts, and event family concentration.
- [x] **Hierarchical Alignment**: Adopted hierarchical search with v2 scoring and repeated walk-forward folds.
- [x] **Regression Safety**: Created a suite of tests in `project/tests/research/` to protect current defaults and schemas.

## 2. Benchmark Findings

The canonical benchmark was rerun as a one-path matrix under:
`data/reports/benchmarks/canonical_d_only_20260418/`.

| Metric | Canonical D |
| :--- | :--- |
| Mode set | D only |
| Jobs | 4 non-negative-control slices |
| Status | 4 success |
| Certification | passed |
| Ledger adjustment | disabled |
| Diversified shortlist | disabled |

**Conclusion**: Use mode D as the only operator path.

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

**B4 — Benchmark comparison modes retired.**
The original comparison matrix is no longer active. `benchmark_governance_spec.yaml`
now records a single canonical path.

## 4. Re-Classification Criteria

Discovery v2 remains stable while ALL of the following are met:

1. `run_benchmark_matrix --preset core_v1` builds only mode D jobs.
2. Ledger adjustment remains disabled.
3. Diversified shortlist remains disabled.
4. The canonical path report has zero noncanonical mode slices.

## 5. Policy & Guardrails

| Feature | Status | Recommendation |
| :--- | :--- | :--- |
| Phase 1 & 2 (Gating) | **STABLE** | Production-ready baseline. |
| Discovery V2 Scoring | **STABLE** | Canonical D path. |
| Concept Ledger (V3) | **DISABLED** | Not part of canonical path. |
| Fold-based Stability | **STABLE** | Required by canonical D path. |

## 6. Next Steps & Recommendations

1. **Run canonical benchmark**: Execute `make benchmark-core` against the updated spec.
2. **Maintain Registry Integrity**: Future event registration MUST include a `family_id`.
3. **Do not add alternate modes** without a new explicit governance decision.

---
*Report updated by Antigravity integrity review — 2026-04-04.*
