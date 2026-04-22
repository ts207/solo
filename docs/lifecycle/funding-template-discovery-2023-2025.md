# Funding Template Discovery, BTCUSDT 2023-2025

Run: `funding_template_discovery_20260422T0100Z`

Scope:

- Symbol: `BTCUSDT`
- Window: `2023-01-01` through `2025-12-31`
- Timeframe: `5m`
- Events: `FUNDING_EXTREME_ONSET`, `FUNDING_PERSISTENCE_TRIGGER`
- Horizons: `24b`, `48b`
- Directions: `long`, `short`
- Expression templates tested: `reversal_or_squeeze`, `mean_reversion`, `continuation`, `exhaustion_reversal`, `convexity_capture`
- Excluded from this pass: filter-only templates and `trend_continuation`

`trend_continuation` was not included because the current template registry does
not mark it compatible with `POSITIONING_EXTREMES`, the family used by the two
funding events. Filter templates such as `only_if_funding` and `only_if_oi` are
overlays, not standalone hypotheses.

## Run Result

The run evaluated all 40 planned hypotheses.

- Generated hypotheses: 40
- Valid metrics rows: 32
- Invalid metrics rows: 8
- Invalid reason: `unsupported_label_target` for `convexity_capture`
- Rejected by `min_t_stat`: 8
- Phase 2 candidates written: 24
- Phase 2 final gate pass: 9
- Promotion/export/registry update stages: intentionally disabled

The valid discovery signal is long-only. Short variants were rejected. The
positive candidates also share high structural overlap, so the result should be
treated as one funding-long continuation cluster, not nine independent edges.

## Template Summary

| event | horizon | template family | direction | n | mean bps | after-cost bps | t-stat | q | status |
|---|---:|---|---|---:|---:|---:|---:|---:|---|
| `FUNDING_PERSISTENCE_TRIGGER` | 24b | `reversal_or_squeeze` | long | 9089 | 9.8636 | 7.8636 | 3.7680 | 0.000165 | tradable |
| `FUNDING_PERSISTENCE_TRIGGER` | 24b | `continuation` | long | 9089 | 9.8636 | 7.8636 | 3.7680 | 0.000165 | tradable |
| `FUNDING_PERSISTENCE_TRIGGER` | 24b | `exhaustion_reversal` | long | 9089 | 9.8636 | 7.8636 | 3.7680 | 0.000165 | tradable |
| `FUNDING_EXTREME_ONSET` | 24b | `reversal_or_squeeze` | long | 496 | 15.5431 | 13.5431 | 2.3536 | 0.018594 | tradable |
| `FUNDING_EXTREME_ONSET` | 24b | `continuation` | long | 496 | 15.5431 | 13.5431 | 2.3536 | 0.018594 | tradable |
| `FUNDING_EXTREME_ONSET` | 24b | `exhaustion_reversal` | long | 496 | 15.5431 | 13.5431 | 2.3536 | 0.018594 | tradable |
| `FUNDING_EXTREME_ONSET` | 48b | `reversal_or_squeeze` | long | 495 | 20.8206 | 18.8206 | 2.1880 | 0.028671 | tradable |
| `FUNDING_EXTREME_ONSET` | 48b | `continuation` | long | 495 | 20.8206 | 18.8206 | 2.1880 | 0.028671 | tradable |
| `FUNDING_EXTREME_ONSET` | 48b | `exhaustion_reversal` | long | 495 | 20.8206 | 18.8206 | 2.1880 | 0.028671 | tradable |

`mean_reversion` did not produce a bridge-tradable branch. `convexity_capture`
did not evaluate under the current Phase 2 label support.

## Event Reflections

### FUNDING_PERSISTENCE_TRIGGER

Persistence has the strongest statistical support in the current-data discovery
run, but only at `24b`. The long `continuation`,
`reversal_or_squeeze`, and `exhaustion_reversal` entries are numerically
identical because the current evaluator maps them to the same forward-return
response for this event family. The result is therefore a single long funding
persistence continuation candidate, not three distinct mechanisms.

The `48b` persistence branch did not survive `min_t_stat` in this run. That
matters because the prior 2023-2024 discovery highlighted a 48-bar persistence
branch; with 2025 included, the signal compresses toward 24 bars.

Next action: validate only `FUNDING_PERSISTENCE_TRIGGER x long x 24b x
continuation`, with the two alias templates treated as overlap controls.

### FUNDING_EXTREME_ONSET

Extreme onset has lower support but cleaner magnitude. Both `24b` and `48b`
long branches pass Phase 2 discovery gates, with the 48-bar branch carrying the
larger after-cost expectancy and the 24-bar branch carrying the stronger t-stat.
The same template overlap applies: `continuation`, `reversal_or_squeeze`, and
`exhaustion_reversal` are not independent here.

This is the better candidate for a bounded horizon comparison because both
`24b` and `48b` remain alive under current data. It should be tested as a single
event family with two horizons, not as separate unrelated template theses.

Next action: validate `FUNDING_EXTREME_ONSET x long x {24b,48b} x
continuation`, with alias-template controls.

## Post-Mortem Implication

The broad template discovery does not revive the original 48-bar funding
persistence thesis as stated. It points to a narrower current-data structure:

1. `FUNDING_PERSISTENCE_TRIGGER`: long `24b`, high support, moderate after-cost
   expectancy.
2. `FUNDING_EXTREME_ONSET`: long `24b/48b`, lower support, stronger per-event
   expectancy.

Do not promote this run. It is a discovery artifact with current-data exposure
and high structural overlap. The next run should be a bounded validation branch
that collapses alias templates to `continuation` as the canonical expression
template and tests only the two surviving event/horizon shapes.

## Canonical Validation

Canonical validation was run on `funding_template_discovery_20260422T0100Z`.

- Validation report: `data/reports/validation/funding_template_discovery_20260422T0100Z/validation_report.json`
- Total candidates: 24
- Validated: 9
- Rejected: 15
- Inconclusive: 0
- Main rejection reasons: `failed_cost_survival`, `failed_multiplicity_threshold`,
  `failed_oos_validation`, `failed_regime_support`

The nine validated rows still collapse to three unique continuation shapes:

| event | horizon | n | after-cost bps | stressed bps | t-stat | q | stability |
|---|---:|---:|---:|---:|---:|---:|---:|
| `FUNDING_PERSISTENCE_TRIGGER` | 24b | 9089 | 7.8636 | 5.8636 | 3.7680 | 0.000165 | 0.6096 |
| `FUNDING_EXTREME_ONSET` | 24b | 496 | 13.5431 | 11.5431 | 2.3536 | 0.018594 | 0.5535 |
| `FUNDING_EXTREME_ONSET` | 48b | 495 | 18.8206 | 16.8206 | 2.1880 | 0.028671 | 0.5859 |

Validation status should not be read as promotion readiness. The validation
stage confirms candidate-gate survival from the completed run; it does not
remove current-data exposure or structural-overlap concerns.

## Yearly Time-Slice Check

Yearly continuation-only slices were run after validation:

- `funding_continuation_slice_2023_20260422T0115Z`
- `funding_continuation_slice_2024_20260422T0115Z`
- `funding_continuation_slice_2025_20260422T0115Z`

Each slice tested only:

- `FUNDING_EXTREME_ONSET`
- `FUNDING_PERSISTENCE_TRIGGER`
- `continuation`
- `long`
- `24b`, `48b`

The operator time-slice report classified the branch as `stable`.

- Report: `data/reports/operator/time_slice_report/funding_continuation_slice_2023_20260422t0115z__funding_continuation_slice_2024_20260422t0115z__funding_continuation_slice_2025_20260422t0115z/time_slice_report.json`
- Rationale: at least two yearly slices show same-direction non-trivial effect
- Recommended next action: `run_regime_split_confirmation`

Yearly candidate metrics:

| year | event | horizon | n | after-cost bps | stressed bps | t-stat | q | status |
|---:|---|---:|---:|---:|---:|---:|---:|---|
| 2023 | `FUNDING_PERSISTENCE_TRIGGER` | 24b | 2624 | 6.2891 | 4.2891 | 2.2330 | 0.012776 | tradable |
| 2024 | `FUNDING_EXTREME_ONSET` | 24b | 157 | 36.5730 | 34.5730 | 3.6548 | 0.000129 | tradable |
| 2024 | `FUNDING_EXTREME_ONSET` | 48b | 157 | 45.1784 | 43.1784 | 3.1651 | 0.000775 | tradable |
| 2024 | `FUNDING_PERSISTENCE_TRIGGER` | 24b | 2816 | 12.5104 | 10.5104 | 2.2016 | 0.013845 | rejected |
| 2024 | `FUNDING_PERSISTENCE_TRIGGER` | 48b | 2816 | 14.4224 | 12.4224 | 2.3111 | 0.010414 | tradable |
| 2025 | `FUNDING_PERSISTENCE_TRIGGER` | 24b | 3609 | 7.5479 | 5.5479 | 2.4839 | 0.006497 | tradable |

Regime-split confirmation reports were also generated for each yearly slice.
All three classify as `regime_consistent`.

## Updated Decision

Do not promote `FUNDING_EXTREME_ONSET` yet. Its magnitude is strong, but the
yearly evidence is concentrated in 2024.

Do not promote a fixed 48-bar persistence thesis. The full current-data pass
and the yearly slices both argue that persistence is horizon-unstable.

The strongest next branch is narrower and more explicit:

`FUNDING_PERSISTENCE_TRIGGER x long x continuation x {24b,48b}`

Treat it as one persistence-continuation mechanism with horizon sensitivity,
not two unrelated claims. The next bounded pass should test whether the
24/48-bar pair can be represented as a stable validation/promotion candidate
without adding symbols, ETH portability, regime filters, or OI confirmation.

## Horizon-Pair Validation

Run: `funding_persistence_horizon_pair_20260422T0130Z`

Scope:

- Symbol: `BTCUSDT`
- Window: `2023-01-01` through `2025-12-31`
- Event: `FUNDING_PERSISTENCE_TRIGGER`
- Template: `continuation`
- Direction: `long`
- Horizons: `24b`, `48b`
- Promotion/export during run: disabled

The horizon-pair run narrowed the branch to one surviving row:

| horizon | n | mean bps | after-cost bps | stressed bps | t-stat | q | stability | validation |
|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 24b | 9089 | 9.8636 | 7.8636 | 5.8636 | 3.7680 | 0.000082 | 0.6096 | validated |
| 48b | - | 3.7298 | - | - | 1.1349 | - | 0.5660 | rejected by `min_t_stat` |

Promotion was then run as a governed review step. It completed after fixing a
program-log parquet type-normalization defect, but produced no active thesis.

- Promotion report: `data/reports/promotions/funding_persistence_horizon_pair_20260422T0130Z/`
- Promoted count: 0
- Research-promoted count: 1
- Primary reject reason: `failed_gate_promo_benchmark_certification`
- Rejection classification: `weak_holdout_support`
- Recommended next action: `run_confirmatory`

Interpretation: the branch is research-valid over the origin window, but not
deploy-promotable. The 48-bar version is dead under the narrowed pair run; only
24-bar persistence continuation remained worth forward confirmation.

## Q1 2026 Forward Confirmation

Run: `funding_persistence_confirm_q1_2026_20260422T0200Z`

Scope:

- Symbol: `BTCUSDT`
- Window: `2026-01-01` through `2026-04-01`
- Event: `FUNDING_PERSISTENCE_TRIGGER`
- Template: `continuation`
- Direction: `long`
- Horizons: `24b`, `48b`
- Lifecycle role: forward confirmation
- Promotion/export during run: disabled

The confirmatory-window planner reported the origin run as blocked by missing
forward data and identified `2026-01` as the next required funding month. The
forward run acquired Bybit derivatives OHLCV/funding data and generated 976
funding-persistence events.

Forward result:

| horizon | n | mean bps | after-cost bps | stressed bps | t-stat | p | q | stability | validation |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 24b | 976 | -11.8514 | -13.8514 | -15.8514 | -2.4113 | 0.992053 | 1.000000 | 0.3511 | rejected |
| 48b | - | - | - | - | - | - | - | - | rejected before final candidates |

Validation rejected the 24-bar candidate for:

- `failed_oos_validation`
- `failed_cost_survival`
- `failed_regime_support`
- `failed_multiplicity_threshold`

The structural confirmatory comparison service could not produce a strict
matched-candidate report because the origin run intentionally did not export
normalized edge-candidate cost-identity fields. That is a reporting limitation,
not a rescue path: direct Phase 2 and validation evidence show the forward
candidate is negative after costs and fails all relevant confirmation gates.

## Final Current Decision

Kill the current `FUNDING_PERSISTENCE_TRIGGER x long x continuation x 24b`
promotion path. It is not a deploy candidate and should not receive more
promotion budget in this formulation.

Do not revive the 48-bar persistence thesis. It failed in the narrowed origin
run and did not reappear in Q1 2026 confirmation.

The funding family remains useful as research context, but the next thesis must
change mechanism, not tune this same persistence-continuation branch. The only
bounded follow-up worth considering is a fresh discovery pass that asks whether
funding persistence is conditional on a separate state, such as OI expansion,
trend regime, or post-normalization behavior. That should be framed as a new
hypothesis, not as promotion repair for this branch.
