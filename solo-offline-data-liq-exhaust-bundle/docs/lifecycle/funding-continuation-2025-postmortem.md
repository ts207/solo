# Funding Continuation 2025 Postmortem

Status: `failed_forward_confirmation`, `non_deployable`, `research_only`.

## Scope

This postmortem covers the BTCUSDT funding-continuation branch selected by the
2023-2024 broad current-data sweep:

- `FUNDING_EXTREME_ONSET`, long, `48b`, `continuation`
- `FUNDING_PERSISTENCE_TRIGGER`, long, `48b`, `continuation`

The 2023-2024 sweep and internal validation made these the strongest funding
continuation candidates. The 2025 forward checks below are confirmatory release
checks, not another broad discovery pass.

## Decision

Do not promote, export, or deploy any funding-continuation candidate from this
validation set.

The built-in validation artifacts were insufficient as a release decision
because they validated the origin-window candidates but did not prove forward
survival in the full unseen 2025 window. The forward runs completed cleanly,
generated feasible hypotheses, and failed statistically at Phase 2 with zero
edge candidates.

## Forward Results

| Branch | Run ID | n | After-cost bps | t-stat | Robustness | Phase 2 candidates | Rejection |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| Extreme onset broad 2025 | `funding_continuation_ext_confirm_2025_20260421T230400Z` | 164 | 13.7546 | 1.3124 | 0.4087 | 0 | `min_t_stat` |
| Extreme onset chop/non-bear rescue | `funding_continuation_ext_chop_nonbear_2025_20260422T001600Z` | 85 | 8.2750 | 0.8219 | 0.4507 | 0 | `min_t_stat` |
| Extreme onset bear control | `funding_continuation_ext_bear_control_2025_20260422T001800Z` | 38 | 40.5868 | 0.9558 | 0.8690 | 0 | `min_t_stat` |
| Persistence trigger broad 2025 | `funding_continuation_per_confirm_2025_20260422T002320Z` | 3609 | -3.0987 | -0.2359 | 0.4746 | 0 | `min_t_stat` |

All four runs have:

- `bridge_candidates_rows: 0`
- `phase2_candidates.parquet` row count: 0
- `edge_candidates_normalized.parquet` row count: 0
- `rejection_reason_counts: {"min_t_stat": 1}`
- operator outcome: `no_signal`

## Why It Failed

FC2, the strongest 2023-2024 candidate, degraded from a promotable-looking
historical effect to an underpowered forward effect:

- Origin 2023-2024: n=332, after-cost 39.9978 bps, t=3.3559, robustness 0.5940.
- Forward 2025: n=164, after-cost 13.7546 bps, t=1.3124, robustness 0.4087.

The event density did not collapse: origin density was 13.63 events per 30 days
and 2025 density was 13.48 events per 30 days. The issue is effect quality, not
detector starvation.

The one bounded rescue branch worsened the evidence. Filtering to
`chop_regime == 1` reduced support from 164 to 85 events, lowered after-cost
expectancy from 13.7546 bps to 8.2750 bps, and lowered t-stat from 1.3124 to
0.8219. The bear-trend control had high mean payoff but only 38 events, severe
dispersion, and t=0.9558.

FC4 had enough support to falsify cleanly. The persistence trigger kept 3609
events in 2025, but after-cost expectancy flipped negative at -3.0987 bps with
t=-0.2359.

## Mechanistic Interpretation

The funding-continuation mechanism was over-broad. In 2023-2024, positive
funding extremes and persistence aligned with continuation over 48 bars. In
2025, the detector still found the event state, but the forward return response
was too weak or inverted after costs. This is consistent with a regime-dependent
crowding/continuation effect rather than a stable standalone event edge.

The failure is not a pipeline error:

- Runs completed with return code 0.
- Feature and event tables were present for the full 2025 window.
- Each confirmatory run produced one feasible hypothesis and one metrics row.
- The Phase 2 gate rejected the hypothesis by `min_t_stat`.

## Research Disposition

Kill for release:

- `FUNDING_EXTREME_ONSET` long `48b` continuation broad form.
- `FUNDING_EXTREME_ONSET` chop/non-bear rescue.
- `FUNDING_EXTREME_ONSET` bear control.
- `FUNDING_PERSISTENCE_TRIGGER` long `48b` continuation broad form.

Keep research-only:

- Funding continuation as a historical mechanism label.
- Funding event tables for later diagnostic comparison.

Do not run another broad funding sweep from this branch. A future funding
proposal needs an independently motivated mechanism with a new condition that
explains why 2025 failed, and it should begin with forward-window falsification
as an explicit stop condition.

The bounded follow-up branch is:

- proposal:
  `spec/proposals/funding_persistence_oi_confirm_long_h48_btc_2025_v1.yaml`
- search spec:
  `spec/search/sequence_funding_persistence_oi_confirm_btc_h48_2025_v1.yaml`
- thesis:
  `FUNDING_PERSISTENCE_TRIGGER -> OI_SPIKE_POSITIVE`, max gap 24 bars, long
  `48b`, `continuation`, BTCUSDT, 2025 only.
- stop condition:
  zero Phase 2 candidates, t-stat below 2.0, or non-positive after-cost
  expectancy kills the funding family for current data.

## OI-Confirmed Follow-Up Outcome

Initial run before OI acquisition:

```text
funding_persistence_oi_c_20260422T003706Z_0208a1f032
```

Result:

- anchor: `FUNDING_PERSISTENCE_TRIGGER -> OI_SPIKE_POSITIVE`
- max gap: 24 bars
- horizon: `48b`
- direction: long
- template: `continuation`
- BTCUSDT 2025 feature rows: 104833
- `FUNDING_PERSISTENCE_TRIGGER` events: 3609
- `OI_SPIKE_POSITIVE` events: 0
- Phase 2 candidates: 0
- edge candidates: 0
- top funnel reason: `no_trigger_hits`
- Phase 2 rejection class: `invalid_metrics`

The run completed mechanically and evaluated one feasible sequence hypothesis,
but the OI confirmation leg never fired. The current 2025 run-scoped feature
lake is not usable for an OI-spike confirmation test:

- `oi_notional` non-null bars: 48
- `oi_notional` unique values: 1
- `oi_delta_1h` unique values: 1
- nonzero `oi_delta_1h` bars: 0
- `ms_oi_state` unique values: 1

This result was a data-coverage blocker, not an economic test. The missing 2025
raw OI was then acquired through the Bybit derivatives OI ingestion path.

Acquisition run:

```text
acquire_btcusdt_oi_2025_20260422
```

Raw OI coverage after acquisition:

- partitions: 12
- rows: 105120
- range: `2025-01-01 00:00:00+00:00` to `2025-12-31 23:55:00+00:00`
- duplicate timestamps: 0
- `open_interest` unique values: 104847
- `open_interest` min/max: 44380.662 / 74585.46
- nonzero sequential OI changes: 105117

Repaired-data follow-up run:

```text
funding_persistence_oi_c_20260422T004455Z_0208a1f032
```

Result:

- anchor: `FUNDING_PERSISTENCE_TRIGGER -> OI_SPIKE_POSITIVE`
- max gap: 24 bars
- horizon: `48b`
- direction: long
- template: `continuation`
- BTCUSDT 2025 feature rows: 104833
- `FUNDING_PERSISTENCE_TRIGGER` events: 3609
- `OI_SPIKE_POSITIVE` events: 169
- sequence hits: 34
- mean return: -16.8756 bps
- after-cost expectancy: -18.8756 bps
- t-stat: -0.5174
- p-value: 0.697545
- hit rate: 0.4706
- robustness: 0.0389
- validation samples: 7
- test samples: 4
- Phase 2 candidates: 0
- edge candidates: 0
- gate failure: `min_t_stat`

Decision: kill the OI-confirmed funding-persistence rescue. After repairing OI
coverage, the sequence has enough support to evaluate but the effect is negative
after costs and statistically failed. Do not run additional funding-continuation
rescue branches from this result. The funding family remains non-deployable under
current artifacts.

## Supporting Artifacts

Ignored detailed reports:

- `data/reports/funding_continuation_validation/fc2_regime_break_postmortem.md`
- `data/reports/funding_continuation_validation/fc2_rescue_control_2025.md`
- `data/reports/funding_continuation_validation/fc4_forward_confirmation_2025.md`

Canonical run artifact directories:

- `data/reports/phase2/funding_continuation_ext_confirm_2025_20260421T230400Z/`
- `data/reports/phase2/funding_continuation_ext_chop_nonbear_2025_20260422T001600Z/`
- `data/reports/phase2/funding_continuation_ext_bear_control_2025_20260422T001800Z/`
- `data/reports/phase2/funding_continuation_per_confirm_2025_20260422T002320Z/`
