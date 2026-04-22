# Broad Current-Data Event Reflections

This document records the event-level diagnosis from the broad current-data
discovery sweep.

Scope:

- Window: `2023-01-01` through `2024-12-31`
- Timeframe: `5m`
- Discovery set: 24 enabled trade-trigger-eligible events
- Templates: `mean_reversion`, `continuation`, `trend_continuation`,
  `exhaustion_reversal`, `momentum_fade`, `overshoot_repair`
- Horizons: `12b`, `24b`, `48b`
- Directions: long and short
- Primary artifact root: `data/reports/broad_current_data_2023_2024/`

Important artifact constraint: the configs listed both `BTCUSDT` and `ETHUSDT`,
but the emitted Phase 2 diagnostics processed `BTCUSDT` as the primary symbol.
The reflections below are therefore tied to the emitted BTCUSDT artifacts. Do
not infer ETH behavior from this run.

Discovery output is evidence only. It is not validation, promotion, export, or
runtime readiness.

## Verdict Legend

- `PURSUE`: compile and validate a bounded follow-up branch.
- `REPAIR`: viable shape, but failed bridge/tradability/robustness; use only as
  context or with a narrow repair hypothesis.
- `REPAIR_BOUNDED`: one narrow retest is justified; do not sweep.
- `MONITOR_OR_KILL`: context-only unless new data or detector support changes.
- `KILL_CURRENT_DATA`: no actionable support in current artifacts.

## Executive Readout

The broad sweep collapses to one mechanism family: BTCUSDT long funding
continuation.

Pursue:

- `FUNDING_EXTREME_ONSET`
- `FUNDING_PERSISTENCE_TRIGGER`

Repair only as context:

- `OI_SPIKE_POSITIVE`
- `FUNDING_NORMALIZATION_TRIGGER`
- `LIQUIDATION_CASCADE_PROXY`

Do not expand as standalone entry triggers under current data:

- information desync
- liquidity dislocation
- liquidation cascade
- OI flush / negative OI spike
- regime transition
- statistical dislocation
- volatility transition

## Event Summary

| Event | Family | Verdict | Detector rows | Best evidence |
| --- | --- | --- | ---: | --- |
| `CROSS_VENUE_DESYNC` | Information Desync | `KILL_CURRENT_DATA` | 0 | no trigger support |
| `INDEX_COMPONENT_DIVERGENCE` | Information Desync | `KILL_CURRENT_DATA` | 0 | no trigger support |
| `LEAD_LAG_BREAK` | Information Desync | `KILL_CURRENT_DATA` | 0 | no trigger support |
| `SPOT_PERP_BASIS_SHOCK` | Information Desync | `KILL_CURRENT_DATA` | 0 | no trigger support |
| `DEPTH_COLLAPSE` | Liquidity Dislocation | `MONITOR_OR_KILL` | 57 | conditional short 24b mean `6.9905` bps, `t=0.8138` |
| `LIQUIDITY_SHOCK` | Liquidity Dislocation | `KILL_CURRENT_DATA` | 0 | no trigger support |
| `LIQUIDITY_STRESS_DIRECT` | Liquidity Dislocation | `KILL_CURRENT_DATA` | 0 | no trigger support |
| `LIQUIDITY_VACUUM` | Liquidity Dislocation | `KILL_CURRENT_DATA` | 0 | no trigger support |
| `FUNDING_EXTREME_ONSET` | Positioning Extremes | `PURSUE` | 332 | normalized long 48b after-cost `39.9978` bps, `t=3.3559`, robustness `0.5940`, `q=0.000791` |
| `FUNDING_FLIP` | Positioning Extremes | `MONITOR_OR_KILL` | 5 | edge summary `3.9186` bps at 5 bars |
| `FUNDING_NORMALIZATION_TRIGGER` | Positioning Extremes | `REPAIR_BOUNDED` | 37 | conditional long 24b mean `26.5184` bps, `t=1.2187` |
| `FUNDING_PERSISTENCE_TRIGGER` | Positioning Extremes | `PURSUE` | 5472 | normalized long 48b after-cost `14.6226` bps, `t=3.1253`, robustness `0.5747`, `q=0.001776` |
| `LIQUIDATION_CASCADE` | Positioning Extremes | `MONITOR_OR_KILL` | 152 | edge summary `9.4387` bps at 5 bars |
| `LIQUIDATION_CASCADE_PROXY` | Positioning Extremes | `REPAIR_BOUNDED` | 35 | conditional long 12b mean `29.6500` bps, `t=1.1451`, robustness `0.8574` |
| `OI_FLUSH` | Positioning Extremes | `MONITOR_OR_KILL` | 114 | edge summary `-2.2525` bps at 1 bar |
| `OI_SPIKE_NEGATIVE` | Positioning Extremes | `MONITOR_OR_KILL` | 234 | edge summary `3.5539` bps at 5 bars |
| `OI_SPIKE_POSITIVE` | Positioning Extremes | `REPAIR` | 222 | raw Phase 2 short 48b after-cost `19.5752` bps, `t=2.0359`, bridge rejected |
| `BETA_SPIKE_EVENT` | Regime Transition | `KILL_CURRENT_DATA` | 0 | no trigger support |
| `CORRELATION_BREAKDOWN_EVENT` | Regime Transition | `KILL_CURRENT_DATA` | 0 | no trigger support |
| `BASIS_DISLOC` | Statistical Dislocation | `KILL_CURRENT_DATA` | 0 | no trigger support |
| `FND_DISLOC` | Statistical Dislocation | `KILL_CURRENT_DATA` | 0 | no trigger support |
| `VOL_RELAXATION_START` | Volatility Transition | `KILL_CURRENT_DATA` | 0 | no trigger support |
| `VOL_SHOCK` | Volatility Transition | `MONITOR_OR_KILL` | 648 | conditional long 24b mean `7.8700` bps, `t=0.8780` |
| `VOL_SPIKE` | Volatility Transition | `MONITOR_OR_KILL` | 735 | conditional long 12b mean `4.3078` bps, `t=0.9180` |

## Information Desync

Family rejection profile: `invalid_metrics=144`.

### CROSS_VENUE_DESYNC

Verdict: `KILL_CURRENT_DATA`.

Reflection: no emitted detector support. This is a current-data and
data-coverage kill, not a permanent theoretical rejection of cross-venue
desync. Skip until spot/perp cross-venue feature coverage changes.

Next action: no follow-up proposal.

### INDEX_COMPONENT_DIVERGENCE

Verdict: `KILL_CURRENT_DATA`.

Reflection: no emitted detector support. The current local feature set does not
produce index-component divergence episodes for this sweep.

Next action: no follow-up proposal.

### LEAD_LAG_BREAK

Verdict: `KILL_CURRENT_DATA`.

Reflection: no emitted detector support. The event may require richer
cross-market or multi-symbol lead-lag features than the emitted artifact path
provided.

Next action: no follow-up proposal.

### SPOT_PERP_BASIS_SHOCK

Verdict: `KILL_CURRENT_DATA`.

Reflection: no emitted detector support. Do not confuse this with the funding
edge found below; spot/perp basis shock did not generate actionable current-data
episodes in this run.

Next action: no follow-up proposal.

## Liquidity Dislocation

Family rejection profile: `invalid_metrics=108`, `min_t_stat=36`.

### DEPTH_COLLAPSE

Verdict: `MONITOR_OR_KILL`.

Reflection: detector support exists with 57 rows, but the best conditional row
is weak: short 24b, mean `6.9905` bps, `t=0.8138`. Robustness looked high in
the conditional row, but statistical strength is far below the Phase 2 gate.

Next action: no standalone entry branch. Revisit only if depth data quality or
liquidity-state conditioning materially improves.

### LIQUIDITY_SHOCK

Verdict: `KILL_CURRENT_DATA`.

Reflection: no emitted detector support. The current artifact path does not
support a liquidity-shock discovery branch.

Next action: no follow-up proposal.

### LIQUIDITY_STRESS_DIRECT

Verdict: `KILL_CURRENT_DATA`.

Reflection: no emitted detector support. Treat this as unavailable under current
data rather than weak-but-tested.

Next action: no follow-up proposal.

### LIQUIDITY_VACUUM

Verdict: `KILL_CURRENT_DATA`.

Reflection: no emitted detector support. Do not sweep templates or horizons
from an empty detector base.

Next action: no follow-up proposal.

## Positioning Extremes

Family rejection profile: `min_sample_size=36`, `min_t_stat=204`.

### FUNDING_EXTREME_ONSET

Verdict: `PURSUE`.

Reflection: primary discovery. The event has 332 detector rows and survives
normalization as BTCUSDT long funding continuation. Best normalized candidate:
long 48b continuation, after-cost `39.9978` bps, stressed after-cost
`37.9978` bps, `t=3.3559`, robustness `0.5940`, `q=0.000791`.

Template variants are duplicate-like aliases here. Prefer `continuation` or
`trend_continuation` as the canonical validation framing; use opposite direction
and mean-reversion aliases as controls, not as separate theses.

Next action: compile a bounded validation proposal for BTCUSDT long 24b/48b.

### FUNDING_FLIP

Verdict: `MONITOR_OR_KILL`.

Reflection: only 5 detector rows. The edge summary is directionally positive
at 5 bars, but sample support is too small for a discovery branch.

Next action: keep as descriptive state only.

### FUNDING_NORMALIZATION_TRIGGER

Verdict: `REPAIR_BOUNDED`.

Reflection: 37 detector rows and a conditional long 24b row with mean
`26.5184` bps, but `t=1.2187` is too weak. This looks more like an exit or
regime-transition descriptor than a primary entry trigger.

Next action: one narrow retest only if used as a context around the stronger
funding-continuation branch.

### FUNDING_PERSISTENCE_TRIGGER

Verdict: `PURSUE`.

Reflection: secondary discovery. It has far broader support than extreme onset:
5472 detector rows. Best normalized candidate: long 48b continuation,
after-cost `14.6226` bps, stressed after-cost `12.6226` bps, `t=3.1253`,
robustness `0.5747`, `q=0.001776`.

This is lower expectancy than `FUNDING_EXTREME_ONSET`, but more stable as a
persistent funding-state branch or context filter.

Next action: validate alongside `FUNDING_EXTREME_ONSET` at 24b/48b.

### LIQUIDATION_CASCADE

Verdict: `MONITOR_OR_KILL`.

Reflection: 152 detector rows and a positive short-horizon edge summary, but no
Phase 2 survivor. Together with the earlier liquidation-exhaustion matrix, this
argues against standalone liquidation entries under current gates.

Next action: keep as context metadata only.

### LIQUIDATION_CASCADE_PROXY

Verdict: `REPAIR_BOUNDED`.

Reflection: 35 detector rows. The best conditional row is long 12b with mean
`29.6500` bps and robustness `0.8574`, but `t=1.1451` is too low. The shape is
interesting but underpowered.

Next action: one narrow retest only as a support/context variable, not as a
standalone trigger.

### OI_FLUSH

Verdict: `MONITOR_OR_KILL`.

Reflection: 114 detector rows, but the edge summary is negative at the best
short horizon. This does not support an entry branch.

Next action: keep out of validation.

### OI_SPIKE_NEGATIVE

Verdict: `MONITOR_OR_KILL`.

Reflection: 234 detector rows and a small positive edge summary, but no Phase 2
candidate. The event is better interpreted as context around leverage stress
than as an entry trigger.

Next action: keep out of validation.

### OI_SPIKE_POSITIVE

Verdict: `REPAIR`.

Reflection: only non-funding event to reach raw Phase 2 candidate rows. Best
raw row: short 48b mean-reversion, after-cost `19.5752` bps, `t=2.0359`,
robustness `0.2920`, `q=0.041759`, bridge rejected. The t-stat is barely above
threshold, but robustness and bridge quality are too weak.

Next action: use as a possible confirmation/context filter for funding
continuation after the baseline funding branch validates. Do not promote or
validate standalone.

## Regime Transition

Family rejection profile: `invalid_metrics=72`.

### BETA_SPIKE_EVENT

Verdict: `KILL_CURRENT_DATA`.

Reflection: no emitted detector support. Current local artifacts do not support
a beta-spike event branch.

Next action: no follow-up proposal.

### CORRELATION_BREAKDOWN_EVENT

Verdict: `KILL_CURRENT_DATA`.

Reflection: no emitted detector support. Current local artifacts do not support
a correlation-breakdown event branch.

Next action: no follow-up proposal.

## Statistical Dislocation

Family rejection profile: `invalid_metrics=72`.

### BASIS_DISLOC

Verdict: `KILL_CURRENT_DATA`.

Reflection: no emitted detector support. Do not spend proposal budget on this
branch until basis-dislocation detector coverage changes.

Next action: no follow-up proposal.

### FND_DISLOC

Verdict: `KILL_CURRENT_DATA`.

Reflection: no emitted detector support. This did not reproduce the funding
continuation edge; the successful signal came from funding positioning events,
not this statistical-dislocation detector.

Next action: no follow-up proposal.

## Volatility Transition

Family rejection profile: `invalid_metrics=36`, `min_t_stat=72`.

### VOL_RELAXATION_START

Verdict: `KILL_CURRENT_DATA`.

Reflection: no emitted detector support. There is no current-data basis for a
standalone relaxation-start entry branch.

Next action: no follow-up proposal.

### VOL_SHOCK

Verdict: `MONITOR_OR_KILL`.

Reflection: detector support is large at 648 rows, but the best conditional row
is long 24b with mean `7.8700` bps and `t=0.8780`. It fails edge strength even
with ample sample support.

Next action: use as a regime/risk filter only if needed.

### VOL_SPIKE

Verdict: `MONITOR_OR_KILL`.

Reflection: detector support is large at 735 rows, but the best conditional row
is long 12b with mean `4.3078` bps and `t=0.9180`. The 2024-only near miss did
not survive the wider 2023-2024 window as a bridge candidate.

Next action: use as a regime/risk filter only if needed.

## Recommended Validation Branch

Validate one branch, not another broad sweep:

- event: `FUNDING_EXTREME_ONSET`
- event: `FUNDING_PERSISTENCE_TRIGGER`
- symbol: `BTCUSDT`
- direction: long
- horizons: 24b and 48b
- canonical template: `continuation` or `trend_continuation`

Do not include `OI_SPIKE_POSITIVE` in the first validation branch. If the
funding branch validates, run a second bounded test where `OI_SPIKE_POSITIVE`
acts only as a confirmation/context filter.

Compiled proposal matrix:

| Run | Proposal | Event | Horizon | Template |
| --- | --- | --- | ---: | --- |
| FC1 | `spec/proposals/funding_continuation_extreme_onset_long_h24_btc_v1.yaml` | `FUNDING_EXTREME_ONSET` | 24 | `continuation` |
| FC2 | `spec/proposals/funding_continuation_extreme_onset_long_h48_btc_v1.yaml` | `FUNDING_EXTREME_ONSET` | 48 | `continuation` |
| FC3 | `spec/proposals/funding_continuation_persistence_long_h24_btc_v1.yaml` | `FUNDING_PERSISTENCE_TRIGGER` | 24 | `continuation` |
| FC4 | `spec/proposals/funding_continuation_persistence_long_h48_btc_v1.yaml` | `FUNDING_PERSISTENCE_TRIGGER` | 48 | `continuation` |

Shared search spec:

```text
spec/search/single_event_funding_continuation_btc_h24_h48_v1.yaml
```

Plan commands:

```bash
edge discover plan --proposal spec/proposals/funding_continuation_extreme_onset_long_h24_btc_v1.yaml
edge discover plan --proposal spec/proposals/funding_continuation_extreme_onset_long_h48_btc_v1.yaml
edge discover plan --proposal spec/proposals/funding_continuation_persistence_long_h24_btc_v1.yaml
edge discover plan --proposal spec/proposals/funding_continuation_persistence_long_h48_btc_v1.yaml
```

Execution commands:

```bash
edge discover run --proposal spec/proposals/funding_continuation_extreme_onset_long_h24_btc_v1.yaml
edge discover run --proposal spec/proposals/funding_continuation_extreme_onset_long_h48_btc_v1.yaml
edge discover run --proposal spec/proposals/funding_continuation_persistence_long_h24_btc_v1.yaml
edge discover run --proposal spec/proposals/funding_continuation_persistence_long_h48_btc_v1.yaml
```

Plan review checklist:

- `estimated_hypothesis_count` is exactly 1 for each proposal.
- Event is one of `FUNDING_EXTREME_ONSET` or `FUNDING_PERSISTENCE_TRIGGER`.
- Template is exactly `continuation`.
- Direction is exactly `long`.
- Horizon is exactly 24 or 48 bars.
- Symbol is exactly `BTCUSDT`.
- Promotion remains disabled until validation artifacts exist.

## Funding Continuation Forward Outcome

FC2 (`FUNDING_EXTREME_ONSET`, long `48b`, `continuation`) passed internal
2023-2024 validation but failed full-2025 forward confirmation.

Forward confirmation run:

```text
funding_continuation_ext_confirm_2025_20260421T230400Z
```

Result:

- n: 164
- after-cost expectancy: 13.7546 bps
- t-stat: 1.3124
- robustness: 0.4087
- Phase 2 candidates: 0
- fail reason: `min_t_stat`
- release signoff: `BLOCK_RELEASE`

The bounded rescue branch (`chop_regime == 1`) also failed:

```text
funding_continuation_ext_chop_nonbear_2025_20260422T001600Z
```

- n: 85
- after-cost expectancy: 8.2750 bps
- t-stat: 0.8219
- Phase 2 candidates: 0
- release signoff: `BLOCK_RELEASE`

The bear-trend falsification control also failed:

```text
funding_continuation_ext_bear_control_2025_20260422T001800Z
```

- n: 38
- after-cost expectancy: 40.5868 bps
- t-stat: 0.9558
- Phase 2 candidates: 0
- release signoff: `BLOCK_RELEASE`

Decision: retire FC2 broad form and keep the `FUNDING_EXTREME_ONSET` long
`48b` continuation branch research-only. Do not promote or export the FC2
rescue/control variants.

Supporting artifacts:

- `data/reports/funding_continuation_validation/fc2_regime_break_postmortem.md`
- `data/reports/funding_continuation_validation/fc2_rescue_control_2025.md`

FC4 (`FUNDING_PERSISTENCE_TRIGGER`, long `48b`, `continuation`) was then tested
as a separate persistence thesis on full 2025, without using it as an FC2 rescue.

Forward confirmation run:

```text
funding_continuation_per_confirm_2025_20260422T002320Z
```

Result:

- n: 3609
- after-cost expectancy: -3.0987 bps
- t-stat: -0.2359
- robustness: 0.4746
- Phase 2 candidates: 0
- fail reason: `min_t_stat`
- release signoff: `BLOCK_RELEASE`

Decision: FC4 is not forward-confirmed. Do not promote or export it. Keep
`FUNDING_PERSISTENCE_TRIGGER` long `48b` continuation research-only.

Supporting artifact:

- `data/reports/funding_continuation_validation/fc4_forward_confirmation_2025.md`

Tracked lifecycle postmortem:

- [Funding continuation 2025 postmortem](funding-continuation-2025-postmortem.md)
