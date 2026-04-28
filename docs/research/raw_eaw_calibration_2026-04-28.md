# Raw EAW Calibration Readout - 2026-04-28

Scope: local raw Bybit perp data under `data/lake/raw/bybit/perp` for `BTCUSDT`
and `ETHUSDT`, focused on 5m OHLCV, 8h funding, and 5m open-interest rows.

This readout intentionally separates raw detector calibration from the older
phase-2 evaluated-hypothesis readout in
`docs/research/dataset_calibration_2026-04-28.md`.

## Raw Inputs

- `ohlcv_5m`: BTC 2021-2025, ETH 2022-2024.
- `funding`: 8h funding rows, projected point-in-time onto 5m bars for detector
  studies.
- `open_interest`: 5m OI rows, merged point-in-time with OHLCV.
- Primary calibration window for raw sweeps: 2022-2024, BTC/ETH.

## Changes Made

### CLIMAX_VOLUME_BAR

Updated `spec/events/CLIMAX_VOLUME_BAR.yaml`:

- `vol_quantile`: `0.992` -> `0.97`
- `ret_quantile`: `0.998` -> `0.97`
- `range_quantile`: `0.995` -> `0.95`

Raw BTC evidence supported a denser threshold when paired with negative funding:

| Setting | Slice | n | 24b mean bps | t | Hit |
|---|---|---:|---:|---:|---:|
| Old spec | BTC, funding-negative long | 100 | 18.88 | 1.173 | 0.530 |
| New spec | BTC, funding-negative long | 471 | 26.72 | 4.428 | 0.575 |

The bounded proposal run confirmed the edit at the canonical pipeline surface:

- Proposal: `spec/proposals/single_event_climax_volume_funding_neg_exhaustion_reversal_long_h24_btc_v1.yaml`
- Run: `single_event_climax_volu_20260428T212745Z_386e107171`
- Candidate: `CLIMAX_VOLUME_BAR / exhaustion_reversal / CARRY_STATE=funding_neg / long / 24b / BTCUSDT`
- Result: `n=309`, `t_net=2.2495`, `net=25.9578 bps`, `robustness=0.7041`, `q=0.012241`
- Gate failures: none in `gate_failures.parquet`

Cell-discovery follow-up confirmed the context cell:

- Spec: `spec/discovery/raw_climax_funding_neg_v1`
- Run: `raw_climax_cell_01`
- Scoreboard rows: 2 (`unconditional` baseline + `negative_funding`)
- Rankable rows: 1
- Selected cell: `CLIMAX_VOLUME_BAR / exhaustion_reversal / CARRY_STATE=funding_neg / long / 24b / BTCUSDT`
- Forward net mean: `33.977295 bps`
- Robustness: `0.7169`
- Contrast lift: `19.6515 bps`
- Rank score: `0.6511505`

ETH did not independently justify this as a global ETH improvement; this is a
BTC-led calibration that keeps the phase-2 `CLIMAX_VOLUME_BAR + CARRY_STATE =
FUNDING_NEG` lane viable on raw data.

Validation and forward-confirmation outcome:

- Validation run: `single_event_climax_volu_20260428T212745Z_386e107171`
- Candidate: `BTCUSDT::cand_63f6cc562c177028`
- In-sample validation: `n=309`, `t_net=2.2495`, `net=25.9578 bps`,
  `q=0.012241`, `robustness=0.7041`
- Built missing BTCUSDT 2025 `market_context` partitions before OOS replay so
  the frozen `carry_state=funding_neg` filter had context coverage.
- Forward confirmation window: `2025-01-01/2025-12-31`
- OOS confirmation: `n=120`, `net=0.8833 bps`, `t_net=0.0759`,
  `hit_rate=0.5167`, `MAE=-69.3387 bps`, `MFE=72.6801 bps`
- Promotion with `--require_forward_confirmation 1`: rejected cleanly with
  `forward_confirmation_drift`; no live thesis exported.

Decision: keep this as a research-promoted/fallback-only mechanism, not a
deployable thesis. The OOS sign did not flip, but the effect collapsed versus
the validation estimate.

OOS drift investigation:

- The OOS failure is not an event-coverage failure. In 2025, replay found `723`
  raw `CLIMAX_VOLUME_BAR` events, `19,104` quality-covered
  `carry_state=funding_neg` rows, and `120` eligible branch trades.
- Apples-to-apples detector replay over 2022-2024 found `2,548` raw climax
  events, `49,920` funding-negative context rows, and `472` eligible branch
  trades with weighted net mean `23.6553 bps`, `t_net=2.3243`.
- The 2025 OOS branch had positive unweighted mean (`10.8990 bps`) but only
  `0.8833 bps` weighted mean because the replay time-decay weighting emphasizes
  the late OOS window.
- 2025 split:
  - H1: `n=70`, weighted net `16.4645 bps`, `t_net=1.2347`
  - H2: `n=48`, weighted net `-2.7217 bps`, `t_net=-0.2222`
  - Q4: `n=39`, unweighted net `-11.7149 bps`, weighted net `-3.3557 bps`
- Horizon diagnostics did not rescue the mechanism. H1 improves at longer
  horizons, but H2 remains weak or negative from `3b` through `48b`.
- Context diagnosis: the simple signed `carry_state=funding_neg` filter became
  too broad in 2025. All `120` eligible 2025 OOS trades had
  `ms_funding_state=0` (neutral), while the 2022-2024 replay included
  persistent/extreme funding-state participation. The branch thesis was
  "negative carry marks positioning stress"; 2025 mostly provided negative
  funding sign without persistent/extreme stress.
- Regime diagnosis: 2025 bearish-trend OOS rows had weighted net
  `-9.9301 bps`, while bullish-trend rows were positive. The original research
  window did not require a trend exclusion, so adding one now would be a new
  mechanism rather than a calibration edit.

Bounded next experiments:

1. Test a stricter stress context:
   `CLIMAX_VOLUME_BAR / exhaustion_reversal / long / 24b / BTCUSDT` gated by
   negative carry plus persistent/extreme funding state or an explicit funding
   magnitude percentile. Kill if 2025 has too few qualifying trades or remains
   drifted.
2. Test a temporal-drift diagnostic, not a deployment proposal: compare
   2025 H1 versus H2 and any future post-2025 data once available. Stop if the
   edge is isolated to early 2025.
3. Only if a mechanism is stated upfront, test trend interaction as a separate
   cell (`funding_neg` crossed with non-bearish or bearish trend). Treat it as
   exploratory until it survives a fresh OOS window.

Immediate stricter-stress feasibility check:

| Context | 2022-2024 n | 2022-2024 weighted net bps | 2022-2024 t | 2025 n | 2025 weighted net bps | 2025 t |
|---|---:|---:|---:|---:|---:|---:|
| `carry_state=funding_neg` | 472 | 23.6553 | 2.3243 | 120 | 0.8833 | 0.0759 |
| `carry_state=funding_neg` + `funding_phase=negative_persistent` | 253 | 25.6535 | 1.7574 | 0 | n/a | n/a |
| `carry_state=funding_neg` + `funding_persistence_state=1` | 18 | 82.8731 | 1.3080 | 3 | -78.6441 | -0.9746 |
| `funding_persistence_state=1` only | 54 | 35.8859 | 1.0268 | 15 | -56.6421 | -1.5820 |

Repo decision: do not add a stricter funding-stress proposal yet. The canonical
persistent-funding branch has no 2025 OOS sample, and the raw persistence-state
variant fails both sample support and sign in 2025. Keep this as research-only
negative evidence unless new post-2025 data creates a fresh OOS window.

Fresh OOS data acquisition:

- Run id: `fresh_oos_btc_2026_20260428`
- Window: `2026-01-01/2026-04-27` using completed UTC days available on
  2026-04-28.
- Ingested Bybit BTCUSDT perp:
  - `ohlcv_5m`: 4 partitions, `33,696` rows,
    `2026-01-01 00:00:00Z` to `2026-04-27 23:55:00Z`
  - `funding`: 4 partitions, `351` rows,
    `2026-01-01 00:00:00Z` to `2026-04-27 16:00:00Z`
  - `open_interest`: 4 partitions, `33,696` rows,
    `2026-01-01 00:00:00Z` to `2026-04-27 23:55:00Z`
- Built standard lake surfaces:
  - `cleaned/perp/BTCUSDT/bars_5m`: 4 partitions, `33,696` rows
  - `features/perp/BTCUSDT/5m/features_feature_schema_v2`: 4 partitions,
    `33,409` rows
  - `features/perp/BTCUSDT/5m/market_context`: 4 partitions, `33,408` rows
- Forward confirmation on the frozen climax/funding-negative thesis:
  - Window: `2026-01-01/2026-04-27`
  - Result: `n=110`, `net=8.3311 bps`, `t_net=0.8759`,
    `hit_rate=0.5273`, `MAE=-59.4058 bps`, `MFE=63.8882 bps`
- Promotion with `--require_forward_confirmation 1` still rejects with
  `forward_confirmation_drift`; no live thesis exported.

Updated decision after fresh data: 2026 improved over 2025, but the confirmatory
t-stat remains below the drift threshold versus the validation estimate
(`2.2495`). The lane remains research-only.

### BAND_BREAK

Fixed `BandBreakDetector.prepare_features` so runtime parameters such as
`band_z_threshold` and `lookback_window` override registry defaults.

Raw threshold sweep did not justify changing the global `BAND_BREAK` threshold:

- BTC high-vol improved at stricter z thresholds.
- ETH all-regime evidence was better around the current `3.0` threshold, but
  the simple raw low-vol slice was weaker than the phase-2 ETH low-vol result.
- Net conclusion: keep global `band_z_threshold: 3.0`.

Current-detector raw recheck with `lookback_window=288`, `band_z_threshold=3.0`,
24-bar long return, and 12-bar event sparsification:

| Slice | n | 24b mean bps | t | Hit |
|---|---:|---:|---:|---:|
| BTC high-vol, any band side | 469 | 16.69 | 2.981 | 0.544 |
| BTC low-vol, any band side | 516 | 3.74 | 1.082 | 0.514 |
| ETH high-vol, any band side | 472 | 10.75 | 1.571 | 0.525 |
| ETH low-vol, any band side | 440 | 5.06 | 1.162 | 0.505 |

Edit made from this raw pass:

- Added `spec/search/single_event_band_break_high_vol_mean_reversion_btc_h24_v1.yaml`
- Added `spec/proposals/single_event_band_break_high_vol_mean_reversion_long_h24_btc_v1.yaml`
- Added `spec/discovery/raw_band_break_high_vol_v1`

This is a bounded BTC high-vol follow-up lane. It does not change global
`BAND_BREAK` thresholds or route high-vol band breaks into runtime policy.

Cell-discovery follow-up rejected the high-vol band-break cell:

- Run: `raw_band_break_highvol_cell_01`
- Scoreboard rows: 2 (`unconditional` baseline + `high_vol`)
- Rankable rows: 0
- High-vol cell forward net mean: `-2.726976 bps`
- High-vol cell robustness: `0.2032`
- Contrast lift: `4.847 bps`
- Blocked reason: `rejected_instability`

Conclusion: keep the proposal artifact as an audit trail, but do not advance
`BAND_BREAK / VOL_REGIME=high / long / 24b` from cell discovery without a new
mechanism or detector-side explanation.

### Funding Context State

Fixed `calculate_ms_funding_probabilities` saturation.

Before the fix, default 1 bps funding was treated as persistent in most of the
raw dataset because the softmax could select persistent/extreme even when the
hard state condition was not met.

New behavior:

- Persistent/extreme funding scores are gated by their hard flags.
- Persistence and extreme baselines use a `1.50x` multiplier over the rolling
  percentile floor.

Raw ready-row density after the fix:

| Symbol | Neutral | Persistent or Extreme | Extreme |
|---|---:|---:|---:|
| BTCUSDT | 87.15% | 12.85% | 7.36% |
| ETHUSDT | 85.27% | 14.73% | 6.19% |

The simple signed `carry_state` remains useful and was not replaced by
`ms_funding_state`.

### Funding Detector Runtime

Replaced funding acceleration-rank calls from the slow Python-loop
`percentile_rank_historical` path with equivalent
`rolling_percentile_rank(..., shift=0, scale=100.0)` calls.

This preserves the funding acceleration rank alignment used by the historical
helper while making full raw-history sweeps practical:

- BTC 2022-2024 `FundingExtremeOnsetDetector.prepare_features`
- Rows: `315,648`
- Runtime after patch: about `2.7s`

## Explicit Non-Changes

No global threshold change was made for these surfaces:

- `BAND_BREAK`: global z threshold stays `3.0`.
- OI detectors: `OI_SPIKE_POSITIVE`, `OI_SPIKE_NEGATIVE`, and `OI_FLUSH` were
  asset- or sign-unstable in raw sweeps.
- Funding detectors: `FUNDING_EXTREME_ONSET`, `FUNDING_PERSISTENCE_TRIGGER`,
  `FUNDING_NORMALIZATION_TRIGGER`, and `FUNDING_FLIP` did not clear a
  cross-asset/fold-stability bar for global threshold edits.
- `BREAKOUT_TRIGGER`: continuation evidence was weak on BTC and only mildly
  positive on ETH under stricter distance filters.
- Regime routing was not used to justify raw threshold changes. A separate
  governed cleanup later registered contextual `OI_SPIKE_POSITIVE` templates so
  `POSITIONING_EXPANSION` routing has executable support.

## Guardrails

- Treat the `CLIMAX_VOLUME_BAR` threshold change as raw BTC-led support for the
  bounded funding-negative exhaustion lane, not as deployment readiness.
- Do not use these raw sweeps to widen symbols, horizons, templates, or routing.
- Keep OI and funding detector threshold changes out until a bounded proposal
  shows stable cross-fold behavior.
- Keep future routing/template edits separate from raw detector calibration
  evidence, with regenerated sidecars and contract verification.
