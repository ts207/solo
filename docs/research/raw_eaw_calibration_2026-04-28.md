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
| Old spec | BTC, funding-negative long | 82 | 14.37 | 0.72 | 0.488 |
| New spec | BTC, funding-negative long | 287 | 14.50 | 1.90 | 0.540 |

Year folds for the new BTC funding-negative slice were positive:

- 2022: `n=162`, mean `6.04`, t `0.596`
- 2023: `n=64`, mean `48.14`, t `2.663`
- 2024: `n=61`, mean `1.66`, t `0.123`

ETH did not independently justify this as a global ETH improvement; this is a
BTC-led calibration that keeps the phase-2 `CLIMAX_VOLUME_BAR + CARRY_STATE =
FUNDING_NEG` lane viable on raw data.

### BAND_BREAK

Fixed `BandBreakDetector.prepare_features` so runtime parameters such as
`band_z_threshold` and `lookback_window` override registry defaults.

Raw threshold sweep did not justify changing the global `BAND_BREAK` threshold:

- BTC high-vol improved at stricter z thresholds.
- ETH all-regime evidence was better around the current `3.0` threshold.
- Net conclusion: keep global `band_z_threshold: 3.0`.

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
