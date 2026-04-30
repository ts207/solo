# Regime Baselines

`project/scripts/run_regime_baselines.py` measures whether simple, ex-ante market
states have stable post-cost forward behavior before event-specific research is
allowed.

The first matrix is `core_v1`:

- `vol_regime=high`
- `vol_regime=low`
- `carry_state=funding_neg`
- `carry_state=funding_pos`
- `vol_regime=high+carry_state=funding_neg`
- `vol_regime=high+carry_state=funding_pos`
- `ms_trend_state=bullish`
- `ms_trend_state=bearish`
- `ms_trend_state=chop`

The funding-squeeze positioning matrix is `funding_squeeze_positioning_v1`. It
tests one ex-ante structural thesis: negative funding pressure may only matter
when positioning is actively expanding into downside/high-vol pressure, rather
than when the market is merely already in negative carry.

Rows:

- primary, proposal-path eligible:
  `carry_state=funding_neg+vol_regime=high+oi_phase=expansion+price_oi_quadrant=price_down_oi_up`
- diagnostic only:
  `funding_phase=negative_persistent+oi_phase=expansion`
- diagnostic only:
  `funding_phase=negative_onset+oi_phase=expansion`
- diagnostic only:
  `funding_regime=crowded+oi_phase=expansion`
- diagnostic only:
  `carry_state=funding_neg+oi_phase=expansion+ms_trend_state=bearish`

Only the primary row can open the proposal/event-lift path. Diagnostic rows are
recorded for interpretation but cannot produce an `allow_event_lift` scorecard
decision.

The engine uses registry-native lowercase filters only and excludes labels that
could imply future confirmation, including `post_*`, `normalized`, `cooldown`,
`refill`, `recovered`, and `rebound_confirmed`.

Run:

```bash
RUN_ID=regime_baselines_core_$(date -u +%Y%m%dT%H%M%SZ)

PYTHONPATH=. .venv/bin/python project/scripts/run_regime_baselines.py \
  --run-id "$RUN_ID" \
  --matrix-id core_v1 \
  --symbols BTCUSDT,ETHUSDT \
  --horizons 12,24,48 \
  --data-root data
```

Funding positioning run:

```bash
RUN_ID=regime_baselines_funding_positioning_$(date -u +%Y%m%dT%H%M%SZ)

PYTHONPATH=. .venv/bin/python project/scripts/run_regime_baselines.py \
  --run-id "$RUN_ID" \
  --matrix-id funding_squeeze_positioning_v1 \
  --symbols BTCUSDT,ETHUSDT \
  --horizons 24 \
  --data-root data
```

Outputs:

- `data/reports/regime_baselines/<run_id>/regime_baselines.json`
- `data/reports/regime_baselines/<run_id>/regime_baselines.parquet`
- `data/reports/regime_baselines/<run_id>/regime_baselines.md`
- `data/reports/regime_baselines/<run_id>/regime_search_burden.json`

The script auto-selects the most complete available market-context lake run for
the requested symbols. Rows are emitted for the full predeclared matrix even when
support is missing; those rows are classified as `insufficient_support` with an
explicit `reason`.

Initial decisions:

- `stable_positive` -> `advance_to_event_lift`
- `year_conditional` -> `park`
- `unstable` -> `monitor`
- `negative` -> `reject`
- `insufficient_support` -> `data_repair`
