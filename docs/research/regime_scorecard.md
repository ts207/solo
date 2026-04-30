# Regime Scorecard

`project/scripts/update_regime_scorecard.py` aggregates completed regime baseline
runs into regime-level decisions. It does not recompute returns.

Input:

- `data/reports/regime_baselines/*/regime_baselines.parquet`

Ignored as inputs:

- `data/reports/regime_baselines/regime_scorecard.parquet`
- `data/reports/regime_baselines/regime_scorecard.json`

Default behavior selects the latest baseline run per `matrix_id`. Use
`--all-runs` to aggregate every available baseline run, or `--run-id` /
`--matrix-id` to scope the input.

Run:

```bash
PYTHONPATH=. .venv/bin/python project/scripts/update_regime_scorecard.py \
  --data-root data
```

Outputs:

- `data/reports/regime_baselines/regime_scorecard.json`
- `data/reports/regime_baselines/regime_scorecard.parquet`
- `data/reports/regime_baselines/regime_scorecard.md`

Decision propagation is strict:

- Any `stable_positive` row allows event lift.
- Otherwise, any `year_conditional` row requires an ex-ante variant.
- Otherwise, any `unstable` row is monitor-only.
- A regime is `negative` only when all measured rows are sufficiently supported
  and negative.
- Any remaining support failure becomes `insufficient_support`.

`event_lift.py` should only consume regimes whose scorecard decision is
`allow_event_lift`.
