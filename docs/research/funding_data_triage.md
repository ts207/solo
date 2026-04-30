# Funding Data Triage

Funding fields can look stale in a bar-level audit because exchange funding
updates are sparse, usually every eight hours, while market-context rows are
materialized at the bar timeframe. Wave 8 therefore needs a focused diagnostic
before treating high repeated-value ratios as a true data blocker.

This triage is read-only. It does not modify the data-quality audit, proposal
compiler, event-lift gate, or source lake.

## Run

```bash
RUN_ID=funding_data_triage_$(date -u +%Y%m%dT%H%M%SZ)

PYTHONPATH=. .venv/bin/python project/scripts/run_funding_data_triage.py \
  --run-id "$RUN_ID" \
  --symbols BTCUSDT,ETHUSDT \
  --data-root data
```

Optional arguments:

```text
--source-run-id <run_id>
--timeframe 5m
--output-root data/reports/funding_data_triage
```

Outputs:

```text
data/reports/funding_data_triage/<run_id>/funding_data_triage.json
data/reports/funding_data_triage/<run_id>/funding_data_triage.parquet
data/reports/funding_data_triage/<run_id>/funding_data_triage.md
```

## Questions Answered

- Are `funding_rate_scaled` and `funding_abs_pct` absent, truly stale, or
  stepwise because funding updates every eight hours?
- Are observed update timestamps aligned to expected funding intervals?
- Does forward-filled funding create false staleness under repeated-value rules?
- Which funding-like fields exist in market context and source funding parquet?
- Can `funding_abs_pct` be recomputed from `funding_rate_scaled`?
- Are requested symbols affected equally?

## Classifications

- `valid_stepwise`: funding changes roughly on the expected cadence; raw stale
  ratio is high because sparse funding values are forward-filled into bar rows.
- `true_stale`: last meaningful update is materially older than the dataset end
  or update gaps exceed the funding cadence tolerance.
- `missing`: source field is absent or has zero coverage.
- `recomputable`: `funding_abs_pct` is missing/stale/invalid but
  `funding_rate_scaled` is valid stepwise.
- `invalid`: malformed, constant across multi-month history, or inconsistent
  with expected cadence.

## Decision Boundary

If funding fields are `valid_stepwise`, patch
`project/research/data_quality_audit.py` to use cadence-aware stale checks for
funding fields, then rerun the data-quality audit and regime baselines.

If funding fields are `true_stale` or `invalid`, repair funding ingestion or
market-context materialization before funding research.

If funding fields are repaired and regimes remain negative, keep
`funding_squeeze` parked despite clean data.
