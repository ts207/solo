# Data Quality Audit

Wave 8 adds a read-only audit layer for mechanism observables. Its purpose is to
identify synthetic, missing, stale, proxy, or insufficient-history market-context
fields before a mechanism advances toward paper or active proposal status.

The audit reads market-context feature parquet from:

```text
data/lake/runs/<source_run_id>/features/perp/<symbol>/<timeframe>/market_context/**/*.parquet
```

If `--source-run-id` is omitted, the CLI uses the same market-context source run
selection as regime baselines: choose the latest run with market-context parquet
coverage for the requested symbols and timeframe.

## Run

```bash
RUN_ID=data_quality_audit_$(date -u +%Y%m%dT%H%M%SZ)

PYTHONPATH=. .venv/bin/python project/scripts/run_data_quality_audit.py \
  --run-id "$RUN_ID" \
  --symbols BTCUSDT,ETHUSDT \
  --data-root data
```

Optional arguments:

```text
--source-run-id <run_id>
--timeframe 5m
--output-root data/reports/data_quality_audit
```

Outputs are written under:

```text
data/reports/data_quality_audit/<run_id>/
```

Required artifacts:

- `data_quality_audit.json`
- `data_quality_audit.parquet`
- `data_quality_audit.md`
- `mechanism_data_quality.json`

## Field Classifications

The Wave 8 field set is intentionally fixed by `FIELD_EXPECTATIONS` in
`project/research/data_quality_audit.py`:

```text
funding_rate_scaled, funding_abs_pct, oi_notional, oi_delta_1h, rv_96,
rv_percentile_24h, spread_bps, slippage_bps, market_depth, basis_zscore,
liquidation_notional, volume, order_book
```

Deterministic classifications:

- `missing`: field absent, no rows, or zero coverage.
- `insufficient_history`: less than 180 days of field history or fewer than
  1000 non-null rows.
- `synthetic`: synthetic/default-filled source markers, implausibly low
  distinct count for continuous fields, or constant/default-filled values over
  at least 95% of non-null rows.
- `stale`: repeated unchanged values above threshold or field updates materially
  stop before the dataset end. Funding fields use cadence-aware stale checks
  because eight-hour funding values are expected to repeat across 5m rows.
- `proxy`: registry, inventory, or expectation metadata marks the field as proxy
  or derived approximation.
- `real`: coverage at least 80%, history at least 180 days, stale ratio below
  20%, and not synthetic. For cadence-aware funding fields, high raw stale ratio
  is allowed when value-change timestamps remain consistent with expected
  funding cadence.

`liquidation_notional` is event-sparse and may legitimately be zero-heavy. A
high zero ratio alone does not make it synthetic.

## Funding Cadence

`funding_rate_scaled` and `funding_abs_pct` are `stepwise_cadence` fields. The
audit records extra row metadata so readers can see why repeated bar values did
or did not block the field:

- `cadence_aware`
- `expected_update_gap_hours`
- `median_update_gap_hours`
- `p95_update_gap_hours`
- `last_update_gap_hours`
- `funding_adjusted_stale_ratio`
- `valid_stepwise_cadence`

When `funding_event_ts` is present, the audit uses that materialized source
timestamp as the update clock. Otherwise it falls back to timestamps where the
field value changes. This avoids treating repeated equal funding prints as
missing updates.

A funding field is treated as cadence-valid when:

- median update gap is at most `expected_update_gap_hours * 1.5`
- p95 update gap is at most `expected_update_gap_hours * 2.5`
- last update gap is at most `expected_update_gap_hours * 2.5`

When these cadence checks pass, the field can classify as `real` despite a high
raw repeated-value `stale_ratio`.

## Mechanism Decisions

Wave 8 uses a conservative hard-coded mechanism observable mapping. Later waves
can derive this from mechanism specs and state/event `features_required`.

Decision rules:

- `data_blocked`: any active mechanism required observable is missing, stale,
  synthetic, or insufficient-history.
- `paper_blocked`: required observables are present, but one or more are proxy.
- `research_allowed`: all required observables are real.
- `draft_only`: mechanism is not active.

This audit is not yet wired into proposal compilation or promotion. It is the
read-only control surface for deciding whether the next step should be data
repair, research continuation, or mechanism stop/park.
