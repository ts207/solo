# Event Incremental Lift

`project/research/event_lift.py` tests whether an event adds value over a viable,
ex-ante regime baseline. It is deliberately gated by the regime scorecard before
normal computation can write artifacts.

## Gate

Normal runs require a scorecard row with:

```text
decision == allow_event_lift
```

If the regime is not eligible, `project/scripts/run_event_lift.py` exits non-zero
before creating `data/reports/event_lift/<run_id>/`.

Expected fail-closed shape:

```text
fail: regime_id=<regime_id> is not eligible for event lift; scorecard decision=<decision>
```

## Audit Mode

`--allow-nonviable-regime-audit` permits control inspection for rejected,
insufficient, or otherwise nonviable regimes. Audit outputs are non-promotable:

```json
{
  "audit_only": true,
  "promotion_eligible": false,
  "classification": "audit_only",
  "decision": "audit_only"
}
```

Audit artifacts must not be consumed as proposal evidence.

## Controls

The engine emits:

- `unconditional_all`
- `regime_only_all`
- `regime_only_matched_non_event`
- `event_only`
- `event_plus_regime`
- `opposite_direction`
- `entry_lag_0`
- `entry_lag_1`
- `entry_lag_2`
- `entry_lag_3`

The matched regime-only control uses the same symbol, same regime, same year
distribution as `event_plus_regime`, same direction, same horizon, overlap
suppression, and excludes event timestamps plus the event cooldown window.

## Outputs

```text
data/reports/event_lift/<run_id>/event_lift.json
data/reports/event_lift/<run_id>/event_lift.parquet
data/reports/event_lift/<run_id>/event_lift.md
```

## Promotion Boundary

Only rows with both conditions below may feed proposal compilation:

```text
decision == advance_to_mechanism_proposal
promotion_eligible == true
```
