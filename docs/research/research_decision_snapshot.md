# Research Decision Snapshot

This report records the current mechanism-level research state after inventory,
data quality, regime scorecard, event-lift, and proposal-evidence gates.

Generate it with:

```bash
PYTHONPATH=. .venv/bin/python project/scripts/update_research_decision_snapshot.py \
  --data-root data
```

Outputs:

```text
data/reports/research_decision_snapshot/research_decision_snapshot.json
data/reports/research_decision_snapshot/research_decision_snapshot.md
```

## Inputs

- `data/reports/regime_baselines/regime_scorecard.json`
- latest `data/reports/data_quality_audit/*/mechanism_data_quality.json`
- `data/reports/event_lift/*/event_lift.json`
- `data/reports/regime_event_inventory/mechanism_inventory.json`

## Decision Meaning

For `funding_squeeze`, the current expected no-go shape is:

```text
data_quality_decision: paper_blocked
data_quality_blocked_fields: []
data_quality_proxy_fields: [basis_zscore]
regime_decision_summary: allow_event_lift=0, reject_directional=9
event_lift_passing_count: 0
proposal_allowed: false
paper_allowed: false
decision: park
```

This prevents drift back into normal funding event searches when the tested
regime surface is directionally negative and no passing event-lift evidence
exists.

## Valid Next Paths

- Define a new ex-ante regime matrix, not a new event.
- Repair or replace `basis_zscore` if papering `funding_squeeze` becomes
  relevant.
- Move to another mechanism only after its data-quality status is acceptable.
