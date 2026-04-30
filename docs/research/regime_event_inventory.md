# Regime-Event Inventory

`project/scripts/update_regime_event_inventory.py` builds a canonical inventory for
the regime-first discovery funnel.

The inventory joins:

- authoritative events from `spec/events/event_registry_unified.yaml`
- context dimensions from `spec/contexts/context_dimension_registry.yaml`
- materializable state dimensions from `spec/states/state_registry.yaml`
- mechanism specs from `spec/mechanisms/registry.yaml`
- Python detector registrations from `project/events/registries/*.py`
- historical search counts from `data/reports/search_ledger/search_burden.json`

Outputs are written to `data/reports/regime_event_inventory/`:

- `context_dimensions.json`
- `state_inventory.json`
- `event_inventory.json`
- `mechanism_inventory.json`
- `regime_event_inventory.parquet`

Use:

```bash
PYTHONPATH=. .venv/bin/python project/scripts/update_regime_event_inventory.py
```

This layer is intentionally registry-first. An event that appears in a mechanism
spec but not in `event_registry_unified.yaml` is emitted as `invalid_unregistered`
and must not advance to active proposal compilation.

Mechanism event-role fields distinguish active candidates from conditional or
draft references:

- `active_candidate_event`
- `conditional_registered_event`
- `draft_event`
- `active_invalid_event_count`
- `conditional_maybe_not_materialized_event_count`

For example, `funding_squeeze` may retain `FUNDING_EXTREME` in `draft_events`
for lineage, but the inventory must keep `active_candidate_event=false` for that
unregistered token and active proposal compilation must remain blocked until
baseline and event-lift evidence exist.
