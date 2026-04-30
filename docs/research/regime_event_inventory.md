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
