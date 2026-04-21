# Offline Data Bundle: single_event_liq_exhaust_down_bounce_20260421T0521Z

This bundle contains the data needed to inspect and rerun the Phase 2 search for the downside liquidation exhaustion reversal campaign without network access.

Restore it from the repo root:

```bash
project/scripts/offline/restore_data_bundle.sh single_event_liq_exhaust_down_bounce_20260421T0521Z
```

The restore script concatenates `bundle.tar.gz.part-*`, verifies SHA-256 checksums from `manifest.json`, and extracts the archive into the repo root.

After restore, the main run artifacts are available at:

```text
data/runs/single_event_liq_exhaust_down_bounce_20260421T0521Z/
data/events/single_event_liq_exhaust_down_bounce_20260421T0521Z/
data/reports/phase2/single_event_liq_exhaust_down_bounce_20260421T0521Z/
data/lake/runs/single_event_liq_exhaust_down_bounce_20260421T0521Z/
```

To rerun only the offline Phase 2 search against the restored run-scoped lake:

```bash
.venv/bin/python -m project.research.phase2_search_engine \
  --run_id single_event_liq_exhaust_down_bounce_20260421T0521Z \
  --symbols BTCUSDT \
  --data_root data \
  --timeframe 5m \
  --search_spec spec/search/single_event_liquidation_exhaustion_reversal_bounce_v1.yaml \
  --experiment_config spec/campaigns/single_event_liq_exhaust_down_bounce_v1_experiment.yaml \
  --program_id single_event_liq_exhaustion_reversal_down_bounce_v1 \
  --registry_root project/configs/registries
```
