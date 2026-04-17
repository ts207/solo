# Spec authoring

## What lives in `spec/`

`spec/` is the authored source of truth for the domain model. Changes here drive the compiled domain graph (`spec/domain/domain_graph.yaml`) and downstream registries. After any spec change, run:

```bash
PYTHONPATH=. python3 project/scripts/build_domain_graph.py
make governance    # sync registries + audit
```

---

## Writing a proposal

Proposals are the input to `edge discover`. A proposal is a structured YAML file under `spec/proposals/`.

**Minimal example:**

```yaml
program_id: vol_spike_long_24b_rv70    # unique; names data/artifacts/experiments/<program_id>/
description: "VOL_SPIKE long mean_reversion 24b, rv>70, BTC 2023-2024"
run_mode: research
objective_name: retail_profitability
promotion_profile: research

symbols:
  - BTCUSDT
timeframe: 5m
start: "2023-01-01"
end: "2024-12-31"
instrument_classes:
  - crypto

search_spec:
  path: spec/productive_search.yaml

knobs:
  - name: discovery_profile
    value: synthetic

hypothesis:
  anchor:
    type: event
    event_id: VOL_SPIKE           # must exist in spec/events/
  filters:
    feature_predicates:
      - feature: rv_pct_17280     # 0-100 percentile scale
        operator: ">"
        threshold: 70
  sampling_policy:
    entry_lag_bars: 1
  template:
    id: mean_reversion            # must be compatible with event family — see below
  direction: long
  horizon_bars: 24
```

### Key fields

| Field | Notes |
|-------|-------|
| `program_id` | Names the experiment memory directory. Use descriptive slugs. |
| `run_mode` | `research` for discovery; `production` for live-facing runs |
| `promotion_profile` | `research` uses relaxed gates; `production` uses full gates |
| `search_spec.path` | `spec/productive_search.yaml` is the standard flat-search spec |
| `hypothesis.anchor.event_id` | Must exist in `spec/events/` and `spec/search_space.yaml` |
| `hypothesis.template.id` | Must be compatible with the event's canonical_regime family |
| `hypothesis.horizon_bars` | Overrides search_spec horizons for single-hypothesis runs |
| `filters.feature_predicates` | Applied at evaluation time — reduces qualifying events |

### Template compatibility

Incompatible template+event combinations are **silently dropped at plan time**. Always verify `estimated_hypothesis_count > 0` in `validated_plan.json`.

| Event family | Valid templates |
|---|---|
| VOLATILITY_EXPANSION / VOLATILITY_TRANSITION | `mean_reversion`, `continuation`, `impulse_continuation`, `vol_breakout`, `trend_continuation`, `pullback_entry` |
| TREND_FAILURE_EXHAUSTION / FORCED_FLOW_AND_EXHAUSTION | `exhaustion_reversal` |
| TREND_STRUCTURE | `exhaustion_reversal`, `mean_reversion`, `impulse_continuation` |

### `rv_pct_17280` threshold

This feature is on a 0–100 percentile scale (mean ≈ 46). A threshold of 70 means the 70th percentile of realized volatility — not 0.70. Use 70, not 0.70.

---

## Writing an event spec

Event specs live in `spec/events/<EVENT_NAME>.yaml`. Required fields:

```yaml
event_type: MY_EVENT
synthetic_coverage: covered
reports_dir: my_family_events          # determines where analyze_events writes output
events_file: my_family_events.parquet  # shared across events in the same family
signal_column: my_event_signal         # boolean column name

parameters:
  # detector-specific thresholds

research_family: MY_FAMILY
trigger: "..."
confirmation: "..."
expected_behavior:
  holding_horizon_bars: ...
  invalidation_conditions: [...]

cluster_id: my_cluster
identity:
  canonical_regime: VOLATILITY_EXPANSION  # determines template compatibility
  subtype: ...
  phase: ...
  evidence_mode: direct | hybrid | proxy
  layer: canonical
  disposition: keep | merge
  asset_scope: single_asset
  venue_scope: single_venue

governance:
  event_kind: market_event
  default_executable: true
  research_only: false
  tier: A | B | C
  operational_role: trigger | context
  deployment_disposition: primary | secondary_or_confirm
  runtime_category: active_runtime_event | passive_context_event

runtime:
  detector: MyEventDetector         # class name in project/events/detectors/
  enabled: true
  signal_column: my_event_signal
  events_file: my_family_events.parquet
  reports_dir: my_family_events
  instrument_classes: [crypto, futures]
  sequence_eligible: true
```

After creating a new event spec:
1. Write the detector class in `project/events/detectors/`
2. Register it in `project/configs/registries/detectors.yaml`
3. Add to `spec/search_space.yaml` (required for `--events` flag)
4. Sync `spec/events/event_registry_unified.yaml` and `project/configs/registries/events.yaml`
5. Rebuild domain graph
6. Run `make governance`

---

## Adding a filter template to an event

Filter templates (e.g. `only_if_regime`, `only_if_highvol`) narrow which events qualify for a hypothesis. Adding one requires a 3-file sync:

1. **`spec/templates/registry.yaml`** — add to the event's template list AND to the operator's `compatible_families`. Use `canonical_regime` (not `canonical_family`) as the key.
2. **`spec/templates/event_template_registry.yaml`** — mirror the same addition
3. **`spec/events/event_registry_unified.yaml`** — mirror the same addition
4. Rebuild domain graph

---

## The `spec/search_space.yaml` requirement

The `--events` flag on `project.pipelines.run_all` is a **filter on events already listed in `spec/search_space.yaml`**, not an addition. If an event is not in `search_space.yaml`, `--events MY_EVENT` silently produces no hypotheses. Add new events to `search_space.yaml` as part of event creation.

---

## The compiled domain graph

`spec/domain/domain_graph.yaml` is 11,500 lines of generated YAML. It is the read model consumed by:
- `project/domain/compiled_registry.py` → `get_domain_registry()` (LRU-cached)
- Research hypothesis expansion and feasibility checking
- Promotion service and live export

Do not edit it manually. Rebuild with:
```bash
PYTHONPATH=. python3 project/scripts/build_domain_graph.py
```
