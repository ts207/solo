# Discover stage

## CLI

```bash
edge discover plan --proposal spec/proposals/your_proposal.yaml          # plan only, no data
edge discover run  --proposal spec/proposals/your_proposal.yaml          # full run
edge discover run  --proposal ... --run_id <existing_run_id>             # reuse cached lake
edge discover list-artifacts --run_id <run_id>
```

---

## Code path

```
project/research/agent_io/issue_proposal.py        ← front door
  ├─ proposal_schema.py                             ← load + validate proposal
  ├─ operator/bounded.py                            ← bounded constraints check
  ├─ proposal_to_experiment.py                      ← translate to experiment config
  │    └─ experiment_engine.build_experiment_plan   ← expand hypotheses, check feasibility
  │         └─ experiment_engine_validators.py      ← check_hypothesis_feasibility per hyp
  └─ execute_proposal.py → project/pipelines/run_all.py  ← stage DAG orchestrator
```

`run_all.py` plans and executes the stage DAG:

```
ingest → build_cleaned_5m → build_features_5m → build_market_context_5m
  → analyze_events__<EVENT>_5m → build_event_registry
  → phase2_search_engine → summarize_discovery_quality
  → promote (if run_candidate_promotion=1) → finalize
```

---

## Writing a proposal

Canonical example: `spec/proposals/canonical_event_hypothesis.yaml`

```yaml
program_id: my_event_long_24b        # unique; names the experiment memory dir
description: "..."
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
  path: spec/productive_search.yaml  # controls which templates/horizons are generated
knobs:
  - name: discovery_profile
    value: synthetic
hypothesis:
  anchor:
    type: event
    event_id: VOL_SPIKE              # must exist in spec/events/
  filters:
    feature_predicates:
      - feature: rv_pct_17280        # 0-100 percentile scale; 70 = 70th pctile
        operator: ">"
        threshold: 70
  sampling_policy:
    entry_lag_bars: 1
  template:
    id: mean_reversion               # must be compatible with event family
  direction: long
  horizon_bars: 24
```

### Template-family compatibility

Proposals that specify an incompatible template produce `estimated_hypothesis_count: 0` in `validated_plan.json` — **no error, no warning visible in output**. Always verify this field before concluding an event has no signal.

| Event family | Valid templates |
|---|---|
| VOLATILITY_EXPANSION / VOLATILITY_TRANSITION | `mean_reversion`, `continuation`, `impulse_continuation`, `vol_breakout` |
| TREND_FAILURE_EXHAUSTION / FORCED_FLOW_AND_EXHAUSTION | `exhaustion_reversal` |
| TREND_STRUCTURE | `exhaustion_reversal`, `mean_reversion`, `impulse_continuation` |

Check compatibility programmatically:
```python
from project.research.search.feasibility import check_hypothesis_feasibility
from project.domain.hypotheses import HypothesisSpec, TriggerSpec
from project.domain.compiled_registry import get_domain_registry
r = check_hypothesis_feasibility(
    HypothesisSpec(trigger=TriggerSpec(trigger_type='event', event_id='VOL_SPIKE'),
                   template_id='mean_reversion', horizon='24b', direction='long', entry_lag=1),
    registry=get_domain_registry()
)
print(r.valid, r.reasons)
```

### `search_spec` and what it controls

`spec/productive_search.yaml` is the standard discovery search spec. It controls:
- which templates are enumerated (expression_templates list)
- horizons (if not overridden by `horizon_bars` in the proposal)
- discovery_search mode (flat vs hierarchical)

The proposal's `hypothesis.template.id` and `horizon_bars` fields override the search spec defaults for single-hypothesis runs.

---

## Key artifacts produced

| Artifact | Path | Notes |
|----------|------|-------|
| Validated plan | `data/artifacts/experiments/<prog>/<run_id>/validated_plan.json` | Check `estimated_hypothesis_count` |
| Evaluation results | `data/artifacts/experiments/<prog>/<run_id>/evaluation_results.parquet` | Primary result — t, rob, n, q, exp |
| Campaign summary | `data/artifacts/experiments/<prog>/campaign_summary.json` | High-level per-event aggregates |
| Phase2 hypotheses | `data/reports/phase2/<run_id>/hypotheses/BTCUSDT/evaluated_hypotheses.parquet` | Overwritten if multiple proposals share run_id |
| Phase2 diagnostics | `data/reports/phase2/<run_id>/phase2_diagnostics.json` | Gate funnel counts |
| Program memory | `data/artifacts/experiments/<prog>/memory/` | Belief state, ledger, event/template statistics |

---

## Reading results

```python
import pandas as pd
df = pd.read_parquet('data/artifacts/experiments/<program_id>/<run_id>/evaluation_results.parquet')
# Key columns: t_stat, robustness_score, n_events, q_value, after_cost_expectancy_per_trade,
#              is_discovery, gate_bridge_tradable, gate_oos_validation, gate_multiplicity
```

`after_cost_expectancy_per_trade` is stored as a fraction; multiply by 10000 for basis points.

**`not_executed_or_missing_data`** in `tested_ledger.parquet` is a campaign-controller reporting artifact — it does not mean the pipeline didn't run. Check `phase2_diagnostics.json` and the per-experiment `evaluation_results.parquet` directly.

---

## Reusing cached lake data

Passing `--run_id <existing_run_id>` tells the pipeline to use that run's already-built lake (cleaned bars + features). This skips the slow ingest/clean/features stages — useful when sweeping horizons or templates on the same data window.

When multiple proposals share a `run_id`, each run overwrites `data/reports/phase2/<run_id>/`. Use `data/artifacts/experiments/<program_id>/evaluation_results.parquet` for per-proposal results.

---

## Debugging a run that produced no results

1. Check `data/artifacts/experiments/<prog>/<run_id>/validated_plan.json` → `estimated_hypothesis_count`. If 0, template is incompatible — see table above.
2. Check `data/reports/phase2/<run_id>/phase2_diagnostics.json` → `gate_funnel` → `pass_min_sample_size`. If 0, the detector fired too few events.
3. Check `data/reports/phase2/<run_id>/phase2_diagnostics.json` → `rejected_invalid_metrics`. These are hypotheses where the detector fired 0 events under the feature filter.
4. Check `data/artifacts/experiments/<prog>/memory/event_statistics.parquet` for the event's `times_evaluated` count.
