# Research Concept Map

This file maps the operator vocabulary to code, specs, and artifacts.

| Concept | Primary location |
|---|---|
| Detector implementations | `project/events/detectors/**` |
| Detector registries | `project/events/registries/**`, `project/events/registry.py`, `project/configs/registries/detectors.yaml` |
| Event specs | `spec/events/*.yaml`, `project/configs/registries/events.yaml` |
| State/regime specs | `spec/states/*.yaml`, `project/configs/registries/states.yaml` |
| Context guards | `project/features/context_guards.py` |
| Runtime context builder | `project/live/context_builder.py` |
| Runtime trade context contract | `project/live/contracts/live_trade_context.py` |
| Proposal schema | `project/research/agent_io/proposal_schema.py` |
| Proposal files | `spec/proposals/**/*.yaml` |
| Discovery reports | `data/reports/phase2/<run_id>/` |
| Experiment-scoped artifacts | `data/artifacts/experiments/<program_id>/<run_id>/` |
| Validation reports | `data/reports/validation/<run_id>/` |
| Promotion reports | `data/reports/promotions/<run_id>/` |
| Live thesis artifact | `data/live/theses/<run_id>/promoted_theses.json` |
| Runtime state and metrics | `artifacts/live_state_<run_id>.json`, `artifacts/live_runtime_metrics_<run_id>.json` |

## Operator source of truth

When names conflict, use the operator vocabulary in `docs/operator/research-vocabulary.md`.

Known overloaded terms include anchor, trigger, event, family, regime, state, context, and filter. Treat them as implementation details unless the operator doc says otherwise.

## Lifecycle path

```text
proposal -> discovery -> discover-doctor -> validation -> forward confirmation -> promotion -> paper -> live-gated runtime
```

Runtime must consume promoted thesis artifacts, not raw discovery outputs.
