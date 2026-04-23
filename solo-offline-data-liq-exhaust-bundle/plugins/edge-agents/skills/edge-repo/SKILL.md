---
name: edge-repo
description: Use for general work inside the Edge repository when the user has not already narrowed the task to a specialist role. Orients Codex on the current lifecycle model, guardrails, command surfaces, and verification before branching into specialist work.
---

# Edge Repo

Use this as the default project skill for `/home/irene/Edge`.

## Read first

1. `CLAUDE.md`
2. `README.md`
3. `CONTRIBUTING.md`
4. `Makefile`

## Core model

- Edge is a governed event-driven crypto research-to-runtime platform.
- The canonical lifecycle is `discover -> validate -> promote -> deploy`.
- The operating unit is a bounded proposal that produces evidence; runtime consumes only exported promoted thesis packages.

## Hard guardrails

- Do not edit these surfaces without explicit human approval:
  - `spec/events/event_registry_unified.yaml`
  - `spec/events/regime_routing.yaml`
  - `project/contracts/pipeline_registry.py`
  - `project/contracts/schemas.py`
  - `project/engine/schema.py`
  - `project/research/experiment_engine_schema.py`
  - `project/strategy/dsl/schema.py`
  - `project/strategy/models/executable_strategy_spec.py`
- Do not widen symbols, regimes, templates, detectors, horizons, or date ranges without saying so explicitly.
- Do not treat discovery output as production readiness.
- Do not rescue weak claims by relaxing thresholds or cost assumptions.

## Default command surface

```bash
edge discover plan --proposal /abs/path/to/proposal.yaml
edge discover run --proposal /abs/path/to/proposal.yaml
edge validate run --run_id <run_id>
edge promote run --run_id <run_id> --symbols BTCUSDT
edge promote export --run_id <run_id>
edge deploy bind-config --run_id <run_id>
edge deploy paper --run_id <run_id> --config project/configs/live_paper_<run_id>.yaml
make discover PROPOSAL=/abs/path/to/proposal.yaml DISCOVER_ACTION=plan
make discover PROPOSAL=/abs/path/to/proposal.yaml DISCOVER_ACTION=run
make promote RUN_ID=<run_id> SYMBOLS=BTCUSDT
make export RUN_ID=<run_id>
./plugins/edge-agents/scripts/edge_preflight_proposal.sh /abs/path/to/proposal.yaml
./plugins/edge-agents/scripts/edge_lint_proposal.sh /abs/path/to/proposal.yaml
./plugins/edge-agents/scripts/edge_explain_proposal.sh /abs/path/to/proposal.yaml
./plugins/edge-agents/scripts/edge_validate_repo.sh contracts
```

## Routing

- If the task is repo maintenance, generated-artifact drift, validation routing, or plugin upkeep, use `edge-maintainer`.
- If the task touches `project/apps/chatgpt/`, use `edge-chatgpt-app-developer`.
- If the task is end-to-end research flow control, use `edge-coordinator`.
- If the task is diagnosing a completed run, use `edge-analyst`.
- If the next step is turning a diagnosis into a tighter bounded mechanism, use `edge-mechanism-hypothesis`.
- If a frozen mechanism must become proposal YAML and commands, use `edge-compiler`.

## Verification default

- After code, plugin, hook, or config changes, run `./plugins/edge-agents/scripts/edge_validate_repo.sh contracts`.
- For structural platform changes, run `./plugins/edge-agents/scripts/edge_validate_repo.sh minimum-green`.
- Keep verification targeted unless the repo contract requires a broader gate.
