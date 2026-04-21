# Lifecycle Overview

Edge converts bounded research proposals into deployable thesis packages through a governed lifecycle:

```text
discover -> validate -> promote -> export/bind-config -> deploy
```

The lifecycle is deliberately staged. A discovery result is evidence, not production readiness. Promotion requires validation artifacts. Runtime consumes only exported promoted thesis bundles.

## Stage Summary

Discover:

- Input: structured operator proposal YAML.
- Front door: `edge discover plan|run --proposal <path>`.
- Core modules: `project.research.agent_io.issue_proposal`, `project.research.agent_io.proposal_to_experiment`, `project.pipelines.run_all`.
- Primary output: phase-2 candidates plus experiment memory under the data root.

Validate:

- Input: completed discovery run ID.
- Front door: `edge validate run --run_id <run_id>`.
- Core module: `project.research.services.evaluation_service`.
- Primary output: validation bundle and `promotion_ready_candidates`.

Promote:

- Input: validated run ID.
- Front door: `edge promote run --run_id <run_id> --symbols BTCUSDT`.
- Core module: `project.research.services.promotion_service`.
- Primary output: promoted candidates, promotion audit, evidence bundles, and live thesis export.

Export and bind config:

- Input: promoted run ID.
- Front doors: `edge promote export`, `edge deploy export`, `edge deploy bind-config`.
- Core modules: `project.research.live_export`, `project.cli`.
- Primary output: `data/live/theses/<run_id>/promoted_theses.json` and a runtime config YAML.

Deploy:

- Input: explicit thesis bundle via `strategy_runtime.thesis_run_id` or `strategy_runtime.thesis_path`.
- Front doors: `edge deploy paper-run --config <config>` and `edge deploy live-run --config <config>`.
- Core modules: `project.scripts.run_live_engine`, `project.live.runner`, `project.live.thesis_store`.

## Invariants

- Structured proposals are the canonical operator input.
- Entry lag must be at least 1 bar to avoid same-bar leakage.
- Event and template compatibility is checked at planning time.
- Incompatible event/template hypotheses can be dropped before evaluation.
- Validation is mandatory before promotion.
- Promotion from exploratory discovery is blocked unless explicitly allowed by promotion policy.
- Runtime thesis loading is explicit. Implicit latest thesis resolution is disabled on the canonical path.
- Live thesis artifacts are schema checked and trust checked before use.
- The governed runtime-core event detector is the supported default. Heuristic detection is explicit legacy compatibility only.
- Historical phase-2 discovery surfaces are adapter-only; canonical discovery enters through structured proposals.
- Schema repair and compatibility artifact lookup are legacy-only and must not be part of canonical deploy.

## Operating Path Labels

Supported: offline/local-data research, canonical discovery/validation/promotion/export, governed runtime-core detection, portfolio decisioning, and docs/governance refresh/check.

Compatibility: legacy artifact readers, explicit heuristic detector mode, and historical phase-2 adapters.

Experimental: trigger-mining and proposal-generation lanes.

Deprecated: removed CLI aliases, implicit latest thesis selection, and downstream schema repair in deploy.

## Current Discovery Notes

- [Broad current-data event reflections](broad-current-data-event-reflections.md)
  records the 2023-2024 BTCUSDT event sweep and the resulting event-level
  follow-up decisions. The sweep supports a bounded BTCUSDT long funding
  continuation validation branch and rejects broad standalone expansion from
  the other event families under current artifacts.
- [Liquidation exhaustion matrix](liquidation-exhaustion-matrix.md) records the
  bounded `LIQUIDATION_EXHAUSTION_REVERSAL` matrix and its non-promotable
  outcome under current gates.

## Boundary Between Research and Runtime

Research stages can explore, reject, and package evidence. Runtime stages should not reinterpret discovery results. The boundary artifact is the promoted thesis bundle:

```text
data/live/theses/<run_id>/promoted_theses.json
```

The runtime decides only against exported theses and current live market/account state. It does not discover new hypotheses.

## Data Root

The canonical data root resolution in `project/core/config.py` is:

1. `EDGE_DATA_ROOT`
2. `BACKTEST_DATA_ROOT`
3. `<repo>/data`

Many pipeline subprocesses receive `BACKTEST_DATA_ROOT` from the proposal execution layer. When debugging artifact paths, always check the resolved data root first.

## Minimum Health Checks

Use targeted checks before trusting a run:

```bash
PYTHONPATH=. ./.venv/bin/python project/scripts/check_domain_graph_freshness.py
PYTHONPATH=. ./.venv/bin/python project/scripts/spec_qa_linter.py
PYTHONPATH=. ./.venv/bin/python -m pytest -s -q project/tests/architecture
```

Run the broader gate before landing structural platform changes:

```bash
make minimum-green-gate
```
