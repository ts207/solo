# Supported Path

This repository declares one supported operating path. Other surfaces remain only when they serve compatibility, experimental exploration, or historical artifact access.

## Supported

- Offline/local-data research mode.
- Canonical discovery, validation, promotion, and export:

```text
edge discover plan|run -> edge validate run -> edge promote run -> edge promote export
```

- Governed runtime-core detector path:

```text
strategy_runtime.event_detector.adapter = governed_runtime_core
```

- Portfolio decision-engine allocation path in `project.portfolio.engine`.
- Cell-first discovery when it hands off through canonical proposal artifacts:

```text
edge discover cells run -> edge discover cells assemble-theses
-> edge discover plan|run -> edge validate run -> edge promote run
```

- Docs/governance refresh and check path:

```bash
PYTHONPATH=. ./.venv/bin/python project/scripts/refresh_docs_governance.py
PYTHONPATH=. ./.venv/bin/python project/scripts/refresh_docs_governance.py --check
```

## Compatibility

- Legacy artifact readers in `project.artifacts.compat`, only when callers pass
  an explicit compatibility switch such as `allow_legacy=True`.
- Explicit heuristic live detector mode. It requires `legacy_heuristic_enabled=true`; it is not a default.
- Historical phase-2 adapter surfaces retained for old artifacts and narrow smoke coverage.

## Experimental

- Trigger-mining and proposal-generation lanes:

```bash
edge discover triggers parameter-sweep --family vol_shock --symbol BTCUSDT
edge discover triggers feature-cluster --symbol BTCUSDT
edge discover triggers emit-registry-payload --proposal <proposal.yaml>
```

These lanes can emit proposal material, but they are not the canonical discovery path.

Cell-first discovery is research-first rather than directly deployable. Its
scoreboard and cluster artifacts are not runtime instructions; only generated
proposal YAML files that re-enter the canonical lifecycle are supported.

## Deprecated

- Removed CLI aliases: `strategy`, `pipeline run-all`, `operator`.
- Implicit latest thesis selection in runtime configs.
- Downstream schema repair or compatibility artifact lookup as part of canonical deploy.

## Stabilization Gate

Use the current-path benchmark as the stabilization check before claiming the supported path is healthy:

```bash
make benchmark-supported-path EXECUTE=0
make benchmark-supported-path EXECUTE=1 OFFLINE_PARQUET_EXECUTION_FIXED=1
```

The benchmark runs only current `HEAD`; it performs no old-vs-current comparison.
`EXECUTE=1` is blocked until the offline parquet execution gate is repaired.
