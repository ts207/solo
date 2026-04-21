# Edge

Edge is a Python 3.11+ trading research and runtime repository organized around one supported lifecycle:

```text
discover -> validate -> promote -> export/bind-config -> deploy
```

Current repository scale:

- 1605 Python modules under `project/`
- 721 test files under `project/tests/`
- 409 YAML spec files under `spec/`

## Supported Operating Mode

Supported:

- offline/local-data research mode
- canonical discovery, validation, promotion, and thesis export
- governed runtime-core event detector
- portfolio decision engine
- docs/governance refresh and check path

Compatibility:

- legacy artifact readers in `project.artifacts.compat`
- explicit heuristic live detector mode with `legacy_heuristic_enabled=true`
- historical phase-2 adapters retained for old artifacts and smoke coverage

Experimental:

- trigger-mining and proposal-generation lanes under `edge discover triggers`

Deprecated:

- removed CLI aliases such as `strategy`, `pipeline run-all`, and `operator`
- implicit latest thesis selection
- downstream schema repair as part of canonical deploy

## Canonical Commands

```bash
PYTHONPATH=. ./.venv/bin/python -m project.cli --help

edge discover plan --proposal spec/proposals/canonical_event_hypothesis.yaml
edge discover run --proposal spec/proposals/canonical_event_hypothesis.yaml
edge validate run --run_id <run_id>
edge promote run --run_id <run_id> --symbols BTCUSDT
edge promote export --run_id <run_id>
edge deploy bind-config --run_id <run_id> --out_dir project/configs
edge deploy paper-run --config project/configs/live_paper_<run_id>.yaml
edge deploy live-run --config project/configs/live_live_<run_id>.yaml
```

## Current-Path Benchmark

Run the frozen current-path suite without any baseline comparison:

```bash
make benchmark-supported-path EXECUTE=0
make benchmark-supported-path EXECUTE=1 OFFLINE_PARQUET_EXECUTION_FIXED=1
```

The suite covers liquidation/exhaustion, liquidity vacuum, volatility shock, and funding dislocation. It reports current candidate counts, validation pass rates, promotion counts, thesis exports, runtime event counts, portfolio allocations, and artifact/contract failures.
`EXECUTE=1` is intentionally blocked unless the offline parquet execution gate is explicitly marked fixed.

## Documentation

Start with `docs/README.md`. Generated governance inventories under `docs/generated/` should be refreshed from code:

```bash
PYTHONPATH=. ./.venv/bin/python project/scripts/refresh_docs_governance.py --check
```
