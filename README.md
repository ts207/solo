# Edge

Edge is a Python 3.11+ trading research and runtime repository organized around a four-stage lifecycle: **discover → validate → promote → deploy**.

1. **Discover** — translate a structured proposal into a bounded experiment plan and run the research pipeline.
2. **Validate** — convert discovery outputs into validation bundles, rejection diagnostics, and promotion-ready tables.
3. **Promote** — package validated candidates into promoted theses and blueprint artifacts.
4. **Deploy** — run paper or live monitoring/trading against exported promoted theses.

Canonical model terms: **Anchor** (trigger specification), **Filter** (conditioning predicate), **Thesis** (promoted deployable unit).

This docset was rewritten from the current repository state, not from the previous markdown set. The repo currently contains:

- 1517 Python modules under `project/`
- 678 test files under `project/tests/`
- 463 YAML spec files under `spec/`
- canonical console entry points defined in `pyproject.toml`
- a contract-enforced package DAG in `project/tests/test_architectural_integrity.py`

## Start here

- `docs/README.md` — docset index and reading order
- `docs/lifecycle/overview.md` — system model, invariants, and top-level flow
- `docs/reference/repository_map.md` — package-by-package repo map
- `docs/reference/commands.md` — CLI commands, scripts, and Make targets
- `docs/operator/runbook.md` — end-to-end operator workflow from proposal to runtime

## Canonical commands

```bash
# Stage 1: cold-start plan or execute a structured proposal
edge discover plan --proposal spec/proposals/canonical_event_hypothesis.yaml
edge discover run  --proposal spec/proposals/canonical_event_hypothesis.yaml

# Stage 2: validate a run
edge validate run --run_id <run_id>

# Stage 3: promote and export live theses
edge promote run --run_id <run_id> --symbols BTCUSDT
edge promote export --run_id <run_id>

# Stage 4: inspect or launch runtime
edge deploy list-theses
edge deploy inspect-thesis --run_id <run_id>
edge deploy paper --run_id <run_id> --config project/configs/live_paper.yaml
```

`spec/proposals/canonical_event_hypothesis_h24.yaml` is a bounded follow-on proposal, not the best cold-start example. It requires proposal memory for its declared `baseline_run_id`.

`edge deploy paper|live --run_id <run_id>` uses `run_id` as a deployment gate and inspection target. The live engine still chooses its thesis source from `strategy_runtime.thesis_run_id` or `strategy_runtime.thesis_path` in the config file.

## Data root and environment

The runtime data root resolves in this order:

1. `EDGE_DATA_ROOT`
2. `BACKTEST_DATA_ROOT`
3. `<repo>/data`

This behavior comes from `project/core/config.py:get_data_root()`.

## Documentation policy

The authored docs under `docs/` are the canonical human-readable docset. The files under `docs/generated/` are generated inventories and audits that should be regenerated from code or specs instead of edited by hand.
