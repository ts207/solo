# Operator Quickstart

This is the shortest current operator path in the repo. Use it before the longer runbook.

## Flow A: Trade An Existing Thesis

Bind a runtime config from an already promoted thesis bundle:

```bash
edge deploy bind-config --run_id <run_id>
```

Dry-check the generated config:

```bash
PYTHONPATH=. ./.venv/bin/python project/scripts/run_live_engine.py \
  --config project/configs/live_paper_<run_id>.yaml \
  --print_session_metadata
```

Launch paper runtime:

```bash
edge deploy paper-run --config project/configs/live_paper_<run_id>.yaml
```

## Flow B: Discover Then Trade

Run the canonical research lifecycle:

```bash
edge discover run --proposal <proposal.yaml>
edge validate run --run_id <run_id>
edge promote run --run_id <run_id> --symbols BTCUSDT
```

Export and bind the promoted thesis bundle:

```bash
edge deploy export --run_id <run_id>
edge deploy bind-config --run_id <run_id>
```

Dry-check and then paper-run:

```bash
PYTHONPATH=. ./.venv/bin/python project/scripts/run_live_engine.py \
  --config project/configs/live_paper_<run_id>.yaml \
  --print_session_metadata

edge deploy paper-run --config project/configs/live_paper_<run_id>.yaml
```

## Current Invariants

- `bind-config` emits exactly one thesis source: `thesis_run_id` by default, or `thesis_path` only when explicitly overridden.
- Runtime consumes only exported promoted thesis bundles.
- `paper-run` and `live-run` are the current deploy launch commands.
- `edge deploy paper` is not a current subcommand.
