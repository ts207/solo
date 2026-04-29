# Command Reference

Use `./.venv/bin/python` or `python3` in this checkout. Bare `python` may not exist in every shell.

## Install

Editable install:

```bash
pip install -e .
```

After install, the canonical command is:

```bash
edge --help
```

Without install:

```bash
PYTHONPATH=. ./.venv/bin/python -m project.cli --help
```

## Discover

```bash
edge discover plan --proposal spec/proposals/canonical_event_hypothesis.yaml
edge discover run --proposal spec/proposals/canonical_event_hypothesis.yaml
edge discover run --proposal spec/proposals/other.yaml --run_id <existing_run_id>
edge discover list-artifacts --run_id <run_id>
edge discover summarize --run_id <run_id>
edge discover explain-empty --run_id <run_id>
edge discover funnel --run_id <run_id>
```

Cell-first discovery:

```bash
edge discover cells coverage-audit
edge discover cells spec-audit --spec_dir <surface>
edge discover cells verify-data --run_id <run_id> --symbols BTCUSDT --start 2024-01-01 --end 2025-12-31
edge discover cells plan --run_id <run_id> --symbols BTCUSDT --start 2024-01-01 --end 2025-12-31
edge discover cells run --run_id <run_id> --symbols BTCUSDT --start 2024-01-01 --end 2025-12-31
edge discover cells summarize --run_id <run_id>
edge discover cells assemble-theses --run_id <run_id>
edge discover cells assemble-theses --run_id <run_id> --per-cell --limit 8
```

This lane compiles authored cell specs into canonical phase-2 execution and
returns generated proposal YAML for canonical handoff. It does not promote
scoreboard rows directly. `--per-cell` bypasses cluster representatives and
assembles from rankable scoreboard rows for second-tier sweeps.

Advanced internal trigger discovery:

```bash
edge discover triggers parameter-sweep --family vol_shock --symbol BTCUSDT
edge discover triggers feature-cluster --symbol BTCUSDT
edge discover triggers emit-registry-payload --proposal spec/proposals/canonical_event_hypothesis.yaml
```

These trigger commands are experimental proposal-generation lanes, not the canonical discovery path.

## Validate

```bash
edge validate run --run_id <run_id>
edge validate specs
edge validate forward-confirm \
  --run_id <run_id> \
  --window <start>/<end> \
  --proposal <proposal.yaml>
```

## Promote

```bash
edge promote run --run_id <run_id> --symbols BTCUSDT
edge promote run --run_id <run_id> --symbols BTCUSDT,ETHUSDT --retail_profile capital_constrained
edge promote export --run_id <run_id>
```

## Deploy

```bash
edge deploy export --run_id <run_id>
edge deploy bind-config --run_id <run_id>
edge deploy bind-config --run_id <run_id> --runtime_mode monitor_only
edge deploy bind-config --run_id <run_id> --runtime_mode simulation
edge deploy bind-config --run_id <run_id> --runtime_mode trading
edge deploy inspect --run_id <run_id>
edge deploy inspect-thesis --run_id <run_id>
edge deploy list-theses
edge deploy paper-run --config project/configs/live_paper_<run_id>.yaml
edge deploy live-run --config project/configs/live_trading_<run_id>.yaml
edge deploy status --run_id <run_id>
edge deploy status --run_id <run_id> --config project/configs/live_paper_<run_id>.yaml
```

`edge deploy paper` is not a current subcommand. Use `paper-run`.

Config naming convention:
- `monitor_only` → `project/configs/live_monitor_<run_id>.yaml`
- `simulation`   → `project/configs/live_paper_<run_id>.yaml`
- `trading`      → `project/configs/live_trading_<run_id>.yaml`

## Make Targets

Lifecycle:

```bash
make first-edge RUN_ID=<run_id> DATA_ROOT=<lake> START=<start> END=<end>
make discover RUN_ID=<run_id> START=<start> END=<end> [DATA_ROOT=...]
make discover-proposal PROPOSAL=spec/proposals/...yaml RUN_ID=<run_id>
make discover-doctor RUN_ID=<run_id> [DATA_ROOT=...]
make summarize RUN_ID=<run_id> [DATA_ROOT=...]
make summarize-proposal RUN_ID=<run_id> [DATA_ROOT=...] [TOP_K=10]
make explain-empty RUN_ID=<run_id> [DATA_ROOT=...]
make funnel RUN_ID=<run_id> [DATA_ROOT=...]
make forward-confirm RUN_ID=<run_id> WINDOW=<start>/<end> [DATA_ROOT=...] [PROPOSAL=...]
make validate RUN_ID=<run_id>
make promote RUN_ID=<run_id> SYMBOLS=BTCUSDT
make export RUN_ID=<run_id>
make bind-config RUN_ID=<run_id> RUNTIME_MODE=monitor_only
make bind-config RUN_ID=<run_id> RUNTIME_MODE=simulation
make bind-config RUN_ID=<run_id> RUNTIME_MODE=trading
make paper-run CONFIG=project/configs/live_paper_<run_id>.yaml
make live-run CONFIG=project/configs/live_trading_<run_id>.yaml
make deploy-status RUN_ID=<run_id> [CONFIG=project/configs/live_paper_<run_id>.yaml]
make list-theses [DATA_ROOT=...]
make benchmark-supported-path EXECUTE=0
make benchmark-supported-path EXECUTE=1 OFFLINE_PARQUET_EXECUTION_FIXED=1
```

Maintenance:

```bash
make check-hygiene
make governance
make minimum-green-gate
make agent-check
make check-protected-paths
make check-docs
make registries
make check-spec-sync
make check-registry-sync
make domain-graph
make check-domain-graph
```

## Direct Python Entrypoints

```bash
PYTHONPATH=. ./.venv/bin/python -m project.cli --help
PYTHONPATH=. ./.venv/bin/python -m project.cli discover --help
PYTHONPATH=. ./.venv/bin/python -m project.cli validate --help
PYTHONPATH=. ./.venv/bin/python -m project.cli promote --help
PYTHONPATH=. ./.venv/bin/python -m project.cli deploy --help
PYTHONPATH=. ./.venv/bin/python -m project.pipelines.run_all --help
PYTHONPATH=. ./.venv/bin/python project/scripts/run_live_engine.py --help
PYTHONPATH=. ./.venv/bin/python project/scripts/certify_paper_startup.py
```

## Proposal Helpers

Plugin wrappers:

```bash
./plugins/edge-agents/scripts/edge_preflight_proposal.sh spec/proposals/canonical_event_hypothesis.yaml
./plugins/edge-agents/scripts/edge_lint_proposal.sh spec/proposals/canonical_event_hypothesis.yaml
./plugins/edge-agents/scripts/edge_explain_proposal.sh spec/proposals/canonical_event_hypothesis.yaml
./plugins/edge-agents/scripts/edge_plan_proposal.sh spec/proposals/canonical_event_hypothesis.yaml
./plugins/edge-agents/scripts/edge_run_proposal.sh spec/proposals/canonical_event_hypothesis.yaml
```

Direct surfaces:

```bash
PYTHONPATH=. ./.venv/bin/python -m project.operator.preflight \
  --proposal spec/proposals/canonical_event_hypothesis.yaml \
  --registry_root project/configs/registries \
  --json_output data/reports/operator_preflight/canonical_event_hypothesis.json
```

## Verification

Fast structural checks:

```bash
PYTHONPATH=. ./.venv/bin/python project/scripts/spec_qa_linter.py
PYTHONPATH=. ./.venv/bin/python project/scripts/check_domain_graph_freshness.py
PYTHONPATH=. ./.venv/bin/python project/scripts/check_markdown_links.py
PYTHONPATH=. ./.venv/bin/python -m pytest -s -q project/tests/architecture
```

Contract wrapper:

```bash
./plugins/edge-agents/scripts/edge_validate_repo.sh contracts
```

Minimum green gate:

```bash
./plugins/edge-agents/scripts/edge_validate_repo.sh minimum-green
make minimum-green-gate
```

## Regeneration

After changing authored specs:

```bash
PYTHONPATH=. ./.venv/bin/python project/scripts/build_domain_graph.py
PYTHONPATH=. ./.venv/bin/python project/scripts/check_domain_graph_freshness.py
```

Broader artifact regeneration:

```bash
./project/scripts/regenerate_artifacts.sh
```

If thesis-overlap artifacts are needed:

```bash
THESIS_RUN_ID=<run_id> ./project/scripts/regenerate_artifacts.sh
```

## Documentation and Governance Refresh

```bash
PYTHONPATH=. ./.venv/bin/python project/scripts/refresh_docs_governance.py
PYTHONPATH=. ./.venv/bin/python project/scripts/build_repo_metrics.py
```

`refresh_docs_governance.py` refreshes repo metrics, the system map, contract strictness inventory, and detector governance artifacts under `docs/generated/`.
