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
```

Advanced internal trigger discovery:

```bash
edge discover triggers parameter-sweep --family vol_shock --symbol BTCUSDT
edge discover triggers feature-cluster --symbol BTCUSDT
edge discover triggers emit-registry-payload --family vol_shock --symbol BTCUSDT
```

## Validate

```bash
edge validate run --run_id <run_id>
edge validate specs
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
edge deploy inspect --run_id <run_id>
edge deploy inspect --run_id <run_id> --config project/configs/live_paper_<run_id>.yaml
edge deploy paper-run --config project/configs/live_paper_<run_id>.yaml
edge deploy live-run --config project/configs/live_live_<run_id>.yaml
edge deploy status --run_id <run_id>
edge deploy status --run_id <run_id> --config project/configs/live_paper_<run_id>.yaml
```

`edge deploy paper` is not a current subcommand. Use `paper-run`.

## Make Targets

Lifecycle:

```bash
make discover PROPOSAL=spec/proposals/canonical_event_hypothesis.yaml DISCOVER_ACTION=plan
make discover PROPOSAL=spec/proposals/canonical_event_hypothesis.yaml DISCOVER_ACTION=run
make validate RUN_ID=<run_id>
make promote RUN_ID=<run_id> SYMBOLS=BTCUSDT
make export RUN_ID=<run_id>
```

Maintenance:

```bash
make test
make test-fast
make lint
make format-check
make format
make style
make governance
make minimum-green-gate
make check-hygiene
```

Known drift:

```text
make deploy-paper
```

currently expands to an unsupported `edge deploy paper` command. Prefer `edge deploy bind-config` plus `edge deploy paper-run` until that target is corrected.

## Direct Python Entrypoints

```bash
PYTHONPATH=. ./.venv/bin/python -m project.cli --help
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

## ChatGPT App Surface

```bash
edge-chatgpt-app backlog
edge-chatgpt-app blueprint
edge-chatgpt-app widget
edge-chatgpt-app tools
edge-chatgpt-app status
edge-chatgpt-app serve --host 127.0.0.1 --port 8000 --path /mcp
```

The app surface should wrap proposal, dashboard, reporting, and operator actions. It should not redefine proposal policy or promotion logic.
