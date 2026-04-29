# Operator Runbook

This runbook is the shortest safe path from a proposal to a paper runtime launch.

## Preconditions

Use the repo virtualenv (from the repo root):

```bash
PYTHONPATH=. ./.venv/bin/python -m project.cli --help
```

Verify spec and domain health:

```bash
PYTHONPATH=. ./.venv/bin/python project/scripts/spec_qa_linter.py
PYTHONPATH=. ./.venv/bin/python project/scripts/check_domain_graph_freshness.py
```

If the domain graph is stale:

```bash
PYTHONPATH=. ./.venv/bin/python project/scripts/build_domain_graph.py
PYTHONPATH=. ./.venv/bin/python project/scripts/check_domain_graph_freshness.py
```

Confirm the data root:

```bash
env | sort | rg '^(EDGE|BACKTEST)_' || true
```

No output means the repo-local `data/` directory is used.

## 1. Choose a Proposal

Use a cold-start proposal unless you intentionally depend on an existing baseline.

Good first example:

```text
spec/proposals/canonical_event_hypothesis.yaml
```

Bounded follow-on proposals with `bounded.baseline_run_id` require that baseline run to exist in the active data root.

## 2. Preflight the Proposal

```bash
./plugins/edge-agents/scripts/edge_preflight_proposal.sh \
  spec/proposals/canonical_event_hypothesis.yaml
```

Then lint and explain:

```bash
./plugins/edge-agents/scripts/edge_lint_proposal.sh \
  spec/proposals/canonical_event_hypothesis.yaml

./plugins/edge-agents/scripts/edge_explain_proposal.sh \
  spec/proposals/canonical_event_hypothesis.yaml
```

Stop if:

- proposal format is not structured hypothesis
- entry lag is below 1
- anchor type is unsupported
- event/template compatibility is invalid
- a bounded baseline is missing

## 3. Plan Discovery

```bash
edge discover plan --proposal spec/proposals/canonical_event_hypothesis.yaml
```

Record the returned `run_id`.

Inspect:

```text
data/artifacts/experiments/<program_id>/<run_id>/validated_plan.json
```

Stop if:

- `estimated_hypothesis_count` is 0
- required detectors/features/states are not expected
- the generated run-all command widened scope unexpectedly

## 4. Run Discovery

```bash
edge discover run --proposal spec/proposals/canonical_event_hypothesis.yaml
```

If reusing a run ID intentionally:

```bash
edge discover run \
  --proposal spec/proposals/other.yaml \
  --run_id <existing_run_id>
```

Stop if the run fails or if diagnostics show no usable evidence.

Inspect:

```text
data/reports/phase2/<run_id>/phase2_diagnostics.json
data/reports/phase2/<run_id>/phase2_candidates.parquet
data/artifacts/experiments/<program_id>/<run_id>/summary.json
```

## 5. Validate

```bash
edge validate run --run_id <run_id>
```

Required outputs:

```text
data/reports/validation/<run_id>/validation_bundle.json
data/reports/validation/<run_id>/promotion_ready_candidates.parquet
```

Stop if:

- validation bundle is missing
- promotion-ready candidates are missing
- all candidates are rejected or inconclusive and that was not expected

## 6. Promote

```bash
edge promote run --run_id <run_id> --symbols BTCUSDT
```

For multiple symbols:

```bash
edge promote run --run_id <run_id> --symbols BTCUSDT,ETHUSDT
```

Inspect:

```text
data/reports/promotions/<run_id>/promotion_diagnostics.json
data/reports/promotions/<run_id>/promoted_candidates.parquet
data/live/theses/<run_id>/promoted_theses.json
```

Stop if:

- promotion diagnostics contain `error`
- no promoted candidates exist and that was not expected
- evidence bundle count is inconsistent with promoted rows
- thesis export failed

## 7. Export or Re-Export Theses

Promotion usually exports theses automatically. To run export explicitly:

```bash
edge promote export --run_id <run_id>
```

or:

```bash
edge deploy export --run_id <run_id>
```

Inspect:

```bash
edge deploy inspect --run_id <run_id>
```

## 8. Bind Runtime Config

```bash
edge deploy bind-config --run_id <run_id>
```

This writes a config similar to:

```text
project/configs/live_paper_<run_id>.yaml
```

Confirm it has:

```yaml
strategy_runtime:
  implemented: true
  thesis_run_id: <run_id>
```

## 9. Paper Runtime Dry Inspection

Print runtime session metadata:

```bash
PYTHONPATH=. ./.venv/bin/python project/scripts/run_live_engine.py \
  --config project/configs/live_paper_<run_id>.yaml \
  --print_session_metadata
```

Run no-credential paper startup certification:

```bash
PYTHONPATH=. ./.venv/bin/python project/scripts/certify_paper_startup.py
```

## 10. Launch Paper Runtime

Set environment:

```bash
export EDGE_ENVIRONMENT=paper
export EDGE_VENUE=bybit
export EDGE_LIVE_CONFIG=project/configs/live_paper_<run_id>.yaml
export EDGE_LIVE_SNAPSHOT_PATH=artifacts/live_state_<run_id>.json
export EDGE_BYBIT_PAPER_API_KEY=<key>
export EDGE_BYBIT_PAPER_API_SECRET=<secret>
```

Launch:

```bash
edge deploy paper-run --config project/configs/live_paper_<run_id>.yaml
```

Do not use `edge deploy paper`; it is not a current deploy subcommand.

## 11. Monitor Status

```bash
edge deploy status \
  --run_id <run_id> \
  --config project/configs/live_paper_<run_id>.yaml
```

Inspect runtime metrics and alert paths from the bound config.

## Recovery Patterns

Zero hypotheses:

- inspect `validated_plan.json`
- check event/template compatibility
- check search limits
- check trigger filters

No candidates:

- inspect `phase2_diagnostics.json`
- check feature rows and event flag columns
- check sample-size and t-stat rejection counts

Validation produces no promotion-ready candidates:

- inspect `validation_bundle.json`
- inspect `rejection_reasons.parquet`
- confirm source candidate table was nonempty

Promotion blocked:

- confirm validation ran
- inspect `promotion_diagnostics.json`
- check source run mode
- check evidence bundles
- check detector governance downgrades

Runtime launch blocked:

- verify `promoted_theses.json`
- verify explicit thesis input in config
- run `--print_session_metadata`
- check required `EDGE_*` variables
- run deploy status

## Return Point for Context Switches

When handing off a run, record:

```text
proposal_path:
program_id:
run_id:
data_root:
validated_plan:
phase2_diagnostics:
validation_bundle:
promotion_diagnostics:
promoted_theses:
bound_config:
next_action:
```

This is enough to resume without re-deriving the run state from scratch.
