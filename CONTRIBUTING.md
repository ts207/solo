# Contributing

## Development setup

Install the package in editable mode:

```bash
pip install -e .
```

Use the repo virtualenv if present. The codebase assumes Python 3.11+.

## Working rules

1. Make changes in the canonical implementation path, not in deleted or deprecated wrappers.
2. Keep commands, contracts, specs, generated artifacts, and tests aligned.
3. Add or update regression coverage for behavior changes.
4. When behavior depends on specs or generated registries, update the source spec or generator first, then regenerate artifacts.

## Useful commands

```bash
# canonical lifecycle surfaces
edge discover plan --proposal spec/proposals/canonical_event_hypothesis_h24.yaml
edge validate run --run_id <run_id>
edge promote run --run_id <run_id> --symbols BTCUSDT
edge promote export --run_id <run_id>
edge deploy paper --run_id <run_id> --config <config.yaml>

# repo health
pytest -q
python -m ruff check .
python -m ruff format --check .
make minimum-green-gate
```

## Interface update rule

If you change commands, contracts, workflows, stage ownership, packaging semantics, artifact layouts, or architecture boundaries, update `README.md`, relevant package READMEs, and regression tests in the same change.


## Pre-commit and commit messages

Install hooks after cloning:

```bash
pip install -e ".[dev]"
pre-commit install
pre-commit install --hook-type commit-msg
```

Commits must follow Conventional Commits:

```text
fix(research): fail zero-feasible discovery runs
feat(promote): require deploy forward confirmation
ci: add minimum green gate workflow
```

Single-character, empty, or placeholder commit messages are rejected by `project/scripts/check_commit_msg.py`.

## Pull request standard

Every PR must include Summary, Test plan, Risk, and Rollback sections. User-visible workflow, artifact, or operator-contract changes must update the relevant docs and tests in the same change.
