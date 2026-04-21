# Edge Documentation

This docset describes the current authored operator and developer model for Edge. It is intentionally smaller than the previous generated-heavy documentation tree: the files here are maintained by hand and should point back to code, specs, and commands for exact behavior.

Generated inventories under `docs/generated/` are not part of this replacement docset. If a future change needs generated audit artifacts, regenerate them from their owning scripts instead of editing them by hand.

## Reading Order

1. [Lifecycle Overview](lifecycle/overview.md)
2. [Discover](lifecycle/discover.md)
3. [Validate](lifecycle/validate.md)
4. [Promote](lifecycle/promote.md)
5. [Deploy](lifecycle/deploy.md)
6. [Liquidation Exhaustion Matrix](lifecycle/liquidation-exhaustion-matrix.md)
7. [Operator Runbook](operator/runbook.md)
8. [Supported Path](reference/supported-path.md)
9. [Architecture Reference](reference/architecture.md)
10. [Command Reference](reference/commands.md)
11. [Specs and Domain Reference](reference/specs-and-domain.md)
12. [Data and Artifacts Reference](reference/data-and-artifacts.md)

## Project Model

Edge is a governed event-driven crypto research-to-runtime platform. The lifecycle is:

```text
discover -> validate -> promote -> export/bind-config -> deploy
```

The operating unit is a bounded proposal that produces evidence. Runtime consumes only exported promoted thesis packages.

Canonical terms:

- Anchor: primary trigger specification.
- Filter: conditioning predicate applied around an anchor.
- Thesis: promoted deployable unit exported for runtime consumption.

## Source of Truth

- Authored specs live under `spec/`.
- The compiled domain graph is `spec/domain/domain_graph.yaml`.
- The command entry point is `project.cli:main`, exposed as `edge` after editable install.
- Pipeline orchestration lives in `project/pipelines/run_all.py`.
- Research policy and promotion logic live under `project/research/`.
- Runtime thesis loading and paper/live execution live under `project/live/` and `project/scripts/run_live_engine.py`.

Before relying on registry behavior, verify the compiled domain graph is fresh:

```bash
PYTHONPATH=. ./.venv/bin/python project/scripts/check_domain_graph_freshness.py
```

If stale, rebuild it:

```bash
PYTHONPATH=. ./.venv/bin/python project/scripts/build_domain_graph.py
```

## Current Canonical Commands

```bash
edge discover plan --proposal spec/proposals/canonical_event_hypothesis.yaml
edge discover run --proposal spec/proposals/canonical_event_hypothesis.yaml
edge validate run --run_id <run_id>
edge promote run --run_id <run_id> --symbols BTCUSDT
edge promote export --run_id <run_id>
edge deploy export --run_id <run_id>
edge deploy bind-config --run_id <run_id>
edge deploy paper-run --config project/configs/live_paper_<run_id>.yaml
edge deploy live-run --config project/configs/live_live_<run_id>.yaml
edge deploy status --run_id <run_id>
```

The current CLI deploy subcommands are `export`, `bind-config`, `inspect`, `paper-run`, `live-run`, and `status`. Use these names when writing automation or runbooks.

## Supported vs Legacy

The supported operating mode is offline/local-data research through canonical discovery, validation, promotion, export, governed runtime-core detection, portfolio decisioning, and docs/governance refresh/check.

Compatibility surfaces are legacy artifact readers, explicit heuristic detector mode, and historical phase-2 adapters. Trigger mining is experimental. Removed aliases, implicit latest thesis selection, and downstream schema repair are deprecated.

Run the current-path stabilization suite without baseline comparison:

```bash
make benchmark-supported-path EXECUTE=0
make benchmark-supported-path EXECUTE=1 OFFLINE_PARQUET_EXECUTION_FIXED=1
```

`EXECUTE=1` is blocked until the offline parquet execution gate is repaired.

## Documentation Update Rule

Update this docset when changing:

- CLI commands or Make targets.
- Proposal schema or lifecycle semantics.
- Stage ownership or artifact paths.
- Validation or promotion prerequisites.
- Thesis export, live thesis loading, or deployment gates.
- Package architecture or guarded contract boundaries.

Keep generated artifacts separate from authored docs.


## Generated Inventories

Generated governance inventories live under `docs/generated/`. Refresh them from code rather than editing them by hand:

```bash
PYTHONPATH=. ./.venv/bin/python project/scripts/refresh_docs_governance.py
PYTHONPATH=. ./.venv/bin/python project/scripts/refresh_docs_governance.py --check
```

Important generated files include:
- `docs/generated/repo_metrics.md`
- `docs/generated/system_map.md`
- `docs/generated/contract_strictness_inventory.md`
- `docs/generated/detector_governance_summary.json`
