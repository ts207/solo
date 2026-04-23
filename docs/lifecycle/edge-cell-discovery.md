# Edge Cell Discovery

Edge cell discovery is a research-first lane for ranking payoff concentrations before
generating canonical proposals.

The lane is intentionally compile-to-phase2:

```text
discovery specs -> generated search spec -> phase2 search engine -> scoreboard
-> redundancy representatives -> generated proposals -> discover/validate/promote
```

It does not route through `project.research.candidate_discovery` or
`project.research.cli.candidate_discovery_cli`.

## Commands

```bash
edge discover cells verify-data --run_id <run_id> --symbols BTCUSDT --start 2024-01-01 --end 2025-12-31
edge discover cells plan --run_id <run_id> --symbols BTCUSDT --start 2024-01-01 --end 2025-12-31
edge discover cells run --run_id <run_id> --symbols BTCUSDT --start 2024-01-01 --end 2025-12-31
edge discover cells summarize --run_id <run_id>
edge discover cells assemble-theses --run_id <run_id>
```

`run` fails before phase-2 execution when the data contract leaves no feasible
cell surface.
`assemble-theses` never promotes directly. It writes proposal YAML files that must
go back through canonical lifecycle commands.

## Authored Specs

The authored search surface lives under `spec/discovery/`:

- `event_atoms.yaml`
- `context_cells.yaml`
- `horizons.yaml`
- `contrast_rules.yaml`
- `ranking_policy.yaml`

The compiler writes generated artifacts under:

```text
data/reports/phase2/<run_id>/generated/
```

## Artifacts

The lane writes these phase-2 run directory artifacts:

- `edge_cell_data_contract.json`
- `generated/generated_edge_cell_search_space.yaml`
- `generated/generated_edge_cell_experiment.json`
- `generated/edge_cell_lineage.parquet`
- `phase2_candidate_universe.parquet`
- `edge_cells_raw.parquet`
- `edge_cells_contrast.parquet`
- `edge_scoreboard.parquet`
- `edge_scoreboard_summary.json`
- `edge_cell_pnl_traces.parquet` when PnL traces are available
- `edge_cell_trigger_traces.parquet` when trigger traces are available
- `edge_clusters.parquet`
- `edge_cluster_representatives.parquet`
- `generated_proposals/*.yaml`
- `thesis_assembly_report.json`

## Ranking Policy

Rows only rank when they clear forward, support, and contrast gates. Positive
expectancy alone is not enough. Unconditional rows can serve as contrast baselines
but are not scored as winners.

The rank score uses:

- forward confirmation
- post-cost expectancy
- fold stability
- complement lift
- context simplicity

## Redundancy

Redundancy starts with the existing candidate diversification service. When
`edge_cell_pnl_traces.parquet` or `edge_cell_trigger_traces.parquet` exists, the
cluster pass also merges candidates with high PnL correlation or trigger overlap.
Cluster artifacts include `cluster_basis`, `max_pnl_similarity`, and
`max_trigger_overlap` so thesis assembly can consume representatives selected with
both structural and behavioral evidence where traces are available.

## Runtime Boundary

Research-only and supportive-only contexts can appear in the scoreboard, but they
do not become deployable proposals by default. Thesis assembly checks condition
routing for runtime contexts, downgrades mapped supportive-only contexts into
proposal metadata, and rejects unmapped or non-executable research contexts.
Generated proposals inherit the source cell run's start, end, timeframe, and
generated search-spec path instead of introducing a new date or search surface.

## Promotion Governance

Cell-origin theses still pass through canonical discover, validate, and promote.
Promotion adds a cell-origin gate on top of existing bundle policy: a row with
`source_discovery_mode=edge_cells` must be a cluster representative, have forward
confirmation, have contrast confirmation, carry an explicit runtime or supportive
mapping, and stay within the accepted context-complexity penalty. Missing any of
those fields keeps the thesis out of promotion even if a later bundle decision is
otherwise permissive.
