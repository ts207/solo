# Command and entry-point inventory

This inventory was re-audited against the current repository command surfaces:

- `pyproject.toml` console scripts
- `project/cli.py`
- `Makefile`
- dedicated CLI modules such as `project/scripts/run_live_engine.py`, `project/research/cli/candidate_discovery_cli.py`, `project/research/cli/promotion_cli.py`, `project/apps/chatgpt/cli.py`, and `project/reliability/cli_smoke.py`

Use this file for what can be invoked directly today. Use `docs/09_operator_runbook.md` for the recommended operator path.

## Command layers

There are four distinct command layers in the repo:

1. **Canonical operator CLI** — `edge ...` in `project/cli.py`
2. **Dedicated service CLIs** — focused command surfaces such as `edge-live-engine`, `edge-phase2-discovery`, and `edge-promote`
3. **Make targets** — convenience wrappers and workflow bundles
4. **Direct scripts/modules** — generation, audits, benchmarking, and lower-level maintenance tools

Default rule:

- use `edge` for stage-oriented operator flows
- use a dedicated CLI when you need one service directly
- use Make targets for shortcuts and bundled workflows
- use direct scripts only when you are intentionally operating below the stage CLI

## Console scripts from `pyproject.toml`

| Console script | Target | Current role |
|---|---|---|
| `edge` | `project.cli:main` | Canonical four-stage lifecycle CLI. |
| `backtest` | `project.cli:main` | Alias for `edge`. |
| `edge-backtest` | `project.cli:main` | Alias for `edge`. |
| `edge-chatgpt-app` | `project.apps.chatgpt.cli:main` | ChatGPT app scaffold, inspection, and lightweight server commands. |
| `edge-run-all` | `project.pipelines.run_all:main` | Direct pipeline orchestrator with many low-level flags. |
| `edge-live-engine` | `project.scripts.run_live_engine:main` | Live-engine launcher and environment preflight. |
| `edge-phase2-discovery` | `project.research.cli.candidate_discovery_cli:main` | Direct candidate-discovery service entry point. |
| `edge-promote` | `project.research.cli.promotion_cli:main` | Direct promotion service entry point. |
| `edge-smoke` | `project.reliability.cli_smoke:main` | Smoke and artifact validation CLI. |
| `compile-strategy-blueprints` | `project.research.compile_strategy_blueprints:main` | Compile strategy blueprints from promoted or candidate inputs. |
| `build-strategy-candidates` | `project.research.build_strategy_candidates:main` | Build strategy candidate payloads from blueprint inputs. |
| `ontology-consistency-audit` | `project.scripts.ontology_consistency_audit:main` | Audit ontology/spec consistency. |

## Canonical `edge` CLI

Top-level commands from `project/cli.py`:

- `discover`
- `validate`
- `promote`
- `deploy`
- `ingest`
- `catalog`

### `edge discover`

Subcommands:

- `run`
- `plan`
- `list-artifacts`
- `triggers`
  - `parameter-sweep`
  - `feature-cluster`
  - `report`
  - `emit-registry-payload`
  - `list`
  - `inspect`
  - `review`
  - `approve`
  - `reject`
  - `mark-adopted`

Key flags:

- `edge discover plan|run --proposal <path>`
- optional: `--registry_root`, `--data_root`, `--run_id`, `--out_dir`
- `discover run` also supports `--check`

Operational note:

- `spec/proposals/canonical_event_hypothesis.yaml` is the current cold-start example
- `spec/proposals/canonical_event_hypothesis_h24.yaml` is a bounded follow-on proposal and is not the best bootstrap example on a clean data root

### `edge validate`

Subcommands:

- `run`
- `report`
- `diagnose`
- `list-artifacts`

Key flags:

- all subcommands require `--run_id`
- `diagnose` additionally accepts `--program_id`
- all support `--data_root`

### `edge promote`

Subcommands:

- `run`
- `export`
- `list-artifacts`

Key flags:

- `run` requires `--run_id` and `--symbols`
- `run` supports `--out_dir` and `--retail_profile`
- `export` and `list-artifacts` require `--run_id` and support `--data_root`

### `edge deploy`

Subcommands:

- `list-theses`
- `inspect-thesis`
- `paper`
- `live`
- `status`

Key flags:

- `inspect-thesis` requires `--run_id`
- `paper` and `live` require `--run_id`
- `paper` and `live` optionally accept `--config`
- all deploy subcommands support `--data_root`

Important runtime semantics:

- `edge deploy paper|live --run_id <run_id>` uses `run_id` as a deployment-gating and inspection input
- the live engine still loads its thesis source from `strategy_runtime.thesis_run_id` or `strategy_runtime.thesis_path` inside the config file
- use the direct `edge-live-engine` CLI when you need explicit runtime-launch control outside the stage wrapper

### `edge ingest`

Single command with explicit flags:

- required: `--run_id`, `--symbols`, `--start`, `--end`
- optional: `--timeframe`, `--exchange`, `--data_type`, `--out_root`, `--concurrency`, `--max_retries`, `--retry_backoff_sec`, `--force`, `--log_path`

Supported values in the current parser:

- `--exchange`: `binance`, `bybit`
- `--data_type`: `ohlcv`, `funding`, `oi`, `mark_price`, `index_price`

### `edge catalog`

Subcommands:

- `list`
- `compare`
- `audit-artifacts`

Key flags:

- `list`: optional `--stage`, `--data_root`
- `compare`: required `--run_id_a`, `--run_id_b`, `--stage`; optional `--data_root`
- `audit-artifacts`: optional `--run_id`, `--since`, `--data_root`, `--emit_inventory`, `--rewrite_stamps`

## Dedicated console scripts

These are real entry points, but they are narrower than `edge` and are best used when you want a single subsystem directly.

### `edge-live-engine`

Target: `project.scripts.run_live_engine:main`

Required flag:

- `--config <yaml>`

Optional flags:

- `--symbols <comma,separated,list>`
- `--snapshot_path <path>`
- `--print_session_metadata`

Behavioral note:

- when `runtime_mode: trading`, this command performs venue/environment validation before starting the runner
- this is the most direct checked-in way to start the live runner without going through `edge deploy`

### `edge-phase2-discovery`

Target: `project.research.cli.candidate_discovery_cli:main`

Required flags:

- `--run_id`
- `--symbols`

High-signal optional flags:

- `--config` (repeatable)
- `--data_root`
- `--event_type`
- `--templates`, `--horizons`, `--directions`
- `--timeframe`, `--horizon_bars`
- `--out_dir`
- `--run_mode`
- `--split_scheme_id`, `--embargo_bars`, `--purge_bars`
- `--discovery_profile`, `--candidate_generation_method`
- `--concept_file`
- `--entry_lag_bars`, `--entry_lags`
- `--fees_bps`, `--slippage_bps`, `--cost_bps`
- `--cost_calibration_mode`, `--cost_min_tob_coverage`, `--cost_tob_tolerance_minutes`
- `--program_id`, `--search_budget`, `--experiment_config`, `--registry_root`
- `--min_validation_n_obs`, `--min_test_n_obs`, `--min_total_n_obs`

Use this when you need the candidate-discovery service directly rather than a structured proposal ingress path.

### `edge-promote`

Target: `project.research.cli.promotion_cli:main`

Required flag:

- `--run_id`

High-signal optional flags:

- `--symbols`
- `--out_dir`
- `--retail_profile`
- `--promotion_profile` (`auto`, `research`, `deploy`)
- promotion gate thresholds such as `--max_q_value`, `--min_events`, `--min_stability_score`, `--min_sign_consistency`, `--min_cost_survival_ratio`, `--max_negative_control_pass_rate`, `--min_tob_coverage`, `--min_dsr`, `--max_overlap_ratio`, and `--max_profile_correlation`
- controls such as `--require_hypothesis_audit`, `--allow_missing_negative_controls`, `--require_multiplicity_diagnostics`, `--allow_discovery_promotion`
- objective/profile overrides: `--program_id`, `--objective_name`, `--objective_spec`, `--retail_profiles_spec`

Use this when you are iterating on promotion logic directly instead of going through `edge promote run`.

### `edge-smoke`

Target: `project.reliability.cli_smoke:main`

Flags:

- `--mode {engine,research,promotion,full,validate-artifacts}`
- `--root <path>`
- `--seed <int>`
- `--storage_mode <mode>`

Outputs a smoke summary JSON under `<root>/reliability/smoke_summary.json`.

### `edge-chatgpt-app`

Target: `project.apps.chatgpt.cli:main`

Subcommands:

- `backlog`
- `blueprint`
- `widget`
- `tools`
- `status`
- `serve`

Important flags:

- `blueprint --profile {operator,repo} [--repo-root <path>]`
- `tools --profile {operator,repo}`
- `serve --profile {operator,repo} [--repo-root <path>] [--host <host>] [--port <port>] [--path <path>]`

### `edge-run-all`

Target: `project.pipelines.run_all:main`

This is the low-level pipeline orchestrator behind many workflow bundles. It is intentionally broader than the stage CLI and accepts many pipeline-planning and stage-toggle flags. Use it when you need direct pipeline choreography, not when you just need the normal discover/validate/promote/deploy path.

### `compile-strategy-blueprints`

Target: `project.research.compile_strategy_blueprints:main`

Required flags:

- `--run_id`
- `--symbols`

High-signal optional flags:

- `--max_per_event`
- `--fees_bps`, `--slippage_bps`, `--cost_bps`
- `--ignore_checklist`
- `--retail_profile`
- `--candidates_file`, `--out_dir`, `--out_path`
- `--allow_non_executable_conditions`, `--allow_naive_entry_fail`, `--allow_fallback_blueprints`
- `--min_events_floor`, `--quality_floor_fallback`
- `--burn_ledger_path`
- `--negative_control_mode`, `--max_synthetic_expectancy_ratio`

### `build-strategy-candidates`

Target: `project.research.build_strategy_candidates:main`

Required flags:

- `--run_id`
- `--symbols`

High-signal optional flags:

- `--top_k_per_event`, `--max_candidates_per_event`, `--max_candidates`
- `--min_edge_score`
- `--include_alpha_bundle`
- `--ignore_checklist`, `--allow_non_promoted`, `--allow_missing_candidate_detail`
- `--enable_fractional_allocation`
- `--retail_profile`
- `--blueprints_file`, `--out_dir`, `--log_path`

### `ontology-consistency-audit`

Target: `project.scripts.ontology_consistency_audit:main`

Flags:

- `--repo-root`
- `--output`
- `--format {text,json}`
- `--fail-on-missing`
- additional check/failure control flags live in the script parser and are part of the governance lane

## Make targets

The Makefile is a convenience layer over the canonical CLI, the pipeline orchestrator, and selected scripts.

### Canonical lifecycle shortcuts

- `make discover`
- `make validate`
- `make promote`
- `make export`
- `make deploy-paper`

Current wrappers:

- `make discover` runs `python -m project.cli discover $(DISCOVER_ACTION) --proposal $(PROPOSAL)`
- `make validate` runs `python -m project.cli validate run --run_id $(RUN_ID)`
- `make promote` runs `python -m project.cli promote run --run_id $(RUN_ID) --symbols $(SYMBOLS)`
- `make export` runs `python -m project.cli promote export --run_id $(RUN_ID)`
- `make deploy-paper` runs `python -m project.cli deploy paper --run_id $(RUN_ID)`

Operational note:

- `make deploy-paper` does **not** forward a `--config` flag; use the `edge deploy paper ... --config ...` CLI directly when runtime config selection matters
- `make discover` is intentionally narrow; use `edge discover ...` directly when you need explicit `--registry_root`, `--data_root`, `--run_id`, or `--out_dir`

### Workflow bundles and benchmarks

- `make run`
- `make baseline`
- `make discover-concept`
- `make discover-target`
- `make discover-blueprints`
- `make discover-edges`
- `make discover-edges-from-raw`
- `make discover-hybrid`
- `make golden-workflow`
- `make golden-synthetic-discovery`
- `make synthetic-demo`
- `make golden-certification`
- `make advanced-discover-triggers-parameter`
- `make advanced-discover-triggers-cluster`
- `make benchmark-maintenance-smoke`
- `make benchmark-maintenance`
- `make benchmark-core`
- `make benchmark-review`
- `make benchmark-certify`
- `make bench-pipeline`
- `make monitor`

### Maintenance and hygiene targets

- `make help`
- `make test`
- `make test-fast`
- `make lint`
- `make format-check`
- `make format`
- `make style`
- `make governance`
- `make pre-commit`
- `make minimum-green-gate`
- `make compile`
- `make clean`
- `make clean-runtime`
- `make clean-all-data`
- `make clean-repo`
- `make debloat`
- `make clean-run-data`
- `make check-hygiene`
- `make clean-hygiene`

Important semantics from the current Makefile:

- `make lint`, `make format-check`, and `make format` operate on **changed Python files** relative to `CHANGED_BASE` and `CHANGED_HEAD`, not the whole repository
- `make style` is `lint + format-check`
- `make governance` runs pipeline governance sync plus event contract and ontology generation/audit steps
- `make minimum-green-gate` is the current full structural integrity gate
- `make clean-repo` is an alias for `make clean`
- `make debloat` is `clean-repo + check-hygiene`

## Direct script families

The repository contains many direct `main()` entry points. Use the inventory below as a map, not as a promise that every script is a stable operator surface.

High-signal script families:

- **Spec/domain generation**
  - `project/scripts/build_unified_event_registry.py`
  - `project/scripts/build_runtime_event_registry.py`
  - `project/scripts/build_domain_graph.py`
  - `project/scripts/build_event_contract_artifacts.py`
  - `project/scripts/build_event_contract_reference.py`
  - `project/scripts/build_event_ontology_artifacts.py`
  - `project/scripts/build_system_map.py`
  - `project/scripts/build_architecture_metrics.py`
- **Audits and consistency checks**
  - `project/scripts/spec_qa_linter.py`
  - `project/scripts/detector_coverage_audit.py`
  - `project/scripts/event_ontology_audit.py`
  - `project/scripts/ontology_consistency_audit.py`
  - `project/scripts/regime_routing_audit.py`
  - `project/scripts/event_template_semantics_audit.py`
- **Runtime and operations**
  - `project/scripts/run_live_engine.py`
  - `project/scripts/monitor_data_freshness.py`
  - `project/scripts/watch_live_runtime_alerts.py`
- **Golden and benchmark flows**
  - `project/scripts/run_golden_workflow.py`
  - `project/scripts/run_golden_regression.py`
  - `project/scripts/run_golden_synthetic_discovery.py`
  - `project/scripts/run_certification_workflow.py`
  - `project/scripts/run_benchmark_matrix.py`
  - `project/scripts/run_benchmark_maintenance_cycle.py`

See `docs/reference/script_inventory.md` for a broader file inventory.

## Ownership rules

- stage-oriented operator commands belong in `project/cli.py`
- low-level dedicated services may expose their own focused CLIs when they are independently useful
- bulk orchestration belongs in `project/pipelines/` and Make workflow bundles
- generation, audits, and maintenance tooling belong in focused scripts under `project/scripts/`
- avoid adding a new wrapper CLI when an existing canonical surface already covers the use case
