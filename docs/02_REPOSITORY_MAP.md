# Repository map

This map is derived from the current tree, not from historical documentation.

## Top-level layout

- `project/` — application code, pipeline orchestration, runtime, research, and tests
- `spec/` — authored YAML/CSV source specs that drive the registry/domain model
- `docs/` — authored documentation plus generated reports
- `data/` — default local data root when no environment override is present
- `deploy/` — deployment-related config roots
- `plugins/` — repo-specific agent/plugin assets

## Primary code packages

### Core substrate

- `project/core` — configuration, coercion, execution costs, feature schema/registry, statistics, validation primitives
- `project/io` — parquet compatibility, repository helpers, runtime adapters, utility I/O
- `project/artifacts` — canonical artifact path helpers
- `project/contracts` — pipeline/system-map/stage contract modules

### Spec and domain model

- `project/spec_registry` — load YAML specs from `spec/`
- `project/specs` — manifest, ontology, gates, invariants, objective and schema helpers
- `project/spec_validation` — grammar/ontology/search validators
- `project/domain` — compiled registry models and loaders

### Research stack

- `project/events` — event registry, ontology, detectors, sequencing, arbitration, governance
- `project/features` — feature/state/context derivation modules
- `project/research` — search, candidate generation, promotion helpers, validations, reporting, knowledge memory
- `project/operator` — bounded proposal semantics, stability, campaign/mutation decision support
- `project/pipelines` — run planner, execution DAG, provenance, stage registry, summaries

### Execution and runtime stack

- `project/engine` — backtest execution, fills, PnL, slippage, risk allocator, runner
- `project/portfolio` — overlap/admission policy, sizing, allocation, risk budgets
- `project/live` — thesis retrieval, live runner, OMS, policy, reflection, reconciliation, kill-switching
- `project/runtime` — replay/timebase/firewall/invariant helpers
- `project/reliability` — smoke, contracts, promotion gates, temporal lint/invariance

### Ancillary surfaces

- `project/apps/chatgpt` — ChatGPT app scaffold and MCP-style resource/tool descriptions
- `project/compilers` — executable-strategy compilation surface
- `project/episodes`, `project/experiments`, `project/schemas`, `project/strategy`, `project/synthetic_truth` — supporting packages for domain, runtime, and synthetic testing

## Current package inventory

| Package | Python files | Test files | Representative top-level modules |
|---|---:|---:|---|
| `project/apps` | 10 | 5 | — |
| `project/artifacts` | 3 | 1 | `catalog`, `compat` |
| `project/compilers` | 3 | 0 | `executable_strategy_spec`, `spec_transformer` |
| `project/contracts` | 7 | 18 | `artifacts`, `pipeline_registry`, `schemas`, `stage_dag`, `system_map`, `temporal_contracts` |
| `project/core` | 28 | 12 | `audited_join`, `bootstrap`, `causal_primitives`, `coercion`, `column_registry`, `config`, `constants`, `context_quality` |
| `project/deploy` | 1 | 0 | — |
| `project/discover` | 1 | 0 | — |
| `project/domain` | 7 | 2 | `compiled_registry`, `hypotheses`, `models`, `registry_loader` |
| `project/engine` | 17 | 21 | `artifacts`, `context_assembler`, `data_loader`, `exchange_constraints`, `execution_model`, `fills`, `pnl`, `portfolio_aggregator` |
| `project/episodes` | 2 | 0 | `registry` |
| `project/eval` | 24 | 26 | `ablation`, `attribution_joiner`, `audit_stats`, `benchmarks`, `cost_model`, `debug_microstructure`, `detection_verification_suite`, `drift_detection` |
| `project/events` | 82 | 35 | `arbitration`, `canonical_audit`, `canonical_registry_sidecars`, `config`, `contract_registry`, `contracts`, `detector_contract`, `emission` |
| `project/experiments` | 4 | 1 | `config_loader`, `schema`, `utils` |
| `project/features` | 16 | 10 | `alignment`, `audit`, `bybit_derivatives`, `carry_state`, `context`, `context_guards`, `context_states`, `event_scoring` |
| `project/io` | 10 | 0 | `experiment_store`, `http_utils`, `parquet_compat`, `repository`, `runtime_adapter`, `selection_log`, `universe`, `url_utils` |
| `project/live` | 38 | 29 | `audit_log`, `binance_client`, `bybit_client`, `context_builder`, `decay`, `decision`, `deployment`, `drift` |
| `project/operator` | 9 | 10 | `bounded`, `campaign_engine`, `decision_engine`, `mutation_engine`, `preflight`, `proposal_tools`, `run_semantics`, `stability` |
| `project/pipelines` | 83 | 138 | `build_sequence_events`, `effective_config`, `execution_engine`, `execution_engine_support`, `execution_manifest`, `execution_result`, `pipeline_audit`, `pipeline_defaults` |
| `project/portfolio` | 8 | 1 | `admission_policy`, `allocation_spec`, `incubation`, `orchestration`, `risk_budget`, `sizing`, `thesis_overlap` |
| `project/promote` | 1 | 0 | — |
| `project/reliability` | 12 | 3 | `audit_utils`, `cli`, `cli_smoke`, `contracts`, `manifest_checks`, `promotion_gate`, `regression_checks`, `schemas` |
| `project/research` | 266 | 115 | `_family_event_utils`, `_hypothesis_defaults`, `_timeframes`, `analyze_conditional_expectancy`, `analyze_events`, `analyze_interaction_lift`, `approval_registry_v2`, `approval_workflow_v2` |
| `project/runtime` | 9 | 7 | `firewall`, `hashing`, `invariants`, `lane_runner`, `normalized_event`, `oms_replay`, `replay`, `timebase` |
| `project/schemas` | 4 | 0 | `control_spec`, `data_contracts`, `strategy_spec` |
| `project/scripts` | 92 | 32 | `audit_detector_precision_recall`, `audit_historical_stat_integrity`, `audit_pipeline_stress`, `audit_pit_compliance`, `audit_promotion_flukes`, `bench_event_flags`, `benchmark_pipeline`, `build_architecture_metrics` |
| `project/spec_registry` | 4 | 2 | `loaders`, `policy`, `search_space` |
| `project/spec_validation` | 7 | 4 | `cli`, `grammar`, `loaders`, `ontology`, `reporting`, `search` |
| `project/specs` | 9 | 9 | `gates`, `invariants`, `loader`, `manifest`, `objective`, `ontology`, `schema_validation`, `utils` |
| `project/strategy` | 35 | 3 | — |
| `project/synthetic_truth` | 10 | 7 | — |
| `project/tests` | 637 | 0 | `conftest`, `test_architectural_integrity`, `test_blueprint_condition_nodes`, `test_causal_fp_state`, `test_data_contracts`, `test_dynamic_exit_causality`, `test_event_flags`, `test_event_hardening_verification` |
| `project/validate` | 1 | 0 | — |


## Spec tree highlights

The spec tree is large and central to the repo. High-signal subtrees:

- `spec/events/` — event definitions, families, ontology mapping, routing, precedence, unified registry
- `spec/states/` — regime/state definitions and registries
- `spec/features/` — feature definitions and metrics
- `spec/ontology/` — ontology templates/features/states
- `spec/runtime/` — runtime lanes, hashing, firewall
- `spec/search/` — benchmark/full/smoke search specs
- `spec/domain/domain_graph.yaml` — compiled domain graph consumed by `project/domain`

### Spec inventory by subtree

| Spec subtree | YAML files | Non-YAML files |
|---|---:|---:|
| `spec/benchmarks` | 6 | 0 |
| `spec/campaigns` | 2 | 0 |
| `spec/concepts` | 4 | 0 |
| `spec/domain` | 1 | 0 |
| `spec/episodes` | 2 | 0 |
| `spec/events` | 90 | 0 |
| `spec/features` | 42 | 0 |
| `spec/grammar` | 7 | 0 |
| `spec/hypotheses` | 2 | 0 |
| `spec/multiplicity` | 2 | 0 |
| `spec/objectives` | 1 | 0 |
| `spec/ontology` | 118 | 0 |
| `spec/proposals` | 3 | 0 |
| `spec/regimes` | 1 | 0 |
| `spec/runtime` | 3 | 0 |
| `spec/search` | 17 | 0 |
| `spec/states` | 82 | 0 |
| `spec/strategies` | 2 | 0 |
| `spec/templates` | 2 | 0 |
| `spec/theses` | 1 | 0 |


## Tests

Tests are first-class in this repo and are distributed by package concern rather than sitting in a tiny unit-test bucket.

### Test inventory by subtree

| Test subtree | Test files |
|---|---:|
| `project/tests/apps` | 5 |
| `project/tests/architecture` | 4 |
| `project/tests/artifacts` | 1 |
| `project/tests/audit` | 3 |
| `project/tests/contracts` | 18 |
| `project/tests/core` | 12 |
| `project/tests/domain` | 2 |
| `project/tests/engine` | 21 |
| `project/tests/eval` | 26 |
| `project/tests/events` | 35 |
| `project/tests/experiments` | 1 |
| `project/tests/features` | 10 |
| `project/tests/live` | 29 |
| `project/tests/operator` | 10 |
| `project/tests/pipelines` | 138 |
| `project/tests/pit` | 3 |
| `project/tests/portfolio` | 1 |
| `project/tests/regressions` | 13 |
| `project/tests/reliability` | 3 |
| `project/tests/replays` | 2 |
| `project/tests/research` | 115 |
| `project/tests/runtime` | 7 |
| `project/tests/scripts` | 32 |
| `project/tests/smoke` | 10 |
| `project/tests/spec_registry` | 2 |
| `project/tests/spec_validation` | 4 |
| `project/tests/specs` | 9 |
| `project/tests/strategies` | 4 |
| `project/tests/strategy` | 3 |
| `project/tests/strategy_dsl` | 4 |
| `project/tests/strategy_templates` | 1 |
| `project/tests/synthetic_truth` | 7 |
| `project/tests/unit` | 1 |


## Generated documentation and audits

`docs/generated/` contains generated inventories and audits such as:

- architecture metrics
- event contract reference
- event ontology mapping and audits
- detector coverage
- regime routing audit
- system map

Those are outputs. The source of truth is still the code and `spec/` inputs that generate them.

## Reference appendices

- `docs/reference/package_inventory.md`
- `docs/reference/spec_inventory.md`
- `docs/reference/test_inventory.md`
- `docs/reference/script_inventory.md`
