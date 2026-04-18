ROOT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
SHARED_VENV_PYTHON := $(abspath $(ROOT_DIR)/../..)/.venv/bin/python
PYTHON ?= $(if $(wildcard $(ROOT_DIR)/.venv/bin/python),$(ROOT_DIR)/.venv/bin/python,$(if $(wildcard $(SHARED_VENV_PYTHON)),$(SHARED_VENV_PYTHON),python3))
PYTHON_COMPILE ?= $(PYTHON)
RUFF ?= $(PYTHON) -m ruff
RUN_ALL := -m project.pipelines.run_all
CLEAN_SCRIPT := $(ROOT_DIR)/project/scripts/clean_data.sh

RUN_ID ?= discovery_2021_2022
SYMBOLS ?= BTCUSDT,ETHUSDT
# Discovery defaults support multi-symbol idea generation under one RUN_ID.
START ?= 2021-01-01
END ?= 2022-12-31
STRATEGIES ?=
ENABLE_CROSS_VENUE_SPOT_PIPELINE ?= 0
CHANGED_BASE ?= origin/main
CHANGED_HEAD ?= HEAD

.PHONY: help \
	discover validate promote export deploy-paper \
	check-hygiene test test-fast lint format-check format style governance pre-commit minimum-green-gate \
	clean clean-runtime clean-all-data clean-repo clean-run-data clean-hygiene debloat compile \
	run baseline discover-concept discover-target discover-blueprints discover-edges discover-edges-from-raw discover-hybrid \
	golden-workflow golden-synthetic-discovery golden-certification synthetic-demo \
	advanced-discover-triggers-parameter advanced-discover-triggers-cluster \
	bench-pipeline benchmark-maintenance-smoke benchmark-maintenance benchmark-core benchmark-review benchmark-certify monitor

help:
	@echo "Canonical lifecycle:"
	@echo "  discover           - Canonical bounded research entry. Usage: make discover PROPOSAL=spec/proposals/canonical_event_hypothesis.yaml DISCOVER_ACTION=plan|run"
	@echo "  validate           - Canonical validation surface. Usage: make validate RUN_ID=<run_id>"
	@echo "  promote            - Canonical promotion surface. Usage: make promote RUN_ID=<run_id> SYMBOLS=BTCUSDT"
	@echo "  export             - Canonical runtime-batch export. Usage: make export RUN_ID=<run_id>"
	@echo "  deploy-paper       - Canonical deployment (Paper). Usage: make deploy-paper RUN_ID=<run_id>"
	@echo ""
	@echo "Maintenance and hygiene:"
	@echo "  check-hygiene      - Enforce tracked-file, root-clutter, and test-root policy"
	@echo "  test               - Run the default pytest suite"
	@echo "  test-fast          - Run the fast pytest profile"
	@echo "  lint               - Ruff lint on changed Python files"
	@echo "  format-check       - Ruff formatter check on changed Python files"
	@echo "  format             - Ruff format changed Python files in-place"
	@echo "  style              - Run lint + format-check on changed Python files"
	@echo "  governance         - Audit specs and sync schemas"
	@echo "  minimum-green-gate - Required baseline for platform stabilization"
	@echo "  clean              - Remove repo caches and local temp files"
	@echo "  clean-runtime      - Remove local runtime outputs"
	@echo "  clean-all-data     - Wipe local data roots"
	@echo "  debloat            - Run repo cleanup then enforce hygiene"
	@echo ""
	@echo "Supported workflow bundles:"
	@echo "  run                - Ingest + Clean + Features (Preparation only)"
	@echo "  baseline           - Full discovery + profitable strategy packaging"
	@echo "  discover-concept   - Run concept-driven discovery against the pipeline planner"
	@echo "  discover-target    - Targeted discovery for specific symbols/events"
	@echo "                       Usage: make discover-target SYMBOLS=BTCUSDT EVENT=VOL_SHOCK"
	@echo "  golden-workflow    - Canonical end-to-end smoke workflow"
	@echo "  golden-certification - Golden workflow plus runtime certification manifest"
	@echo ""
	@echo "Advanced/Internal trigger discovery (Proposal-only):"
	@echo "  advanced-discover-triggers-parameter - Run parameter sweep over detector family"
	@echo "  advanced-discover-triggers-cluster   - Mine recurring feature excursions"
	@echo ""
	@echo "Benchmarks:"
	@echo "  discover-blueprints, discover-edges, discover-edges-from-raw, discover-hybrid"
	@echo "  benchmark-maintenance-smoke, benchmark-maintenance, benchmark-core, benchmark-review, benchmark-certify"

#
# Maintenance and benchmark targets
#
benchmark-maintenance-smoke:
	@echo "Running benchmark maintenance dry-run..."
	PYTHONPATH=. $(PYTHON) project/scripts/run_benchmark_maintenance_cycle.py --preset core_v1 --execute 0
	@echo "Benchmark maintenance smoke check PASSED."

benchmark-maintenance:
	@echo "Executing full benchmark maintenance cycle..."
	PYTHONPATH=. $(PYTHON) project/scripts/run_benchmark_maintenance_cycle.py --preset core_v1 --execute 1
	@echo "Maintenance cycle COMPLETE. Reviewing results:"
	PYTHONPATH=. $(PYTHON) project/scripts/show_benchmark_review.py --latest

minimum-green-gate:
	@echo "Running minimum green gate checks..."
	PYTHONPATH=. PYTHONPYCACHEPREFIX=$(PY_CACHE_PREFIX) $(PYTHON) -m compileall -q project project/tests
	PYTHONPATH=. PYTHONPYCACHEPREFIX=$(PY_CACHE_PREFIX) $(PYTHON) -m pytest project/tests/architecture
	PYTHONPATH=. PYTHONPYCACHEPREFIX=$(PY_CACHE_PREFIX) $(PYTHON) project/scripts/spec_qa_linter.py
	PYTHONPATH=. PYTHONPYCACHEPREFIX=$(PY_CACHE_PREFIX) $(PYTHON) project/scripts/detector_coverage_audit.py --md-out docs/generated/detector_coverage.md --json-out docs/generated/detector_coverage.json --check
	PYTHONPATH=. PYTHONPYCACHEPREFIX=$(PY_CACHE_PREFIX) $(PYTHON) project/scripts/ontology_consistency_audit.py --output docs/generated/ontology_audit.json --check
	PYTHONPATH=. PYTHONPYCACHEPREFIX=$(PY_CACHE_PREFIX) $(PYTHON) project/scripts/build_event_contract_artifacts.py --check
	PYTHONPATH=. PYTHONPYCACHEPREFIX=$(PY_CACHE_PREFIX) $(PYTHON) project/scripts/check_calibration_version_bumps.py
	PYTHONPATH=. PYTHONPYCACHEPREFIX=$(PY_CACHE_PREFIX) $(PYTHON) project/scripts/check_deployable_core_replay_baseline.py
	PYTHONPATH=. PYTHONPYCACHEPREFIX=$(PY_CACHE_PREFIX) $(PYTHON) project/scripts/check_known_episode_replay_baseline.py
	PYTHONPATH=. PYTHONPYCACHEPREFIX=$(PY_CACHE_PREFIX) $(PYTHON) project/scripts/check_deployable_core_truth_review.py
	PYTHONPATH=. PYTHONPYCACHEPREFIX=$(PY_CACHE_PREFIX) $(PYTHON) project/scripts/check_historical_exchange_replay_baseline.py
	PYTHONPATH=. PYTHONPYCACHEPREFIX=$(PY_CACHE_PREFIX) $(PYTHON) project/scripts/event_ontology_audit.py --check
	PYTHONPATH=. PYTHONPYCACHEPREFIX=$(PY_CACHE_PREFIX) $(PYTHON) project/scripts/build_event_ontology_artifacts.py --check
	PYTHONPATH=. PYTHONPYCACHEPREFIX=$(PY_CACHE_PREFIX) $(PYTHON) project/scripts/build_system_map.py --check
	PYTHONPATH=. PYTHONPYCACHEPREFIX=$(PY_CACHE_PREFIX) $(PYTHON) project/scripts/build_architecture_metrics.py --check
	PYTHONPATH=. PYTHONPYCACHEPREFIX=$(PY_CACHE_PREFIX) $(PYTHON) -m pytest -q \
		project/tests/events/test_deployable_core_validation_gate.py \
		project/tests/events/test_deployable_core_replay_baseline.py \
		project/tests/events/test_deployable_core_known_episode_replay.py \
		project/tests/events/test_deployable_core_truth_review.py \
		project/tests/events/test_deployable_core_historical_exchange_replay.py \
		project/tests/regressions/test_monitor_only_venue_immutability.py \
		project/tests/regressions/test_run_success_requires_outputs.py \
		project/tests/regressions/test_stage_registry_path_validity.py
	PYTHONPATH=. PYTHONPYCACHEPREFIX=$(PY_CACHE_PREFIX) $(PYTHON) project/scripts/run_golden_regression.py --run_id smoke_run
	PYTHONPATH=. PYTHONPYCACHEPREFIX=$(PY_CACHE_PREFIX) $(PYTHON) project/scripts/run_golden_workflow.py --root $(GOLDEN_WORKFLOW_ROOT)
	@echo "Minimum green gate PASSED."


DISCOVER_ACTION ?= plan
REVIEW_ACTION ?= diagnose
PROPOSAL ?=
RUN_IDS ?=
RUN_ID ?=
PY_CACHE_PREFIX ?= /tmp/edge-pyc
GOLDEN_WORKFLOW_ROOT ?= /tmp/edge-golden-workflow

#
# Canonical lifecycle
#
discover:
	@if [ -z "$(PROPOSAL)" ]; then echo "Usage: make discover PROPOSAL=path/to/proposal.yaml DISCOVER_ACTION=plan|run"; exit 2; fi
	@if [ "$(DISCOVER_ACTION)" != "plan" ] && [ "$(DISCOVER_ACTION)" != "run" ]; then echo "DISCOVER_ACTION must be one of: plan, run"; exit 2; fi
	PYTHONPATH=. $(PYTHON) -m project.cli discover $(DISCOVER_ACTION) --proposal $(PROPOSAL)

validate:
	@if [ -z "$(RUN_ID)" ]; then echo "Usage: make validate RUN_ID=<run_id>"; exit 2; fi
	PYTHONPATH=. $(PYTHON) -m project.cli validate run --run_id $(RUN_ID)

promote:
	@if [ -z "$(RUN_ID)" ] || [ -z "$(SYMBOLS)" ]; then echo "Usage: make promote RUN_ID=<run_id> SYMBOLS=BTCUSDT"; exit 2; fi
	PYTHONPATH=. $(PYTHON) -m project.cli promote run --run_id $(RUN_ID) --symbols $(SYMBOLS)

export:
	@if [ -z "$(RUN_ID)" ]; then echo "Usage: make export RUN_ID=<run_id>"; exit 2; fi
	PYTHONPATH=. $(PYTHON) -m project.cli promote export --run_id $(RUN_ID)

deploy-paper:
	@if [ -z "$(RUN_ID)" ]; then echo "Usage: make deploy-paper RUN_ID=<run_id>"; exit 2; fi
	PYTHONPATH=. $(PYTHON) -m project.cli deploy paper --run_id $(RUN_ID)

# Advanced/Internal trigger discovery (Proposal-generating only)
# Manual review required before registry adoption.
advanced-discover-triggers-parameter:
	PYTHONPATH=. $(PYTHON) -m project.cli discover triggers parameter-sweep --family $(or $(FAMILY),vol_shock) --symbol $(or $(SYMBOLS),BTCUSDT)

advanced-discover-triggers-cluster:
	PYTHONPATH=. $(PYTHON) -m project.cli discover triggers feature-cluster --symbol $(or $(SYMBOLS),BTCUSDT)
TIMEFRAMES ?= 5m
CONCEPT ?= 

#
# Supported workflow bundles
#
discover-target:
	$(PYTHON) $(RUN_ALL) \
		--run_id $(if $(RUN_ID),$(RUN_ID),discovery_$(shell date +%Y%m%d_%H%M%S)) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END) \
		--timeframes $(TIMEFRAMES) \
		--run_phase2_conditional 1 \
		--phase2_event_type $(EVENT) \
		--run_edge_candidate_universe 1 \
		--run_strategy_builder 0 \
		--run_recommendations_checklist 0

.PHONY: discover-concept
discover-concept:
	$(PYTHON) $(RUN_ALL) \
		--run_id $(if $(RUN_ID),$(RUN_ID),concept_$(shell date +%Y%m%d_%H%M%S)) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END) \
		--timeframes $(TIMEFRAMES) \
		--concept $(CONCEPT) \
		--run_phase2_conditional 1 \
		--run_edge_candidate_universe 1 \
		--run_strategy_builder 0 \
		--run_recommendations_checklist 0 \
		--strategy_blueprint_ignore_checklist 1 \
		--strategy_blueprint_allow_fallback 0 \
		--run_ingest_liquidation_snapshot 0 \
		--run_ingest_open_interest_hist 0

discover-blueprints:
	$(PYTHON) $(RUN_ALL) \
		--run_id $(RUN_ID) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END) \
		--run_phase2_conditional 1 \
		--phase2_event_type all \
		--run_edge_candidate_universe 1 \
		--run_strategy_blueprint_compiler 1 \
		--run_strategy_builder 1 \
		--run_recommendations_checklist 1

run:
	$(PYTHON) $(RUN_ALL) \
		--run_id $(RUN_ID) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END)

baseline:
	$(PYTHON) $(RUN_ALL) \
		--run_id $(RUN_ID) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END) \
		--run_phase2_conditional 1 \
		--phase2_event_type all \
		--run_edge_candidate_universe 1 \
		--run_strategy_blueprint_compiler 1 \
		--run_strategy_builder 1 \
		--run_recommendations_checklist 1 \
		--run_profitable_selector 1

golden-workflow:
	$(PYTHON) -m project.scripts.run_golden_workflow

golden-synthetic-discovery:
	$(PYTHON) -m project.scripts.run_golden_synthetic_discovery

synthetic-demo:
	$(PYTHON) -m project.scripts.run_demo_synthetic_proposal

golden-certification:
	$(PYTHON) -m project.scripts.run_certification_workflow

governance:
	$(PYTHON) project/scripts/pipeline_governance.py --audit --sync
	PYTHONPATH=. $(PYTHON) project/scripts/build_event_contract_artifacts.py
	PYTHONPATH=. $(PYTHON) project/scripts/event_ontology_audit.py
	PYTHONPATH=. $(PYTHON) project/scripts/build_event_ontology_artifacts.py

pre-commit:
	bash project/scripts/pre_commit.sh

discover-edges:
	$(PYTHON) $(RUN_ALL) \
		--run_id $(RUN_ID) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END) \
		--run_phase2_conditional 1 \
		--phase2_event_type all \
		--run_edge_candidate_universe 1 \
		--run_strategy_builder 0 \
		--run_recommendations_checklist 0 \
		--strategy_blueprint_ignore_checklist 1 \
		--strategy_blueprint_allow_fallback 0 \
		--run_ingest_liquidation_snapshot 0 \
		--run_ingest_open_interest_hist 0

discover-edges-from-raw:
	$(PYTHON) $(RUN_ALL) \
		--run_id $(RUN_ID) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END) \
		--skip_ingest_ohlcv 1 \
		--skip_ingest_funding 1 \
		--skip_ingest_spot_ohlcv 1 \
		--enable_cross_venue_spot_pipeline $(ENABLE_CROSS_VENUE_SPOT_PIPELINE) \
		--run_phase2_conditional 1 \
		--phase2_event_type all \
		--run_edge_candidate_universe 1 \
		--run_strategy_builder 0 \
		--run_recommendations_checklist 0

discover-hybrid:
	$(PYTHON) $(RUN_ALL) \
		--run_id $(RUN_ID) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END) \
		--run_phase2_conditional 1 \
		--phase2_event_type all \
		--run_edge_candidate_universe 1 \
		--run_expectancy_analysis 1 \
		--run_expectancy_robustness 1

test:
	$(PYTHON) -m pytest -q

test-fast:
	$(PYTHON) -m pytest -q -m "not slow" --maxfail=1

lint:
	@base="$(CHANGED_BASE)"; \
	if ! git rev-parse --verify --quiet "$$base" >/dev/null; then \
		base="HEAD~1"; \
	fi; \
	files="$$(git diff --name-only --diff-filter=ACMR "$$base" "$(CHANGED_HEAD)" -- '*.py')"; \
	if [ -z "$$files" ]; then \
		echo "No changed Python files to lint."; \
		exit 0; \
	fi; \
	echo "Linting changed Python files:"; \
	echo "$$files"; \
	$(RUFF) check --select E9,F63,F7,F82 $$files

format-check:
	@base="$(CHANGED_BASE)"; \
	if ! git rev-parse --verify --quiet "$$base" >/dev/null; then \
		base="HEAD~1"; \
	fi; \
	files="$$(git diff --name-only --diff-filter=ACMR "$$base" "$(CHANGED_HEAD)" -- '*.py')"; \
	if [ -z "$$files" ]; then \
		echo "No changed Python files to format-check."; \
		exit 0; \
	fi; \
	echo "Format-checking changed Python files:"; \
	echo "$$files"; \
	$(RUFF) format --check $$files

format:
	@base="$(CHANGED_BASE)"; \
	if ! git rev-parse --verify --quiet "$$base" >/dev/null; then \
		base="HEAD~1"; \
	fi; \
	files="$$(git diff --name-only --diff-filter=ACMR "$$base" "$(CHANGED_HEAD)" -- '*.py')"; \
	if [ -z "$$files" ]; then \
		echo "No changed Python files to format."; \
		exit 0; \
	fi; \
	echo "Formatting changed Python files:"; \
	echo "$$files"; \
	$(RUFF) format $$files

style: lint format-check

monitor:
	$(PYTHON) project/scripts/monitor_data_freshness.py --symbols $(or $(SYMBOLS),BTCUSDT,ETHUSDT) --timeframe 5m --max_staleness_bars 3

#
# Quality and cleanup
#
bench-pipeline:
	$(PYTHON) project/scripts/benchmark_pipeline.py


compile:
	$(PYTHON_COMPILE) -m compileall $(ROOT_DIR)/project

clean:
	$(CLEAN_SCRIPT) repo

clean-runtime:
	$(CLEAN_SCRIPT) runtime

clean-all-data:
	$(CLEAN_SCRIPT) all

clean-repo: clean

debloat: clean-repo check-hygiene

clean-run-data:
	$(CLEAN_SCRIPT) data

check-hygiene:
	bash $(ROOT_DIR)/project/scripts/check_repo_hygiene.sh

clean-hygiene:
	$(CLEAN_SCRIPT) hygiene

benchmark-core:
	$(PYTHON) -m project.scripts.run_benchmark_matrix --preset core_v1 --execute 0

benchmark-review:
	$(PYTHON) -m project.scripts.show_benchmark_review --latest

benchmark-certify:
	$(PYTHON) -m project.scripts.run_benchmark_maintenance_cycle --preset core_v1
