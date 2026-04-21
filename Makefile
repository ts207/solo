ROOT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
SHARED_VENV_PYTHON := $(abspath $(ROOT_DIR)/../..)/.venv/bin/python
PYTHON ?= $(if $(wildcard $(ROOT_DIR)/.venv/bin/python),$(ROOT_DIR)/.venv/bin/python,$(if $(wildcard $(SHARED_VENV_PYTHON)),$(SHARED_VENV_PYTHON),python3))
PY_CACHE_PREFIX ?= /tmp/edge-pyc

DISCOVER_ACTION ?= plan
PROPOSAL ?=
RUN_ID ?=
SYMBOLS ?= BTCUSDT,ETHUSDT
EXECUTE ?= 0

.PHONY: help discover validate promote export deploy-paper benchmark-supported-path liquidation-exhaustion-plan-matrix \
	benchmark-maintenance-smoke benchmark-maintenance governance pre-commit \
	check-hygiene test test-fast lint format-check format style minimum-green-gate \
	clean clean-runtime clean-all-data clean-repo clean-run-data clean-hygiene debloat \
	advanced-discover-triggers-parameter advanced-discover-triggers-cluster

help:
	@echo "Canonical supported path:"
	@echo "  discover                 make discover PROPOSAL=spec/proposals/...yaml DISCOVER_ACTION=plan|run"
	@echo "  validate                 make validate RUN_ID=<run_id>"
	@echo "  promote                  make promote RUN_ID=<run_id> SYMBOLS=BTCUSDT"
	@echo "  export                   make export RUN_ID=<run_id>"
	@echo "  deploy-paper             make deploy-paper CONFIG=path/to/live.yaml"
	@echo "  benchmark-supported-path make benchmark-supported-path EXECUTE=0|1"
	@echo "  liquidation-exhaustion-plan-matrix make liquidation-exhaustion-plan-matrix [PHASE=A|B|C|S|T]"
	@echo "    Phase A: baseline runs  B: bridge candidates  C: context splits  S: short-horizon template/detector configs  T: threshold tuning"
	@echo ""
	@echo "Compatibility/experimental:"
	@echo "  advanced-discover-triggers-parameter, advanced-discover-triggers-cluster"
	@echo ""
	@echo "Maintenance:"
	@echo "  test, test-fast, style, governance, minimum-green-gate, clean-runtime"

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
	@if [ -z "$(CONFIG)" ]; then echo "Usage: make deploy-paper CONFIG=path/to/live.yaml"; exit 2; fi
	PYTHONPATH=. $(PYTHON) -m project.cli deploy paper-run --config $(CONFIG)

benchmark-supported-path:
	@if [ "$(EXECUTE)" = "1" ] && [ "$(OFFLINE_PARQUET_EXECUTION_FIXED)" != "1" ]; then echo "EXECUTE=1 blocked until offline parquet execution is fixed. Set OFFLINE_PARQUET_EXECUTION_FIXED=1 only after that gate is repaired."; exit 2; fi
	PYTHONPATH=. $(PYTHON) project/scripts/run_supported_path_benchmark.py --execute $(EXECUTE)

liquidation-exhaustion-plan-matrix:
	PYTHONPATH=. $(PYTHON) project/scripts/plan_liquidation_exhaustion_matrix.py --phase $(or $(PHASE),A)

advanced-discover-triggers-parameter:
	PYTHONPATH=. $(PYTHON) -m project.cli discover triggers parameter-sweep --family $(or $(FAMILY),vol_shock) --symbol $(or $(SYMBOLS),BTCUSDT)

advanced-discover-triggers-cluster:
	PYTHONPATH=. $(PYTHON) -m project.cli discover triggers feature-cluster --symbol $(or $(SYMBOLS),BTCUSDT)

benchmark-maintenance-smoke:
	PYTHONPATH=. $(PYTHON) project/scripts/run_benchmark_maintenance_cycle.py --preset core_v1 --execute 0

benchmark-maintenance:
	PYTHONPATH=. $(PYTHON) project/scripts/run_benchmark_maintenance_cycle.py --preset core_v1 --execute 1
	PYTHONPATH=. $(PYTHON) project/scripts/show_benchmark_review.py --latest

governance:
	PYTHONPATH=. $(PYTHON) project/scripts/pipeline_governance.py --audit --sync
	PYTHONPATH=. $(PYTHON) project/scripts/build_event_contract_artifacts.py
	PYTHONPATH=. $(PYTHON) project/scripts/event_ontology_audit.py
	PYTHONPATH=. $(PYTHON) project/scripts/build_event_ontology_artifacts.py

pre-commit:
	bash project/scripts/pre_commit.sh

test:
	PYTHONPATH=. $(PYTHON) -m pytest -q

test-fast:
	PYTHONPATH=. $(PYTHON) -m pytest -q project/tests/pipelines/test_cli_contract.py project/tests/live/test_event_detector_adapters.py project/tests/portfolio

lint:
	PYTHONPATH=. $(PYTHON) -m ruff check project

format-check:
	PYTHONPATH=. $(PYTHON) -m ruff format --check project

format:
	PYTHONPATH=. $(PYTHON) -m ruff format project

style: lint format-check

minimum-green-gate:
	PYTHONPATH=. PYTHONPYCACHEPREFIX=$(PY_CACHE_PREFIX) $(PYTHON) -m compileall -q project project/tests
	PYTHONPATH=. PYTHONPYCACHEPREFIX=$(PY_CACHE_PREFIX) $(PYTHON) -m pytest -q project/tests/pipelines/test_cli_contract.py project/tests/live/test_event_detector_adapters.py project/tests/portfolio
	PYTHONPATH=. PYTHONPYCACHEPREFIX=$(PY_CACHE_PREFIX) $(PYTHON) project/scripts/refresh_docs_governance.py --check

clean:
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
	rm -rf .pytest_cache .mypy_cache .ruff_cache

clean-runtime:
	rm -rf data/reports data/research data/events data/live/theses

clean-run-data:
	rm -rf data/runs/*

clean-all-data:
	rm -rf data/lake/* data/runs/* data/reports data/research data/events data/live/theses

clean-hygiene: clean

clean-repo: clean clean-runtime

debloat: clean-repo
