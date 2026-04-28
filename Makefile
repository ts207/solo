PYTHON ?= python3
PYTHONPATH ?= .
CLI := PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m project.cli

RUN_ID ?=
PROPOSAL ?=
FIRST_EDGE_PROPOSAL ?= spec/proposals/single_event_liquidation_exhaustion_reversal_bounce_h24_v1.yaml
DEFAULT_CELL_SPEC_DIR ?= spec/discovery
FIRST_EDGE_SPEC_DIR ?= spec/discovery/tier2_liquidation_exhaustion_focused_v1
SPEC_DIR ?= $(DEFAULT_CELL_SPEC_DIR)
SEARCH_BUDGET ?=
ASSEMBLE_LIMIT ?= 20
PROMOTION_PROFILE ?= research
TOP_K ?= 10
SYMBOLS ?= BTCUSDT,ETHUSDT
TIMEFRAME ?= 5m
START ?=
END ?=
DATA_ROOT ?=
OUT_DIR ?= project/configs
CONFIG ?=
RUNTIME_MODE ?= monitor_only
REGISTRY_ROOT ?= project/configs/registries
EXECUTE ?= 0
RUNTIME_MAX_ROWS ?= 500
MONITOR_REPORT ?=

.PHONY: help first-edge discover discover-proposal-plan discover-proposal list-artifacts summarize summarize-proposal explain-empty proposal-inspect \
	validate promote export bind-config paper-run live-run deploy-status list-theses \
	deploy-paper check-domain-graph domain-graph check-registry-sync benchmark-supported-path \
	discover-cells-verify discover-cells-plan discover-cells-run \
	check-hygiene clean clean-runtime clean-run-data clean-all-data clean-hygiene \
	governance minimum-green-gate

help:
	@printf '%s\n' \
	  'Canonical stage targets:' \
	  '  make first-edge RUN_ID=<run_id> DATA_ROOT=<lake> START=<start> END=<end>' \
	  '  make discover RUN_ID=<run_id> START=<start> END=<end> [DATA_ROOT=...] [SPEC_DIR=...] [REGISTRY_ROOT=...]' \
	  '  make discover-proposal PROPOSAL=<proposal.yaml> RUN_ID=<run_id> [DATA_ROOT=...] [REGISTRY_ROOT=...]' \
	  '  make list-artifacts RUN_ID=<run_id> [DATA_ROOT=...]' \
	  '  make summarize RUN_ID=<run_id> [DATA_ROOT=...]' \
	  '  make summarize-proposal RUN_ID=<run_id> [DATA_ROOT=...] [TOP_K=10]' \
	  '  make explain-empty RUN_ID=<run_id> [DATA_ROOT=...]' \
	  '  make proposal-inspect PROPOSAL=<proposal.yaml> [RUN_ID=<run_id>] [DATA_ROOT=...]' \
	  '  make validate RUN_ID=<run_id> [DATA_ROOT=...]' \
	  '  make promote RUN_ID=<run_id> SYMBOLS=BTCUSDT,ETHUSDT [OUT_DIR=...]' \
	  '  make export RUN_ID=<run_id> [DATA_ROOT=...]' \
	  '  make bind-config RUN_ID=<run_id> [OUT_DIR=project/configs] [RUNTIME_MODE=monitor_only] [SYMBOLS=...] [DATA_ROOT=...]' \
	  '  make paper-run CONFIG=project/configs/live_paper_<run_id>.yaml' \
	  '  make live-run CONFIG=project/configs/live_live_<run_id>.yaml' \
	  '  make deploy-status RUN_ID=<run_id> [CONFIG=project/configs/live_paper_<run_id>.yaml] [DATA_ROOT=...]' \
	  '  make list-theses [DATA_ROOT=...]' \
	  '' \
	  'Auxiliary targets:' \
	  '  make check-hygiene' \
	  '  make clean-runtime|clean-run-data|clean-all-data|clean-hygiene' \
	  '  make check-domain-graph' \
	  '  make check-registry-sync' \
	  '  make check-spec-sync' \
	  '  make domain-graph' \
	  '  make governance' \
	  '  make minimum-green-gate' \
	  '  make benchmark-supported-path EXECUTE=0|1 [DATA_ROOT=...] [RUNTIME_MAX_ROWS=500]' \
	  '  make discover-cells-verify RUN_ID=<run_id> START=<start> END=<end> [SPEC_DIR=spec/discovery]' \
	  '  make discover-cells-plan   RUN_ID=<run_id> START=<start> END=<end> [SPEC_DIR=spec/discovery/expanded_v2]' \
	  '  make discover-cells-run    RUN_ID=<run_id> START=<start> END=<end> [SPEC_DIR=spec/discovery/expanded_v2]'

check-hygiene:
	@bash ./project/scripts/check_repo_hygiene.sh

clean:
	@bash ./project/scripts/clean_data.sh repo

clean-runtime:
	@bash ./project/scripts/clean_data.sh runtime

clean-run-data:
	@bash ./project/scripts/clean_data.sh data

clean-all-data:
	@bash ./project/scripts/clean_data.sh all

clean-hygiene:
	@bash ./project/scripts/clean_data.sh hygiene

check-domain-graph:
	@$(CLI) validate specs --root . >/dev/null
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) project/scripts/check_domain_graph_freshness.py

check-registry-sync:
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) project/scripts/check_registry_sync.py

domain-graph:
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) project/scripts/build_domain_graph.py
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) project/scripts/check_domain_graph_freshness.py

discover-proposal-plan:
	@test -n "$(PROPOSAL)" || (echo 'PROPOSAL is required' >&2; exit 2)
	@$(CLI) discover plan --proposal "$(PROPOSAL)" $(if $(RUN_ID),--run_id "$(RUN_ID)",) $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",) --registry_root "$(REGISTRY_ROOT)"

discover-plan:
	@test -n "$(RUN_ID)" || (echo 'RUN_ID is required' >&2; exit 2)
	@test -n "$(START)" || (echo 'START is required' >&2; exit 2)
	@test -n "$(END)" || (echo 'END is required' >&2; exit 2)
	@$(CLI) discover cells plan --run_id "$(RUN_ID)" --symbols "$(SYMBOLS)" --timeframe "$(TIMEFRAME)" --start "$(START)" --end "$(END)" --spec_dir "$(SPEC_DIR)" --registry_root "$(REGISTRY_ROOT)" $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",)

first-edge:
	@test -n "$(RUN_ID)" || (echo 'RUN_ID is required' >&2; exit 2)
	@test -n "$(DATA_ROOT)" || (echo 'DATA_ROOT is required (lake mount)' >&2; exit 2)
	@test -n "$(START)" || (echo 'START is required' >&2; exit 2)
	@test -n "$(END)" || (echo 'END is required' >&2; exit 2)
	@$(CLI) discover cells run --run_id "$(RUN_ID)" --symbols "$(SYMBOLS)" --timeframe "$(TIMEFRAME)" --start "$(START)" --end "$(END)" --data_root "$(DATA_ROOT)" --spec_dir "$(FIRST_EDGE_SPEC_DIR)" --registry_root "$(REGISTRY_ROOT)" $(if $(SEARCH_BUDGET),--search_budget "$(SEARCH_BUDGET)",)
	@$(CLI) discover cells summarize --run_id "$(RUN_ID)" --data_root "$(DATA_ROOT)"
	@$(CLI) discover cells assemble-theses --run_id "$(RUN_ID)" --data_root "$(DATA_ROOT)" --limit "$(ASSEMBLE_LIMIT)"

discover:
	@test -n "$(RUN_ID)" || (echo 'RUN_ID is required' >&2; exit 2)
	@test -n "$(START)" || (echo 'START is required' >&2; exit 2)
	@test -n "$(END)" || (echo 'END is required' >&2; exit 2)
	@$(CLI) discover cells run --run_id "$(RUN_ID)" --symbols "$(SYMBOLS)" --timeframe "$(TIMEFRAME)" --start "$(START)" --end "$(END)" --spec_dir "$(SPEC_DIR)" --registry_root "$(REGISTRY_ROOT)" $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",) $(if $(SEARCH_BUDGET),--search_budget "$(SEARCH_BUDGET)",)
	@$(CLI) discover cells summarize --run_id "$(RUN_ID)" $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",)
	@$(CLI) discover cells assemble-theses --run_id "$(RUN_ID)" $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",) --limit "$(ASSEMBLE_LIMIT)"

discover-proposal:
	@test -n "$(PROPOSAL)" || (echo 'PROPOSAL is required' >&2; exit 2)
	@$(CLI) discover run --proposal "$(PROPOSAL)" $(if $(RUN_ID),--run_id "$(RUN_ID)",) $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",) --registry_root "$(REGISTRY_ROOT)" $(if $(PROMOTION_PROFILE),--promotion_profile "$(PROMOTION_PROFILE)",)

list-artifacts:
	@test -n "$(RUN_ID)" || (echo 'RUN_ID is required' >&2; exit 2)
	@$(CLI) discover list-artifacts --run_id "$(RUN_ID)" $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",)

summarize:
	@test -n "$(RUN_ID)" || (echo 'RUN_ID is required' >&2; exit 2)
	@$(CLI) discover cells summarize --run_id "$(RUN_ID)" $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",)

summarize-proposal:
	@test -n "$(RUN_ID)" || (echo 'RUN_ID is required' >&2; exit 2)
	@$(CLI) discover summarize --run_id "$(RUN_ID)" $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",) --top_k "$(TOP_K)"

explain-empty:
	@test -n "$(RUN_ID)" || (echo 'RUN_ID is required' >&2; exit 2)
	@$(CLI) discover explain-empty --run_id "$(RUN_ID)" $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",)

proposal-inspect:
	@test -n "$(PROPOSAL)" || (echo 'PROPOSAL is required' >&2; exit 2)
	@$(CLI) proposal inspect --proposal "$(PROPOSAL)" $(if $(RUN_ID),--run_id "$(RUN_ID)",) $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",)

validate:
	@test -n "$(RUN_ID)" || (echo 'RUN_ID is required' >&2; exit 2)
	@$(CLI) validate run --run_id "$(RUN_ID)" $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",)

promote:
	@test -n "$(RUN_ID)" || (echo 'RUN_ID is required' >&2; exit 2)
	@test -n "$(SYMBOLS)" || (echo 'SYMBOLS is required' >&2; exit 2)
	@$(CLI) promote run --run_id "$(RUN_ID)" --symbols "$(SYMBOLS)" $(if $(OUT_DIR),--out_dir "$(OUT_DIR)",)

export:
	@test -n "$(RUN_ID)" || (echo 'RUN_ID is required' >&2; exit 2)
	@$(CLI) deploy export --run_id "$(RUN_ID)" $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",)

bind-config:
	@test -n "$(RUN_ID)" || (echo 'RUN_ID is required' >&2; exit 2)
	@mkdir -p "$(OUT_DIR)"
	@$(CLI) deploy bind-config --run_id "$(RUN_ID)" --out_dir "$(OUT_DIR)" --runtime_mode "$(RUNTIME_MODE)" --symbols "$(SYMBOLS)" $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",) $(if $(MONITOR_REPORT),--monitor_report "$(MONITOR_REPORT)",)

paper-run:
	@test -n "$(CONFIG)" || (echo 'CONFIG is required' >&2; exit 2)
	@$(CLI) deploy paper-run --config "$(CONFIG)"

live-run:
	@test -n "$(CONFIG)" || (echo 'CONFIG is required' >&2; exit 2)
	@$(CLI) deploy live-run --config "$(CONFIG)"

deploy-status:
	@test -n "$(RUN_ID)" || (echo 'RUN_ID is required' >&2; exit 2)
	@$(CLI) deploy status --run_id "$(RUN_ID)" $(if $(CONFIG),--config "$(CONFIG)",) $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",)

list-theses:
	@$(CLI) deploy list-theses $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",)

monitor-lead-thesis:
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) project/scripts/monitor_research_thesis.py --run_id stat_stretch_04 --data_root data

deploy-paper: bind-config

governance:
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) project/scripts/refresh_docs_governance.py

minimum-green-gate:
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) project/scripts/spec_qa_linter.py
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) project/scripts/check_domain_graph_freshness.py
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m pytest -q -s project/tests/architecture
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m pytest -q -s project/tests/contracts/test_live_environment_config_contract.py
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m pytest -q -s project/tests/live/test_cli_deploy_run.py
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m pytest -q -s project/tests/pipelines/test_cli_contract.py
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m pytest -q -s project/tests/scripts/test_monitor_research_thesis.py
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m pytest -q -s project/tests/live/test_deploy_admission.py
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m pytest -q -s project/tests/live/test_deploy_admission_v2.py
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m pytest -q -s project/tests/live/test_tiny_live_admission_e2e.py
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m pytest -q -s project/tests/live/test_live_approval.py
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m pytest -q -s project/tests/live/test_runtime_admission.py
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m pytest -q -s project/tests/live/test_paper_ledger.py
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m pytest -q -s project/tests/live/test_paper_ledger_runtime_init.py
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m pytest -q -s project/tests/validate/test_forward_confirm_oos.py
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m pytest -q -s project/tests/promote/test_paper_gate.py

discover-cells-verify:
	@test -n "$(RUN_ID)" || (echo 'RUN_ID is required' >&2; exit 2)
	@test -n "$(START)" || (echo 'START is required' >&2; exit 2)
	@test -n "$(END)" || (echo 'END is required' >&2; exit 2)
	@$(CLI) discover cells verify-data --run_id "$(RUN_ID)" --symbols "$(SYMBOLS)" --timeframe "$(TIMEFRAME)" --start "$(START)" --end "$(END)" --spec_dir "$(SPEC_DIR)" $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",)

discover-cells-plan:
	@test -n "$(RUN_ID)" || (echo 'RUN_ID is required' >&2; exit 2)
	@test -n "$(START)" || (echo 'START is required' >&2; exit 2)
	@test -n "$(END)" || (echo 'END is required' >&2; exit 2)
	@$(CLI) discover cells plan --run_id "$(RUN_ID)" --symbols "$(SYMBOLS)" --timeframe "$(TIMEFRAME)" --start "$(START)" --end "$(END)" --spec_dir "$(SPEC_DIR)" $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",)

discover-cells-run:
	@test -n "$(RUN_ID)" || (echo 'RUN_ID is required' >&2; exit 2)
	@test -n "$(START)" || (echo 'START is required' >&2; exit 2)
	@test -n "$(END)" || (echo 'END is required' >&2; exit 2)
	@$(CLI) discover cells run --run_id "$(RUN_ID)" --symbols "$(SYMBOLS)" --timeframe "$(TIMEFRAME)" --start "$(START)" --end "$(END)" --spec_dir "$(SPEC_DIR)" $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",)

benchmark-supported-path:
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) project/scripts/run_supported_path_benchmark.py --execute $(EXECUTE) $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",) --runtime_max_rows $(RUNTIME_MAX_ROWS)

# Advanced/Internal trigger discovery
FAMILY ?=
advanced-discover-triggers-parameter:
	@test -n "$(FAMILY)" || (echo 'FAMILY is required' >&2; exit 2)
	@$(CLI) discover triggers parameter-sweep --family "$(FAMILY)" --symbol "$(SYMBOLS)" $(if $(OUTPUT_DIR),--output_dir "$(OUTPUT_DIR)",)

advanced-discover-triggers-cluster:
	@$(CLI) discover triggers feature-cluster --symbol "$(SYMBOLS)" $(if $(OUTPUT_DIR),--output_dir "$(OUTPUT_DIR)",)

funnel:
	@test -n "$(RUN_ID)" || (echo 'RUN_ID is required' >&2; exit 2)
	@$(CLI) discover funnel --run_id "$(RUN_ID)" $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",)

CANDIDATE_ID ?=
forward-confirm:
	@test -n "$(RUN_ID)" || (echo 'RUN_ID is required' >&2; exit 2)
	@test -n "$(WINDOW)" || (echo 'WINDOW is required, e.g. 2025-07-01/2025-09-30' >&2; exit 2)
	@$(CLI) validate forward-confirm \
	  --run_id "$(RUN_ID)" \
	  --window "$(WINDOW)" \
	  $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",) \
	  $(if $(PROPOSAL),--proposal "$(PROPOSAL)",) \
	  $(if $(CANDIDATE_ID),--candidate_id "$(CANDIDATE_ID)",)


registries:
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) project/scripts/build_template_registries.py

check-spec-sync:
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) project/scripts/build_template_registries.py --check
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) project/scripts/check_registry_sync.py
