PYTHON ?= python3
PYTHONPATH ?= .
CLI := PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m project.cli

RUN_ID ?=
PROPOSAL ?=
SYMBOLS ?= BTCUSDT,ETHUSDT
TIMEFRAME ?= 5m
START ?=
END ?=
DATA_ROOT ?=
OUT_DIR ?= data/runtime
RUNTIME_MODE ?= paper
REGISTRY_ROOT ?= project/configs/registries
SPEC_DIR ?= spec/discovery
EXECUTE ?= 0
RUNTIME_MAX_ROWS ?= 500

.PHONY: help discover validate promote deploy-paper \
	check-domain-graph domain-graph benchmark-supported-path \
	discover-plan discover-cells-verify discover-cells-plan discover-cells-run

help:
	@printf '%s\n' \
	  'Canonical stage targets:' \
	  '  make discover PROPOSAL=<proposal.yaml> RUN_ID=<run_id> [DATA_ROOT=...] [REGISTRY_ROOT=...]' \
	  '  make validate RUN_ID=<run_id> [DATA_ROOT=...]' \
	  '  make promote RUN_ID=<run_id> SYMBOLS=BTCUSDT,ETHUSDT [OUT_DIR=...]' \
	  '  make deploy-paper RUN_ID=<run_id> OUT_DIR=<dir> [SYMBOLS=...] [DATA_ROOT=...]' \
	  '' \
	  'Auxiliary targets:' \
	  '  make check-domain-graph' \
	  '  make domain-graph' \
	  '  make benchmark-supported-path EXECUTE=0|1 [DATA_ROOT=...] [RUNTIME_MAX_ROWS=500]' \
	  '  make discover-cells-verify RUN_ID=<run_id> START=<start> END=<end> [SPEC_DIR=spec/discovery]' \
	  '  make discover-cells-plan   RUN_ID=<run_id> START=<start> END=<end> [SPEC_DIR=spec/discovery/expanded_v2]' \
	  '  make discover-cells-run    RUN_ID=<run_id> START=<start> END=<end> [SPEC_DIR=spec/discovery/expanded_v2]'

check-domain-graph:
	@$(CLI) validate specs --root . >/dev/null
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) project/scripts/check_domain_graph_freshness.py

domain-graph:
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) project/scripts/build_domain_graph.py
	@PYTHONPATH=$(PYTHONPATH) $(PYTHON) project/scripts/check_domain_graph_freshness.py

discover-plan:
	@test -n "$(PROPOSAL)" || (echo 'PROPOSAL is required' >&2; exit 2)
	@$(CLI) discover plan --proposal "$(PROPOSAL)" $(if $(RUN_ID),--run_id "$(RUN_ID)",) $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",) --registry_root "$(REGISTRY_ROOT)"

discover:
	@test -n "$(PROPOSAL)" || (echo 'PROPOSAL is required' >&2; exit 2)
	@$(CLI) discover run --proposal "$(PROPOSAL)" $(if $(RUN_ID),--run_id "$(RUN_ID)",) $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",) --registry_root "$(REGISTRY_ROOT)"

validate:
	@test -n "$(RUN_ID)" || (echo 'RUN_ID is required' >&2; exit 2)
	@$(CLI) validate run --run_id "$(RUN_ID)" $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",)

promote:
	@test -n "$(RUN_ID)" || (echo 'RUN_ID is required' >&2; exit 2)
	@test -n "$(SYMBOLS)" || (echo 'SYMBOLS is required' >&2; exit 2)
	@$(CLI) promote run --run_id "$(RUN_ID)" --symbols "$(SYMBOLS)" $(if $(OUT_DIR),--out_dir "$(OUT_DIR)",)

deploy-paper:
	@test -n "$(RUN_ID)" || (echo 'RUN_ID is required' >&2; exit 2)
	@test -n "$(OUT_DIR)" || (echo 'OUT_DIR is required' >&2; exit 2)
	@mkdir -p "$(OUT_DIR)"
	@$(CLI) deploy bind-config --run_id "$(RUN_ID)" --out_dir "$(OUT_DIR)" --runtime_mode "$(RUNTIME_MODE)" --symbols "$(SYMBOLS)" $(if $(DATA_ROOT),--data_root "$(DATA_ROOT)",)

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
