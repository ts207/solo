ROOT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)

.PHONY: help
.PHONY: benchmark-edge-cells

help:
	@$(MAKE) -C $(ROOT_DIR) help

benchmark-edge-cells:
	@PYTHONPATH=. ./.venv/bin/python -m project.cli discover cells run \
		--run_id BENCHMARK_EDGE_CELLS \
		--symbols BTCUSDT \
		--start 2024-01-01 \
		--end 2024-03-01
	@PYTHONPATH=. ./.venv/bin/python -m project.cli discover cells assemble-theses \
		--run_id BENCHMARK_EDGE_CELLS

%:
	@$(MAKE) -C $(ROOT_DIR) $@
