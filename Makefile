ROOT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)

.PHONY: help

help:
	@$(MAKE) -C $(ROOT_DIR) help

%:
	@$(MAKE) -C $(ROOT_DIR) $@
