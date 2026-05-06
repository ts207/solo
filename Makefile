ROOT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)
PYTHON ?= .venv/bin/python

.PHONY: help detector-shadow-report detector-variant-validation detector-tune detector-exit-lab detector-targeted-expansion detector-mtf-lab detector-mtf-diagnose detector-regime-lab detector-oi-flush-lab

help:
	@$(MAKE) -C $(ROOT_DIR) help

detector-shadow-report:
	@$(PYTHON) -m project.scripts.detector_shadow_report

detector-variant-validation:
	@$(PYTHON) -m project.scripts.detector_variant_validation

detector-tune:
	@$(PYTHON) -m project.scripts.detector_tuning_lab

detector-exit-lab:
	@$(PYTHON) -m project.scripts.detector_exit_lab

detector-targeted-expansion:
	@$(PYTHON) -m project.scripts.detector_targeted_expansion $(if $(FAMILY),--family "$(FAMILY)",)

detector-mtf-lab:
	@$(PYTHON) -m project.scripts.detector_mtf_lab

detector-mtf-diagnose:
	@$(PYTHON) -m project.scripts.detector_mtf_diagnose $(if $(VARIANT),--variant "$(VARIANT)",)

detector-regime-lab:
	@$(PYTHON) -m project.scripts.detector_regime_lab

detector-oi-flush-lab:
	@$(PYTHON) -m project.scripts.detector_oi_flush_lab $(if $(DEEP_REGIME),--deep-regime-grid,)

%:
	@$(MAKE) -C $(ROOT_DIR) $@
