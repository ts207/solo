ROOT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)
PYTHON ?= .venv/bin/python
SYMBOLS ?= BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,LINKUSDT,AVAXUSDT,ADAUSDT,DOGEUSDT,LTCUSDT
BOOK_TICKER_RUN_ID ?= bybit_book_ticker_snapshot
AUDIT_SYMBOLS ?= $(SYMBOLS)
AUDIT_YEARS ?= 2022,2023,2024,2025
VARIANT ?= FUNDING_WINDOW_DRIFT
FORWARD_YEARS ?= 2026

.PHONY: help detector-shadow-report detector-variant-validation detector-tune detector-exit-lab detector-targeted-expansion detector-mtf-lab detector-mtf-diagnose detector-regime-lab detector-oi-flush-lab detector-oi-flush-forward-shadow detector-cross-sectional-lab detector-vol-compression-lab detector-funding-divergence-lab detector-session-lab detector-session-diagnose detector-time-of-day-lab detector-slow-timeframe-lab forward-shadow-status data-feed-audit ingest-bybit-book-ticker

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
	@$(PYTHON) -m project.scripts.detector_oi_flush_lab $(if $(DEEP_REGIME),--deep-regime-grid,) $(if $(FULL_EXIT),--full-exit-grid,)

detector-oi-flush-forward-shadow:
	@$(PYTHON) -m project.scripts.detector_oi_flush_forward_shadow --symbols "$(SYMBOLS)" --years "$(FORWARD_YEARS)"

detector-cross-sectional-lab:
	@$(PYTHON) -m project.scripts.detector_cross_sectional_lab --symbols "$(SYMBOLS)" --years "$(AUDIT_YEARS)"

detector-vol-compression-lab:
	@$(PYTHON) -m project.scripts.detector_vol_compression_lab --symbols "$(SYMBOLS)" --years "$(AUDIT_YEARS)"

detector-funding-divergence-lab:
	@$(PYTHON) -m project.scripts.detector_funding_divergence_lab --symbols "$(SYMBOLS)" --years "$(AUDIT_YEARS)"

detector-session-lab:
	@$(PYTHON) -m project.scripts.detector_session_lab --symbols "$(SYMBOLS)" --years "$(AUDIT_YEARS)"

detector-session-diagnose:
	@$(PYTHON) -m project.scripts.detector_session_diagnose --variant "$(VARIANT)" --symbols "$(SYMBOLS)" --years "$(AUDIT_YEARS)"

detector-time-of-day-lab:
	@$(PYTHON) -m project.scripts.detector_time_of_day_lab --symbols "$(SYMBOLS)" --years "$(AUDIT_YEARS)"

detector-slow-timeframe-lab:
	@$(PYTHON) -m project.scripts.detector_slow_timeframe_lab --symbols "$(SYMBOLS)" --years "$(AUDIT_YEARS)"

forward-shadow-status:
	@$(PYTHON) -m project.scripts.forward_shadow_status --symbols "$(SYMBOLS)"

data-feed-audit:
	@$(PYTHON) -m project.scripts.data_feed_audit --symbols "$(AUDIT_SYMBOLS)" --years "$(AUDIT_YEARS)"

ingest-bybit-book-ticker:
	@$(PYTHON) -m project.pipelines.ingest.ingest_bybit_derivatives_book_ticker --run_id "$(BOOK_TICKER_RUN_ID)" --symbols "$(SYMBOLS)"

%:
	@$(MAKE) -C $(ROOT_DIR) $@
