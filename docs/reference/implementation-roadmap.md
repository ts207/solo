# Implementation Roadmap

This repo is being hardened into a governed research-to-runtime trading platform.

## Current priority order

1. Preserve research artifacts: prevent silent `RUN_ID` collisions.
2. Make the vocabulary explicit: detector, event, context, regime, filter, template, proposal, thesis, policy.
3. Make detector events self-explaining: context present, missing, defaulted, and data quality.
4. Add preflight and diagnostics before adding more research machinery.
5. Harden runtime defaults before any live-capital path.

## Non-goals for this cleanup

- Do not tune detector thresholds for PnL.
- Do not add broad discovery surfaces.
- Do not add composite/proxy detectors.
- Do not auto-create filters from winning backtests.

## North star

No unexplained edge. No silent artifact mutation. No hidden filters. No implicit deployment.
