# Contract hardening continuation

This patch continues from the available `solo_ready_v8_contract_hardened_v3.zip` base.

Implemented:

- Moves live approval/cap-profile enforcement out of `PromotedThesis` model construction and into runtime admission.
- Adds runtime manifest validation for mode allowance, expiry, thesis/state consistency, and required contract/evidence/risk hashes for shadow/trading.
- Treats zero-valued global runtime exposure caps as disabled while preserving explicit per-family caps.
- Extends detector contracts with polarity/magnitude/severity-bucket support flags.
- Adds default `BaseDetectorV2` event-side, magnitude, and severity-bucket emission.
- Makes promotion cost-survival prefer explicit evaluator stress metrics when present.
- Adds evaluator mechanism diagnostics: `mechanism_success_rate`, `mechanism_label`, `mechanism_valid`.

Validation performed in this environment:

- Python compilation for patched modules.
- Direct runtime-admission smoke tests.
- Domain graph rebuild and freshness check.
- Zip integrity check.

Full repository pytest was not completed because targeted pytest invocations timed out in this execution environment.
