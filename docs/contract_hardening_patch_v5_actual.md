# Contract hardening patch v5 actual

Continuation from `solo_ready_v8_contract_hardened_v4_actual.zip`.

## Added

- Promotion hard blocks now treat upstream event-template compatibility verdicts as authoritative.
  - `compatibility_status=forbidden` and `compatibility_status=research_only` cannot promote.
  - `compatibility_promotion_allowed=false` cannot promote.
  - Promotion rejection carries the first compatibility reason code when available.

- Promotion hard blocks now require side-policy resolution for promotion-path rows.
  - Explicit `direction=long|short` is accepted.
  - Side-policy-driven rows require event polarity via `event_side` or non-zero `event_direction`.
  - Missing polarity produces `side_policy_resolution_missing_event_polarity`.

- Runtime thesis loading can now require manifest admission at load time.
  - `ThesisStore.from_path(..., require_runtime_manifest=True, runtime_mode=...)`
  - `ThesisStore.from_run_id(..., require_runtime_manifest=True, runtime_mode=...)`
  - The live runner passes these flags when `strategy_runtime.implemented=true`.

- Runtime mode vocabulary now includes `shadow` in the runner mode precheck.

- Cell discovery now initializes thesis eligibility deterministically for unconditional cells.

## Validation

Validated with:

```bash
python -m py_compile \
  project/research/promotion/promotion_decisions.py \
  project/research/cell_discovery/compiler.py \
  project/live/thesis_store.py \
  project/live/runner.py \
  project/tests/live/test_contract_hardening_v5_actual.py

PYTHONPATH=. python project/scripts/build_domain_graph.py
PYTHONPATH=. python project/scripts/check_domain_graph_freshness.py
```

Result:

```text
domain graph freshness: OK
```

Also ran direct smoke calls covering:

- compatibility promotion blocking
- side-policy polarity enforcement
- explicit direction bypass
- runtime manifest-required thesis-store rejection path

The full pytest suite was not completed in this execution environment.
