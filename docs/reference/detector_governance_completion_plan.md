# Detector Governance Completion Plan

Updated: 2026-04-18

This audit compares the repository against the two-speed detector update plan:

- Speed 1: harden a narrow deployable detector core.
- Speed 2: classify the rest of the detector surface as research, context, or composite before tuning families.

## Current Repo State

Generated governance artifacts now report:

- Governed detectors: 71
- V2 detectors: 27
- Legacy detectors: 44
- Legacy retired-safe detectors: 44
- Runtime-default v2 detectors: 9
- Runtime-default non-v2 detectors: 0
- Compatibility aliases: 3
- V2 calibration artifacts: 27
- Roles: 61 trigger, 5 context, 4 composite, 1 research_only
- Bands: 9 deployable_core, 45 research_trigger, 5 context_only, 12 composite_or_fragile

The runtime-default detector set is now the deployable core:

- BASIS_DISLOC
- FND_DISLOC
- LIQUIDATION_CASCADE
- LIQUIDITY_SHOCK
- LIQUIDITY_STRESS_DIRECT
- LIQUIDITY_VACUUM
- SPOT_PERP_BASIS_SHOCK
- VOL_SHOCK
- VOL_SPIKE

## Done

1. Package and test import stability

- `project` now imports without eager circular dependencies.
- `project.live` and `project.strategy.runtime` expose lazy or explicit public exports needed by tests.
- synthetic truth scenario imports resolve from the existing definitions module.

2. Runtime deployable core narrowed

- `project.events.policy.DEPLOYABLE_CORE_EVENT_TYPES` is the single live-safe detector set.
- `LIVE_SAFE_EVENT_TYPES` aliases the deployable core.
- source registry entries now expose detector band, planning eligibility, runtime eligibility, promotion eligibility, and primary-anchor eligibility.
- registry runtime-default eligibility is blocked for non-core v2 detectors.
- legacy detectors cannot be runtime-default, promotion-eligible, or primary-anchor eligible.

3. First-class detector banding exists

- all 71 source registry entries now have one of four bands: deployable_core, research_trigger, context_only, composite_or_fragile.
- generated governance artifacts include detector band counts and band columns in runtime, promotion, and role matrices.
- contract tests assert every source registry entry has the required governance fields.

4. Detector v2 governance coverage exists

- `DetectorContract` surfaces role, maturity, planning default, runtime default, promotion eligibility, primary-anchor eligibility, calibration mode, threshold schema version, merge/cooldown config, confidence/severity/quality capabilities, aliases, required columns, and allowed templates/horizons.
- inventory helpers expose governed, legacy, v2, runtime, and promotion-eligible detector views.
- contract tests cover loading, filtered views, legacy retirement, and runtime-v2-only behavior.

5. Family v2 work is partially implemented

- Wave 1 liquidity, liquidation, and volatility v2 tests are present.
- Wave 2 basis, funding, and OI v2 tests are present.
- Wave 3 desync and regime transition v2 tests are present.
- runtime/promotion tests consume detector maturity and metadata.

6. Calibration registry exists

- registry lookup supports repo artifact roots plus packaged fallback artifacts.
- packaged fallback calibration artifacts exist for BASIS_DISLOC, BETA_SPIKE_EVENT, CROSS_VENUE_DESYNC, LIQUIDITY_VACUUM, OI_FLUSH, and VOL_SHOCK.
- calibration matrix generation is covered by tests.

7. Governance artifact builder works

- `project.scripts.build_detector_governance_artifacts` emits version coverage, runtime matrix, promotion matrix, role inventory, calibration matrix, legacy retirement, band counts, and JSON summary artifacts.

8. Alias policy is compatibility-only

- `spec/events/event_alias_policy.yaml` defines the only event aliases.
- aliases are load-time compatibility shims and cannot create planning, runtime, promotion, or detector identities.
- canonical registered event ids such as LIQUIDITY_STRESS_DIRECT and DEPTH_COLLAPSE now preserve their own identities instead of collapsing into adjacent detectors.
- generated alias policy artifacts are emitted under `docs/generated`.

9. V2 output schema gate exists

- v2 detector output normalization now exposes the target `family` field alongside `canonical_family`.
- empty v2 detector outputs return the full governed output columns.
- malformed non-empty outputs fail schema validation instead of being silently padded.
- all 27 v2 detectors are exercised against the full required v2 output schema.

10. V2 calibration registry coverage exists

- all 27 v2 detectors have packaged calibration artifacts.
- artifacts are keyed by event name, detector version, symbol group, and timeframe group.
- artifacts include dataset lineage, training period, validation period, parameter vector, robustness summary, and failure notes.
- duplicate calibration keys are tested and rejected by coverage checks.

## Remaining Gaps

1. Source spec still carries legacy compatibility fields

- The governed runtime view is now narrow and source YAML has explicit eligibility fields.
- The older `default_executable` field remains for compatibility artifacts, but detector planning/runtime/promotion policy now reads the explicit `planning_eligible`, `runtime_eligible`, `promotion_eligible`, `primary_anchor_eligible`, and `detector_band` fields.
- Required next action: deprecate `default_executable` from authored docs and generated legacy reports after external consumers are confirmed clear.

2. Band classification needs downstream enforcement

- Band classification is now present in source, compiled domain registry rows, active event contracts, event reference artifacts, planning defaults, runtime eligibility, and promotion governance.
- Required next action: audit any older ad hoc scripts outside the tested governance path for role/name-based inference and move them to the contract helpers.

3. Detector output schema is not fully enforced across all implementations

- V2 contract metadata exists, and output-schema tests now cover all 27 v2 detectors.
- Legacy/v1 adapters still need an explicit decision: emit the same schema or remain retired behind compatibility boundaries.

4. Calibration is partial

- Calibration lookup and artifact shape now cover every v2 detector.
- The required key is enforced as `event_name + detector_version + symbol_group + timeframe_group`.
- Threshold updates still need explicit version bump rules and artifact diff checks.
- Current packaged artifacts are baseline governance fixtures. Empirical recalibration with real dataset lineage is still required before any threshold promotion.

5. Validation harness is not complete

- Unit and synthetic fixture coverage exists for important families.
- Deployable-core replay stability, perturbation, future-bar invariance, sparse missing-data, and required-column fail-closed checks now run as a minimum-green-gate test.
- Deployable-core deterministic replay outputs now have a checked-in baseline comparison gate covering event counts, timing, phase/quality distribution, numeric summaries, and stable event signatures.
- Deployable-core known-episode replay now has reproducible market-slice fixtures for basis/funding dislocation and liquidity/liquidation/volatility cascade scenarios, with expected-present and expected-absent detector sets enforced.
- Deployable-core truth review now enforces false-negative, false-positive, event-explosion, confidence, and severity gates over reproducible known-episode fixtures.
- Deployable-core historical exchange-data replay now has one checked-in Bybit BTCUSDT 5m market-context feature slice covering real liquidation and volatility behavior from 2024-01-01 through 2024-01-03.
- Historical exchange-data coverage is still narrow: the pinned slice does not validate spot/perp basis or funding-dislocation behavior because the source feature artifact lacks usable spot coverage for that interval.

6. Generated docs are incomplete for the target state

- Generated coverage and contract docs exist.
- The planned reference docs are still missing or incomplete:
  - `docs/reference/detector_contract.md`
  - `docs/reference/detector_governance.md`
  - `docs/reference/detector_calibration.md`
  - `docs/reference/detector_families.md`
  - per-detector generated pages

7. Alias policy downstream migration still needs monitoring

- Alias shims are now compatibility-only.
- Remaining risk is downstream code that may still semantically expect a canonical event id to be converted to another canonical event id.

## Completion Plan

### Phase 1: Canonical governance source of truth

Owner files:

- `spec/events/event_registry_unified.yaml`
- `spec/events/event_contract_overrides.yaml`
- `project/events/registry.py`
- `project/events/policy.py`
- governance artifact builder scripts

Actions:

- Done: add explicit detector band for all 71 detectors: deployable_core, research_trigger, context_only, composite_or_fragile.
- Done: split planning eligible, runtime eligible, promotion eligible, and primary-anchor eligible into distinct fields.
- Done: keep aliases as load-time compatibility shims only.
- Done: move default planning and runtime eligibility consumers off `default_executable`.
- Done: expose eligibility fields through the compiled domain registry, active event contracts, generated event contract reference, and promotion governance.
- Done: regenerate governance artifacts and assert:
  - runtime-default set is exactly the nine deployable-core detectors.
  - context detectors are never primary anchors.
  - composite/research-only detectors are never runtime-default.
  - legacy detectors remain retired-safe.

Stop condition:

- Met: governance summary reports 71 governed detectors, 0 runtime non-v2, 9 runtime v2, every detector has a band, and alias compatibility policy is explicit and tested.

### Phase 2: Detector v2 contract adapter

Owner files:

- detector base classes under `project/events/detectors/*`
- family helpers under `project/events/families/*`
- `project/events/detector_contract.py`
- output-schema tests

Actions:

- Done for v2 surface: introduce a shared output normalizer for detector emissions.
- Done for v2 surface: require outputs to include event version, family, subtype, phase, evidence mode, severity, confidence, trigger value, threshold snapshot, required-context flag, data-quality flag, merge key, cooldown, source features, and detector metadata.
- Keep v1/legacy detectors behind adapters until retired.

Stop condition:

- Met for v2: one contract test validates the full output schema for every registered v2 detector without detector-specific exceptions.

### Phase 3: Calibration registry completion

Owner files:

- `project/events/calibration/registry.py`
- `project/events/calibration/artifacts/detectors/*`
- calibration artifact builder scripts
- calibration tests

Actions:

- Done: fill calibration artifacts for all 27 v2 detectors.
- Done: enforce lookup by event name, detector version, symbol group, and timeframe group.
- Done: add artifact validation for dataset lineage, training/validation periods, parameter vector, robustness summary, and failure notes.
- Done: add a minimum-green-gate check that threshold-affecting calibration artifact changes require `detector_version` or `threshold_version` movement.

Stop condition:

- Partially met: calibration matrix has no missing rows for v2 detectors, and threshold-affecting artifact edits now require a version bump.
- Remaining stop condition: replace packaged baseline fixtures with empirical calibration artifacts before live threshold promotion.

### Phase 4: Validation harness gates

Owner files:

- `project/tests/events/*`
- synthetic truth fixtures
- replay and perturbation test suites
- promotion governance tests

Actions:

- Done: add a minimum-green-gate validation test for all 9 deployable-core detectors covering replay stability, irrelevant feature perturbation, appended future-bar invariance, sparse inert NaNs, and missing required-column fail-closed behavior.
- Done: add a checked-in deterministic replay baseline and minimum-green-gate checker for all 9 deployable-core detectors.
- Done: add known-episode replay baselines for reproducibly materialized deployable-core market slices and enforce expected-present/expected-absent detector behavior.
- Done: add deployable-core truth review gates for known episodes covering false negatives, false positives, event explosions, confidence floors, and severity floors.
- Done: add a pinned real historical exchange-data replay baseline for a Bybit BTCUSDT 5m market-context feature slice, enforcing expected-present liquidation/volatility detections and expected-absent deployable-core detectors.
- Add synthetic generators for liquidity cliff, liquidation cascade, vol shock then relaxation, funding extreme onset, cross-venue desync, and chop-to-trend shift.
- Extend historical exchange-data replay coverage to spot/perp basis and additional venues/symbols once suitable pinned feature slices include the required counterpart series.
- Add perturbation tests for delayed data, missing counterpart series, sparse volumes, stale timestamps, duplicated rows, and outlier spikes.
- Add invariance tests for bucket-boundary changes, tiny timestamp shifts, single-row omissions, and non-material feature noise.

Stop condition:

- maturity promotion requires contract completeness, calibration artifact, unit/synthetic/replay/perturbation/invariance coverage, and generated operator docs.

### Phase 5: Runtime and promotion hardening

Owner files:

- live event ingestion
- runtime matching
- promotion services
- thesis export and lineage code

Actions:

- Ensure runtime consumes detector version, threshold version, confidence, severity, evidence mode, and data-quality flag.
- Make degraded data, proxy-only evidence, and missing prerequisites fail closed or downgrade deterministically.
- Persist detector metadata into thesis lineage and promotion audit outputs.

Stop condition:

- production promotion is impossible from context-only, research-only, composite, or runtime-disabled primary anchors.

### Phase 6: Documentation generation

Owner files:

- `docs/reference/*`
- `docs/generated/*`
- docs generation scripts

Actions:

- Add reference docs for detector contract, governance, calibration, and families.
- Generate one page per detector with canonical name, aliases, band, role, maturity, evidence mode, runtime/planning/promotion status, calibration version, validation status, inputs, thresholds, failure modes, and template compatibility.

Stop condition:

- a detector can be understood operationally without reading source code.

## Immediate Next Work Unit

The next bounded implementation unit should close Phase 3:

1. Add threshold-change/version-bump enforcement.
2. Add a clear `baseline_fixture` versus `empirical_calibration` promotion rule.
3. Start empirical recalibration replacement for deployable-core detectors first.
