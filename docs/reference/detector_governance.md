# Detector Governance

Updated: 2026-04-18

Detector governance is code-derived. The canonical implementation-facing capability fields live on detector classes and flow into `DetectorContract` through `project/events/registry.py`.

## Canonical Metadata

Each governed detector is classified with one normalized metadata surface:

- `event_version`
- `required_columns`
- `supports_confidence`
- `supports_severity`
- `supports_quality_flag`
- `cooldown_semantics`
- `merge_key_strategy`
- `role`
- `evidence_mode`
- `maturity`
- `detector_band`
- `planning_default`
- `promotion_eligible`
- `runtime_default`
- `primary_anchor_eligible`

Registry/spec rows may restate these fields for readability, but parity validation treats implementation metadata as authoritative for the implementation-facing capability fields.

## Eligibility Model

The generated eligibility matrix under [docs/generated/detector_eligibility_matrix.md](/home/irene/Edge/docs/generated/detector_eligibility_matrix.md) is the compact policy table for:

- planning
- promotion
- runtime
- anchor

Interpretation rules:

- `runtime_default=True` means the detector is part of the deployable runtime core.
- `promotion_eligible=True` means the detector can become a thesis candidate.
- `primary_anchor_eligible=True` means the detector can anchor a proposal directly.
- context and composite detectors are governed explicitly and are not inferred from naming.

## Migration Policy

The generated migration ledger under [docs/generated/detector_migration_ledger.md](/home/irene/Edge/docs/generated/detector_migration_ledger.md) assigns every governed detector to one bucket and one target state.

Buckets:

- `runtime_core_first`
- `promotion_eligible_middle_layer`
- `research_perimeter`

Target states:

- `migrate_to_v2`: detector is already v2 or should remain on the v2 migration path.
- `wrap_v1`: detector stays available through adapter compatibility boundaries instead of broad migration.
- `keep_v1`: legacy detector remains available for research planning while not being promoted into runtime or promotion.
- `demote`: detector remains governed but intentionally outside runtime and promotion.
- `retire`: legacy detector should leave the active planning surface instead of receiving migration effort.

Owner mapping:

- `workstream_c`: runtime-core and promotion-eligible detector migration.
- `workstream_b`: base metadata protocol, v1 adapter perimeter, and non-runtime research governance.

## Generated Sources

The main generated governance artifacts are:

- [docs/generated/detector_runtime_matrix.md](/home/irene/Edge/docs/generated/detector_runtime_matrix.md)
- [docs/generated/detector_promotion_matrix.md](/home/irene/Edge/docs/generated/detector_promotion_matrix.md)
- [docs/generated/detector_eligibility_matrix.md](/home/irene/Edge/docs/generated/detector_eligibility_matrix.md)
- [docs/generated/detector_migration_ledger.md](/home/irene/Edge/docs/generated/detector_migration_ledger.md)
- [docs/generated/detector_version_coverage.md](/home/irene/Edge/docs/generated/detector_version_coverage.md)
- [docs/generated/legacy_detector_retirement.md](/home/irene/Edge/docs/generated/legacy_detector_retirement.md)

Regenerate them with:

```bash
PYTHONPATH=. python3 project/scripts/build_detector_governance_artifacts.py --output-dir docs/generated
```

## Enforcement

Governance drift should fail in tests, not in docs review. The minimum checks are:

- registry contract equals implementation metadata for implementation-facing fields
- runtime-default detectors are v2-only
- context detectors are not treated as anchors
- generated governance artifacts are built from the same contract inventory as runtime and planning views
