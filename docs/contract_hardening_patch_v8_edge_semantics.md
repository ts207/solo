# Contract Hardening v8 — Edge Semantics

This patch adds the missing semantic layer between event emission and edge promotion.

## Added

- `project/events/polarity.py` as the canonical polarity/semantics module.
- First-class event-output fields:
  - `polarity_semantics`
  - `polarity_source`
  - `magnitude_source`
  - `anchor_role`
- Registry-level polarity semantics and anchor-role classifications for all authored events.
- Polarity-aware compatibility verdicts that prevent, for example, basis-spread events from passing as outright price-direction templates.
- Promotion mechanism-evidence gate for concrete mechanism templates.
- Detector polarity audit script and generated outputs:
  - `project/scripts/audit_detector_polarity.py`
  - `data/reports/detector_polarity_audit.csv`
  - `docs/generated/detector_polarity_audit.md`
- Regression tests in `project/tests/events/test_edge_semantics_v8.py`.

## Design invariant

A candidate edge must not be evaluated or promoted as a directional strategy unless its event side has declared semantics and the selected template is compatible with those semantics.

Examples:

- `BASIS_DISLOC + trend_continuation` is forbidden because basis side is not price side.
- `BASIS_DISLOC + basis_convergence` is semantically valid.
- Execution/temporal guards cannot anchor standalone alpha templates.

## Remaining work

- Add explicit detector-class overrides for high-value events whose polarity should not rely on generic inference.
- Expand event-template compatibility matrix coverage for all primary/promotion candidates.
- Add variant detector adapters for funding and price/OI events that are authored as registry variants.
