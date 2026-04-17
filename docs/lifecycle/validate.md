# Validate stage

## CLI

```bash
edge validate run            --run_id <run_id>
edge validate report         --run_id <run_id>
edge validate diagnose       --run_id <run_id> [--program_id <prog>]
edge validate list-artifacts --run_id <run_id>
```

---

## What validate does

Validate answers a narrower question than discover: not "can this search space produce candidates?" but "do the selected candidates remain credible under formal gates, regime slicing, and robustness testing?"

It reads whichever candidate table exists (in priority order):
1. `data/reports/promotions/<run_id>/promotion_statistical_audit.parquet`
2. `data/reports/edge_candidates/<run_id>/edge_candidates_normalized.parquet`
3. `data/reports/phase2/<run_id>/phase2_candidates.parquet`

---

## Code path

```
project/validate/__init__.py                         ← façade
  └─ project/research/services/evaluation_service.py ← builds ValidationBundle
       └─ project/research/validation/result_writer.py ← writes canonical outputs
```

---

## Outputs

All under `data/reports/validation/<run_id>/`:

| File | Type | Purpose |
|------|------|---------|
| `validation_bundle.json` | JSON | Narrative summary + structured contract |
| `validation_report.json` | JSON | Detailed per-candidate report |
| `effect_stability_report.json` | JSON | Fold stability and robustness detail |
| `validated_candidates.parquet` | Parquet | Machine-readable validated set |
| `rejection_reasons.parquet` | Parquet | Why each candidate was rejected |
| `promotion_ready_candidates.parquet` | Parquet | Promotion intake table |

`promotion_ready_candidates.parquet` is the canonical handoff into promotion. The promotion service expects this table to be already normalized and machine-readable.

---

## Reporting and diagnostics

`edge validate report` calls `project/operator/stability.py:write_regime_split_report` — produces regime/stability views for manually reviewing effect concentration.

`edge validate diagnose` calls `project/operator/stability.py:write_negative_result_diagnostics` — useful for understanding why a run that should have found something didn't.

---

## Common failure modes

- No candidate tables exist for the run → discover stage needs to produce results first
- Candidate table is empty after filtering → usually an event+template incompatibility upstream
- Artifacts are malformed → fix the producing stage; do not patch the validation writer to accept bad payloads
