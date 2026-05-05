# Contract hardening patch v3

This patch continues the event/regime/template strategy-hardening plan with promotion/runtime and portfolio admission integration.

## Added

- Extended thesis deployment maturity states:
  - `candidate`
  - `validated`
  - `forward_confirmed`
  - `paper_enabled`
  - `paper_approved`
  - `shadow_enabled`
  - `micro_live_approved`
  - `scaled_live_approved`
- Added runtime-mode admission helper `deployment_state_allows_runtime(...)`.
- Added `RuntimeThesisManifest` for immutable runtime-admission metadata.
- Added optional strict paper-gate checks for:
  - paper/live signal parity
  - feature freshness
  - event detection latency
  - paper fill attribution
  - thesis reconciliation errors
- Added portfolio conflict arbitration via `PortfolioAdmissionPolicy.resolve_signal_conflicts(...)`.

## Runtime policy

- `monitor_only`: any non-retired/non-disabled thesis.
- `simulation`: paper-enabled or stronger.
- `shadow`: shadow-enabled or stronger.
- `trading`: `micro_live_approved`, `scaled_live_approved`, or legacy `live_enabled` only.

## Portfolio arbitration policy

- Guard/veto candidates can reject alpha candidates.
- Active overlap groups are excluded.
- Opposing same-symbol/timeframe/horizon signals are resolved by risk-adjusted support score.
- Same-side concentration in the same bucket is accepted but downscaled.

## Validation performed

```bash
python -m py_compile project/live/contracts/promoted_thesis.py \
  project/live/runtime_admission.py \
  project/live/thesis_state.py \
  project/live/paper_gate.py \
  project/portfolio/admission_policy.py

PYTHONPATH=. python project/scripts/build_domain_graph.py
PYTHONPATH=. python project/scripts/check_domain_graph_freshness.py

PYTHONPATH=. pytest -q \
  project/tests/contracts/test_promoted_thesis_contract.py \
  project/tests/promote/test_paper_gate.py \
  project/tests/live/test_runtime_admission.py \
  --tb=short
```

All targeted checks above passed.
