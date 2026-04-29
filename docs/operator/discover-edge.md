# Discover Edge Operator Guide

## Goal

Find a bounded candidate edge. Discovery is not live-readiness.

Discovery produces evidence artifacts. Validation, promotion, and explicit operator approval are all required before any thesis reaches live capital.

---

## Default path

1. Check repo and spec health.
2. Run cell discovery.
3. Summarize cell results.
4. Assemble generated proposals.
5. Inspect one generated proposal.
6. Run canonical discovery (proposal → phase-2).
7. Run discover-doctor to gate on quality.
8. Validate.
9. Forward-confirm on a held-out window.
10. Promote only if validation and forward confirmation both justify it.

---

## Minimal command path

### 1. Check repo health

```bash
make check-domain-graph
```

### 2. Run cell discovery

```bash
make discover \
  RUN_ID=<run_id> \
  DATA_ROOT=<lake> \
  START=<start> \
  END=<end> \
  SYMBOLS=BTCUSDT \
  SPEC_DIR=<surface>
```

This runs `discover cells run` → `summarize` → `assemble-theses` in sequence.

### 3. Summarize results

```bash
make summarize RUN_ID=<run_id> DATA_ROOT=<lake>
```

### 4. Inspect a generated proposal

```bash
make proposal-inspect \
  PROPOSAL=data/runs/<run_id>/generated_proposals/<proposal>.yaml \
  RUN_ID=<run_id> \
  DATA_ROOT=<lake>
```

### 5. Run canonical discovery on the proposal

```bash
make discover-proposal \
  PROPOSAL=data/runs/<run_id>/generated_proposals/<proposal>.yaml \
  RUN_ID=<run_id> \
  DATA_ROOT=<lake>
```

### 6. Run discover-doctor (required quality gate)

```bash
make discover-doctor RUN_ID=<run_id> DATA_ROOT=<lake>
```

- **Exit 0** (`validate_ready` / `review_candidate`): proceed only as directed by `next_safe_command`. `validate_ready` is still candidate evidence, not an edge claim.
- **Exit 1** (`blocked` / `rejected`): do not validate. Inspect `phase2_diagnostics.json` and address or move to the next bounded cell.

The JSON report includes `evidence_class`, `requires`, `next_safe_command`, and `forbidden_rescue_actions`. Do not call a candidate an edge until forward confirmation passes.

### 7. Validate

```bash
make validate RUN_ID=<run_id> DATA_ROOT=<lake>
```

### 8. Forward-confirm on a held-out window

```bash
make forward-confirm \
  RUN_ID=<run_id> \
  WINDOW=<forward_start>/<forward_end> \
  DATA_ROOT=<lake> \
  PROPOSAL=data/runs/<run_id>/generated_proposals/<proposal>.yaml
```

### 9. Promote

Only after steps 7 and 8 both pass:

```bash
make promote RUN_ID=<run_id> SYMBOLS=BTCUSDT
```

---

## Equivalent CLI commands

```bash
edge discover cells run --run_id <run_id> --data_root <lake> --start <start> --end <end>
edge discover cells summarize --run_id <run_id> --data_root <lake>
edge discover cells assemble-theses --run_id <run_id> --data_root <lake>
edge discover cells assemble-theses --run_id <run_id> --data_root <lake> --per-cell --limit 8
edge proposal inspect --proposal data/runs/<run_id>/generated_proposals/<proposal>.yaml --run_id <run_id>
edge discover run --proposal data/runs/<run_id>/generated_proposals/<proposal>.yaml --run_id <run_id>
# (run discover-doctor via make — no direct edge CLI subcommand)
edge validate run --run_id <run_id>
edge validate forward-confirm \
  --run_id <run_id> \
  --window <forward_start>/<forward_end> \
  --proposal data/runs/<run_id>/generated_proposals/<proposal>.yaml
edge promote run --run_id <run_id> --symbols BTCUSDT
```

---

## Diagnosing zero-hypothesis or empty-candidate cases

```bash
edge discover explain-empty --run_id <run_id> --data_root <lake>
edge discover funnel --run_id <run_id> --data_root <lake>
```

Inspect:
- `data/artifacts/experiments/<program_id>/<run_id>/validated_plan.json` — `estimated_hypothesis_count`
- `data/reports/phase2/<run_id>/phase2_diagnostics.json` — feature rows, metric rows, rejection counts

---

## Stop conditions

Stop and do not proceed to the next stage when:

| Condition | Action |
|---|---|
| `validated_plan.json` has `estimated_hypothesis_count == 0` | Inspect event/template compatibility; use `explain-empty` |
| `phase2_diagnostics.json` shows zero feature rows | Check data root, lake coverage, and start/end window |
| `phase2_diagnostics.json` shows zero metric rows | Check event flag columns and phase-2 gate thresholds |
| Candidate table is empty after discovery | Relax search spec or widen cell surface before retrying |
| `validation_bundle.json` is missing after validate | Rerun validation; check pipeline errors |
| `promotion_ready_candidates.parquet` is missing | Inspect `rejection_reasons.parquet` |
| `forward_confirmation.json` fails or sign-flips | Do not promote; investigate regime change or overfitting |
| `promotion_diagnostics.json` contains `error` | Fix the error before promoting |

---

## AI-agent boundary

An AI agent operating in this repo may:

- Run any `discover`, `validate`, `forward-confirm`, and `promote` commands above.
- Inspect generated artifacts (`phase2_diagnostics.json`, `validated_plan.json`, etc.).
- Create bounded proposal YAML.
- Run `make discover-doctor` and respect its exit code.

An AI agent must **not**:

- Run `edge deploy live-run` or `make live-run`.
- Set `runtime_mode=trading` without explicit human authorization.
- Edit `data/live/theses/**` or `data/reports/approval/**`.
- Treat discovery or promotion output as live approval.

See `AGENTS.md` for the full operating contract.
