# Design Spec: True OOS Forward Confirmation

Forward-confirm should be an independent execution path at the orchestration level, but it should reuse existing deterministic eval/event/return/cost components underneath. It must perform a true Out-Of-Sample (OOS) rerun of a frozen thesis rather than snapshotting existing research artifacts.

## Problem
The current `forward-confirm` implementation merely snapshots representative metrics from `phase2_candidates.parquet`. This does not provide actual OOS evidence and can hide selection bias or leakage from the research phase.

## Goals
- Implement a true OOS replay orchestration.
- Re-use existing deterministic evaluation primitives.
- Forbid research artifact reuse for metrics.
- Ensure strict OOS boundaries (no window overlap).
- **Zero Selection Leakage**: The forward-confirm command must never rank, sort, or select candidates during OOS confirmation.

## Architecture

### 1. Orchestration Boundary
`forward-confirm` will be updated to:
1.  **Load Frozen Thesis**: Identify the thesis to be confirmed. The forward-confirm command must never rank, sort, or select candidates. The loading priority is:
    -   Explicit `--proposal` path (YAML), if provided.
    -   `data/live/theses/<run_id>/promoted_theses.json` (if promotion already happened).
    -   `run_manifest.json` exact proposal path / frozen thesis ID.
    -   Explicit `--candidate_id`, if that candidate ID was frozen earlier.
    -   Otherwise fail.
    -   *Note*: `phase2_candidates.parquet` may only be read when an immutable `candidate_id`/`thesis_id` is supplied by a frozen manifest or CLI argument.
2.  **Validate OOS Window**: Ensure the requested OOS window does not overlap with the original research/selection window.
3.  **Load OOS Data**: Load market data (bars/features) for the requested OOS window.
    -   Feature warmup bars before `oos_start` are allowed for causal rolling features.
4.  **Recompute Evidence**: detect events, compute forward returns, and apply cost models for the OOS window using the frozen thesis.
    -   Metrics may only include signals with `signal_ts` inside `[oos_start, oos_end]`.
    -   **Simple Safe Rule**: Drop signals where `exit_ts > oos_end`.
5.  **Score**: Aggregate metrics (t-stat, mean return, etc.) for the OOS signal set.
6.  **Persist**: Write `forward_confirmation.json` and a detailed evidence bundle.

### 2. Component Reuse
The following deterministic components will be reused from `project.research` and `project.eval`:
- `project.research.search.evaluator.evaluate_hypothesis_batch`: Core metric computation.
- `project.research.search.search_feature_utils.prepare_search_features_for_symbol`: Data loading and feature preparation.
- `project.domain.hypotheses.HypothesisSpec`: Thesis definition.

### 3. Data Flow
```
[Frozen Thesis Spec] + [OOS Market Data] 
    -> [Evaluator] 
    -> [OOS Metrics] 
    -> [forward_confirmation.json]
```

## Hard Leakage Rules
- **No Overlap**: Fail if OOS window overlaps with the original research window.
- **No Search**: Searching, ranking, or tuning is strictly forbidden during forward-confirm.
- **No Artifact Reuse**: `phase2_candidates.parquet` must NOT be used as a metric source.
- **No Sorting**: The loader must not call `sort_values`, `idxmax`, `nlargest`, or use `rank_score` to choose a candidate.

## Implementation Details

### File Changes
- `project/validate/forward_confirm.py`:
    - Deprecate `phase2_candidate_metric_snapshot` (replace with fail-closed placeholder initially).
    - Implement `oos_frozen_thesis_replay_v1`.
    - Add logic to load the frozen thesis from `run_id`, `--proposal`, or `--candidate_id`.
    - Add logic to call `evaluate_hypothesis_batch` on OOS data.

### CLI Arguments
Add support for:
- `--proposal <path>`: Path to a frozen proposal YAML.
- `--candidate_id <id>`: Specific candidate ID to confirm.

### Schema
The output `forward_confirmation.json` will follow this structure:
```json
{
  "run_id": "...",
  "confirmed_at": "...",
  "oos_window_start": "...",
  "oos_window_end": "...",
  "method": "oos_frozen_thesis_replay_v1",
  "metrics": {
    "event_count": 120,
    "trade_count": 120,
    "mean_return_net_bps": 4.5,
    "t_stat_net": 2.1,
    "hit_rate": 0.55,
    "mae_bps": 12.0,
    "mfe_bps": 18.0
  },
  "evidence_bundle_path": "...",
  "source": {
    "thesis_path": "...",
    "data_root": "...",
    "window": "..."
  }
}
```

## Testing Strategy
- **Unit Tests**:
    - Verify `forward-confirm` fails on window overlap.
    - Verify `forward-confirm` does not read `phase2_candidates.parquet` for metrics.
    - Verify OOS rows correctly determine metrics.
- **Anti-Regression Test**:
    - Fail if `forward-confirm` loader logic contains `sort_values`, `idxmax`, `nlargest`, or `rank_score`.
- **Integration Tests**:
    - Run `forward-confirm` on a smoke test `run_id` and verify the produced JSON.

## Separate Task: CI Improvements
- **Patch CI branch trigger**: Update `.github/workflows/ci.yml` to trigger on both `Main` and `main` branches. This will be a separate, isolated PR/commit.
