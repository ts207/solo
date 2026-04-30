# Regime Failure Decomposition

The failure decomposition tool is designed to diagnose *why* a specific regime failed to generate a tradable edge. It isolates gross signal edge from execution friction, and tests alternate entry timings to see if the execution assumptions are hiding a true signal.

## Core Classifications

- `insufficient_data`: Fewer than 50 overlap-suppressed samples exist.
- `no_gross_edge`: The gross signal (before costs) is negative or zero. The mechanism fails logically.
- `cost_killed`: The gross signal is positive, but after applying conservative execution costs, the net return becomes negative.
- `adverse_timing`: The net edge on the signal bar is negative, but delaying the entry by 1 or 2 bars yields a positive edge (indicating microstructural execution traps on the entry bar itself).
- `one_year_artifact`: A positive net edge exists, but it is heavily concentrated (>50%) in a single calendar year, indicating an overfit.

## Usage

```bash
python3 project/scripts/run_failure_decomposition.py --source-run-id <run_id>
```

This generates `data/reports/portfolio_autopsy/failure_decomposition.md` (untracked) containing the diagnostic matrices.
