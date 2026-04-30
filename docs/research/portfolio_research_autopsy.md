# Portfolio Research Autopsy

The portfolio research autopsy is a global report generator that finalizes the mechanism discovery cycle. It enforces strict "NO-GO" decisions on fundamentally unviable regimes.

## Purpose

When multiple mechanisms fail at the baseline structural level (either due to no gross edge or being `cost_killed`), we formally park them. This report outlines the terminal decision and the strict "reopen only if" conditions required to ever look at these mechanism paths again.

## Usage

```bash
python3 project/scripts/update_portfolio_research_autopsy.py
```

Outputs the final markdown NO-GO report to `data/reports/portfolio_autopsy/portfolio_research_autopsy.md`.
