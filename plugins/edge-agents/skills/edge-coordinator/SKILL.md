---
name: edge-coordinator
description: Coordinate bounded Edge research loops using the repo's current lifecycle surfaces. Use when the user wants stage discipline, drift checks, or help choosing whether to diagnose, formulate, compile, validate, promote, or deploy.
---

# Edge Coordinator

Use this when coordinating a research loop.

## Read first

1. `docs/lifecycle/overview.md`
2. `docs/lifecycle/discover.md`
3. `docs/lifecycle/validate.md`
4. `docs/lifecycle/promote.md`
5. `docs/reference/commands.md`

## Role

- Enforce the sequence `discover -> validate -> promote -> export -> deploy`.
- Keep one bounded regime-scoped question at a time.
- Use explicit run export when the goal is a runtime thesis batch from one run.

## Required discipline

- Prevent scope drift across symbols, dates, templates, trigger families, horizons, regimes, and conditioning axes.
- Reject outputs that widen the original question without justification.
- Stop immediately if the run failed before candidate artifacts exist or if required files are missing in a way that blocks valid diagnosis.

## Standard bounded research flow

1. Read the proposal and identify the exact bounded claim.
2. Run or inspect preflight, lint, and explain output first.
3. Run or inspect the validated plan before execution.
4. Execute at most one bounded run at a time.
5. Diagnose the completed run with the analyst workflow.
6. Use regime or time-slice comparison when stability or confirmation matters.
7. Formulate 1-3 frozen follow-up hypotheses from the diagnosis.
8. Compile only valid hypotheses into repo-native proposal YAML.
9. Review preflight, lint, explain, and plan again before follow-up execution.

## Output standard

- Always state the current stage.
- Always state the next bounded action.
- If stopping, say exactly which rule or artifact blocked progress.
