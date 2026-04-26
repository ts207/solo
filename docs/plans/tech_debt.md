# Technical debt plan

Status: in-progress  
Owner: core maintainers  
Target date: rolling  
Success criteria: CI, pre-commit, registry sync, research-rigor gates, funnel artifacts, and deploy confirmation are enforced before broader module decomposition.

## Done or implemented in this bundle

- Research feasibility fails closed for zero-feasible runs unless explicitly allowed.
- Phase-2 gating computes gross and net diagnostics and gates on net t-stat/net expectancy.
- Funnel artifacts are emitted and queryable.
- Deploy promotion requires a forward-confirmation artifact.
- CI workflows were added for lint, fast tests, coverage on critical packages, and minimum-green gates.
- Pre-commit hooks were added for formatting, registry sync, banned imports, and commit-message hygiene.
- PR template and known-limitations document were added.

## In progress

- Registry generation should be moved to a single source of truth rather than checked only for drift.
- Large modules should be decomposed through pure-move PRs that preserve public re-export surfaces.
- Atomic-write enforcement should move from convention to architectural test coverage.

## Deferred

- Correlation-aware multiplicity correction.
- Cell-first discovery default.
- Scenario sandbox and chaos drills.
- Public benchmark suite.
