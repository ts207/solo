# Edge documentation

This docset is meant to stand on its own. Read it as the map of the repository, the lifecycle guide for the trading system, and the operator handbook for running it safely.

## Start here

If you want the shortest path to a working model of the repo:

1. [lifecycle/overview.md](lifecycle/overview.md)
2. [reference/full_repo_surface.md](reference/full_repo_surface.md)
3. [reference/repository_map.md](reference/repository_map.md)
4. [reference/architecture.md](reference/architecture.md)

That sequence gives you:
- the lifecycle the repo is built around,
- the full directory and package surface,
- the ownership of each major package,
- the structural rules enforced by tests.

## Reading paths by goal

### Understand the system end to end

1. [lifecycle/overview.md](lifecycle/overview.md)
2. [lifecycle/discover.md](lifecycle/discover.md)
3. [lifecycle/validate.md](lifecycle/validate.md)
4. [lifecycle/promote.md](lifecycle/promote.md)
5. [lifecycle/deploy.md](lifecycle/deploy.md)
6. [operator/runbook.md](operator/runbook.md)

### Understand the full repository surface

1. [reference/full_repo_surface.md](reference/full_repo_surface.md)
2. [reference/repository_map.md](reference/repository_map.md)
3. [reference/commands.md](reference/commands.md)
4. [reference/assurance.md](reference/assurance.md)

### Add or modify specs

1. [reference/spec_authoring.md](reference/spec_authoring.md)
2. [lifecycle/discover.md](lifecycle/discover.md)
3. [reference/architecture.md](reference/architecture.md)
4. [reference/assurance.md](reference/assurance.md)

### Run paper or live safely

1. [lifecycle/promote.md](lifecycle/promote.md)
2. [lifecycle/deploy.md](lifecycle/deploy.md)
3. [operator/runbook.md](operator/runbook.md)

## Lifecycle guides

| Stage | File | Main question answered |
|-------|------|------------------------|
| Overview | [lifecycle/overview.md](lifecycle/overview.md) | What is this system, and what invariants govern it? |
| Discover | [lifecycle/discover.md](lifecycle/discover.md) | How does a proposal become a governed discovery run? |
| Validate | [lifecycle/validate.md](lifecycle/validate.md) | How do candidate rows become a formal validation bundle? |
| Promote | [lifecycle/promote.md](lifecycle/promote.md) | How do validated candidates become promoted theses? |
| Deploy | [lifecycle/deploy.md](lifecycle/deploy.md) | How does runtime load and act on promoted theses? |

## Reference docs

| File | Main coverage |
|------|---------------|
| [reference/full_repo_surface.md](reference/full_repo_surface.md) | Top-level directories, major packages, spec tree, artifact tree, tests, entrypoints |
| [reference/repository_map.md](reference/repository_map.md) | Package ownership, dependency flow, recommended reading order |
| [reference/architecture.md](reference/architecture.md) | Enforced import boundaries and extension rules |
| [reference/commands.md](reference/commands.md) | CLI commands, scripts, Make targets, operational entrypoints |
| [reference/spec_authoring.md](reference/spec_authoring.md) | Event, template, proposal, and generated-domain workflow |
| [reference/assurance.md](reference/assurance.md) | Test surface, governance, certification, and minimum-green checks |

## Operations

| File | Main coverage |
|------|---------------|
| [operator/runbook.md](operator/runbook.md) | Practical paper-engine bring-up, shutdown, restart, and failure handling |

## Research views

| File | Main coverage |
|------|---------------|
| [research/results.md](research/results.md) | Results index and result surfaces |
| [research/campaign_results.md](research/campaign_results.md) | Campaign-style summaries |
| [research/reflections.md](research/reflections.md) | Pattern summaries and research reflections |
| [research/narrative.md](research/narrative.md) | Longer-form narrative interpretation |

## Generated reference

`generated/` contains machine-derived inventories and audits. These files are not the place to explain the system, but they are useful when you need a mechanically produced index or consistency report.

High-value generated documents:
- `generated/system_map.md` — stage and dependency inventory
- `generated/detector_coverage.md` — detector coverage across the event surface
- `generated/event_contract_reference.md` — event catalog and contract view
- `generated/event_ontology_mapping.md` — canonical event mapping view

Refresh generated docs through the governance and generator scripts referenced in [reference/assurance.md](reference/assurance.md).

## Fast answers

- **What is the repo’s main architectural idea?** Research produces evidence, promotion packages evidence into thesis contracts, and runtime trades only against thesis contracts. Start with [lifecycle/overview.md](lifecycle/overview.md).
- **Where is the full repo map?** [reference/full_repo_surface.md](reference/full_repo_surface.md)
- **Where should I start reading code?** [reference/repository_map.md](reference/repository_map.md)
- **Where is the live engine?** `project/live/` plus `project/scripts/run_live_engine.py`, explained in [lifecycle/deploy.md](lifecycle/deploy.md)
- **Where is research logic?** `project/research/`, explained in [reference/repository_map.md](reference/repository_map.md)
- **Where are event definitions?** `spec/events/`, with runtime consumers in `project/events/`
- **Where do artifacts land?** [reference/full_repo_surface.md](reference/full_repo_surface.md)
- **Where are the operator procedures?** [operator/runbook.md](operator/runbook.md)
