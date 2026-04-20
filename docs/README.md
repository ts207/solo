# Edge documentation

This docset is the map of the repository, the lifecycle guide for the trading system, and the operator handbook for running it safely.

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
| [reference/detector_governance.md](reference/detector_governance.md) | Detector banding, eligibility model, migration policy, generated governance artifacts |

## Operations

| File | Main coverage |
|------|---------------|
| [operator/runbook.md](operator/runbook.md) | Practical paper-engine bring-up, shutdown, restart, and failure handling |

## Research views

| File | Main coverage |
|------|---------------|
| [research/results.md](research/results.md) | Auto-generated results index (updated after each pipeline run) |
| [research/reflections.md](research/reflections.md) | Human observations and auto-detected patterns |
| [research/campaign_results.md](research/campaign_results.md) | Historical: initial discovery campaign data (2026-04-17) |
| [research/narrative.md](research/narrative.md) | Historical: initial discovery campaign narrative (2026-04-17) |

## Research state

No promoted theses. Prior results cleared — produced under gates and policy that had since-fixed bugs. Lake must be re-ingested; historical BTC lake runs are not on disk.

Start with a fresh discovery run:
```bash
edge discover run --proposal spec/proposals/broad_vol-spike_long_mr_24b.yaml
```
Then pass `--run_id <run_id>` on subsequent proposals to reuse that lake.

## Generated reference

`generated/` contains machine-derived inventories and audits. Not the place to explain the system, but useful when you need a mechanically produced index or consistency report.

High-value generated documents:
- `generated/system_map.md` — stage and dependency inventory
- `generated/detector_coverage.md` — detector coverage across the event surface
- `generated/event_contract_reference.md` — event catalog and contract view
- `generated/event_ontology_mapping.md` — canonical event mapping view
- `generated/detector_eligibility_matrix.md` — planning/promotion/runtime/anchor eligibility per detector

Refresh generated docs:
```bash
make governance
PYTHONPATH=. python3 project/scripts/build_detector_governance_artifacts.py --output-dir docs/generated
```

## Fast answers

- **What is the repo's main architectural idea?** Research produces evidence → promotion packages it into thesis contracts → runtime trades only against thesis contracts. Start with [lifecycle/overview.md](lifecycle/overview.md).
- **Where is the full repo map?** [reference/full_repo_surface.md](reference/full_repo_surface.md)
- **Where should I start reading code?** [reference/repository_map.md](reference/repository_map.md)
- **Where is the live engine?** `project/live/` plus `project/scripts/run_live_engine.py`, explained in [lifecycle/deploy.md](lifecycle/deploy.md)
- **Where is research logic?** `project/research/`, explained in [reference/repository_map.md](reference/repository_map.md)
- **Where are event definitions?** `spec/events/`, with runtime consumers in `project/events/`
- **Where do artifacts land?** `data/` — see [reference/full_repo_surface.md](reference/full_repo_surface.md)
- **Where are the operator procedures?** [operator/runbook.md](operator/runbook.md)
- **How do I run discovery?** `edge discover run --proposal spec/proposals/<your>.yaml` — see [lifecycle/discover.md](lifecycle/discover.md)
