# Edge — documentation

## Navigation

### Start here

- **[lifecycle/overview.md](lifecycle/overview.md)** — system model, four-stage lifecycle, repo-wide invariants

### Lifecycle guides

| Stage | File | What it covers |
|-------|------|----------------|
| Discover | [lifecycle/discover.md](lifecycle/discover.md) | Proposal authoring, pipeline execution, discovery artifacts |
| Validate | [lifecycle/validate.md](lifecycle/validate.md) | Candidate validation, robustness testing, artifact contract |
| Promote | [lifecycle/promote.md](lifecycle/promote.md) | Thesis packaging, live export, deployment states |
| Deploy | [lifecycle/deploy.md](lifecycle/deploy.md) | Live/paper runtime, config contract, environment setup |

### Reference

| File | What it covers |
|------|----------------|
| [reference/repository_map.md](reference/repository_map.md) | Package layout, ownership, generated vs authored files |
| [reference/architecture.md](reference/architecture.md) | Package dependency DAG, design decisions, extension guidance |
| [reference/commands.md](reference/commands.md) | All CLI commands, Make targets, and direct scripts |
| [reference/spec_authoring.md](reference/spec_authoring.md) | How to write proposals, event specs, and templates |
| [reference/assurance.md](reference/assurance.md) | Test surface, minimum green gate, governance, benchmarks |

### Operations

| File | What it covers |
|------|----------------|
| [operator/runbook.md](operator/runbook.md) | Paper engine bring-up, startup certification, failure handling |

### Research

| File | What it covers |
|------|----------------|
| [research/results.md](research/results.md) | All hypothesis results — auto-generated after every run |
| [research/campaign_results.md](research/campaign_results.md) | Organized results by event with horizon/filter detail |
| [research/reflections.md](research/reflections.md) | Observations + auto-detected patterns (ceiling, incompatibility, regime breaks) |
| [research/narrative.md](research/narrative.md) | Written reflections: technical and trading research |

### Generated reference

`generated/` contains code-derived audits (event contracts, ontology mappings, detector coverage). These are outputs — do not edit them. Run `make governance` to regenerate.

---

## What the repo does

Edge is a research-to-runtime trading system. A structured YAML proposal enters at `edge discover`, passes through statistical evaluation, promotion gating, and thesis export, and exits as a live/paper runtime config bound to a governed thesis package. The codebase spans a pipeline DAG (~1400 Python files), a compiled spec/domain model (~440 YAML specs), a backtest and live execution stack, and a comprehensive test suite (~560 test files).

## Finding things quickly

- **Where does X live in the code?** → `reference/repository_map.md`
- **What commands are available?** → `reference/commands.md`
- **How do I write a proposal?** → `reference/spec_authoring.md`
- **What do the current research results show?** → `research/results.md`
- **How do I bring up the paper engine?** → `operator/runbook.md`
- **What does "bridge gate" mean?** → `lifecycle/overview.md` (gates section)
