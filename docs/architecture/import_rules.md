# Import rules

The architectural integrity test remains the source of truth for package-boundary enforcement. The pre-commit and CI helper `project/scripts/check_banned_imports.py` mirrors its current deep-import allowlist.

## Preferred import surfaces

| Deep import to avoid | Preferred surface |
|---|---|
| `project.artifacts.catalog` | `project.artifacts` |
| `project.compilers.executable_strategy_spec` | `project.compilers` |
| `project.portfolio.allocation_spec` | `project.portfolio` |
| `project.portfolio.sizing` | `project.portfolio` |
| `project.spec_validation.loaders` | `project.spec_validation` |
| `project.spec_validation.ontology` | `project.spec_validation` |
| `project.spec_validation.search` | `project.spec_validation` |
| `project.eval.splits` | `project.eval` |
| `project.live.runner` | `project.live` |
| `project.live.health_checks` | `project.live` |
| `project.live.state` | `project.live` |

Owner modules may import their own internals. Tests and scripts are exempt where the architectural integrity test already exempts them.
