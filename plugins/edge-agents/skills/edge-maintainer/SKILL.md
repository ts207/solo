---
name: edge-maintainer
description: Route Edge repo changes through the correct validation, generated-artifact regeneration, plugin sync, and maintenance commands. Use for developer upkeep, doc drift, plugin upkeep, app-surface maintenance, or choosing the smallest correct verification loop.
---

# Edge Maintainer

Use this skill for developer maintenance and repo-upkeep work in `/home/irene/Edge`.

## Read first

1. `docs/reference/assurance.md`
2. `docs/reference/commands.md`
3. `docs/README.md`
4. `Makefile`

## Role

- Route the current change through the smallest correct maintenance loop.
- Prefer canonical repo entrypoints over ad hoc script sequences.
- Regenerate generated docs instead of editing them by hand.
- Keep plugin maintenance thin and repo-aligned.

## Primary routing table

### Proposal or lifecycle-surface change

Use focused proposal checks when a concrete proposal is involved:

```bash
./plugins/edge-agents/scripts/edge_preflight_proposal.sh /abs/path/to/proposal.yaml
./plugins/edge-agents/scripts/edge_lint_proposal.sh /abs/path/to/proposal.yaml
./plugins/edge-agents/scripts/edge_explain_proposal.sh /abs/path/to/proposal.yaml
edge discover plan --proposal /abs/path/to/proposal.yaml
```

Use the broader gate when command behavior, stage wiring, or shared lifecycle logic changed:

```bash
./plugins/edge-agents/scripts/edge_validate_repo.sh minimum-green
```

Then inspect:

- `README.md`
- `docs/README.md`
- `docs/reference/commands.md`
- `docs/lifecycle/overview.md`

### Event, ontology, or registry change

Use:

```bash
make governance
./plugins/edge-agents/scripts/edge_validate_repo.sh minimum-green
```

Then inspect:

- `docs/generated/event_contract_reference.md`
- `docs/generated/event_ontology_mapping.md`
- `docs/generated/system_map.md`
- `docs/reference/spec_authoring.md`

### Runtime-thesis export or overlap change

Use:

```bash
edge promote export --run_id <run_id>
PYTHONPATH=. ./.venv/bin/python -m project.scripts.build_thesis_overlap_artifacts --run_id <run_id>
```

Then inspect:

- `data/live/theses/<run_id>/promoted_theses.json`
- `data/live/theses/index.json`
- `docs/lifecycle/promote.md`
- `docs/lifecycle/deploy.md`

### Architectural boundary change

Use:

```bash
./plugins/edge-agents/scripts/edge_validate_repo.sh minimum-green
PYTHONPATH=. ./.venv/bin/python -m project.scripts.build_system_map --check
```

Then inspect:

- `docs/reference/architecture.md`
- `docs/reference/repository_map.md`
- `docs/generated/system_map.md`

### Plugin change

Use:

```bash
./plugins/edge-agents/scripts/edge_sync_plugin.sh targets
./plugins/edge-agents/scripts/edge_sync_plugin.sh check
./plugins/edge-agents/scripts/edge_sync_plugin.sh sync
```

### Repo-level validation or governance work

Use:

```bash
./plugins/edge-agents/scripts/edge_validate_repo.sh contracts
./plugins/edge-agents/scripts/edge_governance.sh
```

## ChatGPT app surface

When the task touches `project/apps/chatgpt/`, use:

```bash
./plugins/edge-agents/scripts/edge_chatgpt_app.sh backlog
./plugins/edge-agents/scripts/edge_chatgpt_app.sh blueprint
./plugins/edge-agents/scripts/edge_chatgpt_app.sh widget
./plugins/edge-agents/scripts/edge_chatgpt_app.sh serve --host 127.0.0.1 --port 8000 --path /mcp
```

Treat this as an interface layer around canonical lifecycle surfaces, not a separate runtime.

## Hard rules

- Do not teach wrappers as if they own repo policy.
- Do not manually edit `docs/generated/*` when a generator exists.
- Do not stop at a narrow check if the change type implies additional regeneration.
- If plugin source changes, remember the installed plugin cache is stale until synced.
