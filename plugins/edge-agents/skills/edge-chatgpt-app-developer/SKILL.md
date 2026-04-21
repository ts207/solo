---
name: edge-chatgpt-app-developer
description: Develop or inspect the Edge ChatGPT app scaffold while keeping proposal, lifecycle, reporting, and dashboard behavior routed through canonical repo surfaces. Use when the task touches project/apps/chatgpt or the MCP-facing interface.
---

# Edge ChatGPT App Developer

Use this skill for work in `project/apps/chatgpt/`.

## Read first

1. `project/apps/chatgpt/README.md`
2. `docs/reference/commands.md`
3. `docs/reference/assurance.md`
4. `docs/lifecycle/overview.md`

## Role

- Treat the ChatGPT app as an interface layer around canonical lifecycle surfaces.
- Inspect the app shape with `edge-chatgpt-app` helpers before changing handlers or UI.
- Keep proposal policy, stage logic, promotion logic, and deploy checks in canonical repo code.

## Main commands

```bash
./plugins/edge-agents/scripts/edge_chatgpt_app.sh backlog
./plugins/edge-agents/scripts/edge_chatgpt_app.sh blueprint
./plugins/edge-agents/scripts/edge_chatgpt_app.sh widget
./plugins/edge-agents/scripts/edge_chatgpt_app.sh serve --host 127.0.0.1 --port 8000 --path /mcp
```

## Working rules

- Read `handlers.py`, `tool_catalog.py`, `resources.py`, `server.py`, `cli.py`, and relevant UI files as interface code.
- Route proposal, stage, report, and dashboard behavior through canonical helpers.
- If a requested change belongs in `project.cli`, `project.operator`, or research services, move the change there instead of duplicating logic in the app.
- Use the maintainer workflow after app-surface changes when docs, generated artifacts, or test-coupled surfaces may drift.

## Verification

- Use `edge-chatgpt-app backlog|blueprint|widget` as cheap interface sanity checks.
- Use `./plugins/edge-agents/scripts/edge_validate_repo.sh contracts` for app changes that affect tool wiring.
- Use `./plugins/edge-agents/scripts/edge_validate_repo.sh minimum-green` when behavior crosses lifecycle boundaries.
