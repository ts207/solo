# Edge ChatGPT app scaffold

This package is the ChatGPT-app and MCP-oriented interface layer for the repo.

## What it is

A scaffold around the canonical Edge operator workflow, not a parallel research engine.

## Key files

- `handlers.py` — wraps proposal, dashboard, reporting, and operator actions
- `tool_catalog.py` — tool definitions and metadata
- `resources.py` — widget resource payloads
- `server.py` — MCP/server blueprint and serving surface
- `cli.py` — local inspection and serve helper
- `ui/operator_dashboard.html` — UI resource

## CLI

```bash
edge-chatgpt-app backlog
edge-chatgpt-app blueprint
edge-chatgpt-app widget
edge-chatgpt-app serve --host 127.0.0.1 --port 8000 --path /mcp
```

## Dependency rule

This layer should call the canonical repo surfaces:

- proposal tools
- operator workflow
- reporting and dashboard helpers

It should not redefine proposal policy or promotion logic.

## Relationship to docs

See:

- `docs/README.md`
- `docs/operator_command_inventory.md`
- `docs/02_REPOSITORY_MAP.md`
