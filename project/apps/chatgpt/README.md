# Edge ChatGPT app scaffold

This package is the ChatGPT-app and MCP-oriented interface layer for the repo.

## What it is

A scaffold around the canonical Edge operator workflow, not a parallel research engine. It exposes the Edge research lifecycle as an MCP (Model Context Protocol) server.

## Safety and Hardening

The app includes several safety layers to prevent unauthorized live mutations and concurrent conflicts:

- **Operator Confirmations:** Mutation tools (discover, promote) require explicit operator acknowledgement fields.
- **Run Locking:** Prevents concurrent mutations of the same run via file-based locks.
- **Path Guards:** Restricts file writes to allowed data directories and blocks sensitive paths (credentials, live theses, system services).
- **App Modes:** Controlled via `EDGE_CHATGPT_APP_MODE` (default: `paper_only`). Rejects commands containing forbidden patterns like `runtime_mode=trading`.
- **Admin Gating:** Destructive repo-level tools are hidden unless `EDGE_ENABLE_ADMIN_TOOLS=1` is set.

## CLI

```bash
# Check status and found runs
edge-chatgpt-app status

# Inspect available tools
edge-chatgpt-app tools --profile operator

# Serve MCP over HTTP
edge-chatgpt-app serve --host 127.0.0.1 --port 8000 --path /mcp
```

## Deployment

### Recommended Environment Defaults

```bash
EDGE_CHATGPT_APP_MODE=paper_only
EDGE_DISABLE_LIVE_TOOLS=1
EDGE_ENABLE_ADMIN_TOOLS=0
```

### Local Test

```bash
curl http://127.0.0.1:8000/
```

Expected: `{"ok":true,"app":"edge-chatgpt-app","profile":"operator","transport":"streamable-http","mcp_endpoint":"/mcp"}`

## Dependency rule

This layer should call the canonical repo surfaces. It should not redefine proposal policy or promotion logic.
