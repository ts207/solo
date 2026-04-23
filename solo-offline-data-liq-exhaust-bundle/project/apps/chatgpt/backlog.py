from __future__ import annotations

IMPLEMENTATION_BACKLOG: tuple[dict[str, object], ...] = (
    {
        "phase": 1,
        "name": "Stabilize the server contract",
        "items": [
            "Install the Python MCP SDK and bind the scaffolded tool catalog to a real Streamable HTTP server.",
            "Add a dedicated /mcp health surface and local connector test instructions.",
            "Version the widget URI when the HTML bundle changes in a breaking way.",
        ],
    },
    {
        "phase": 2,
        "name": "Ship the read-mostly operator flow",
        "items": [
            "Expose proposal explain, lint, preview, diagnostics, compare, and regime report tools through the live MCP server.",
            "Trim structured outputs so ChatGPT only receives user-relevant fields, not raw internal diagnostics or filesystem noise.",
            "Add golden tests that pin exact tool outputs for realistic proposal and run fixtures.",
        ],
    },
    {
        "phase": 3,
        "name": "Add controlled mutation flows",
        "items": [
            "Expose issue-plan and issue-run only after confirmation-oriented prompt metadata and tests are in place.",
            "Separate scratch artifact writes from durable operator issuance where possible.",
            "Document which tool calls are expected to create proposal memory, manifests, or reports.",
        ],
    },
    {
        "phase": 4,
        "name": "Upgrade the UI path",
        "items": [
            "Split data tools from render tools consistently so the widget does not remount on every backend call.",
            "Replace the single-file widget with a bundled UI only if the HTML view becomes limiting.",
            "Persist meaningful widget state with the MCP Apps bridge instead of relying on rerenders.",
        ],
    },
    {
        "phase": 5,
        "name": "Submission hardening",
        "items": [
            "Audit every tool response for internal IDs, paths, timestamps, tokens, and other unnecessary user-related fields.",
            "Verify tool hint annotations match real behavior, especially read-only versus scratch-writing flows.",
            "Prepare reviewer test cases that pass on ChatGPT web and mobile layouts.",
        ],
    },
)
