---
name: edge-thesis-bootstrap
description: Inspect or maintain Edge thesis packaging artifacts after promotion. Use when the task is runtime thesis export, overlap inspection, or package maintenance rather than another discovery experiment.
---

# Edge Thesis Package Maintenance

Use this for packaging maintenance, not for raw discovery.

## Read first

1. `README.md`
2. `CONTRIBUTING.md`
3. `project/research/README.md`

## Preferred front door

Use explicit run export when the goal is a runtime thesis batch from a specific run:

```bash
./plugins/edge-agents/scripts/edge_export_theses.sh <run_id>
```

Use package and overlap builders only when repairing or inspecting a specific packaging block.

## Required review surfaces

- `data/live/theses/<run_id>/promoted_theses.json`
- `data/live/theses/index.json`
- `project/research/live_export.py`
- `project/live/`

## Hard rules

- Do not describe research-level outputs as production-ready.
- Regenerate overlap artifacts after overlap logic changes.
- Treat exported thesis artifacts as authoritative over hand-written notes.
