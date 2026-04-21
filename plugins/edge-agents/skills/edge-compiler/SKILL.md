---
name: edge-compiler
description: Compile a frozen Edge mechanism hypothesis into repo-native proposal YAML and exact commands. Use when a bounded hypothesis is ready for translation, plan-only validation, and controlled execution.
---

# Edge Compiler

Use this after a mechanism has been frozen.

## Read first

1. `docs/lifecycle/discover.md`
2. `docs/reference/spec_authoring.md`
3. `docs/reference/commands.md`
4. `project/research/agent_io/proposal_schema.py`

## Current front door

- Prefer `edge discover plan|run` and `make discover` in user-facing output.
- Use proposal inspection wrappers for preflight, lint, and explanation.
- Mention lower-level `project.research.agent_io.*` modules only when debugging translation or contract drift.

## Required checks before compiling

- event exists in the canonical registry
- template is valid for the event family
- regime or context filter exists when used
- horizons are valid for the proposal path
- `entry_lags >= 1`
- search controls stay bounded

## Important horizon rule

- Proposal compilation uses integer bar horizons.
- Do not silently rewrite horizons. Warn when a horizon is non-canonical but still syntactically accepted.

## Required output

- proposal path under `spec/proposals/`
- full proposal YAML
- preflight command
- lint command
- explain command
- plan-only command
- execution command
- explicit plan review checklist

## Do not do

- do not modify the hypothesis to make it fit
- do not add events, templates, or regimes
- do not imply that proposal execution equals thesis promotion
