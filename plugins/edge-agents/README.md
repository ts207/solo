# Edge Agents plugin

Repo-local Codex/plugin surface for the Edge repository.

## Purpose

Provide guided wrappers around the canonical bounded workflow without creating a parallel operator model.
The plugin tracks the repo's current lifecycle front door:

`discover -> validate -> promote -> export -> bind-config -> paper/live run`

## What is included

- skills for repo orientation, maintenance, ChatGPT-app development, coordination, analysis, and proposal compilation
- thin wrappers around the canonical `edge`, `make discover|validate|promote|export|bind-config|paper-run|live-run|deploy-status`, and focused maintenance surfaces
- hook definitions for contract-sensitive edits and recent-run awareness

## Important scripts

- `scripts/edge_query_knowledge.sh`
- `scripts/edge_preflight_proposal.sh`
- `scripts/edge_lint_proposal.sh`
- `scripts/edge_explain_proposal.sh`
- `scripts/edge_plan_proposal.sh`
- `scripts/edge_run_proposal.sh`
- `scripts/edge_diagnose_run.sh`
- `scripts/edge_regime_report.sh`
- `scripts/edge_chatgpt_app.sh`
- `scripts/edge_sync_plugin.sh`
- `scripts/edge_governance.sh`
- `scripts/edge_validate_repo.sh`
- `scripts/edge_verify_contracts.sh`
- `scripts/edge_verify_run.sh`
- `scripts/edge_compare_runs.sh`
- `scripts/edge_show_run_artifacts.sh`
- `scripts/edge_export_theses.sh`
- `scripts/edge_package_theses.sh`

## Dependency rule

These wrappers should remain thin around:

- `edge discover|validate|promote|deploy`
- `make discover|validate|promote|export|bind-config|paper-run|live-run|deploy-status`
- `python -m project.operator.preflight`
- `python -m project.operator.proposal_tools`
- `python -m project.scripts.run_researcher_verification`
- generated run and thesis artifacts

They are convenience surfaces, not policy owners.

## Maintenance focus

The plugin now helps route common developer change types:

- proposal or lifecycle-surface changes -> targeted proposal checks plus `make minimum-green-gate` when behavior changes
- event, ontology, or registry changes -> `make governance` plus the minimum green gate when generated artifacts or behavior changes
- runtime-thesis or overlap changes -> explicit run export plus overlap regeneration
- ChatGPT app changes -> `edge-chatgpt-app` inspection/serve helpers plus canonical operator surfaces
- plugin changes -> local plugin-cache sync and sync checks

## Supported command groups

- `make discover|validate|promote|export|bind-config|paper-run|live-run|deploy-status`
- `edge_validate_repo.sh contracts|minimum-green|all`
- `edge_sync_plugin.sh targets|check|sync`
- `edge_export_theses.sh`

## Relationship to repo surfaces

Use the root `README.md`, package READMEs, Make targets, and `edge` CLI commands as the current operator-facing surfaces.
