# AGENTS.md

This file establishes the operating contract and safety boundaries for AI agents (Claude Code, Gemini CLI, etc.) working in the Edge repository.

Generic agent policy lives here. Claude Code-specific implementation notes live in `CLAUDE.md`.

---

## Mission

Operate Edge as a governed crypto research-to-runtime repo. Prioritize reproducibility, artifact lineage, and runtime safety.

## Default check

Run after any code, spec, docs, or lifecycle change:

```bash
make agent-check
```

For small local edits, use the fast structural check while iterating:

```bash
make agent-check-fast
```

Run the full check before finalizing PRs, commits, meaningful research runs, or any change that touches lifecycle behavior:

```bash
make agent-check
```

---

## Allowed by default

- Read source, specs, docs, and generated reports.
- Create bounded proposal YAML.
- Run `discover`, `validate`, `promote`, and `deploy` inspection commands in non-live modes.
- Update docs, source, and tests within the declared task scope.
- Regenerate generated docs through their owning scripts.
- Run `discover_doctor`, results-index refresh, reflections refresh, validation, and forward-confirmation inspection commands.

## Human approval required

- `runtime_mode=trading`
- `edge deploy live-run`
- Editing `data/live/theses/**`
- Editing `data/reports/approval/**`
- Changing cap profiles
- Setting or using production credentials
- Deleting run data or report artifacts
- Modifying `.env*`
- Broadening symbols, horizons, or templates as a rescue tactic
- Broadening any search surface after failure, including events, contexts, templates, symbols, horizons, date windows, or feature proxies.

## Forbidden

- Commit or print API keys or secrets.
- Treat discovery or promotion output as live approval.
- Rescue failed candidates by changing horizon, context, symbol, template, or date window after seeing failed evidence.
- Drop bad years without declaring an ex-ante regime-conditional thesis.
- Loosen thresholds after seeing results.
- Manually edit generated docs (files under `docs/generated/`).
- Modify protected live artifacts unless explicitly authorized.

## Required loop

1. State objective.
2. Identify relevant files.
3. Make the smallest coherent change.
4. Run `make agent-check-fast` after small edits and `make agent-check` before finalizing.
5. Summarize changed files, checks run, remaining risks, and next safe command.

---

## Agent Operating Contract

AI agents MUST adhere to these rules to maintain repository integrity and research quality.

### 1. Discovery Quality Gate
Before validating, promoting, or claiming an "edge" was found, agents MUST run the `discover-doctor`:
```bash
make discover-doctor RUN_ID=<run_id> DATA_ROOT=<lake>
```
- **Exit 0 (validate_ready/review_candidate)**: Proceed to validation or manual review. This is candidate evidence, not an edge claim.
- **Exit 1 (blocked/rejected)**: DO NOT validate. Inspect `phase2_diagnostics.json` or move to the next bounded cell.

Forward confirmation is the boundary for calling a candidate an edge. Paper execution evidence is the boundary for calling it tradable.

### 2. Empirical Reproduction
Before fixing a bug, agents MUST:
1. Create a minimal reproduction script or test case.
2. Confirm the failure state.
3. Apply the fix and verify it passes.

### 3. Architecture Integrity
The repository enforces a strict package dependency DAG. Key rules:
- `project.core` → `project.research` is ONE-WAY.
- Do not import from `project.research` inside `project.core`.
- Run `make minimum-green-gate` to verify architectural compliance.

### 4. Spec-Driven Truth
- All authored truth lives in `spec/`.
- Rebuild the domain graph after any spec changes: `make domain-graph`.
- Never manually edit `spec/domain/domain_graph.yaml`.

---

## 5. Protected Artifact Write Policy

AI agents are FORBIDDEN from modifying the following paths without an explicit user directive. These paths represent governed state, production configurations, or sensitive credentials.

**Protected Paths:**
- `data/live/theses/**` (Governed research output)
- `data/reports/approval/**` (Certification artifacts)
- `project/configs/live_trading_*.yaml` (Production binding configs)
- `project/configs/live_production.yaml` (Production environment config)
- `deploy/systemd/*.service` (Systemd units)
- `.env*` and `deploy/env/*.env` (Credentials and environment variables)

**Enforcement:**
- Agents MUST run `make agent-check` before finalizing any PR or major change.
- `make agent-check` includes a `check-protected-paths` step that fails if any of these files are modified.
- To override, the user must explicitly acknowledge and perform the write or authorize the agent for a specific task.

---

## Agent Checklists

### New Edge Discovery
- [ ] Define bounded search surface in `spec/discovery/`.
- [ ] Run discovery: `make discover RUN_ID=<id> START=<start> END=<end>`.
- [ ] **Run Doctor**: `make discover-doctor RUN_ID=<id>`.
- [ ] If `validate_ready`, run validation: `make validate RUN_ID=<id>`.
- [ ] Review `evaluation_results.parquet` and `scoreboard.parquet`.

### Bug Fix / Refactor
- [ ] Reproduce failure with a test.
- [ ] Apply changes.
- [ ] Run `make minimum-green-gate`.
- [ ] Run `make agent-check`.

---

## Makefile Target: `make agent-check`

This target runs the full suite of checks designed to verify repo health from an agent's perspective.

```bash
make agent-check
```
Includes:
1. `minimum-green-gate` (Compile + Architecture + Regression)
2. `check-hygiene` (Lint + Formatting)
3. `check-registry-sync` (Spec vs Code sync)
4. `check-domain-graph` (Freshness check)
5. `check-protected-paths` (Protected artifact write policy)

Fast iteration target:

```bash
make agent-check-fast
```

Includes:
1. `spec_qa_linter.py`
2. `check_domain_graph_freshness.py`
3. `check_protected_paths.py`
4. `project/tests/architecture`
