# AGENTS.md

This file establishes the operating contract and safety boundaries for AI agents (Claude Code, Gemini CLI, etc.) working in the Edge repository.

---

## Agent Operating Contract

AI agents MUST adhere to these rules to maintain repository integrity and research quality.

### 1. Discovery Quality Gate
Before promoting any run or claiming an "edge" was found, agents MUST run the `discover-doctor`:
```bash
make discover-doctor RUN_ID=<run_id> DATA_ROOT=<lake>
```
- **Exit 0 (validate_ready/review_candidate)**: Proceed to validation or manual review.
- **Exit 1 (blocked/rejected)**: DO NOT validate. Inspect `phase2_diagnostics.json` or move to the next bounded cell.

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
- `project/configs/live_live_*.yaml` (Production binding configs)
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

This target runs a suite of checks designed to verify repo health from an agent's perspective.

```bash
make agent-check
```
Includes:
1. `minimum-green-gate` (Compile + Architecture + Regression)
2. `check-hygiene` (Lint + Formatting)
3. `check-registry-sync` (Spec vs Code sync)
4. `check-domain-graph` (Freshness check)
