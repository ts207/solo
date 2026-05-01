from __future__ import annotations

from typing import Any

from project.apps.chatgpt.repo_only import REPO_TOOL_CATALOG
from project.apps.chatgpt.resources import WIDGET_URI
from project.apps.chatgpt.schemas import (
    CatalogListRunsInput,
    CodexOperatorInvokeInput,
    CompareRunsInput,
    DiscoverRunInput,
    ListThesesInput,
    MemorySummaryInput,
    OperatorDashboardInput,
    PromoteRunInput,
    ProposalExplainInput,
    ProposalIssueInput,
    ProposalLintInput,
    ProposalPreflightInput,
    ProposalPreviewInput,
    ProposalWriteInput,
    RegimeReportInput,
    RenderOperatorSummaryInput,
    RunDiagnosticsInput,
    ToolDefinition,
    ToolHints,
    ValidateRunInput,
)


def _schema(model: type[Any]) -> str:
    return f"{model.__module__}.{model.__name__}"


def _json_schema(model: type[Any]) -> dict[str, Any]:
    return model.model_json_schema()


TOOL_CATALOG: tuple[ToolDefinition, ...] = (
    ToolDefinition(
        name="edge_preflight_proposal",
        title="Preflight Edge proposal",
        description="Validate proposal shape, data coverage, and artifact writability before issuing a Stage 1 discovery run. Run this before edge_discover_run to catch config and data problems early.",
        handler="project.apps.chatgpt.handlers.preflight_proposal",
        input_model=_schema(ProposalPreflightInput),
        input_schema=_json_schema(ProposalPreflightInput),
        invoking_text="Running preflight",
        invoked_text="Preflight ready",
        category="mutation",
        hints=ToolHints(
            read_only=False,
            destructive=False,
            open_world=False,
            justification="Writes a temporary artifact root probe and may create local scratch directories during validation.",
        ),
    ),
    ToolDefinition(
        name="edge_write_proposal",
        title="Write Edge proposal",
        description="Write a research proposal YAML or JSON to disk. Use this to save a generated proposal before issuing a discovery run with edge_discover_run.",
        handler="project.apps.chatgpt.handlers.write_proposal",
        input_model=_schema(ProposalWriteInput),
        input_schema=_json_schema(ProposalWriteInput),
        invoking_text="Writing proposal",
        invoked_text="Proposal saved",
        category="mutation",
        hints=ToolHints(
            read_only=False,
            destructive=False,
            open_world=False,
            justification="Writes a new proposal file to the configured project proposal directory.",
        ),
    ),
    ToolDefinition(
        name="edge_explain_proposal",
        title="Explain Edge proposal",
        description="Resolve a bounded proposal into required detectors, features, states, and operator-facing constraints.",
        handler="project.apps.chatgpt.handlers.explain_proposal_summary",
        input_model=_schema(ProposalExplainInput),
        input_schema=_json_schema(ProposalExplainInput),
        invoking_text="Explaining proposal",
        invoked_text="Proposal explained",
        category="mutation",
        hints=ToolHints(
            read_only=False,
            destructive=False,
            open_world=False,
            justification="Uses the current proposal translation path, which stages temporary local files while building the plan.",
        ),
    ),
    ToolDefinition(
        name="edge_lint_proposal",
        title="Lint Edge proposal",
        description="Check whether a proposal stays bounded and warn when the search surface looks too broad.",
        handler="project.apps.chatgpt.handlers.lint_proposal_summary",
        input_model=_schema(ProposalLintInput),
        input_schema=_json_schema(ProposalLintInput),
        invoking_text="Linting proposal",
        invoked_text="Lint ready",
        category="mutation",
        hints=ToolHints(
            read_only=False,
            destructive=False,
            open_world=False,
            justification="Stages temporary proposal artifacts during validation even though it does not issue a durable run.",
        ),
    ),
    ToolDefinition(
        name="edge_preview_plan",
        title="Preview Edge plan",
        description="Translate a proposal into a validated plan and run-all overrides without issuing a durable proposal record.",
        handler="project.apps.chatgpt.handlers.preview_plan",
        input_model=_schema(ProposalPreviewInput),
        input_schema=_json_schema(ProposalPreviewInput),
        invoking_text="Previewing plan",
        invoked_text="Plan preview ready",
        category="mutation",
        hints=ToolHints(
            read_only=False,
            destructive=False,
            open_world=False,
            justification="Uses scratch writes to stage and validate the experiment config, but avoids durable proposal memory updates.",
        ),
    ),
    ToolDefinition(
        name="edge_issue_plan",
        title="Issue Edge plan",
        description="Record a proposal in program memory and execute Stage 1 discovery in plan-only mode (no run, no artifacts). Use this to validate the compiled experiment config and anchor space before committing to a full run.",
        handler="project.apps.chatgpt.handlers.issue_plan",
        input_model=_schema(ProposalIssueInput),
        input_schema=_json_schema(ProposalIssueInput),
        invoking_text="Issuing plan",
        invoked_text="Plan issued",
        category="mutation",
        hints=ToolHints(
            read_only=False,
            destructive=False,
            open_world=False,
            justification="Creates proposal memory rows and local artifacts for a durable plan issuance.",
        ),
    ),
    ToolDefinition(
        name="edge_issue_run",
        title="Issue Edge run",
        description="Record a proposal in program memory and execute Stage 1 discovery (full run). Prefer edge_discover_run for direct execution without the proposal-memory write; use this when durable proposal tracking is required.",
        handler="project.apps.chatgpt.handlers.issue_run",
        input_model=_schema(ProposalIssueInput),
        input_schema=_json_schema(ProposalIssueInput),
        invoking_text="Issuing run",
        invoked_text="Run issued",
        category="mutation",
        hints=ToolHints(
            read_only=False,
            destructive=False,
            open_world=False,
            justification="Starts a bounded local workflow that creates durable artifacts, manifests, and proposal memory records.",
        ),
    ),
    ToolDefinition(
        name="edge_invoke_operator",
        title="Invoke Edge operator via Codex MCP",
        description="Run or continue a Codex MCP session inside the Edge repository to inspect, edit, and repair local files or tooling. Use this for repo maintenance through Codex, not for issuing a new Edge run.",
        handler="project.apps.chatgpt.handlers.invoke_codex_operator",
        input_model=_schema(CodexOperatorInvokeInput),
        input_schema=_json_schema(CodexOperatorInvokeInput),
        invoking_text="Invoking Codex",
        invoked_text="Codex repo task finished",
        category="mutation",
        hints=ToolHints(
            read_only=False,
            destructive=True,
            open_world=False,
            justification="Invokes or continues a Codex MCP session against the local Edge workspace for repo inspection or repair, which may edit files, run commands, or perform irreversible local mutations depending on the task.",
        ),
    ),
    ToolDefinition(
        name="edge_get_negative_result_diagnostics",
        title="Get Edge diagnostics",
        description="Stage 2 / post-validate: explain why a discovery run failed to produce a hypothesis-supporting candidate and suggest the next bounded action (repair, sweep, or hold).",
        handler="project.apps.chatgpt.handlers.get_negative_result_diagnostics",
        input_model=_schema(RunDiagnosticsInput),
        input_schema=_json_schema(RunDiagnosticsInput),
        invoking_text="Loading diagnostics",
        invoked_text="Diagnostics ready",
        category="data",
        hints=ToolHints(
            read_only=True,
            destructive=False,
            open_world=False,
            justification="Reads existing run artifacts and reports without changing local or public state.",
        ),
    ),
    ToolDefinition(
        name="edge_get_regime_report",
        title="Get Edge regime report",
        description="Stage 2 / post-validate: summarize regime stability for a discovery run and highlight sign flips, consistent behavior, or decay risk across market regimes.",
        handler="project.apps.chatgpt.handlers.get_regime_report",
        input_model=_schema(RegimeReportInput),
        input_schema=_json_schema(RegimeReportInput),
        invoking_text="Loading regime report",
        invoked_text="Regime report ready",
        category="data",
        hints=ToolHints(
            read_only=True,
            destructive=False,
            open_world=False,
            justification="Reads existing reports and candidate artifacts only.",
        ),
    ),
    ToolDefinition(
        name="edge_compare_runs",
        title="Compare Edge runs",
        description="Compare two or more existing runs across time slices and summarize the strongest or most unstable slice.",
        handler="project.apps.chatgpt.handlers.compare_runs",
        input_model=_schema(CompareRunsInput),
        input_schema=_json_schema(CompareRunsInput),
        invoking_text="Comparing runs",
        invoked_text="Comparison ready",
        category="data",
        hints=ToolHints(
            read_only=True,
            destructive=False,
            open_world=False,
            justification="Reads run summaries and operator reports without changing any state.",
        ),
    ),
    ToolDefinition(
        name="edge_get_operator_dashboard",
        title="Get Edge operator dashboard",
        description="Load proposal memory, recent proposals, prior run results, and the current candidate pipeline board for the active Edge program so operators can query project status in one place.",
        handler="project.apps.chatgpt.handlers.get_operator_dashboard",
        input_model=_schema(OperatorDashboardInput),
        input_schema=_json_schema(OperatorDashboardInput),
        invoking_text="Loading dashboard",
        invoked_text="Dashboard ready",
        category="data",
        hints=ToolHints(
            read_only=True,
            destructive=False,
            open_world=False,
            justification="Reads existing memory tables, belief-state JSON, and run manifests without creating or mutating local state.",
        ),
    ),
    ToolDefinition(
        name="edge_render_operator_summary",
        title="Render Edge operator summary",
        description="Render a compact operator dashboard from data returned by another Edge tool. Pass the full payload from edge_get_operator_dashboard via the dashboard field, or pass a prepared summary payload from another data tool.",
        handler="project.apps.chatgpt.handlers.render_operator_summary",
        input_model=_schema(RenderOperatorSummaryInput),
        input_schema=_json_schema(RenderOperatorSummaryInput),
        invoking_text="Rendering dashboard",
        invoked_text="Dashboard ready",
        category="render",
        ui_resource_uri=WIDGET_URI,
        output_template_uri=WIDGET_URI,
        hints=ToolHints(
            read_only=True,
            destructive=False,
            open_world=False,
            justification="Only formats already-prepared structured content for presentation in the widget.",
        ),
    ),
    ToolDefinition(
        name="edge_get_memory_summary",
        title="Get Edge memory summary",
        description="Load belief state, next actions, and recent evidence for a program. Use this for a lightweight program status check.",
        handler="project.apps.chatgpt.handlers.get_memory_summary",
        input_model=_schema(MemorySummaryInput),
        input_schema=_json_schema(MemorySummaryInput),
        invoking_text="Loading memory",
        invoked_text="Memory ready",
        category="data",
        hints=ToolHints(
            read_only=True,
            destructive=False,
            open_world=False,
            justification="Reads program memory tables and belief state without mutating local or project state.",
        ),
    ),
    ToolDefinition(
        name="edge_discover_run",
        title="Run Edge Stage 1 discovery",
        description="Execute Stage 1 discovery for an existing proposal YAML file on disk. Runs the full pipeline: feature extraction, signal search, Phase-2 candidate evaluation, and run-manifest writing. Use edge_preflight_proposal first to validate the proposal and data coverage. For plan-only preview use edge_preview_plan.",
        handler="project.apps.chatgpt.handlers.discover_run",
        input_model=_schema(DiscoverRunInput),
        input_schema=_json_schema(DiscoverRunInput),
        invoking_text="Running Stage 1 discovery",
        invoked_text="Discovery complete",
        category="mutation",
        hints=ToolHints(
            read_only=False,
            destructive=False,
            open_world=False,
            justification="Creates durable run artifacts (manifests, phase2 candidates, edge candidates) in the local data root.",
        ),
    ),
    ToolDefinition(
        name="edge_validate_run",
        title="Run Edge Stage 2 validation",
        description="Execute Stage 2 validation for a completed Stage 1 discovery run. Loads candidate tables, applies validation gates, and produces a validation bundle. Required before Stage 3 promotion.",
        handler="project.apps.chatgpt.handlers.validate_run",
        input_model=_schema(ValidateRunInput),
        input_schema=_json_schema(ValidateRunInput),
        invoking_text="Running Stage 2 validation",
        invoked_text="Validation complete",
        category="mutation",
        hints=ToolHints(
            read_only=False,
            destructive=False,
            open_world=False,
            justification="Reads discovery artifacts and writes a validation bundle to the local data root.",
        ),
    ),
    ToolDefinition(
        name="edge_promote_run",
        title="Run Edge Stage 3 promotion",
        description="Execute Stage 3 promotion for a validated discovery run. Packages surviving candidates into a thesis batch for Stage 4 deployment. Requires a successful Stage 2 validation.",
        handler="project.apps.chatgpt.handlers.promote_run",
        input_model=_schema(PromoteRunInput),
        input_schema=_json_schema(PromoteRunInput),
        invoking_text="Running Stage 3 promotion",
        invoked_text="Promotion complete",
        category="mutation",
        hints=ToolHints(
            read_only=False,
            destructive=False,
            open_world=False,
            justification="Writes thesis batch JSON and promotion artifacts to live/theses in the local data root.",
        ),
    ),
    ToolDefinition(
        name="edge_list_theses",
        title="List promoted thesis batches",
        description="List all promoted thesis batches available in the Stage 4 deployment inventory. Returns batch run IDs, thesis counts, and promotion timestamps. Use edge_catalog_list for a broader run overview.",
        handler="project.apps.chatgpt.handlers.list_theses",
        input_model=_schema(ListThesesInput),
        input_schema=_json_schema(ListThesesInput),
        invoking_text="Loading thesis inventory",
        invoked_text="Thesis inventory ready",
        category="data",
        hints=ToolHints(
            read_only=True,
            destructive=False,
            open_world=False,
            justification="Reads live/theses directory and promotion manifests without changing any state.",
        ),
    ),
    ToolDefinition(
        name="edge_catalog_list",
        title="List Edge run catalog",
        description="List recent runs from the run-manifest catalog with stage-presence annotations (discover / validate / promote / deploy). Optionally filter by stage to see which runs have completed that stage's artifacts.",
        handler="project.apps.chatgpt.handlers.catalog_list_runs",
        input_model=_schema(CatalogListRunsInput),
        input_schema=_json_schema(CatalogListRunsInput),
        invoking_text="Loading run catalog",
        invoked_text="Run catalog ready",
        category="data",
        hints=ToolHints(
            read_only=True,
            destructive=False,
            open_world=False,
            justification="Reads run manifests and checks artifact directory presence without modifying any state.",
        ),
    ),
)


def get_tool_definition(name: str, *, profile: str = "operator") -> ToolDefinition:
    for tool in get_tool_catalog(profile):
        if tool.name == name:
            return tool
    raise KeyError(name)


def get_tool_catalog(profile: str = "operator") -> tuple[ToolDefinition, ...]:
    import os
    normalized = str(profile or "operator").strip().lower()
    if normalized == "operator":
        catalog = list(TOOL_CATALOG)
        # Filter out admin-only tools unless explicitly enabled
        if os.environ.get("EDGE_ENABLE_ADMIN_TOOLS") != "1":
            catalog = [t for t in catalog if t.name != "edge_invoke_operator"]
        return tuple(catalog)
    if normalized == "repo":
        return REPO_TOOL_CATALOG
    raise ValueError(f"Unknown ChatGPT app profile: {profile}")


def get_profile_metadata(profile: str = "operator") -> dict[str, str]:
    normalized = str(profile or "operator").strip().lower()
    if normalized == "operator":
        return {
            "profile": "operator",
            "app_name": "Edge Operator",
            "version": "0.1.0",
            "description": "ChatGPT app scaffolding for Edge operator workflows.",
            "instructions": (
                "Edge is a bounded crypto research operator surface. Use the proposal tools to inspect, write, or issue bounded runs. "
                "The end-to-end research cycle is: 1. Inspect repo/runs, 2. Write proposal (edge_write_proposal), 3. Preflight (edge_preflight_proposal), 4. Run discovery (edge_discover_run). "
                "Prefer report tools for existing runs, and use the render tool only after a data tool has returned compact structured content."
            ),
        }
    if normalized == "repo":
        return {
            "profile": "repo",
            "app_name": "Edge Repository",
            "version": "0.1.0",
            "description": "Read-only repository inspection surface for ChatGPT.",
            "instructions": (
                "This profile exposes read-only repository inspection tools only. Use it to read files, inspect diffs, and search the repo root. "
                "Do not assume mutation or workflow-issuing tools exist in this profile."
            ),
        }
    raise ValueError(f"Unknown ChatGPT app profile: {profile}")
