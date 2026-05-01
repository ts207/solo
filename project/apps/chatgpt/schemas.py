from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class ProposalRequestBase(BaseModel):
    proposal: str = Field(description="Path to a proposal YAML or JSON file.")
    registry_root: str = Field(
        default="project/configs/registries",
        description="Registry root used to resolve search and spec defaults.",
    )
    data_root: str | None = Field(
        default=None,
        description="Optional data root override. Falls back to BACKTEST_DATA_ROOT or repo-local data.",
    )
    out_dir: str | None = Field(
        default=None,
        description="Optional artifact output directory. If omitted, the ChatGPT wrappers use a scratch directory where possible.",
    )


class ProposalPreviewInput(ProposalRequestBase):
    include_experiment_config: bool = Field(
        default=True,
        description="Include the translated experiment config in the returned structured content.",
    )


class ProposalLintInput(ProposalRequestBase):
    pass


class ProposalExplainInput(ProposalRequestBase):
    pass


class ProposalPreflightInput(ProposalRequestBase):
    json_output: str | None = Field(
        default=None,
        description="Optional path to a JSON report copy.",
    )


class ProposalWriteInput(BaseModel):
    proposal_content: str = Field(
        description="Full YAML or JSON content of the research proposal."
    )
    filename: str = Field(
        description="Target filename (e.g. 'my_proposal.yaml'). Must be repo-relative or just the basename."
    )
    directory: str = Field(
        default="project/configs/proposals",
        description="Target directory inside the repository.",
    )


class ProposalIssueInput(BaseModel):
    proposal: str = Field(description="Path to a proposal YAML or JSON file.")
    registry_root: str = Field(
        default="project/configs/registries",
        description="Registry root used to resolve search and spec defaults.",
    )
    data_root: str | None = Field(
        default=None,
        description="Optional data root override.",
    )
    run_id: str | None = Field(
        default=None,
        description="Optional explicit run identifier. If omitted, Edge generates one.",
    )
    check: bool = Field(
        default=False,
        description="Run the existing bounded proposal check path before returning.",
    )


class RunDiagnosticsInput(BaseModel):
    run_id: str = Field(description="Existing run identifier.")
    program_id: str | None = Field(
        default=None,
        description="Optional program identifier when the report builder needs it.",
    )
    data_root: str | None = Field(
        default=None,
        description="Optional data root override.",
    )


class RegimeReportInput(BaseModel):
    run_id: str = Field(description="Existing run identifier.")
    data_root: str | None = Field(
        default=None,
        description="Optional data root override.",
    )


class CompareRunsInput(BaseModel):
    run_ids: list[str] = Field(
        min_length=2,
        max_length=6,
        description="Two or more run identifiers to compare across slices. Max 6.",
    )
    program_id: str | None = Field(
        default=None,
        description="Optional program identifier for the comparison report.",
    )
    data_root: str | None = Field(
        default=None,
        description="Optional data root override.",
    )


class DiscoverRunConfirmations(BaseModel):
    understands_writes_artifacts: bool = Field(
        default=False,
        description="Acknowledge that this run will write durable artifacts to the data root.",
    )
    no_live_trading: bool = Field(
        default=False,
        description="Confirm that this run is for research/paper purposes only and will not be used for live trading without further promotion.",
    )
    no_threshold_relaxation: bool = Field(
        default=False,
        description="Confirm that no safety thresholds or validation gates are being relaxed for this run.",
    )
    no_posthoc_rescue: bool = Field(
        default=False,
        description="Acknowledge that failed candidates will not be 'rescued' or manually promoted if they miss gates.",
    )


class DiscoverRunInput(BaseModel):
    proposal: str = Field(
        description=(
            "Repo-relative or absolute path to a proposal YAML or JSON file "
            "(e.g. project/configs/proposals/my_proposal.yaml). "
            "The file must already exist on disk. Use edge_preview_plan to validate inline YAML."
        )
    )
    registry_root: str = Field(
        default="project/configs/registries",
        description="Registry root used to resolve search and spec defaults.",
    )
    run_id: str | None = Field(
        default=None,
        description="Optional explicit run identifier. Edge generates one if omitted.",
    )
    data_root: str | None = Field(
        default=None,
        description="Optional data root override. Falls back to BACKTEST_DATA_ROOT or repo-local data.",
    )
    check: bool = Field(
        default=False,
        description="If true, run the bounded proposal check path before executing.",
    )
    confirmations: DiscoverRunConfirmations = Field(
        default_factory=DiscoverRunConfirmations,
        description="Required operator confirmations for discovery runs.",
    )


class ValidateRunInput(BaseModel):
    run_id: str = Field(description="Run identifier from a completed Stage 1 discovery run.")
    data_root: str | None = Field(
        default=None,
        description="Optional data root override.",
    )
    timeout_sec: int = Field(
        default=600,
        ge=30,
        le=3600,
        description="Maximum seconds to wait for the validation pipeline before returning a timeout result.",
    )


class PromoteRunConfirmations(BaseModel):
    canonical_validation_passed: bool = Field(
        default=False,
        description="Confirm that the Stage 2 validation run passed with canonical settings.",
    )
    promotion_gates_must_hold: bool = Field(
        default=False,
        description="Acknowledge that promotion gates will be strictly enforced and cannot be bypassed.",
    )
    do_not_export_rejected_candidates: bool = Field(
        default=False,
        description="Confirm that candidates rejected by gates must not be exported to the live thesis batch.",
    )
    confirm_governed_write: bool = Field(
        default=False,
        description="Explicit confirmation to write governed promotion artifacts to the live/theses directory.",
    )


class PromoteRunInput(BaseModel):
    run_id: str = Field(description="Run identifier from a completed Stage 2 validation run.")
    symbols: str = Field(
        description="Comma-separated symbol list for the promotion bundle (e.g. BTCUSDT,ETHUSDT).",
    )
    retail_profile: str = Field(
        default="capital_constrained",
        description="Promotion profile key. Typical values: capital_constrained, research.",
    )
    data_root: str | None = Field(
        default=None,
        description="Optional data root override.",
    )
    timeout_sec: int = Field(
        default=300,
        ge=30,
        le=3600,
        description="Maximum seconds to wait for the promotion pipeline.",
    )
    confirmations: PromoteRunConfirmations = Field(
        default_factory=PromoteRunConfirmations,
        description="Required operator confirmations for promotion runs.",
    )


class ListThesesInput(BaseModel):
    data_root: str | None = Field(
        default=None,
        description="Optional data root override. Falls back to BACKTEST_DATA_ROOT or repo-local data.",
    )


class CatalogListRunsInput(BaseModel):
    stage: str | None = Field(
        default=None,
        description="Optional stage filter: discover, validate, promote, or deploy.",
    )
    data_root: str | None = Field(
        default=None,
        description="Optional data root override.",
    )
    limit: int = Field(
        default=20,
        ge=1,
        le=100,
        description="Maximum number of runs to return.",
    )



class CodexOperatorInvokeInput(BaseModel):
    task: str = Field(
        description="Instruction passed to Codex for inspecting, editing, or repairing the Edge repository itself.",
        min_length=1,
    )
    thread_id: str | None = Field(
        default=None,
        description="Optional Codex MCP thread ID. When provided, the app continues that Codex session via codex-reply instead of starting a new one.",
    )
    sandbox: Literal["read-only", "workspace-write"] = Field(
        default="workspace-write",
        description="Sandbox passed to the initial Codex MCP session. Use read-only for inspection and workspace-write for code or artifact changes.",
    )
    model: str | None = Field(
        default=None,
        description="Optional Codex MCP model override for a new session.",
    )
    profile: str | None = Field(
        default=None,
        description="Optional Codex CLI profile override for a new session.",
    )
    timeout_sec: int = Field(
        default=300,
        ge=15,
        le=3600,
        description="Maximum seconds to wait for the Codex MCP tool call before returning a timeout payload and post-run probes. Increase this for larger repo fixes.",
    )


class OperatorDashboardInput(BaseModel):
    program_id: str | None = Field(
        default=None,
        description="Optional program identifier. If omitted, the dashboard picks the most recently active program with memory or runs.",
    )
    run_id: str | None = Field(
        default=None,
        description="Optional run identifier to pin in the prior-results panel.",
    )
    data_root: str | None = Field(
        default=None,
        description="Optional data root override. Falls back to BACKTEST_DATA_ROOT or repo-local data.",
    )
    limit: int = Field(
        default=8,
        ge=1,
        le=24,
        description="Maximum number of recent runs, proposals, reflections, and evidence rows to include per section.",
    )


class MemorySummaryInput(BaseModel):
    program_id: str | None = Field(
        default=None,
        description="Optional program identifier. If omitted, the tool picks the active program.",
    )
    data_root: str | None = Field(
        default=None,
        description="Optional data root override.",
    )
    limit: int = Field(
        default=8,
        ge=1,
        le=24,
        description="Maximum number of reflections and evidence rows to include.",
    )


class RenderSection(BaseModel):
    heading: str
    body: str


class RenderOperatorSummaryInput(BaseModel):
    dashboard: dict[str, Any] | None = Field(
        default=None,
        description="Optional full payload returned by edge_get_operator_dashboard. When provided, the render tool passes it through to the widget.",
    )
    title: str | None = Field(default=None, description="Primary card title shown in the widget.")
    status: str | None = Field(default=None, description="Short status label such as pass, warn, or fail.")
    subtitle: str | None = Field(default=None, description="Optional subtitle shown under the title.")
    summary: dict[str, Any] = Field(
        default_factory=dict,
        description="Compact structured summary to show in the widget header.",
    )
    sections: list[RenderSection] = Field(
        default_factory=list,
        description="Ordered detail sections rendered in the widget body.",
    )
    source_tool: str | None = Field(
        default=None,
        description="Name of the data tool that produced the payload rendered here.",
    )


class ToolHints(BaseModel):
    read_only: bool = Field(description="Whether the tool strictly reads data without changing any state.")
    destructive: bool = Field(description="Whether the tool can cause irreversible changes.")
    open_world: bool = Field(description="Whether the tool can affect public internet state.")
    justification: str = Field(description="Short reviewer-facing explanation of the hint classification.")


class ToolDefinition(BaseModel):
    name: str
    title: str
    description: str
    handler: str
    input_model: str
    input_schema: dict[str, Any] = Field(default_factory=dict)
    invoking_text: str
    invoked_text: str
    hints: ToolHints
    category: Literal["data", "mutation", "render"]
    ui_resource_uri: str | None = None
    output_template_uri: str | None = None
