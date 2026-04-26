from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator, model_validator

# ---------------------------------------------------------------------------
# Deployment lifecycle states
#   Legacy:  monitor_only | paper_only | live_enabled | retired
#   Sprint 7: adds richer escalation states
# ---------------------------------------------------------------------------
DeploymentState = Literal[
    # Legacy states (kept for backward compat)
    "monitor_only",
    "paper_only",
    # Sprint 7 lifecycle states
    "promoted",
    "paper_enabled",
    "paper_approved",
    "live_eligible",
    "live_enabled",
    "live_paused",
    "live_disabled",
    "retired",
]

ALL_DEPLOYMENT_STATES: frozenset[str] = frozenset({
    "monitor_only",
    "paper_only",
    "promoted",
    "paper_enabled",
    "paper_approved",
    "live_eligible",
    "live_enabled",
    "live_paused",
    "live_disabled",
    "retired",
})

# States that permit live order submission
LIVE_TRADEABLE_STATES: frozenset[str] = frozenset({"live_enabled"})

# States that require an explicit live approval record to be present and approved
LIVE_APPROVAL_REQUIRED_STATES: frozenset[str] = frozenset({"live_eligible", "live_enabled"})


class LiveApproval(BaseModel):
    """Operator sign-off metadata required before a thesis can reach live_enabled."""

    model_config = ConfigDict(frozen=True)

    live_approval_status: Literal["pending", "approved", "rejected", "revoked", ""] = ""
    approved_by: str = ""
    approved_at: str = ""  # ISO-8601 UTC timestamp
    approval_reason: str = ""
    risk_profile_id: str = ""
    paper_run_min_days_required: int = Field(default=0, ge=0)
    paper_run_observed_days: int = Field(default=0, ge=0)
    paper_run_quality_status: Literal["sufficient", "insufficient", "pending", ""] = ""
    venue_allowlist: list[str] = Field(default_factory=list)
    symbol_allowlist: list[str] = Field(default_factory=list)

    @property
    def is_approved(self) -> bool:
        return self.live_approval_status == "approved"

    @property
    def paper_duration_satisfied(self) -> bool:
        if self.paper_run_min_days_required <= 0:
            return True
        return self.paper_run_observed_days >= self.paper_run_min_days_required


class ThesisCapProfile(BaseModel):
    """Per-thesis risk cap profile, enforced at order-submission time."""

    model_config = ConfigDict(frozen=True)

    max_notional: float = Field(default=0.0, ge=0.0)
    max_position_notional: float = Field(default=0.0, ge=0.0)
    max_daily_loss: float = Field(default=0.0, ge=0.0)
    max_active_orders: int = Field(default=0, ge=0)
    max_active_positions: int = Field(default=0, ge=0)
    # Scope of kill-switch triggered by this thesis's breach
    kill_switch_scope: Literal["thesis", "symbol", "family", "global"] = "thesis"

    @property
    def is_configured(self) -> bool:
        """True if at least one hard cap has been explicitly set."""
        return (
            self.max_notional > 0.0 or self.max_position_notional > 0.0 or self.max_daily_loss > 0.0
        )


class ThesisEvidence(BaseModel):
    model_config = ConfigDict(frozen=True)

    sample_size: int = Field(ge=0)
    validation_samples: int = Field(default=0, ge=0)
    test_samples: int = Field(default=0, ge=0)
    estimate_bps: float | None = None
    net_expectancy_bps: float | None = None
    q_value: float | None = None
    stability_score: float | None = None
    cost_survival_ratio: float | None = None
    tob_coverage: float | None = None
    rank_score: float | None = None
    promotion_track: str = ""
    policy_version: str = ""
    bundle_version: str = ""
    stat_regime: str = ""
    audit_status: str = ""
    artifact_audit_version: str = ""


class ThesisLineage(BaseModel):
    model_config = ConfigDict(frozen=True)

    run_id: str = Field(min_length=1)
    candidate_id: str = Field(min_length=1)
    hypothesis_id: str = ""
    plan_row_id: str = ""
    blueprint_id: str = ""
    proposal_id: str = ""
    validation_run_id: str = ""
    validation_status: str = ""
    validation_reason_codes: list[str] = Field(default_factory=list)
    validation_artifact_paths: dict[str, str] = Field(default_factory=dict)

    # Batch Identity Fields
    export_batch_id: str = ""
    export_generated_at: str = ""
    source_run_id: str = ""
    thesis_version: str = "1.0.0"
    source_event_name: str = ""
    source_event_version: str = ""
    source_detector_class: str = ""
    source_evidence_mode: str = ""
    source_threshold_version: str = ""
    source_calibration_artifact: str = ""
    source_discovery_mode: str = ""
    source_cell_id: str = ""
    source_scoreboard_run_id: str = ""
    source_event_atom: str = ""
    source_context_cell: str = ""
    source_contrast_lift_bps: float | None = None


class ThesisGovernance(BaseModel):
    model_config = ConfigDict(frozen=True)

    tier: str = ""
    operational_role: str = ""
    deployment_disposition: str = ""
    evidence_mode: str = ""
    overlap_group_id: str = ""
    trade_trigger_eligible: bool = False
    requires_stronger_evidence: bool = False
    readiness_status: str = ""
    inventory_reason_code: str = ""


class ThesisRequirements(BaseModel):
    model_config = ConfigDict(frozen=True)

    trigger_events: list[str] = Field(default_factory=list)
    confirmation_events: list[str] = Field(default_factory=list)
    required_episodes: list[str] = Field(default_factory=list)
    disallowed_regimes: list[str] = Field(default_factory=list)
    required_states: list[str] = Field(default_factory=list)
    supportive_states: list[str] = Field(default_factory=list)
    deployment_gate: str = ""
    sequence_mode: str = ""
    minimum_episode_confidence: float = 0.0


class ThesisSource(BaseModel):
    model_config = ConfigDict(frozen=True)

    source_program_id: str = ""
    source_campaign_id: str = ""
    source_run_mode: str = ""
    objective_name: str = ""
    event_contract_ids: list[str] = Field(default_factory=list)
    episode_contract_ids: list[str] = Field(default_factory=list)


class PromotedThesis(BaseModel):
    model_config = ConfigDict(frozen=True)

    thesis_id: str = Field(min_length=1)
    promotion_class: Literal["paper_promoted", "production_promoted"] = (
        "paper_promoted"
    )
    deployment_state: DeploymentState = "paper_only"
    # Maximum permitted deployment mode; operator sets this ceiling.
    # A thesis can only reach live_enabled if deployment_mode_allowed >= live_eligible.
    deployment_mode_allowed: Literal["paper_only", "live_eligible", "live_enabled"] = "paper_only"
    # Live approval metadata — must be present and approved for live_enabled theses
    live_approval: LiveApproval = Field(default_factory=LiveApproval)
    # Per-thesis risk cap profile — must be configured for live_enabled theses
    cap_profile: ThesisCapProfile = Field(default_factory=ThesisCapProfile)
    evidence_gaps: list[str] = Field(default_factory=list)
    status: Literal["pending_blueprint", "active", "paused", "retired"] = "pending_blueprint"
    evidence_freshness_date: str = ""
    review_due_date: str = ""
    staleness_class: Literal["fresh", "watch", "stale", "unknown"] = "unknown"
    symbol_scope: dict[str, Any] = Field(default_factory=dict)
    timeframe: str = Field(min_length=1)
    primary_event_id: str = Field(min_length=1)
    # Legacy compatibility metadata only. Runtime matching should prefer
    # primary_event_id and requirements.trigger_events.
    event_family: str = ""
    canonical_regime: str = ""
    event_side: Literal["long", "short", "both", "conditional", "unknown"] = "unknown"
    required_context: dict[str, Any] = Field(default_factory=dict)
    supportive_context: dict[str, Any] = Field(default_factory=dict)
    required_state_ids: list[str] = Field(default_factory=list)
    supportive_state_ids: list[str] = Field(default_factory=list)
    expected_response: dict[str, Any] = Field(default_factory=dict)
    invalidation: dict[str, Any] = Field(default_factory=dict)
    freshness_policy: dict[str, Any] = Field(default_factory=dict)
    risk_notes: list[str] = Field(default_factory=list)
    evidence: ThesisEvidence
    lineage: ThesisLineage
    governance: ThesisGovernance = Field(default_factory=ThesisGovernance)
    requirements: ThesisRequirements = Field(default_factory=ThesisRequirements)
    source: ThesisSource = Field(default_factory=ThesisSource)

    @model_validator(mode="before")
    @classmethod
    def _populate_compat_event_fields(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        requirements = (
            dict(data.get("requirements", {})) if isinstance(data.get("requirements"), dict) else {}
        )
        trigger_clause = data.get("trigger_clause")
        if not requirements.get("trigger_events"):
            if isinstance(trigger_clause, dict):
                requirements["trigger_events"] = list(trigger_clause.get("events", []))
            elif isinstance(trigger_clause, list):
                requirements["trigger_events"] = list(trigger_clause)
        confirmation_clause = data.get("confirmation_clause")
        if not requirements.get("confirmation_events"):
            if isinstance(confirmation_clause, dict):
                requirements["confirmation_events"] = list(confirmation_clause.get("events", []))
            elif isinstance(confirmation_clause, list):
                requirements["confirmation_events"] = list(confirmation_clause)
        if requirements:
            data["requirements"] = requirements

        if "context_clause" in data and not isinstance(data.get("required_context"), dict):
            context_clause = data.get("context_clause")
            if isinstance(context_clause, dict):
                data["required_context"] = dict(context_clause)
        if "invalidation_clause" in data and not isinstance(data.get("invalidation"), dict):
            invalidation_clause = data.get("invalidation_clause")
            if isinstance(invalidation_clause, dict):
                data["invalidation"] = dict(invalidation_clause)

        governance = (
            dict(data.get("governance", {})) if isinstance(data.get("governance"), dict) else {}
        )
        overlap_group_id = str(data.get("overlap_group_id", "")).strip()
        if overlap_group_id and not str(governance.get("overlap_group_id", "")).strip():
            governance["overlap_group_id"] = overlap_group_id
        if governance:
            data["governance"] = governance

        primary_event_id = str(data.get("primary_event_id", "")).strip()
        event_family = str(data.get("event_family", "")).strip()
        if not primary_event_id and event_family:
            primary_event_id = event_family
        if primary_event_id:
            data["primary_event_id"] = primary_event_id
        if event_family:
            data["event_family"] = event_family
        return data

    @field_validator("thesis_id", "timeframe", "primary_event_id")
    @classmethod
    def _validate_non_empty(cls, value: str) -> str:
        if not str(value).strip():
            raise ValueError("field must be non-empty")
        return str(value).strip()

    @field_validator("primary_event_id", "event_family", "canonical_regime")
    @classmethod
    def _normalize_optional_tokens(cls, value: str) -> str:
        return str(value).strip().upper()

    @computed_field(return_type=dict)
    @property
    def trigger_clause(self) -> dict[str, Any]:
        return {"events": list(self.requirements.trigger_events)}

    @computed_field(return_type=dict)
    @property
    def confirmation_clause(self) -> dict[str, Any]:
        return {"events": list(self.requirements.confirmation_events)}

    @computed_field(return_type=dict)
    @property
    def invalidation_clause(self) -> dict[str, Any]:
        return dict(self.invalidation)

    @computed_field(return_type=dict)
    @property
    def context_clause(self) -> dict[str, Any]:
        return dict(self.required_context)

    @computed_field(return_type=str)
    @property
    def overlap_group_id(self) -> str:
        return str(self.governance.overlap_group_id or "")
