"""
DeploymentApprovalRecord — signed operator artifact required before live_enabled.

This is a standalone record (not embedded in the thesis) so it can be:
  - produced by the escalation workflow independently of the research artifact
  - audited without touching the thesis file
  - revoked by writing a new record with status='revoked'

One record per thesis per escalation event.  The canonical record is the one
with the highest approved_at timestamp and status='approved'.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from project.core.exceptions import DataIntegrityError
from project.live.contracts.promoted_thesis import ThesisCapProfile


class PaperRunMetrics(BaseModel):
    """Summary statistics from the paper run used to justify escalation."""

    model_config = ConfigDict(frozen=True)

    observation_start: str = ""
    observation_end: str = ""
    days_observed: int = Field(default=0, ge=0)
    signal_count: int = Field(default=0, ge=0)
    fill_count: int = Field(default=0, ge=0)
    avg_realized_slippage_bps: Optional[float] = None
    avg_realized_fee_bps: Optional[float] = None
    avg_net_edge_bps: Optional[float] = None
    max_drawdown_pct: Optional[float] = None
    instability_flags: List[str] = Field(default_factory=list)
    quality_summary: str = ""


class ChecklistItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    passed: bool
    notes: str = ""


class DeploymentApprovalRecord(BaseModel):
    """
    Signed operator artifact.  Required for any thesis with deployment_state='live_enabled'.

    Lifecycle:
      operator creates record -> status='pending'
      operator signs off     -> status='approved'
      operator revokes       -> status='revoked' (new record, original immutable)
    """

    model_config = ConfigDict(frozen=True)

    record_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    thesis_id: str = Field(min_length=1)
    thesis_version: str = ""
    promotion_run_id: str = ""
    validation_run_id: str = ""

    status: Literal["pending", "approved", "rejected", "revoked"] = "pending"
    approved_by: str = ""
    approved_at: str = ""  # ISO-8601 UTC
    rejection_reason: str = ""
    revocation_reason: str = ""

    # What this approval enables
    target_deployment_state: Literal["live_eligible", "live_enabled"] = "live_enabled"
    risk_profile_id: str = ""
    cap_profile: ThesisCapProfile = Field(default_factory=ThesisCapProfile)

    # Evidence used to justify this approval
    paper_metrics: PaperRunMetrics = Field(default_factory=PaperRunMetrics)
    checklist: List[ChecklistItem] = Field(default_factory=list)
    notes: str = ""

    @model_validator(mode="after")
    def _validate_approved_fields(self) -> "DeploymentApprovalRecord":
        if self.status == "approved":
            missing = []
            if not self.approved_by:
                missing.append("approved_by")
            if not self.approved_at:
                missing.append("approved_at")
            if not self.risk_profile_id:
                missing.append("risk_profile_id")
            if missing:
                raise ValueError(
                    f"approved DeploymentApprovalRecord missing required fields: {missing}"
                )
        return self

    @property
    def is_valid_for_live(self) -> bool:
        """True only if this record fully authorises live trading."""
        return (
            self.status == "approved"
            and bool(self.approved_by)
            and bool(self.approved_at)
            and bool(self.risk_profile_id)
            and self.cap_profile.is_configured
        )

    @property
    def failed_checklist_items(self) -> List[str]:
        return [item.name for item in self.checklist if not item.passed]

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump(mode="json")

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DeploymentApprovalRecord":
        return cls.model_validate(data)

    @classmethod
    def from_file(cls, path: str | Path) -> "DeploymentApprovalRecord":
        p = Path(path)
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DataIntegrityError(f"Failed to read deployment approval record {p}: {exc}") from exc
        if not isinstance(data, dict):
            raise DataIntegrityError(f"Deployment approval record {p} must be a JSON object")
        return cls.from_dict(data)

    def save(self, path: str | Path) -> Path:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(self.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
        return p


def create_approval_record(
    *,
    thesis_id: str,
    approved_by: str,
    risk_profile_id: str,
    cap_profile: ThesisCapProfile,
    paper_metrics: PaperRunMetrics,
    checklist: List[ChecklistItem],
    thesis_version: str = "",
    promotion_run_id: str = "",
    validation_run_id: str = "",
    notes: str = "",
) -> DeploymentApprovalRecord:
    """Helper: build an approved DeploymentApprovalRecord stamped with utcnow."""
    return DeploymentApprovalRecord(
        thesis_id=thesis_id,
        thesis_version=thesis_version,
        promotion_run_id=promotion_run_id,
        validation_run_id=validation_run_id,
        status="approved",
        approved_by=approved_by,
        approved_at=datetime.now(timezone.utc).isoformat(),
        risk_profile_id=risk_profile_id,
        cap_profile=cap_profile,
        paper_metrics=paper_metrics,
        checklist=checklist,
        notes=notes,
        target_deployment_state="live_enabled",
    )
