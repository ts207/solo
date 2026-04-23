"""
DeploymentGate — enforces the live approval contract at thesis load time.

Rules (applied in order):
  1. Thesis must be in a recognised deployment state.
  2. Theses with deployment_state in LIVE_APPROVAL_REQUIRED_STATES must have
     live_approval.live_approval_status == 'approved'.
  3. live_enabled theses must additionally have:
       - approved_by and approved_at populated
       - cap_profile.is_configured == True
       - deployment_mode_allowed in {'live_eligible', 'live_enabled'}
       - paper_run quality satisfied (if min days > 0)

Any violation produces a GateRejection, not an exception, so callers can
aggregate violations across a batch and decide policy (warn vs raise).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List

from project.live.contracts.promoted_thesis import (
    ALL_DEPLOYMENT_STATES,
    LIVE_APPROVAL_REQUIRED_STATES,
    LIVE_TRADEABLE_STATES,
    PromotedThesis,
)

_LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class GateRejection:
    thesis_id: str
    deployment_state: str
    reasons: List[str]

    def __str__(self) -> str:
        joined = "; ".join(self.reasons)
        return f"[{self.thesis_id} / {self.deployment_state}] {joined}"


def check_thesis(thesis: PromotedThesis) -> List[str]:
    """
    Return a list of violation strings for *one* thesis.
    Empty list means the thesis passes the gate for its declared deployment_state.
    """
    state = str(thesis.deployment_state or "").strip().lower()
    violations: List[str] = []

    if state not in ALL_DEPLOYMENT_STATES:
        return [f"unrecognized deployment_state: {state}"]

    if state not in LIVE_APPROVAL_REQUIRED_STATES:
        # Non-live states have no extra requirements from this gate.
        return violations

    # -- approval record check ------------------------------------------------
    approval = thesis.live_approval
    if approval.live_approval_status != "approved":
        violations.append(
            f"live_approval.live_approval_status is '{approval.live_approval_status}', "
            "expected 'approved'"
        )
    if not approval.approved_by:
        violations.append("live_approval.approved_by is empty")
    if not approval.approved_at:
        violations.append("live_approval.approved_at is empty")
    if not approval.risk_profile_id:
        violations.append("live_approval.risk_profile_id is empty")

    # -- paper run quality check ----------------------------------------------
    if approval.paper_run_min_days_required > 0 and not approval.paper_duration_satisfied:
        violations.append(
            f"paper run duration insufficient: "
            f"observed={approval.paper_run_observed_days}d, "
            f"required={approval.paper_run_min_days_required}d"
        )
    if approval.paper_run_quality_status == "insufficient":
        violations.append("paper_run_quality_status is 'insufficient'")

    # -- live_enabled-specific checks -----------------------------------------
    if state in LIVE_TRADEABLE_STATES:
        if not thesis.cap_profile.is_configured:
            violations.append(
                "cap_profile has no hard caps configured"
                " (max_notional / max_position_notional / max_daily_loss all zero)"
            )
        if thesis.deployment_mode_allowed not in ("live_eligible", "live_enabled"):
            violations.append(
                f"deployment_mode_allowed is '{thesis.deployment_mode_allowed}'; "
                "must be 'live_eligible' or 'live_enabled' for live trading"
            )

    return violations


class DeploymentGate:
    """
    Evaluates a batch of PromotedThesis objects and produces rejections for any
    that violate the live approval contract.

    Usage:
        gate = DeploymentGate(strict=True)
        gate.validate_batch(theses)   # raises on first violation if strict
        rejections = gate.check_batch(theses)  # returns list of GateRejection
    """

    def __init__(self, *, strict: bool = True) -> None:
        self.strict = strict

    def check_thesis(self, thesis: PromotedThesis) -> List[str]:
        return check_thesis(thesis)

    def check_batch(self, theses: List[PromotedThesis]) -> List[GateRejection]:
        rejections: List[GateRejection] = []
        for thesis in theses:
            reasons = check_thesis(thesis)
            if reasons:
                rejections.append(
                    GateRejection(
                        thesis_id=thesis.thesis_id,
                        deployment_state=thesis.deployment_state,
                        reasons=reasons,
                    )
                )
        return rejections

    def validate_batch(self, theses: List[PromotedThesis]) -> None:
        """Raise RuntimeError on the first gate violation (if strict)."""
        rejections = self.check_batch(theses)
        if not rejections:
            return
        for r in rejections:
            _LOG.error("DeploymentGate rejection: %s", r)
        if self.strict:
            raise RuntimeError(
                f"DeploymentGate blocked {len(rejections)} thesis/theses"
                f" from loading into live runtime. First: {rejections[0]}"
            )

    def filter_tradeable(self, theses: List[PromotedThesis]) -> List[PromotedThesis]:
        """
        Return only theses that are in LIVE_TRADEABLE_STATES AND pass the gate.
        Non-live theses are excluded silently.
        """
        live = [t for t in theses if t.deployment_state in LIVE_TRADEABLE_STATES]
        clean: List[PromotedThesis] = []
        for thesis in live:
            reasons = check_thesis(thesis)
            if reasons:
                _LOG.warning(
                    "DeploymentGate excluded thesis %s from live trading: %s",
                    thesis.thesis_id,
                    "; ".join(reasons),
                )
            else:
                clean.append(thesis)
        return clean
