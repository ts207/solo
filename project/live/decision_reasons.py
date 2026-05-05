from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

DecisionDomain = Literal[
    "signal",
    "execution",
    "portfolio",
    "thesis",
    "data_quality",
    "operator",
    "unknown",
]


@dataclass(frozen=True)
class RuntimeDecisionReason:
    code: str
    domain: DecisionDomain
    message: str
    details: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "domain": self.domain,
            "message": self.message,
            "details": dict(self.details or {}),
        }


SIGNAL_CONTEXT_FAILED = "signal_context_failed"
EXECUTION_CONTEXT_DEGRADED = "execution_context_degraded"
PORTFOLIO_RISK_FAILED = "portfolio_risk_failed"
THESIS_REQUIREMENT_FAILED = "thesis_requirement_failed"
DATA_QUALITY_FAILED = "data_quality_failed"
OPERATOR_APPROVAL_REQUIRED = "operator_approval_required"

_DOMAIN_BY_CODE: dict[str, DecisionDomain] = {
    SIGNAL_CONTEXT_FAILED: "signal",
    EXECUTION_CONTEXT_DEGRADED: "execution",
    PORTFOLIO_RISK_FAILED: "portfolio",
    THESIS_REQUIREMENT_FAILED: "thesis",
    DATA_QUALITY_FAILED: "data_quality",
    OPERATOR_APPROVAL_REQUIRED: "operator",
}


def build_decision_reason(
    code: str,
    *,
    message: str | None = None,
    details: dict[str, Any] | None = None,
) -> RuntimeDecisionReason:
    normalized = str(code or "unknown").strip().lower() or "unknown"
    domain = _DOMAIN_BY_CODE.get(normalized, "unknown")
    return RuntimeDecisionReason(
        code=normalized,
        domain=domain,
        message=message or normalized.replace("_", " "),
        details=details,
    )


def classify_skip_reason(code: str) -> DecisionDomain:
    return build_decision_reason(code).domain
