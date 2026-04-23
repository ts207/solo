from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


HISTORICAL_TRUST_TRUSTED = "trusted_under_current_rules"
HISTORICAL_TRUST_LEGACY = "legacy_but_interpretable"
HISTORICAL_TRUST_REQUIRES_REVALIDATION = "requires_revalidation"

CANONICAL_HISTORICAL_TRUST_STATUSES = frozenset(
    {
        HISTORICAL_TRUST_TRUSTED,
        HISTORICAL_TRUST_LEGACY,
        HISTORICAL_TRUST_REQUIRES_REVALIDATION,
    }
)

_TRUST_SEVERITY = {
    HISTORICAL_TRUST_TRUSTED: 0,
    HISTORICAL_TRUST_LEGACY: 1,
    HISTORICAL_TRUST_REQUIRES_REVALIDATION: 2,
}


@dataclass(frozen=True)
class HistoricalTrustStamp:
    historical_trust_status: str
    historical_trust_reason: str
    canonical_reuse_allowed: bool
    compat_reuse_allowed: bool
    inference_confidence: str = "high"

    def to_dict(self) -> dict[str, object]:
        return {
            "historical_trust_status": self.historical_trust_status,
            "historical_trust_reason": self.historical_trust_reason,
            "canonical_reuse_allowed": self.canonical_reuse_allowed,
            "compat_reuse_allowed": self.compat_reuse_allowed,
            "inference_confidence": self.inference_confidence,
        }


def trusted_under_current_rules(
    reason: str = "validated_against_current_contract",
    *,
    confidence: str = "high",
) -> HistoricalTrustStamp:
    return HistoricalTrustStamp(
        historical_trust_status=HISTORICAL_TRUST_TRUSTED,
        historical_trust_reason=str(reason).strip() or "validated_against_current_contract",
        canonical_reuse_allowed=True,
        compat_reuse_allowed=True,
        inference_confidence=confidence,
    )


def legacy_but_interpretable(
    reason: str = "interpretable_but_not_current_contract",
    *,
    confidence: str = "medium",
) -> HistoricalTrustStamp:
    return HistoricalTrustStamp(
        historical_trust_status=HISTORICAL_TRUST_LEGACY,
        historical_trust_reason=str(reason).strip() or "interpretable_but_not_current_contract",
        canonical_reuse_allowed=False,
        compat_reuse_allowed=True,
        inference_confidence=confidence,
    )


def requires_revalidation(
    reason: str = "artifact_requires_revalidation",
    *,
    confidence: str = "high",
) -> HistoricalTrustStamp:
    return HistoricalTrustStamp(
        historical_trust_status=HISTORICAL_TRUST_REQUIRES_REVALIDATION,
        historical_trust_reason=str(reason).strip() or "artifact_requires_revalidation",
        canonical_reuse_allowed=False,
        compat_reuse_allowed=False,
        inference_confidence=confidence,
    )


def aggregate_historical_trust(stamps: Iterable[HistoricalTrustStamp]) -> HistoricalTrustStamp:
    materialized = [stamp for stamp in stamps if isinstance(stamp, HistoricalTrustStamp)]
    if not materialized:
        return requires_revalidation("no_historical_trust_evidence")
    ordered = sorted(
        materialized,
        key=lambda stamp: _TRUST_SEVERITY.get(stamp.historical_trust_status, 99),
        reverse=True,
    )
    top = ordered[0]
    return HistoricalTrustStamp(
        historical_trust_status=top.historical_trust_status,
        historical_trust_reason=top.historical_trust_reason,
        canonical_reuse_allowed=all(stamp.canonical_reuse_allowed for stamp in materialized),
        compat_reuse_allowed=all(stamp.compat_reuse_allowed for stamp in materialized),
        inference_confidence=top.inference_confidence,
    )


__all__ = [
    "CANONICAL_HISTORICAL_TRUST_STATUSES",
    "HISTORICAL_TRUST_LEGACY",
    "HISTORICAL_TRUST_REQUIRES_REVALIDATION",
    "HISTORICAL_TRUST_TRUSTED",
    "HistoricalTrustStamp",
    "aggregate_historical_trust",
    "legacy_but_interpretable",
    "requires_revalidation",
    "trusted_under_current_rules",
]
