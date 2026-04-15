from __future__ import annotations

from project.live.contracts import PromotedThesis


def thesis_meta_quality_score(thesis: PromotedThesis) -> float:
    """Return a bounded meta-quality score in [0, 1].

    This is intentionally a ranking/calibration sidecar, not a semantic gate.
    It only combines already-governed fields from thesis evidence and governance.
    """
    evidence = thesis.evidence
    governance = thesis.governance

    score = 0.0
    if evidence.rank_score is not None:
        score += max(0.0, min(0.25, float(evidence.rank_score) * 0.25))
    if evidence.stability_score is not None:
        score += max(0.0, min(0.20, float(evidence.stability_score) * 0.20))
    if evidence.cost_survival_ratio is not None:
        ratio = max(0.0, min(1.5, float(evidence.cost_survival_ratio)))
        score += min(0.15, ratio / 1.5 * 0.15)
    if evidence.q_value is not None:
        q = max(0.0, min(1.0, float(evidence.q_value)))
        score += (1.0 - q) * 0.15
    sample = max(0, int(evidence.sample_size or 0))
    score += min(0.10, sample / 500.0 * 0.10)

    tier = str(governance.tier or "").strip().upper()
    if tier == "A":
        score += 0.10
    elif tier == "B":
        score += 0.07
    elif tier == "C":
        score += 0.03

    if bool(governance.trade_trigger_eligible):
        score += 0.05

    return max(0.0, min(1.0, score))
