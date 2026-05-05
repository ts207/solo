from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json


@dataclass(frozen=True)
class PaperGateResult:
    status: str
    eligible_next_state: str
    reason_codes: list[str]
    summary_path: str


def evaluate_paper_gate(summary_path: Path, *, require_runtime_checks: bool = False) -> PaperGateResult:
    if not summary_path.exists():
        return PaperGateResult(
            status="fail",
            eligible_next_state="paper_enabled",
            reason_codes=["missing_paper_summary"],
            summary_path=str(summary_path),
        )

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    reasons: list[str] = []

    if int(summary.get("trade_count", 0)) < 30:
        reasons.append("insufficient_paper_trades")

    if float(summary.get("mean_net_bps", 0.0)) <= 0:
        reasons.append("nonpositive_mean_net_bps")

    if float(summary.get("cumulative_net_bps", 0.0)) <= 0:
        reasons.append("nonpositive_cumulative_net_bps")

    if float(summary.get("hit_rate", 0.0)) <= 0.50:
        reasons.append("weak_hit_rate")

    if float(summary.get("degraded_cost_fraction", 1.0)) > 0.20:
        reasons.append("cost_attribution_degraded")

    if not bool(summary.get("paper_gate_ready", False)):
        reasons.append("paper_quality_summary_not_gate_ready")

    if float(summary.get("max_drawdown_bps", 999999.0)) >= 500.0:
        reasons.append("excessive_paper_drawdown")

    # Contract-hardening v3: optional runtime parity/freshness checks.  These are
    # opt-in to preserve compatibility with older paper summaries that did not
    # record these fields.  New promotion pipelines should pass
    # require_runtime_checks=True before shadow/live escalation.
    if require_runtime_checks:
        if not bool(summary.get("paper_live_signal_parity_pass", False)):
            reasons.append("paper_live_signal_parity_failed")
        if not bool(summary.get("feature_freshness_pass", False)):
            reasons.append("feature_freshness_failed")
        if not bool(summary.get("event_detection_latency_pass", False)):
            reasons.append("event_detection_latency_failed")
        if not bool(summary.get("paper_fill_attribution_pass", False)):
            reasons.append("paper_fill_attribution_failed")
        if int(summary.get("unresolved_thesis_reconciliation_errors", 0)) > 0:
            reasons.append("unresolved_thesis_reconciliation_errors")

    status = "pass" if not reasons else "fail"

    return PaperGateResult(
        status=status,
        eligible_next_state="paper_approved" if status == "pass" else "paper_enabled",
        reason_codes=reasons,
        summary_path=str(summary_path),
    )


def evaluate_paper_gate_strict(summary_path: Path) -> PaperGateResult:
    """Evaluate paper gate including paper/live parity, freshness, latency, and reconciliation checks."""
    return evaluate_paper_gate(summary_path, require_runtime_checks=True)
