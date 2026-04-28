from dataclasses import dataclass
from pathlib import Path
import json


@dataclass(frozen=True)
class PaperGateResult:
    status: str
    eligible_next_state: str
    reason_codes: list[str]
    summary_path: str


def evaluate_paper_gate(summary_path: Path) -> PaperGateResult:
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

    status = "pass" if not reasons else "fail"

    return PaperGateResult(
        status=status,
        eligible_next_state="paper_approved" if status == "pass" else "paper_enabled",
        reason_codes=reasons,
        summary_path=str(summary_path),
    )
