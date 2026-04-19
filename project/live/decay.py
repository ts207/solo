from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List

_LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class DecayRule:
    rule_id: str
    metric: str  # edge, hit_rate, payoff, slippage
    threshold: float
    window_samples: int
    action: str  # warn, downsize, disable
    downsize_factor: float = 0.5


@dataclass
class ThesisHealthSnapshot:
    thesis_id: str
    timestamp: str
    health_state: str  # healthy, watch, degraded, disabled
    realized_edge_bps: float
    expected_edge_bps: float
    hit_rate: float
    sample_count: int
    actions_taken: List[str] = field(default_factory=list)
    reason_codes: List[str] = field(default_factory=list)


def default_decay_rules() -> List[DecayRule]:
    """
    Conservative default decay rules applied when the operator provides none.

    - edge_decay: downsize to 50% when realized edge falls below 50% of expected
      for 10+ samples.  Protects against gradual alpha erosion.
    - slippage_spike: downsize when realized slippage exceeds 2× research
      calibration (20 bps) for 5+ samples.  Catches execution regime change.
    - hit_rate_decay: emit warning when hit rate falls below 40% for 10+ samples.
      Early signal of regime incompatibility.
    """
    return [
        DecayRule(
            rule_id="edge_decay_default",
            metric="edge",
            threshold=0.50,
            window_samples=10,
            action="downsize",
            downsize_factor=0.50,
        ),
        DecayRule(
            rule_id="slippage_spike_default",
            metric="slippage",
            threshold=20.0,
            window_samples=5,
            action="downsize",
            downsize_factor=0.50,
        ),
        DecayRule(
            rule_id="hit_rate_decay_default",
            metric="hit_rate",
            threshold=0.40,
            window_samples=10,
            action="warn",
        ),
    ]


class DecayMonitor:
    def __init__(self, rules: List[DecayRule]):
        self.rules = rules
        self.health_history: List[ThesisHealthSnapshot] = []

    def assess_thesis_health(
        self,
        thesis_id: str,
        realized_metrics: Dict[str, Any],
        expected_metrics: Dict[str, Any],
    ) -> ThesisHealthSnapshot:
        """
        Assess health of a single thesis based on realized vs expected performance.
        """
        realized_edge = float(realized_metrics.get("avg_realized_net_edge_bps", 0.0))
        expected_edge = float(expected_metrics.get("net_expectancy_bps", 0.0))
        realized_hit_rate = float(realized_metrics.get("hit_rate", 0.0))
        sample_count = int(realized_metrics.get("sample_count", 0))

        health_state = "healthy"
        actions = []
        reasons = []

        realized_slippage = float(realized_metrics.get("avg_realized_slippage_bps", 0.0))
        payoff_ratio = float(realized_metrics.get("payoff_ratio", 0.0))

        for rule in self.rules:
            if sample_count < rule.window_samples:
                continue

            triggered = False
            if rule.metric == "edge":
                if realized_edge < rule.threshold * expected_edge:
                    triggered = True
            elif rule.metric == "hit_rate":
                if realized_hit_rate < rule.threshold:
                    triggered = True
            elif rule.metric == "slippage":
                # Trigger when realized slippage exceeds threshold bps (cost drag)
                if realized_slippage > rule.threshold:
                    triggered = True
            elif rule.metric == "payoff":
                # Trigger when payoff ratio (avg_win / avg_loss) falls below threshold
                if payoff_ratio > 0.0 and payoff_ratio < rule.threshold:
                    triggered = True

            if triggered:
                reasons.append(f"decay_{rule.metric}")
                if rule.action == "disable":
                    health_state = "disabled"
                    actions.append("disable")
                elif rule.action == "downsize" and health_state != "disabled":
                    health_state = "degraded"
                    actions.append(f"downsize_{rule.downsize_factor}")
                elif rule.action == "warn" and health_state not in ("disabled", "degraded"):
                    health_state = "watch"
                    actions.append("warn")

        snapshot = ThesisHealthSnapshot(
            thesis_id=thesis_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            health_state=health_state,
            realized_edge_bps=realized_edge,
            expected_edge_bps=expected_edge,
            hit_rate=realized_hit_rate,
            sample_count=sample_count,
            actions_taken=actions,
            reason_codes=reasons,
        )
        self.health_history.append(snapshot)
        return snapshot

    def thesis_decay_rate(self, thesis_id: str, *, window: int = 20) -> float:
        return calculate_thesis_decay_rate(
            [item for item in self.health_history if item.thesis_id == thesis_id][-window:]
        )


def calculate_thesis_decay_rate(snapshots: List[ThesisHealthSnapshot]) -> float:
    if not snapshots:
        return 0.0
    degraded = sum(
        1
        for item in snapshots
        if item.health_state in {"watch", "degraded", "disabled"} or item.reason_codes
    )
    return float(degraded / len(snapshots))
