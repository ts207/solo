from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

_LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class DecayRule:
    rule_id: str
    metric: str  # edge, hit_rate, payoff, slippage
    threshold: float
    window_samples: int
    action: str  # warn, downsize, disable
    downsize_factor: float = 0.5
    # Escalate a repeated breach to disable after this many sample-equivalents.
    # Supports explicit realised fields (edge_breach_samples,
    # decay_edge_breach_samples, breach_sample_count) and repeated breached
    # assessment windows (streak * window_samples).
    disable_threshold_samples: int | None = None


@dataclass
class ThesisHealthSnapshot:
    thesis_id: str
    timestamp: str
    health_state: str  # healthy, watch, degraded, disabled
    realized_edge_bps: float
    expected_edge_bps: float
    hit_rate: float
    sample_count: int
    actions_taken: list[str] = field(default_factory=list)
    reason_codes: list[str] = field(default_factory=list)


def default_decay_rules() -> list[DecayRule]:
    """
    Conservative default decay rules applied when the operator provides none.

    - edge_decay: downsize to 50% when realized edge falls below 50% of expected
      for 10+ samples; auto-disable after a persistent 20-sample-equivalent breach.
    - slippage_spike: downsize when realized slippage exceeds 2× research
      calibration (20 bps) for 5+ samples; auto-disable after 10-sample persistence.
    - hit_rate_decay: emit warning when hit rate falls below 40% for 10+ samples;
      auto-disable after a persistent 20-sample-equivalent breach.
    """
    return [
        DecayRule(
            rule_id="edge_decay_default",
            metric="edge",
            threshold=0.50,
            window_samples=10,
            action="downsize",
            downsize_factor=0.50,
            disable_threshold_samples=20,
        ),
        DecayRule(
            rule_id="slippage_spike_default",
            metric="slippage",
            threshold=20.0,
            window_samples=5,
            action="downsize",
            downsize_factor=0.50,
            disable_threshold_samples=10,
        ),
        DecayRule(
            rule_id="hit_rate_decay_default",
            metric="hit_rate",
            threshold=0.40,
            window_samples=10,
            action="warn",
            disable_threshold_samples=20,
        ),
    ]


class DecayMonitor:
    def __init__(self, rules: list[DecayRule]):
        self.rules = rules
        self.health_history: list[ThesisHealthSnapshot] = []
        self._trigger_streaks: dict[tuple[str, str], int] = {}

    @staticmethod
    def _explicit_breach_samples(realized_metrics: dict[str, Any], metric: str) -> int | None:
        for key in (
            f"decay_{metric}_breach_samples",
            f"{metric}_breach_samples",
            "breach_sample_count",
        ):
            if key in realized_metrics and realized_metrics.get(key) is not None:
                try:
                    return max(0, int(realized_metrics.get(key, 0) or 0))
                except (TypeError, ValueError):
                    return None
        return None

    def _breach_is_persistent(
        self,
        *,
        thesis_id: str,
        rule: DecayRule,
        realized_metrics: dict[str, Any],
        triggered: bool,
    ) -> bool:
        key = (str(thesis_id), str(rule.rule_id))
        if not triggered:
            self._trigger_streaks[key] = 0
            return False

        streak = int(self._trigger_streaks.get(key, 0)) + 1
        self._trigger_streaks[key] = streak
        threshold = rule.disable_threshold_samples
        if threshold is None or threshold <= 0:
            return False

        explicit_samples = self._explicit_breach_samples(realized_metrics, rule.metric)
        if explicit_samples is not None and explicit_samples >= int(threshold):
            return True
        return streak * max(1, int(rule.window_samples)) >= int(threshold)

    def assess_thesis_health(
        self,
        thesis_id: str,
        realized_metrics: dict[str, Any],
        expected_metrics: dict[str, Any],
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

            persistent = self._breach_is_persistent(
                thesis_id=thesis_id,
                rule=rule,
                realized_metrics=realized_metrics,
                triggered=triggered,
            )
            if triggered:
                base_reason = f"decay_{rule.metric}"
                reasons.append(base_reason)
                if persistent:
                    reasons.append(f"{base_reason}_persistent")
                    health_state = "disabled"
                    if "disable" not in actions:
                        actions.append("disable")
                    continue
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
            timestamp=datetime.now(UTC).isoformat(),
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


def calculate_thesis_decay_rate(snapshots: list[ThesisHealthSnapshot]) -> float:
    if not snapshots:
        return 0.0
    degraded = sum(
        1
        for item in snapshots
        if item.health_state in {"watch", "degraded", "disabled"} or item.reason_codes
    )
    return float(degraded / len(snapshots))
