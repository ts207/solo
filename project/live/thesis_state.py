from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime

_LOG = logging.getLogger(__name__)


@dataclass
class RuntimeThesisState:
    thesis_id: str
    promotion_class: str
    deployment_mode: str  # monitor_only, paper_only, live_enabled
    state: str = "eligible"  # eligible, active, paused, degraded, disabled
    size_scalar: float = 1.0
    disable_reason: str = ""
    last_health_update: str = ""
    cap_breach_count: int = 0

    def transition_to(self, new_state: str, reason: str = ""):
        allowed_states = {"eligible", "active", "paused", "degraded", "disabled"}
        if new_state not in allowed_states:
            raise ValueError(f"Invalid state: {new_state}")

        _LOG.info(
            "Thesis %s transitioning from %s to %s. Reason: %s",
            self.thesis_id,
            self.state,
            new_state,
            reason,
        )
        self.state = new_state
        if reason:
            self.disable_reason = reason


class ThesisStateManager:
    def __init__(self):
        self.states: dict[str, RuntimeThesisState] = {}

    def register_thesis(self, thesis_id: str, promotion_class: str, deployment_mode: str):
        if thesis_id not in self.states:
            self.states[thesis_id] = RuntimeThesisState(
                thesis_id=thesis_id,
                promotion_class=promotion_class,
                deployment_mode=deployment_mode,
            )

    def get_state(self, thesis_id: str) -> RuntimeThesisState | None:
        return self.states.get(thesis_id)

    def update_health(self, thesis_id: str, health_state: str, actions: list[str]):
        state = self.get_state(thesis_id)
        if not state:
            return

        state.last_health_update = datetime.now(UTC).isoformat()

        if health_state == "disabled":
            state.size_scalar = 0.0
            state.transition_to("disabled", reason="decay_monitor_disable")
        elif health_state == "degraded":
            state.transition_to("degraded", reason="decay_monitor_degraded")
            # Extract downsize factor if present
            for action in actions:
                if action.startswith("downsize_"):
                    try:
                        state.size_scalar = float(action.split("_")[1])
                    except ValueError:
                        pass
        elif health_state == "watch":
            if state.state == "active":
                state.transition_to("active", reason="decay_monitor_watch")
        elif health_state == "healthy":
            if state.state in ("degraded", "watch"):
                state.transition_to("active", reason="decay_monitor_recovered")
                state.size_scalar = 1.0

    def apply_live_quality_decision(
        self,
        thesis_id: str,
        *,
        action: str,
        risk_scale: float,
        reason: str,
    ) -> None:
        state = self.get_state(thesis_id)
        if not state:
            return
        state.last_health_update = datetime.now(UTC).isoformat()
        if action == "disable":
            state.size_scalar = 0.0
            state.transition_to("disabled", reason=f"live_quality:{reason}")
        elif action == "downscale":
            state.size_scalar = max(0.0, min(1.0, float(risk_scale)))
            state.transition_to("degraded", reason=f"live_quality:{reason}")
        elif action == "allow" and state.state == "degraded":
            state.size_scalar = 1.0
            state.transition_to("active", reason="live_quality_recovered")
