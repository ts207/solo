from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from project.domain.compiled_registry import get_domain_registry


class TriggerType:
    EVENT = "event"
    STATE = "state"
    TRANSITION = "transition"
    FEATURE_PREDICATE = "feature_predicate"
    SEQUENCE = "sequence"
    INTERACTION = "interaction"


@dataclass(frozen=True)
class TriggerSpec:
    trigger_type: str
    event_id: Optional[str] = None
    event_direction: Optional[str] = None
    state_id: Optional[str] = None
    state_active: bool = True
    from_state: Optional[str] = None
    to_state: Optional[str] = None
    feature: Optional[str] = None
    operator: Optional[str] = None
    threshold: Optional[float] = None
    sequence_id: Optional[str] = None
    events: Optional[List[str]] = None
    max_gap: Optional[List[int]] = None
    interaction_id: Optional[str] = None
    left: Optional[str] = None
    right: Optional[str] = None
    left_direction: Optional[str] = None
    right_direction: Optional[str] = None
    op: Optional[str] = None
    lag: Optional[int] = None
    _enable_validation: bool = field(default=True, init=False, repr=False)

    def validate(self) -> None:
        if not self._enable_validation:
            return

        registry = get_domain_registry()
        t = self.trigger_type

        valid_states = list(registry.valid_state_ids)
        if (
            t in {TriggerType.STATE, TriggerType.TRANSITION, TriggerType.INTERACTION}
            and not valid_states
        ):
            raise ValueError("State registry is empty or missing")

        if t == TriggerType.EVENT:
            if not self.event_id:
                raise ValueError("TriggerSpec(event) requires event_id")
            if not registry.has_event(self.event_id):
                raise ValueError(f"Unknown event_id: {self.event_id!r}")
            if self.event_direction and self.event_direction.lower() not in {"up", "down", "non_directional"}:
                raise ValueError(
                    f"Invalid event_direction: {self.event_direction!r}. "
                    "Expected one of 'up', 'down', 'non_directional'."
                )
        elif t == TriggerType.STATE:
            if not self.state_id:
                raise ValueError("TriggerSpec(state) requires state_id")
            if valid_states and self.state_id.upper() not in [s.upper() for s in valid_states]:
                raise ValueError(f"Unknown state_id: {self.state_id!r}")
        elif t == TriggerType.TRANSITION:
            if not self.from_state or not self.to_state:
                raise ValueError("TriggerSpec(transition) requires from_state and to_state")
            if valid_states:
                if self.from_state.upper() not in [s.upper() for s in valid_states]:
                    raise ValueError(f"Unknown from_state: {self.from_state!r}")
                if self.to_state.upper() not in [s.upper() for s in valid_states]:
                    raise ValueError(f"Unknown to_state: {self.to_state!r}")
        elif t == TriggerType.FEATURE_PREDICATE:
            if not self.feature or not self.operator or self.threshold is None:
                raise ValueError(
                    "TriggerSpec(feature_predicate) requires feature, operator, and threshold"
                )
            if self.operator.strip() not in [">=", "<=", ">", "<", "=="]:
                raise ValueError(f"Invalid operator: {self.operator!r}")
        elif t == TriggerType.SEQUENCE:
            if not self.sequence_id or not self.events:
                raise ValueError("TriggerSpec(sequence) requires sequence_id and events")
            if self.max_gap is not None:
                if len(self.max_gap) != len(self.events) - 1:
                    raise ValueError(
                        f"Sequence max_gap length ({len(self.max_gap)}) must be len(events)-1 ({len(self.events) - 1})"
                    )
                if any(g < 0 for g in self.max_gap):
                    raise ValueError("Sequence max_gap values must be non-negative")
            for eid in self.events:
                if not registry.has_event(eid):
                    raise ValueError(f"Unknown event in sequence: {eid!r}")
        elif t == TriggerType.INTERACTION:
            if not self.interaction_id or not self.left or not self.right or not self.op:
                raise ValueError(
                    "TriggerSpec(interaction) requires interaction_id, left, right, and op"
                )
            if self.op.lower() not in ["and", "or", "confirm", "exclude"]:
                raise ValueError(f"Invalid interaction operator: {self.op!r}")
            for direction_field in ("left_direction", "right_direction"):
                direction_value = getattr(self, direction_field)
                if direction_value and direction_value.lower() not in {"up", "down", "non_directional"}:
                    raise ValueError(
                        f"Invalid {direction_field}: {direction_value!r}. "
                        "Expected one of 'up', 'down', 'non_directional'."
                    )
            for side_id in [self.left, self.right]:
                sid_up = side_id.upper()
                in_events = registry.has_event(sid_up)
                in_states = valid_states and sid_up in [s.upper() for s in valid_states]
                if not (in_events or in_states):
                    raise ValueError(
                        f"Interaction component {side_id!r} not found in event or state registries"
                    )
        else:
            raise ValueError(f"Unknown trigger_type: {self.trigger_type!r}")

    def to_dict(self) -> Dict[str, Any]:
        t = self.trigger_type
        d: Dict[str, Any] = {"trigger_type": t}
        if t == TriggerType.EVENT:
            d["event_id"] = self.event_id
            if self.event_direction:
                d["event_direction"] = self.event_direction
        elif t == TriggerType.STATE:
            d["state_id"] = self.state_id
            d["state_active"] = self.state_active
        elif t == TriggerType.TRANSITION:
            d["from_state"] = self.from_state
            d["to_state"] = self.to_state
        elif t == TriggerType.FEATURE_PREDICATE:
            d["feature"] = self.feature
            d["operator"] = self.operator
            d["threshold"] = self.threshold
        elif t == TriggerType.SEQUENCE:
            d["sequence_id"] = self.sequence_id
            d["events"] = self.events
            d["max_gap"] = self.max_gap
        elif t == TriggerType.INTERACTION:
            d["interaction_id"] = self.interaction_id
            d["left"] = self.left
            d["right"] = self.right
            d["left_direction"] = self.left_direction
            d["right_direction"] = self.right_direction
            d["op"] = self.op
            d["lag"] = self.lag
        return d

    def label(self) -> str:
        t = self.trigger_type
        if t == TriggerType.EVENT:
            if self.event_direction:
                return f"event:{self.event_id}:{self.event_direction}"
            return f"event:{self.event_id}"
        if t == TriggerType.STATE:
            suffix = "" if self.state_active else ":inactive"
            return f"state:{self.state_id}{suffix}"
        if t == TriggerType.TRANSITION:
            return f"transition:{self.from_state}→{self.to_state}"
        if t == TriggerType.FEATURE_PREDICATE:
            return f"pred:{self.feature}{self.operator}{self.threshold}"
        if t == TriggerType.SEQUENCE:
            return f"seq:{self.sequence_id}"
        if t == TriggerType.INTERACTION:
            return f"int:{self.interaction_id}({self.op})"
        return f"unknown:{t}"

    @classmethod
    def event(cls, event_id: str, *, event_direction: str | None = None) -> "TriggerSpec":
        return cls(
            trigger_type=TriggerType.EVENT,
            event_id=event_id.upper().strip(),
            event_direction=event_direction,
        )

    @classmethod
    def state(cls, state_id: str, active: bool = True) -> "TriggerSpec":
        return cls(
            trigger_type=TriggerType.STATE, state_id=state_id.upper().strip(), state_active=active
        )

    @classmethod
    def transition(cls, from_state: str, to_state: str) -> "TriggerSpec":
        return cls(
            trigger_type=TriggerType.TRANSITION,
            from_state=from_state.upper().strip(),
            to_state=to_state.upper().strip(),
        )

    @classmethod
    def feature_predicate(cls, feature: str, operator: str, threshold: float) -> "TriggerSpec":
        return cls(
            trigger_type=TriggerType.FEATURE_PREDICATE,
            feature=feature.strip(),
            operator=operator.strip(),
            threshold=float(threshold),
        )

    @classmethod
    def sequence(
        cls, sequence_id: str, events: List[str], max_gap: Optional[List[int]] = None
    ) -> "TriggerSpec":
        return cls(
            trigger_type=TriggerType.SEQUENCE,
            sequence_id=sequence_id.upper().strip(),
            events=[e.upper().strip() for e in events],
            max_gap=max_gap,
        )

    @classmethod
    def interaction(
        cls,
        interaction_id: str,
        left: str,
        right: str,
        op: str,
        lag: int = 6,
        *,
        left_direction: str | None = None,
        right_direction: str | None = None,
    ) -> "TriggerSpec":
        return cls(
            trigger_type=TriggerType.INTERACTION,
            interaction_id=interaction_id.upper().strip(),
            left=left.upper().strip(),
            right=right.upper().strip(),
            left_direction=left_direction,
            right_direction=right_direction,
            op=op.lower().strip(),
            lag=lag,
        )

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "TriggerSpec":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})

    def __post_init__(self):
        if self.trigger_type:
            object.__setattr__(self, "trigger_type", self.trigger_type.lower().strip())
        for field_name in [
            "event_id",
            "state_id",
            "from_state",
            "to_state",
            "sequence_id",
            "interaction_id",
            "left",
            "right",
            "feature",
        ]:
            val = getattr(self, field_name)
            if val is not None:
                object.__setattr__(self, field_name, str(val).upper().strip())
        for field_name in ["operator", "op"]:
            val = getattr(self, field_name)
            if val is not None:
                object.__setattr__(self, field_name, str(val).lower().strip())
        for field_name in ["event_direction", "left_direction", "right_direction"]:
            val = getattr(self, field_name)
            if val is not None:
                object.__setattr__(self, field_name, str(val).lower().strip())
        if self.events:
            object.__setattr__(self, "events", [str(e).upper().strip() for e in self.events])
        if self.threshold is not None:
            object.__setattr__(self, "threshold", float(self.threshold))

        # Commutativity for interaction
        if self.trigger_type == TriggerType.INTERACTION and self.op in ("and", "or"):
            if self.left and self.right and self.left > self.right:
                left, right = self.left, self.right
                object.__setattr__(self, "left", right)
                object.__setattr__(self, "right", left)

        self.validate()


@dataclass(frozen=True)
class HypothesisSpec:
    trigger: TriggerSpec
    direction: str
    horizon: str
    template_id: str
    context: Optional[Dict[str, str]] = field(default=None)
    feature_condition: Optional[TriggerSpec] = field(default=None)
    filter_template_id: Optional[str] = field(default=None)
    entry_lag: int = 1
    cost_profile: str = "standard"
    objective_profile: str = "mean_return"
    _enable_validation: bool = field(default=True, init=False, repr=False)
    _hid: Optional[str] = field(default=None, init=False, repr=False)

    def __post_init__(self):
        if not self._enable_validation:
            object.__setattr__(self.trigger, "_enable_validation", False)
            if self.feature_condition:
                object.__setattr__(self.feature_condition, "_enable_validation", False)
        if not self.context:
            object.__setattr__(self, "context", None)
        else:
            object.__setattr__(self, "context", {k: v for k, v in sorted(self.context.items())})
        object.__setattr__(self, "direction", self.direction.lower().strip())
        if self.filter_template_id is not None:
            object.__setattr__(self, "filter_template_id", str(self.filter_template_id).strip())
        self.trigger.validate()
        if self.feature_condition:
            self.feature_condition.validate()
        payload = json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))
        object.__setattr__(
            self, "_hid", "hyp_" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]
        )

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "trigger": self.trigger.to_dict(),
            "direction": self.direction,
            "horizon": self.horizon,
            "template_id": self.template_id,
            "entry_lag": self.entry_lag,
            "cost_profile": self.cost_profile,
            "objective_profile": self.objective_profile,
        }
        if self.context:
            d["context"] = {k: v for k, v in sorted(self.context.items())}
        if self.feature_condition is not None:
            d["feature_condition"] = self.feature_condition.to_dict()
        if self.filter_template_id:
            d["filter_template_id"] = self.filter_template_id
        return d

    def hypothesis_id(self) -> str:
        return self._hid or (
            "hyp_"
            + hashlib.sha256(
                json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest()[:20]
        )

    def semantic_branch_key(self) -> Dict[str, Any]:
        registry = get_domain_registry()
        operator = registry.get_operator(self.template_id)
        operator_raw = operator.raw if operator is not None and isinstance(operator.raw, dict) else {}
        trigger_payload = self.trigger.to_dict()
        return {
            "trigger": trigger_payload,
            "event_filter": self.feature_condition.to_dict()
            if self.feature_condition is not None
            else None,
            "context_filter": {k: v for k, v in sorted((self.context or {}).items())},
            "side_policy": str(operator_raw.get("side_policy", self.direction)).strip(),
            "direction": self.direction,
            "entry_delay": self.entry_lag,
            "horizon": self.horizon,
            "target_label": str(operator_raw.get("label_target", "")).strip(),
            "cost_model": self.cost_profile,
        }

    def semantic_branch_hash(self) -> str:
        payload = json.dumps(self.semantic_branch_key(), sort_keys=True, separators=(",", ":"))
        return "branch_" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]

    def label(self) -> str:
        parts = [self.trigger.label(), self.direction, self.horizon, self.template_id]
        if self.context:
            parts.append(f"[{','.join(f'{k}={v}' for k, v in sorted(self.context.items()))}]")
        if self.filter_template_id:
            parts.append(f"filter={self.filter_template_id}")
        return "|".join(parts)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "HypothesisSpec":
        trigger = TriggerSpec.from_dict(d["trigger"])
        fc = TriggerSpec.from_dict(d["feature_condition"]) if "feature_condition" in d else None
        return cls(
            trigger=trigger,
            direction=d["direction"],
            horizon=d["horizon"],
            template_id=d["template_id"],
            context=d.get("context"),
            feature_condition=fc,
            filter_template_id=d.get("filter_template_id"),
            entry_lag=d.get("entry_lag", 1),
            cost_profile=d.get("cost_profile", "standard"),
            objective_profile=d.get("objective_profile", "mean_return"),
        )
