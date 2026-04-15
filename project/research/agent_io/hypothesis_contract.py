from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class NormalizationWarning:
    code: str
    field_path: str
    message: str

    def to_dict(self) -> Dict[str, str]:
        return {
            "code": self.code,
            "field_path": self.field_path,
            "message": self.message,
        }


# Warning and error codes for Sprint 2
STATE_ANCHOR_LEGACY_EXECUTION_ONLY = "state_anchor_legacy_execution_only"
UNSUPPORTED_STATE_ANCHOR_EXECUTION = "unsupported_state_anchor_execution"
UNSUPPORTED_SAMPLING_POLICY_EXECUTION = "unsupported_sampling_policy_execution"
STRICT_TRANSITION_REQUIRED = "strict_transition_required"
MISSING_TRANSITION_HISTORY = "missing_transition_history"
TRANSITION_SEMANTICS_AMBIGUOUS = "transition_semantics_ambiguous"
DEPRECATED_STATE_ANCHOR = "deprecated_state_anchor"


@dataclass(frozen=True)
class FeaturePredicateSpec:
    feature: str
    operator: str
    threshold: Any

    def to_dict(self) -> Dict[str, Any]:
        return {
            "feature": self.feature,
            "operator": self.operator,
            "threshold": self.threshold,
        }


@dataclass(frozen=True)
class AnchorSpec:
    type: str
    event_id: Optional[str] = None
    state_id: Optional[str] = None
    from_state: Optional[str] = None
    to_state: Optional[str] = None
    events: Optional[List[str]] = None
    max_gap_bars: Optional[int] = None
    feature: Optional[str] = None
    operator: Optional[str] = None
    threshold: Optional[Any] = None

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"type": self.type}
        if self.event_id is not None:
            out["event_id"] = self.event_id
        if self.state_id is not None:
            out["state_id"] = self.state_id
        if self.from_state is not None:
            out["from_state"] = self.from_state
        if self.to_state is not None:
            out["to_state"] = self.to_state
        if self.events is not None:
            out["events"] = list(self.events)
        if self.max_gap_bars is not None:
            out["max_gap_bars"] = self.max_gap_bars
        if self.feature is not None:
            out["feature"] = self.feature
        if self.operator is not None:
            out["operator"] = self.operator
        if self.threshold is not None:
            out["threshold"] = self.threshold
        return out


@dataclass(frozen=True)
class FilterSpec:
    states: List[str] = field(default_factory=list)
    regimes: List[str] = field(default_factory=list)
    feature_predicates: List[FeaturePredicateSpec] = field(default_factory=list)
    contexts: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "states": list(self.states),
            "regimes": list(self.regimes),
            "feature_predicates": [f.to_dict() for f in self.feature_predicates],
            "contexts": dict(self.contexts),
        }


@dataclass(frozen=True)
class SamplingPolicySpec:
    mode: str = "episodic"
    entry_lag_bars: int = 1
    overlap_policy: str = "suppress"
    every_n_bars: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "mode": self.mode,
            "entry_lag_bars": self.entry_lag_bars,
            "overlap_policy": self.overlap_policy,
        }
        if self.every_n_bars is not None:
            out["every_n_bars"] = self.every_n_bars
        return out


@dataclass(frozen=True)
class TemplateSpec:
    id: str
    params: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "params": dict(self.params),
        }


@dataclass(frozen=True)
class StructuredHypothesisSpec:
    anchor: AnchorSpec
    filters: FilterSpec
    sampling_policy: SamplingPolicySpec
    template: TemplateSpec
    direction: str
    horizon_bars: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "anchor": self.anchor.to_dict(),
            "filters": self.filters.to_dict(),
            "sampling_policy": self.sampling_policy.to_dict(),
            "template": self.template.to_dict(),
            "direction": self.direction,
            "horizon_bars": self.horizon_bars,
        }


@dataclass(frozen=True)
class StructuredProposal:
    program_id: str
    start: str
    end: str
    symbols: List[str]
    timeframe: str
    hypothesis: StructuredHypothesisSpec
    instrument_classes: List[str] = field(default_factory=list)
    objective_name: str = "default"
    promotion_profile: str = "research"
    search_spec: Dict[str, Any] = field(default_factory=dict)
    avoid_region_keys: List[str] = field(default_factory=list)
    bounded: Optional[Any] = None
    knobs: List[Dict[str, Any]] = field(default_factory=list)
    artifacts: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "program_id": self.program_id,
            "start": self.start,
            "end": self.end,
            "symbols": list(self.symbols),
            "timeframe": self.timeframe,
            "hypothesis": self.hypothesis.to_dict(),
            "instrument_classes": list(self.instrument_classes),
            "objective_name": self.objective_name,
            "promotion_profile": self.promotion_profile,
            "search_spec": dict(self.search_spec),
            "avoid_region_keys": list(self.avoid_region_keys),
            "knobs": [dict(k) for k in self.knobs],
            "artifacts": dict(self.artifacts),
        }
        if self.bounded is not None:
            if hasattr(self.bounded, "to_dict"):
                out["bounded"] = self.bounded.to_dict()
            else:
                out["bounded"] = self.bounded
        return out


def load_structured_proposal_payload(payload: Dict[str, Any]) -> StructuredProposal:
    proposal, warnings = normalize_structured_proposal(payload)
    # For this function, we don't return warnings, but we could log them if needed.
    return proposal


def normalize_structured_proposal(
    raw: Dict[str, Any],
) -> Tuple[StructuredProposal, List[NormalizationWarning]]:
    warnings: List[NormalizationWarning] = []

    program_id = str(raw.get("program_id", "")).strip()
    if not program_id:
        raise ValueError("program_id is required")

    start = str(raw.get("start", "")).strip()
    if not start:
        raise ValueError("start is required")

    end = str(raw.get("end", "")).strip()
    if not end:
        raise ValueError("end is required")

    symbols = _as_str_list(raw.get("symbols"), "symbols")
    if not symbols:
        raise ValueError("symbols are required")

    timeframe = str(raw.get("timeframe", "")).strip()
    if not timeframe:
        raise ValueError("timeframe is required")

    hypothesis_raw = raw.get("hypothesis")
    if not isinstance(hypothesis_raw, dict):
        raise ValueError("hypothesis must be an object")

    hypothesis, hypothesis_warnings = normalize_structured_hypothesis(hypothesis_raw)
    warnings.extend(hypothesis_warnings)

    instrument_classes = _as_str_list(raw.get("instrument_classes"), "instrument_classes")
    objective_name = str(raw.get("objective_name", "default")).strip()
    promotion_profile = str(raw.get("promotion_profile", "research")).strip()

    search_spec = raw.get("search_spec")
    if search_spec is None:
        search_spec = {}
    if not isinstance(search_spec, dict):
        raise ValueError("search_spec must be an object")

    avoid_region_keys = raw.get("avoid_region_keys")
    if avoid_region_keys is None:
        avoid_region_keys = []
    if not isinstance(avoid_region_keys, list):
        raise ValueError("avoid_region_keys must be a list")

    bounded = raw.get("bounded")
    knobs = raw.get("knobs")
    if knobs is None:
        knobs = []
    if not isinstance(knobs, list):
        raise ValueError("knobs must be a list")

    artifacts = raw.get("artifacts")
    if artifacts is None:
        artifacts = {}
    if not isinstance(artifacts, dict):
        raise ValueError("artifacts must be an object")

    proposal = StructuredProposal(
        program_id=program_id,
        start=start,
        end=end,
        symbols=symbols,
        timeframe=timeframe,
        hypothesis=hypothesis,
        instrument_classes=instrument_classes,
        objective_name=objective_name,
        promotion_profile=promotion_profile,
        search_spec=search_spec,
        avoid_region_keys=[str(item).strip() for item in avoid_region_keys if str(item).strip()],
        bounded=bounded,
        knobs=knobs,
        artifacts=artifacts,
    )

    return proposal, warnings


def is_sampling_policy_executable(policy: SamplingPolicySpec) -> bool:
    """Check if the sampling policy is supported for execution in the current engine."""
    if policy.mode != "episodic":
        return False
    if policy.overlap_policy not in {"suppress"}:
        # In Sprint 2, we only honestly support suppress for structured episodic.
        return False
    return True


def is_anchor_executable(anchor: AnchorSpec) -> bool:
    """Check if the anchor type is supported for execution in the current engine."""
    # Sprint 2 executable anchors: event, transition, sequence
    return anchor.type in {"event", "transition", "sequence"}


def sampling_policy_requires_non_legacy_execution(policy: SamplingPolicySpec) -> bool:
    """Check if the sampling policy requires semantics the current engine cannot enforce."""
    return policy.mode not in {"episodic"}


def validate_structured_hypothesis_for_execution(
    hypothesis: StructuredHypothesisSpec,
) -> List[NormalizationWarning]:
    """Perform strict validation for execution eligibility.
    
    Returns a list of warnings or raises ValueError for fatal execution mismatches.
    """
    warnings: List[NormalizationWarning] = []

    # Sampling policy enforcement
    if not is_sampling_policy_executable(hypothesis.sampling_policy):
        raise ValueError(
            f"{UNSUPPORTED_SAMPLING_POLICY_EXECUTION}: sampling mode '{hypothesis.sampling_policy.mode}' "
            f"or overlap policy '{hypothesis.sampling_policy.overlap_policy}' is not executable."
        )

    # Anchor enforcement
    if hypothesis.anchor.type == "state":
        # State anchor is demoted to legacy-only or refused.
        # In Sprint 2, we only allow it if explicitly requested and reduced to legacy mask,
        # but here we follow the rule: if mode is not episodic (which we already checked), fail.
        # But wait, the rule says: "if sampling_policy.mode is anything other than the exact 
        # legacy-compatible persistent mask behavior then execution translation must fail."
        # Current engine's state-trigger is essentially "active now".
        # If the user asks for 'episodic' with 'state', they might mean 'onset'.
        # We refuse it to avoid ambiguity.
        raise ValueError(
            f"{UNSUPPORTED_STATE_ANCHOR_EXECUTION}: structured state anchors are not "
            "executable as primary anchors in the new path."
        )

    if not is_anchor_executable(hypothesis.anchor):
        raise ValueError(
            f"unsupported_anchor_execution: anchor type '{hypothesis.anchor.type}' is not executable."
        )

    if hypothesis.anchor.type == "transition":
        if not hypothesis.anchor.from_state or not hypothesis.anchor.to_state:
            raise ValueError(
                f"{STRICT_TRANSITION_REQUIRED}: transition requires both from_state and to_state."
            )

    return warnings


def normalize_structured_hypothesis(
    raw: Dict[str, Any],
) -> Tuple[StructuredHypothesisSpec, List[NormalizationWarning]]:
    warnings: List[NormalizationWarning] = []

    # Anchor
    anchor_raw = raw.get("anchor")
    if not isinstance(anchor_raw, dict):
        raise ValueError("hypothesis.anchor must be an object")
    anchor, anchor_warnings = _normalize_anchor(anchor_raw)
    warnings.extend(anchor_warnings)

    # Filters
    filters_raw = raw.get("filters")
    if filters_raw is None:
        filters_raw = {}
    if not isinstance(filters_raw, dict):
        raise ValueError("hypothesis.filters must be an object")
    filters = _normalize_filters(filters_raw)

    # Sampling Policy
    sampling_raw = raw.get("sampling_policy")
    if sampling_raw is None:
        sampling_raw = {}
    if not isinstance(sampling_raw, dict):
        raise ValueError("hypothesis.sampling_policy must be an object")
    sampling_policy = _normalize_sampling_policy(sampling_raw)

    # Template
    template_raw = raw.get("template")
    if not isinstance(template_raw, dict):
        raise ValueError("hypothesis.template must be an object")
    template = _normalize_template(template_raw)

    direction = str(raw.get("direction", "")).strip().lower()
    if not direction:
        raise ValueError("hypothesis.direction is required")

    horizon_bars = raw.get("horizon_bars")
    if horizon_bars is None:
        raise ValueError("hypothesis.horizon_bars is required")
    try:
        horizon_bars = int(horizon_bars)
    except (TypeError, ValueError):
        raise ValueError("hypothesis.horizon_bars must be an integer")
    if horizon_bars < 1:
        raise ValueError("hypothesis.horizon_bars must be >= 1")

    spec = StructuredHypothesisSpec(
        anchor=anchor,
        filters=filters,
        sampling_policy=sampling_policy,
        template=template,
        direction=direction,
        horizon_bars=horizon_bars,
    )

    return spec, warnings


def structured_proposal_to_dict(proposal: StructuredProposal) -> Dict[str, Any]:
    return proposal.to_dict()


def _normalize_anchor(raw: Dict[str, Any]) -> Tuple[AnchorSpec, List[NormalizationWarning]]:
    warnings: List[NormalizationWarning] = []
    anchor_type = str(raw.get("type", "")).strip().lower()
    if not anchor_type:
        raise ValueError("anchor.type is required")

    event_id = raw.get("event_id")
    state_id = raw.get("state_id")
    from_state = raw.get("from_state")
    to_state = raw.get("to_state")
    events = raw.get("events")
    max_gap_bars = raw.get("max_gap_bars")
    feature = raw.get("feature")
    operator = raw.get("operator")
    threshold = raw.get("threshold")

    if anchor_type == "event":
        if not event_id:
            raise ValueError("anchor.type 'event' requires event_id")
    elif anchor_type == "transition":
        if not from_state or not to_state:
            raise ValueError("anchor.type 'transition' requires both from_state and to_state")
    elif anchor_type == "sequence":
        events = _as_str_list(events, "anchor.events")
        if len(events) < 2:
            raise ValueError("anchor.type 'sequence' requires at least two event ids in 'events'")
    elif anchor_type == "feature_crossing":
        if not feature:
            raise ValueError("anchor.type 'feature_crossing' requires feature")
        if operator not in {"crosses_above", "crosses_below"}:
            raise ValueError(
                "anchor.type 'feature_crossing' requires operator in {'crosses_above', 'crosses_below'}"
            )
        if threshold is None:
            raise ValueError("anchor.type 'feature_crossing' requires threshold")
    elif anchor_type == "state":
        warnings.append(
            NormalizationWarning(
                code=DEPRECATED_STATE_ANCHOR,
                field_path="hypothesis.anchor.type",
                message="'state' as anchor is deprecated. Use filters instead.",
            )
        )
    else:
        raise ValueError(f"Unsupported anchor type: {anchor_type}")

    if max_gap_bars is not None:
        try:
            max_gap_bars = int(max_gap_bars)
        except (TypeError, ValueError):
            raise ValueError("anchor.max_gap_bars must be an integer")

    spec = AnchorSpec(
        type=anchor_type,
        event_id=str(event_id) if event_id else None,
        state_id=str(state_id) if state_id else None,
        from_state=str(from_state) if from_state else None,
        to_state=str(to_state) if to_state else None,
        events=events,
        max_gap_bars=max_gap_bars,
        feature=str(feature) if feature else None,
        operator=str(operator) if operator else None,
        threshold=threshold,
    )
    return spec, warnings


def _normalize_filters(raw: Dict[str, Any]) -> FilterSpec:
    states = [s.upper() for s in _as_str_list(raw.get("states"), "filters.states")]
    regimes = [r.upper() for r in _as_str_list(raw.get("regimes"), "filters.regimes")]

    feature_predicates_raw = raw.get("feature_predicates")
    feature_predicates: List[FeaturePredicateSpec] = []
    if feature_predicates_raw:
        if not isinstance(feature_predicates_raw, list):
            raise ValueError("filters.feature_predicates must be a list")
        for i, fp in enumerate(feature_predicates_raw):
            if not isinstance(fp, dict):
                raise ValueError(f"filters.feature_predicates[{i}] must be an object")
            feature = fp.get("feature")
            operator = fp.get("operator")
            threshold = fp.get("threshold")
            if not feature or not operator or threshold is None:
                raise ValueError(
                    f"filters.feature_predicates[{i}] requires feature, operator, and threshold"
                )
            feature_predicates.append(
                FeaturePredicateSpec(
                    feature=str(feature),
                    operator=str(operator),
                    threshold=threshold,
                )
            )

    contexts = raw.get("contexts")
    if contexts is None:
        contexts = {}
    if not isinstance(contexts, dict):
        raise ValueError("filters.contexts must be an object")

    return FilterSpec(
        states=states,
        regimes=regimes,
        feature_predicates=feature_predicates,
        contexts=dict(contexts),
    )


def _normalize_sampling_policy(raw: Dict[str, Any]) -> SamplingPolicySpec:
    mode = str(raw.get("mode", "episodic")).strip().lower()
    allowed_modes = {"episodic", "onset_only", "once_per_episode", "every_n_bars", "continuous"}
    if mode not in allowed_modes:
        raise ValueError(f"sampling_policy.mode must be one of {allowed_modes}")

    entry_lag_bars = raw.get("entry_lag_bars", 1)
    try:
        entry_lag_bars = int(entry_lag_bars)
    except (TypeError, ValueError):
        raise ValueError("sampling_policy.entry_lag_bars must be an integer")
    if entry_lag_bars < 1:
        raise ValueError("sampling_policy.entry_lag_bars must be >= 1")

    overlap_policy = str(raw.get("overlap_policy", "suppress")).strip().lower()
    allowed_overlaps = {"suppress", "allow", "cap"}
    if overlap_policy not in allowed_overlaps:
        raise ValueError(f"sampling_policy.overlap_policy must be one of {allowed_overlaps}")

    every_n_bars = raw.get("every_n_bars")
    if mode == "every_n_bars":
        if every_n_bars is None:
            raise ValueError("sampling_policy.every_n_bars is required when mode is 'every_n_bars'")
        try:
            every_n_bars = int(every_n_bars)
        except (TypeError, ValueError):
            raise ValueError("sampling_policy.every_n_bars must be an integer")
        if every_n_bars < 1:
            raise ValueError("sampling_policy.every_n_bars must be >= 1")

    return SamplingPolicySpec(
        mode=mode,
        entry_lag_bars=entry_lag_bars,
        overlap_policy=overlap_policy,
        every_n_bars=every_n_bars,
    )


def _normalize_template(raw: Dict[str, Any]) -> TemplateSpec:
    template_id = raw.get("id")
    if not template_id:
        raise ValueError("template.id is required")

    params = raw.get("params")
    if params is None:
        params = {}
    if not isinstance(params, dict):
        raise ValueError("template.params must be an object")

    return TemplateSpec(
        id=str(template_id),
        params=dict(params),
    )


def _as_str_list(values: Any, field_name: str) -> List[str]:
    if values is None:
        return []
    if isinstance(values, str):
        cleaned = values.strip()
        return [cleaned] if cleaned else []
    if not isinstance(values, (list, tuple, set)):
        raise ValueError(f"{field_name} must be a string or list of strings")
    out = [str(value).strip() for value in values if str(value).strip()]
    return out
