from __future__ import annotations

import json
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from project.research.regime_event_inventory import (
    ContextRegistry,
    load_authoritative_event_registry,
    load_context_registry,
    load_template_ids,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_REGISTRY_PATH = REPO_ROOT / "spec" / "mechanisms" / "registry.yaml"

REQUIRED_MECHANISM_FIELDS = (
    "mechanism_id",
    "version",
    "claim",
    "forced_actor",
    "observable_pressure.required",
    "candidate_events",
    "allowed_templates",
    "allowed_directions",
    "allowed_horizons_bars",
    "required_falsification",
    "kill_conditions",
    "forbidden_rescue_actions",
)


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a YAML object")
    return payload


def _as_str_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        token = value.strip()
        return [token] if token else []
    if not isinstance(value, (list, tuple, set)):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _as_int_list(value: Any) -> list[int]:
    out: list[int] = []
    values = value if isinstance(value, (list, tuple, set)) else [value]
    for item in values:
        if item is None or isinstance(item, bool):
            continue
        try:
            out.append(int(item))
        except (TypeError, ValueError):
            continue
    return out


def _normalize_token(value: Any) -> str:
    return str(value or "").strip().lower()


def _parse_context_token(value: Any) -> tuple[str, str]:
    token = str(value or "").strip()
    if "=" not in token:
        return "", ""
    key, raw_value = token.split("=", 1)
    return _normalize_token(key), _normalize_token(raw_value)


def _normalize_context_map_with_canonicalizations(
    contexts: dict[str, Any] | None,
) -> tuple[dict[str, list[str]], list[str]]:
    normalized: dict[str, list[str]] = {}
    canonicalizations: list[str] = []
    for raw_key, raw_values in (contexts or {}).items():
        key = _normalize_token(raw_key)
        values = _as_str_list(raw_values)
        if key and values:
            normalized_values: list[str] = []
            for value in values:
                canonical_value = _normalize_token(value)
                normalized_values.append(canonical_value)
                raw_label = f"{str(raw_key).strip()}={str(value).strip()}"
                canonical_label = f"{key}={canonical_value}"
                if raw_label != canonical_label:
                    canonicalizations.append(f"{raw_label} canonicalized to {canonical_label}")
            normalized[key] = normalized_values
    return normalized, canonicalizations


def _normalize_context_map(contexts: dict[str, Any] | None) -> dict[str, list[str]]:
    return _normalize_context_map_with_canonicalizations(contexts)[0]


@lru_cache(maxsize=1)
def _authoritative_events() -> dict[str, dict[str, Any]]:
    return load_authoritative_event_registry()


@lru_cache(maxsize=1)
def _template_ids() -> set[str]:
    return load_template_ids()


@lru_cache(maxsize=1)
def _context_registry() -> ContextRegistry:
    return load_context_registry()


@dataclass(frozen=True)
class MechanismIssue:
    id: str
    status: str
    detail: str

    def to_dict(self) -> dict[str, str]:
        return {"id": self.id, "status": self.status, "detail": self.detail}


@dataclass(frozen=True)
class MechanismRegistryEntry:
    mechanism_id: str
    path: Path
    priority: str
    status: str
    deploy_relevance: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": str(self.path),
            "priority": self.priority,
            "status": self.status,
            "deploy_relevance": self.deploy_relevance,
        }


@dataclass(frozen=True)
class MechanismRegistry:
    version: str
    mechanisms: dict[str, MechanismRegistryEntry]
    path: Path | None = None

    def resolve(self, mechanism_id: str) -> MechanismRegistryEntry:
        key = str(mechanism_id or "").strip()
        if key not in self.mechanisms:
            raise KeyError(f"Unknown mechanism_id: {mechanism_id}")
        return self.mechanisms[key]


@dataclass(frozen=True)
class MechanismSpec:
    mechanism_id: str
    version: str
    status: str
    priority: str
    claim: str
    forced_actor: list[str]
    observable_pressure: dict[str, Any]
    candidate_events: list[str]
    allowed_templates: list[str]
    allowed_directions: list[str]
    allowed_horizons_bars: list[int]
    allowed_contexts: dict[str, list[str]]
    required_falsification: list[str]
    kill_conditions: list[str]
    allowed_rescue_actions: list[str]
    forbidden_rescue_actions: list[str]
    source_path: Path | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: dict[str, Any], *, source_path: Path | None = None) -> MechanismSpec:
        allowed_contexts_raw = payload.get("allowed_contexts") or {}
        if not isinstance(allowed_contexts_raw, dict):
            allowed_contexts_raw = {}
        allowed_contexts = {
            group: _as_str_list(allowed_contexts_raw.get(group))
            for group in ("preferred", "allowed", "forbidden")
        }
        return cls(
            mechanism_id=str(payload.get("mechanism_id", "") or "").strip(),
            version=str(payload.get("version", "") or "").strip(),
            status=str(payload.get("status", "draft") or "draft").strip(),
            priority=str(payload.get("priority", "medium") or "medium").strip(),
            claim=str(payload.get("claim", "") or "").strip(),
            forced_actor=_as_str_list(payload.get("forced_actor")),
            observable_pressure=dict(payload.get("observable_pressure") or {}),
            candidate_events=[item.upper() for item in _as_str_list(payload.get("candidate_events"))],
            allowed_templates=_as_str_list(payload.get("allowed_templates")),
            allowed_directions=[item.lower() for item in _as_str_list(payload.get("allowed_directions"))],
            allowed_horizons_bars=_as_int_list(payload.get("allowed_horizons_bars")),
            allowed_contexts=allowed_contexts,
            required_falsification=_as_str_list(payload.get("required_falsification")),
            kill_conditions=_as_str_list(payload.get("kill_conditions")),
            allowed_rescue_actions=_as_str_list(payload.get("allowed_rescue_actions")),
            forbidden_rescue_actions=_as_str_list(payload.get("forbidden_rescue_actions")),
            source_path=source_path,
            raw=dict(payload),
        )

    def context_sets(self) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
        allowed = {
            _parse_context_token(item)
            for item in self.allowed_contexts.get("preferred", []) + self.allowed_contexts.get("allowed", [])
        }
        forbidden = {_parse_context_token(item) for item in self.allowed_contexts.get("forbidden", [])}
        return {item for item in allowed if item != ("", "")}, {
            item for item in forbidden if item != ("", "")
        }


@dataclass(frozen=True)
class CandidateHypothesis:
    event_id: str
    template_id: str
    direction: str
    horizon_bars: int
    contexts: dict[str, list[str]] = field(default_factory=dict)
    required_falsification: list[str] = field(default_factory=list)
    forbidden_rescue_actions: list[str] = field(default_factory=list)
    context_justification: str = ""
    context_canonicalizations: list[str] = field(default_factory=list)

    @classmethod
    def from_proposal_payload(cls, payload: dict[str, Any]) -> CandidateHypothesis:
        hypothesis = payload.get("hypothesis") or {}
        if not isinstance(hypothesis, dict):
            hypothesis = {}
        anchor = hypothesis.get("anchor") or {}
        template = hypothesis.get("template") or {}
        filters = hypothesis.get("filters") or {}
        if not isinstance(filters, dict):
            filters = {}
        contexts = filters.get("contexts")
        if contexts is None:
            contexts = payload.get("contexts")
        artifacts = payload.get("artifacts") or {}
        normalized_contexts, context_canonicalizations = _normalize_context_map_with_canonicalizations(
            contexts if isinstance(contexts, dict) else {}
        )
        return cls(
            event_id=str(anchor.get("event_id", "") or "").strip().upper(),
            template_id=str(template.get("id", "") or "").strip(),
            direction=str(hypothesis.get("direction", "") or "").strip().lower(),
            horizon_bars=int(hypothesis.get("horizon_bars", 0) or 0),
            contexts=normalized_contexts,
            required_falsification=_as_str_list(payload.get("required_falsification")),
            forbidden_rescue_actions=_as_str_list(payload.get("forbidden_rescue_actions")),
            context_justification=str(
                payload.get("mechanism_context_justification")
                or artifacts.get("mechanism_context_justification")
                or ""
            ).strip(),
            context_canonicalizations=context_canonicalizations,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "template_id": self.template_id,
            "direction": self.direction,
            "horizon_bars": self.horizon_bars,
            "contexts": dict(self.contexts),
            "required_falsification": list(self.required_falsification),
            "forbidden_rescue_actions": list(self.forbidden_rescue_actions),
            "context_justification": self.context_justification,
            "context_canonicalizations": list(self.context_canonicalizations),
        }


@dataclass(frozen=True)
class MechanismPreflightReport:
    schema_version: str
    proposal: str
    status: str
    classification: str
    mechanism_id: str
    checks: list[MechanismIssue]
    required_falsification: list[str]
    forbidden_rescue_actions: list[str]
    next_safe_command: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "proposal": self.proposal,
            "status": self.status,
            "classification": self.classification,
            "mechanism_id": self.mechanism_id,
            "checks": [check.to_dict() for check in self.checks],
            "required_falsification": list(self.required_falsification),
            "forbidden_rescue_actions": list(self.forbidden_rescue_actions),
            "next_safe_command": self.next_safe_command,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=True)


def load_mechanism_registry(path: Path = DEFAULT_REGISTRY_PATH) -> MechanismRegistry:
    payload = _load_yaml(path)
    mechanisms_raw = payload.get("mechanisms") or {}
    if not isinstance(mechanisms_raw, dict):
        raise ValueError("mechanisms must be an object")
    entries: dict[str, MechanismRegistryEntry] = {}
    for mechanism_id, raw_entry in mechanisms_raw.items():
        if not isinstance(raw_entry, dict):
            raise ValueError(f"registry entry {mechanism_id} must be an object")
        raw_path = Path(str(raw_entry.get("path", "") or ""))
        resolved_path = raw_path if raw_path.is_absolute() else REPO_ROOT / raw_path
        entries[str(mechanism_id)] = MechanismRegistryEntry(
            mechanism_id=str(mechanism_id),
            path=resolved_path,
            priority=str(raw_entry.get("priority", "") or ""),
            status=str(raw_entry.get("status", "") or ""),
            deploy_relevance=bool(raw_entry.get("deploy_relevance", False)),
        )
    return MechanismRegistry(
        version=str(payload.get("version", "") or ""),
        mechanisms=entries,
        path=path,
    )


def load_mechanism(path_or_id: str | Path) -> MechanismSpec:
    path = Path(path_or_id)
    if path.exists():
        payload = _load_yaml(path)
        return MechanismSpec.from_dict(payload, source_path=path)

    registry = load_mechanism_registry()
    entry = registry.resolve(str(path_or_id))
    if not entry.path.exists():
        raise FileNotFoundError(f"Mechanism spec not found: {entry.path}")
    payload = _load_yaml(entry.path)
    return MechanismSpec.from_dict(payload, source_path=entry.path)


def validate_mechanism_spec(spec: MechanismSpec) -> list[MechanismIssue]:
    issues: list[MechanismIssue] = []

    field_values: dict[str, Any] = {
        "mechanism_id": spec.mechanism_id,
        "version": spec.version,
        "claim": spec.claim,
        "forced_actor": spec.forced_actor,
        "observable_pressure.required": (spec.observable_pressure.get("required") or []),
        "candidate_events": spec.candidate_events,
        "allowed_templates": spec.allowed_templates,
        "allowed_directions": spec.allowed_directions,
        "allowed_horizons_bars": spec.allowed_horizons_bars,
        "required_falsification": spec.required_falsification,
        "kill_conditions": spec.kill_conditions,
        "forbidden_rescue_actions": spec.forbidden_rescue_actions,
    }
    for field_name in REQUIRED_MECHANISM_FIELDS:
        value = field_values[field_name]
        if not value:
            issues.append(
                MechanismIssue(
                    id=f"missing_{field_name.replace('.', '_')}",
                    status="fail",
                    detail=f"{field_name} is required",
                )
            )

    if "forward_confirmation" not in spec.required_falsification:
        issues.append(
            MechanismIssue(
                id="missing_forward_confirmation",
                status="fail",
                detail="required_falsification must include forward_confirmation",
            )
        )
    if "promote_without_forward_confirmation" not in spec.forbidden_rescue_actions:
        issues.append(
            MechanismIssue(
                id="missing_forward_confirmation_rescue_ban",
                status="fail",
                detail="forbidden_rescue_actions must ban promotion without forward confirmation",
            )
        )
    return issues


def _record_check(checks: list[MechanismIssue], check_id: str, passed: bool, detail: str) -> None:
    checks.append(MechanismIssue(id=check_id, status="pass" if passed else "fail", detail=detail))


def _check_authoritative_event(
    checks: list[MechanismIssue],
    candidate: CandidateHypothesis,
) -> None:
    event_row = _authoritative_events().get(candidate.event_id)
    if event_row is None:
        checks.append(
            MechanismIssue(
                id="event_in_authoritative_registry",
                status="fail",
                detail=f"{candidate.event_id} is not in the authoritative registry",
            )
        )
        checks.append(
            MechanismIssue(
                id="event_executable_or_research_only_declared",
                status="fail",
                detail=f"{candidate.event_id} cannot be executable because it is unregistered",
            )
        )
        return

    checks.append(
        MechanismIssue(
            id="event_in_authoritative_registry",
            status="pass",
            detail=f"{candidate.event_id} is in the authoritative registry",
        )
    )
    executable = bool(event_row.get("default_executable"))
    research_only = bool(event_row.get("research_only"))
    checks.append(
        MechanismIssue(
            id="event_executable_or_research_only_declared",
            status="pass" if executable or research_only else "fail",
            detail=f"{candidate.event_id} is executable or explicitly research-only"
            if executable or research_only
            else f"{candidate.event_id} is registered but not executable or research-only",
        )
    )


def _check_template_registry(
    checks: list[MechanismIssue],
    candidate: CandidateHypothesis,
) -> None:
    _record_check(
        checks,
        "template_in_template_registry",
        candidate.template_id in _template_ids(),
        f"{candidate.template_id} is in the template registry"
        if candidate.template_id in _template_ids()
        else f"{candidate.template_id} is not in the template registry",
    )


def _check_context_registry(
    checks: list[MechanismIssue],
    candidate: CandidateHypothesis,
) -> None:
    registry = _context_registry()
    checks.extend(
        MechanismIssue(id="context_canonicalized", status="pass", detail=detail)
        for detail in candidate.context_canonicalizations
    )

    unknown_dimensions: list[str] = []
    invalid_values: list[str] = []
    unmaterializable_dimensions: list[str] = []
    for dimension, values in candidate.contexts.items():
        if not registry.has_dimension(dimension):
            unknown_dimensions.append(dimension)
            continue
        if not registry.is_materializable(dimension):
            unmaterializable_dimensions.append(dimension)
        invalid_values.extend(
            f"{dimension}={value}"
            for value in values
            if not registry.is_value_allowed(dimension, value)
        )

    _record_check(
        checks,
        "context_dimension_known",
        not unknown_dimensions,
        "All context dimensions are known"
        if not unknown_dimensions
        else f"Unknown context dimensions: {', '.join(sorted(set(unknown_dimensions)))}",
    )
    _record_check(
        checks,
        "context_value_allowed",
        not invalid_values,
        "All context values are registry-allowed"
        if not invalid_values
        else f"Invalid context values: {', '.join(sorted(set(invalid_values)))}",
    )
    _record_check(
        checks,
        "context_materializable",
        not unmaterializable_dimensions,
        "All context dimensions are materializable"
        if not unmaterializable_dimensions
        else (
            "Context dimensions are known but not materializable: "
            f"{', '.join(sorted(set(unmaterializable_dimensions)))}"
        ),
    )


def validate_candidate_against_mechanism(
    candidate: CandidateHypothesis,
    mechanism: MechanismSpec,
    *,
    proposal_path: str = "",
) -> MechanismPreflightReport:
    checks: list[MechanismIssue] = []

    spec_issues = validate_mechanism_spec(mechanism)
    checks.extend(spec_issues)
    spec_ok = not any(issue.status == "fail" for issue in spec_issues)

    _check_authoritative_event(checks, candidate)
    _check_template_registry(checks, candidate)
    _check_context_registry(checks, candidate)

    _record_check(
        checks,
        "event_allowed",
        candidate.event_id in mechanism.candidate_events,
        f"{candidate.event_id} allowed by {mechanism.mechanism_id}",
    )
    _record_check(
        checks,
        "template_allowed",
        candidate.template_id in mechanism.allowed_templates,
        f"{candidate.template_id} allowed by {mechanism.mechanism_id}",
    )
    _record_check(
        checks,
        "direction_allowed",
        candidate.direction in mechanism.allowed_directions,
        f"{candidate.direction} allowed by {mechanism.mechanism_id}",
    )
    _record_check(
        checks,
        "horizon_allowed",
        candidate.horizon_bars in mechanism.allowed_horizons_bars,
        f"{candidate.horizon_bars} bars allowed by {mechanism.mechanism_id}",
    )

    allowed_contexts, forbidden_contexts = mechanism.context_sets()
    context_checks = 0
    forbidden_hits: list[str] = []
    unsupported_hits: list[str] = []
    for dimension, values in candidate.contexts.items():
        for value in values:
            context_checks += 1
            pair = (dimension, value)
            label = f"{dimension}={value}"
            if pair in forbidden_contexts:
                forbidden_hits.append(label)
            elif pair not in allowed_contexts:
                unsupported_hits.append(label)

    if forbidden_hits:
        checks.append(
            MechanismIssue(
                id="context_forbidden",
                status="fail",
                detail=f"Forbidden mechanism contexts: {', '.join(sorted(forbidden_hits))}",
            )
        )
    elif unsupported_hits and not candidate.context_justification:
        checks.append(
            MechanismIssue(
                id="context_allowed_or_justified",
                status="fail",
                detail=f"Unsupported contexts need explicit justification: {', '.join(sorted(unsupported_hits))}",
            )
        )
    elif context_checks == 0 and not candidate.context_justification:
        checks.append(
            MechanismIssue(
                id="context_allowed_or_justified",
                status="fail",
                detail="At least one mechanism context or explicit context justification is required",
            )
        )
    else:
        checks.append(
            MechanismIssue(
                id="context_allowed_or_justified",
                status="pass",
                detail="Candidate contexts are allowed or explicitly justified",
            )
        )

    missing_falsification = sorted(
        set(mechanism.required_falsification) - set(candidate.required_falsification)
    )
    _record_check(
        checks,
        "required_falsification_declared",
        not missing_falsification,
        "All mechanism falsification gates declared"
        if not missing_falsification
        else f"Missing falsification gates: {', '.join(missing_falsification)}",
    )

    missing_rescue_bans = sorted(
        set(mechanism.forbidden_rescue_actions) - set(candidate.forbidden_rescue_actions)
    )
    _record_check(
        checks,
        "forbidden_rescue_actions_included",
        not missing_rescue_bans,
        "All mechanism rescue bans included"
        if not missing_rescue_bans
        else f"Missing forbidden rescue actions: {', '.join(missing_rescue_bans)}",
    )

    has_failure = any(issue.status == "fail" for issue in checks)
    classification = "mechanism_backed" if spec_ok and not has_failure else "mechanism_violation"
    return MechanismPreflightReport(
        schema_version="mechanism_preflight_v1",
        proposal=proposal_path,
        status="fail" if has_failure else "pass",
        classification=classification,
        mechanism_id=mechanism.mechanism_id,
        checks=checks,
        required_falsification=mechanism.required_falsification,
        forbidden_rescue_actions=mechanism.forbidden_rescue_actions,
        next_safe_command=f"make discover-proposal PROPOSAL={proposal_path} RUN_ID=<run_id> DATA_ROOT=<lake>",
    )
