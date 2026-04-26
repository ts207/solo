from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class AccessRequest:
    role: str
    provenance: str
    is_exec_state: bool
    event_id: str


def _str_list(value) -> list[str]:
    if isinstance(value, list):
        return [str(x).strip().lower() for x in value if str(x).strip()]
    token = str(value).strip().lower()
    return [token] if token else []


def evaluate_access(
    request: AccessRequest,
    *,
    firewall_spec: Mapping[str, object],
) -> tuple[bool, str]:
    roles = firewall_spec.get("roles")
    if not isinstance(roles, Mapping):
        return False, f"invalid firewall roles spec for event_id={request.event_id}"
    role_cfg = roles.get(request.role)
    if not isinstance(role_cfg, Mapping):
        return False, f"unknown role '{request.role}' for event_id={request.event_id}"
    allowed_provenance = set(_str_list(role_cfg.get("allowed_provenance")))
    if str(request.provenance).strip().lower() not in allowed_provenance:
        return (
            False,
            f"role={request.role} provenance={request.provenance} not allowed for event_id={request.event_id}",
        )
    allow_exec_state = bool(role_cfg.get("allow_exec_state", False))
    if request.is_exec_state and not allow_exec_state:
        return (
            False,
            f"role={request.role} cannot access execution state for event_id={request.event_id}",
        )
    constraints = firewall_spec.get("constraints")
    if isinstance(constraints, Mapping):
        forbid_posttrade_for_alpha = bool(constraints.get("forbid_posttrade_for_alpha", False))
        if (
            forbid_posttrade_for_alpha
            and request.role == "alpha"
            and request.provenance == "execution"
        ):
            return (
                False,
                f"alpha role cannot read post-trade execution provenance for event_id={request.event_id}",
            )
    return True, ""


def audit_access_requests(
    requests: Iterable[AccessRequest],
    *,
    firewall_spec: Mapping[str, object],
    max_examples: int = 20,
) -> dict[str, object]:
    counters: dict[str, int] = {
        "unknown_role": 0,
        "provenance_forbidden": 0,
        "exec_state_forbidden": 0,
        "invalid_firewall_spec": 0,
    }
    examples: list[str] = []
    total = 0
    for req in requests:
        total += 1
        ok, msg = evaluate_access(req, firewall_spec=firewall_spec)
        if ok:
            continue
        lowered = msg.lower()
        if "unknown role" in lowered:
            counters["unknown_role"] += 1
        elif "provenance" in lowered:
            counters["provenance_forbidden"] += 1
        elif "execution state" in lowered or "post-trade" in lowered:
            counters["exec_state_forbidden"] += 1
        elif "invalid firewall" in lowered:
            counters["invalid_firewall_spec"] += 1
        else:
            counters["provenance_forbidden"] += 1
        if len(examples) < int(max_examples):
            examples.append(msg)
    violation_count = int(sum(int(v) for v in counters.values()))
    return {
        "status": "pass" if violation_count == 0 else "failed",
        "event_count": int(total),
        "violation_count": int(violation_count),
        "violations_by_type": counters,
        "violation_examples": examples,
    }
