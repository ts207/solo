from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from project.artifacts import promoted_theses_path
from project.core.config import get_data_root
from project.live.cap_profiles import validate_thesis_caps_against_profile
from project.live.contracts.promoted_thesis import PromotedThesis, deployment_state_allows_runtime
from project.live.deploy_admission import assert_deploy_admission
from project.live.live_approval import load_live_approval
from project.live.paper_gate import evaluate_paper_gate
from project.live.runtime_admission import validate_runtime_manifest

_VALID_SCHEMA_VERSION = "promoted_theses_v1"


@dataclass(frozen=True)
class _Check:
    name: str
    status: str
    message: str = ""
    details: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"name": self.name, "status": self.status}
        if self.message:
            payload["message"] = self.message
        if self.details:
            payload["details"] = self.details
        return payload


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise
    except Exception as exc:  # noqa: BLE001 - diagnostic surface should not crash opaquely
        raise ValueError(f"cannot parse JSON at {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"JSON payload at {path} must be an object")
    return payload


def _thesis_dicts(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    theses = payload.get("theses", [])
    if not isinstance(theses, list):
        return []
    return [dict(item) for item in theses if isinstance(item, Mapping)]


def _parse_theses(payload: Mapping[str, Any]) -> tuple[list[PromotedThesis], list[str]]:
    out: list[PromotedThesis] = []
    errors: list[str] = []
    for index, item in enumerate(_thesis_dicts(payload)):
        try:
            out.append(PromotedThesis.model_validate(item))
        except Exception as exc:  # noqa: BLE001 - collect all thesis parse failures
            thesis_id = str(item.get("thesis_id", f"index_{index}"))
            errors.append(f"{thesis_id}: {exc}")
    return out, errors


def _runtime_manifest_checks(theses: list[PromotedThesis], runtime_mode: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for thesis in theses:
        try:
            validate_runtime_manifest(thesis, runtime_mode, require_manifest=True)
            rows.append({"thesis_id": thesis.thesis_id, "status": "pass"})
        except Exception as exc:  # noqa: BLE001 - diagnostic report
            rows.append({"thesis_id": thesis.thesis_id, "status": "fail", "message": str(exc)})
    return rows


def _deployment_state_checks(theses: list[PromotedThesis], runtime_mode: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for thesis in theses:
        allowed = deployment_state_allows_runtime(str(thesis.deployment_state), runtime_mode)
        rows.append(
            {
                "thesis_id": thesis.thesis_id,
                "deployment_state": str(thesis.deployment_state),
                "runtime_mode": runtime_mode,
                "status": "pass" if allowed else "fail",
                "message": "" if allowed else f"state {thesis.deployment_state!r} cannot run in {runtime_mode!r}",
            }
        )
    return rows


def _forward_confirmation_check(run_id: str, data_root: Path) -> _Check:
    path = data_root / "reports" / "validation" / run_id / "forward_confirmation.json"
    if not path.exists():
        return _Check("forward_confirmation", "fail", f"missing at {path}")
    payload = _load_json_object(path)
    metrics = payload.get("metrics", {}) if isinstance(payload.get("metrics"), Mapping) else {}
    details = {"path": str(path), "method": payload.get("method", ""), "metrics": dict(metrics)}
    if payload.get("method") != "oos_frozen_thesis_replay_v1":
        return _Check("forward_confirmation", "fail", "unexpected method", details)
    if str(metrics.get("status", "")).lower() == "fail":
        return _Check("forward_confirmation", "fail", str(metrics.get("reason", "failed")), details)
    if int(metrics.get("event_count", 0) or 0) <= 0:
        return _Check("forward_confirmation", "fail", "no OOS events", details)
    if float(metrics.get("mean_return_net_bps", 0.0) or 0.0) <= 0:
        return _Check("forward_confirmation", "fail", "net bps is nonpositive", details)
    if float(metrics.get("t_stat_net", 0.0) or 0.0) <= 0:
        return _Check("forward_confirmation", "fail", "net t-stat is nonpositive", details)
    return _Check("forward_confirmation", "pass", details=details)


def _trading_checks(theses: list[PromotedThesis], data_root: Path) -> list[_Check]:
    checks: list[_Check] = []
    run_ids = sorted({str(thesis.lineage.run_id) for thesis in theses if thesis.lineage and thesis.lineage.run_id})
    for run_id in run_ids:
        checks.append(_forward_confirmation_check(run_id, data_root))
    for thesis in theses:
        paper_path = data_root / "reports" / "paper" / thesis.thesis_id / "paper_quality_summary.json"
        paper = evaluate_paper_gate(paper_path)
        checks.append(
            _Check(
                "paper_gate",
                "pass" if paper.status == "pass" else "fail",
                "" if paper.status == "pass" else ", ".join(paper.reason_codes),
                {"thesis_id": thesis.thesis_id, "path": str(paper_path), "status": paper.status},
            )
        )
        approval_path = data_root / "reports" / "approval" / thesis.thesis_id / "live_approval.json"
        try:
            approval = load_live_approval(approval_path)
            approval.validate_for_live()
            if approval.thesis_id != thesis.thesis_id:
                raise ValueError(f"approval thesis_id mismatch: {approval.thesis_id} != {thesis.thesis_id}")
            cap_reasons = validate_thesis_caps_against_profile(thesis.cap_profile, approval.cap_profile_id)
            if cap_reasons:
                raise ValueError(", ".join(cap_reasons))
            checks.append(_Check("live_approval", "pass", details={"thesis_id": thesis.thesis_id, "path": str(approval_path)}))
        except Exception as exc:  # noqa: BLE001 - diagnostic report
            checks.append(_Check("live_approval", "fail", str(exc), {"thesis_id": thesis.thesis_id, "path": str(approval_path)}))
    return checks


def build_deploy_readiness_report(
    *,
    run_id: str,
    runtime_mode: str,
    data_root: Path | None = None,
    thesis_path: Path | None = None,
    monitor_report_path: Path | None = None,
) -> dict[str, Any]:
    runtime_mode = str(runtime_mode or "monitor_only").strip().lower()
    resolved_data_root = data_root or get_data_root()
    resolved_thesis_path = thesis_path or promoted_theses_path(run_id, resolved_data_root)
    checks: list[_Check] = []

    if not resolved_thesis_path.exists():
        checks.append(_Check("thesis_artifact", "fail", f"missing at {resolved_thesis_path}"))
        return {
            "kind": "deploy_readiness",
            "status": "fail",
            "run_id": str(run_id),
            "runtime_mode": runtime_mode,
            "data_root": str(resolved_data_root),
            "thesis_path": str(resolved_thesis_path),
            "checks": [check.to_dict() for check in checks],
        }

    checks.append(_Check("thesis_artifact", "pass", details={"path": str(resolved_thesis_path)}))
    try:
        payload = _load_json_object(resolved_thesis_path)
    except Exception as exc:  # noqa: BLE001
        checks.append(_Check("thesis_payload", "fail", str(exc)))
        return {
            "kind": "deploy_readiness",
            "status": "fail",
            "run_id": str(run_id),
            "runtime_mode": runtime_mode,
            "data_root": str(resolved_data_root),
            "thesis_path": str(resolved_thesis_path),
            "checks": [check.to_dict() for check in checks],
        }

    schema_version = str(payload.get("schema_version", "") or "")
    checks.append(
        _Check(
            "thesis_payload_schema",
            "pass" if schema_version == _VALID_SCHEMA_VERSION else "fail",
            "" if schema_version == _VALID_SCHEMA_VERSION else f"expected {_VALID_SCHEMA_VERSION!r}, got {schema_version!r}",
            {"schema_version": schema_version},
        )
    )
    theses, parse_errors = _parse_theses(payload)
    checks.append(
        _Check(
            "thesis_parse",
            "pass" if theses and not parse_errors else "fail",
            "; ".join(parse_errors),
            {"parsed_theses": len(theses), "parse_errors": parse_errors[:10]},
        )
    )
    state_rows = _deployment_state_checks(theses, runtime_mode)
    checks.append(
        _Check(
            "deployment_state_runtime",
            "pass" if state_rows and all(row["status"] == "pass" for row in state_rows) else "fail",
            details={"theses": state_rows},
        )
    )
    manifest_rows = _runtime_manifest_checks(theses, runtime_mode)
    checks.append(
        _Check(
            "runtime_manifest",
            "pass" if manifest_rows and all(row["status"] == "pass" for row in manifest_rows) else "fail",
            details={"theses": manifest_rows},
        )
    )
    if runtime_mode == "trading":
        checks.extend(_trading_checks(theses, resolved_data_root))
    try:
        assert_deploy_admission(
            thesis_path=resolved_thesis_path,
            runtime_mode=runtime_mode,
            monitor_report_path=monitor_report_path,
            data_root=resolved_data_root,
        )
        checks.append(_Check("deploy_admission", "pass"))
    except Exception as exc:  # noqa: BLE001 - explain final gate
        checks.append(_Check("deploy_admission", "fail", str(exc)))

    status = "pass" if checks and all(check.status == "pass" for check in checks) else "fail"
    return {
        "kind": "deploy_readiness",
        "status": status,
        "run_id": str(run_id),
        "runtime_mode": runtime_mode,
        "data_root": str(resolved_data_root),
        "thesis_path": str(resolved_thesis_path),
        "monitor_report_path": str(monitor_report_path or ""),
        "checks": [check.to_dict() for check in checks],
    }
