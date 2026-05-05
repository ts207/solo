from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel

from project.live.contracts.promoted_thesis import (
    LIVE_APPROVAL_REQUIRED_STATES,
    LIVE_TRADEABLE_STATES,
    RuntimeThesisManifest,
    deployment_state_allows_runtime,
)

_REQUIRED_MANIFEST_HASHES = (
    "event_contract_hash",
    "template_contract_hash",
    "domain_graph_hash",
    "evidence_bundle_hash",
    "risk_contract_hash",
)


def _parse_utc(value: str) -> datetime | None:
    token = str(value or "").strip()
    if not token:
        return None
    try:
        dt = datetime.fromisoformat(token.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def validate_runtime_manifest(thesis: Any, runtime_mode: str, *, require_manifest: bool = False) -> None:
    """Validate immutable runtime-admission manifest when it is present.

    Legacy thesis artifacts may omit manifest hashes for monitor/simulation mode,
    but shadow/trading admission requires the manifest to be complete enough to
    bind the thesis to the contracts and evidence used for promotion.
    """

    manifest = getattr(thesis, "runtime_manifest", None)
    structured = isinstance(thesis, BaseModel)
    if manifest is None or (not isinstance(manifest, (RuntimeThesisManifest, dict)) and not structured):
        if require_manifest or (runtime_mode in {"shadow", "trading"} and structured):
            raise ValueError("runtime_manifest required for implemented runtime")
        return
    if isinstance(manifest, dict):
        manifest = RuntimeThesisManifest.model_validate(manifest)
    if not isinstance(manifest, RuntimeThesisManifest):
        if require_manifest:
            raise ValueError("runtime_manifest required for implemented runtime")
        return

    if require_manifest and not str(manifest.thesis_id or "").strip():
        raise ValueError("runtime_manifest required for implemented runtime")

    thesis_id = str(getattr(thesis, "thesis_id", "")).strip()
    if manifest.thesis_id and thesis_id and manifest.thesis_id != thesis_id:
        raise ValueError(
            f"runtime_manifest thesis_id mismatch: {manifest.thesis_id!r} != {thesis_id!r}"
        )

    state = str(getattr(thesis, "deployment_state", "")).strip()
    if manifest.promotion_state and state and manifest.promotion_state != state:
        raise ValueError(
            f"runtime_manifest promotion_state mismatch: {manifest.promotion_state!r} != {state!r}"
        )

    if manifest.allowed_runtime_modes and runtime_mode not in set(manifest.allowed_runtime_modes):
        raise ValueError(
            f"runtime_manifest does not allow {runtime_mode!r}; allowed={manifest.allowed_runtime_modes!r}"
        )

    expiry = _parse_utc(manifest.expires_at_utc)
    if expiry is not None and expiry <= datetime.now(UTC):
        raise ValueError("runtime_manifest expired")

    if runtime_mode in {"shadow", "trading"}:
        missing = [name for name in _REQUIRED_MANIFEST_HASHES if not str(getattr(manifest, name, "")).strip()]
        if missing:
            raise ValueError(f"runtime_manifest missing required hashes: {missing}")


def validate_runtime_mode_against_theses(runtime_mode: str, theses: list, *, require_manifest: bool = False) -> None:
    """Validate runtime mode, thesis maturity state, manifest, and live gates."""
    runtime_mode = str(runtime_mode).lower().strip()

    if runtime_mode not in {"monitor_only", "simulation", "shadow", "trading"}:
        raise ValueError(f"Unsupported runtime_mode: {runtime_mode}")

    for thesis in theses:
        state = str(getattr(thesis, "deployment_state", "unknown"))
        if not deployment_state_allows_runtime(state, runtime_mode):
            raise ValueError(
                f"Thesis in state {state!r} cannot run in {runtime_mode} mode."
            )

        validate_runtime_manifest(thesis, runtime_mode, require_manifest=require_manifest)

        structured_thesis = isinstance(thesis, BaseModel)
        if structured_thesis and (runtime_mode == "trading" or state in LIVE_APPROVAL_REQUIRED_STATES):
            live_approval = getattr(thesis, "live_approval", None)
            if not bool(getattr(live_approval, "is_approved", False)):
                raise ValueError(
                    f"Thesis in state {state!r} requires approved live_approval for {runtime_mode} mode."
                )
            cap_profile = getattr(thesis, "cap_profile", None)
            if not bool(getattr(cap_profile, "is_configured", False)):
                raise ValueError(
                    f"Thesis in state {state!r} requires configured cap_profile for {runtime_mode} mode."
                )

        if runtime_mode == "trading" and state not in LIVE_TRADEABLE_STATES:
            raise ValueError(f"Thesis in state {state!r} is not live-tradeable.")
