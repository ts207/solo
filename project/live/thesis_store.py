from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from pathlib import Path

from project.artifacts import live_thesis_index_path, promoted_theses_path
from project.core.exceptions import (
    AmbiguousLatestResolutionError,
    CompatibilityRequiredError,
    DataIntegrityError,
    MalformedArtifactError,
    MissingArtifactError,
    SchemaMismatchError,
)
from project.live.contracts import PromotedThesis
from project.live.deployment import DeploymentGate
from project.research.contracts.historical_trust import (
    HISTORICAL_TRUST_LEGACY,
    HISTORICAL_TRUST_REQUIRES_REVALIDATION,
)
from project.research.historical_trust import inspect_artifact_trust

_LOG = logging.getLogger(__name__)


def _load_payload(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MalformedArtifactError(f"Failed to read thesis artifact {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise SchemaMismatchError(f"Thesis artifact {path} did not contain a JSON object payload")
    return payload


def _validate_store_payload(payload: dict, *, path: Path) -> None:
    required = {
        "schema_version": str,
        "run_id": str,
        "generated_at_utc": str,
        "thesis_count": int,
        "active_thesis_count": int,
        "pending_thesis_count": int,
        "theses": list,
    }
    for field_name, field_type in required.items():
        if field_name not in payload:
            raise SchemaMismatchError(f"Thesis artifact {path} missing required field {field_name!r}")
        if not isinstance(payload[field_name], field_type):
            raise SchemaMismatchError(
                f"Thesis artifact {path} field {field_name!r} must be {field_type.__name__}"
            )
    if payload["schema_version"] != "promoted_theses_v1":
        raise SchemaMismatchError(
            f"Unsupported thesis artifact schema_version {payload['schema_version']!r} at {path}"
        )
    if payload["thesis_count"] != len(payload["theses"]):
        raise SchemaMismatchError(f"Thesis artifact {path} thesis_count does not match theses payload")
    active_count = sum(
        1 for thesis in payload["theses"] if isinstance(thesis, dict) and thesis.get("status") == "active"
    )
    pending_count = sum(
        1
        for thesis in payload["theses"]
        if isinstance(thesis, dict) and thesis.get("status") == "pending_blueprint"
    )
    if payload["active_thesis_count"] != active_count:
        raise SchemaMismatchError(
            f"Thesis artifact {path} active_thesis_count does not match thesis statuses"
        )
    if payload["pending_thesis_count"] != pending_count:
        raise SchemaMismatchError(
            f"Thesis artifact {path} pending_thesis_count does not match thesis statuses"
        )


def _validate_index_payload(payload: dict, *, path: Path) -> None:
    required = {
        "schema_version": str,
        "latest_run_id": str,
        "default_resolution_disabled": bool,
        "runs": dict,
    }
    for field_name, field_type in required.items():
        if field_name not in payload:
            raise SchemaMismatchError(f"Thesis index {path} missing required field {field_name!r}")
        if not isinstance(payload[field_name], field_type):
            raise SchemaMismatchError(
                f"Thesis index {path} field {field_name!r} must be {field_type.__name__}"
            )
    if payload["schema_version"] != "promoted_thesis_index_v1":
        raise SchemaMismatchError(
            f"Unsupported thesis index schema_version {payload['schema_version']!r} at {path}"
        )
    latest_run_id = str(payload.get("latest_run_id", "")).strip()
    runs = payload.get("runs", {})
    if latest_run_id and latest_run_id not in runs:
        raise AmbiguousLatestResolutionError(
            f"Thesis index {path} latest_run_id={latest_run_id!r} is missing from runs metadata"
        )


def _matches_symbol(thesis: PromotedThesis, symbol: str) -> bool:
    token = str(symbol or "").strip().upper()
    if not token:
        return True
    scope = thesis.symbol_scope or {}
    candidate_symbol = str(scope.get("candidate_symbol", "")).strip().upper()
    if candidate_symbol == token:
        return True
    symbols = [str(item).strip().upper() for item in scope.get("symbols", []) if str(item).strip()]
    return token in symbols


def _event_ids_for_matching(thesis: PromotedThesis) -> set[str]:
    tokens = {
        str(thesis.primary_event_id or "").strip().upper(),
    }
    tokens.update(
        str(item).strip().upper()
        for item in thesis.requirements.trigger_events
        if str(item).strip()
    )
    tokens.update(
        str(item).strip().upper()
        for item in thesis.requirements.confirmation_events
        if str(item).strip()
    )
    tokens.update(
        str(item).strip().upper() for item in thesis.source.event_contract_ids if str(item).strip()
    )
    return {token for token in tokens if token}


def _family_tokens_for_matching(thesis: PromotedThesis) -> set[str]:
    token = str(thesis.event_family or "").strip().upper()
    return {token} if token else set()


class ThesisStore:
    def __init__(
        self,
        theses: Iterable[PromotedThesis],
        *,
        run_id: str = "",
        source_path: str | Path | None = None,
        schema_version: str = "",
        generated_at_utc: str = "",
    ) -> None:
        self._theses = list(theses)
        self.run_id = str(run_id or "").strip()
        self.source_path = Path(source_path) if source_path is not None else None
        self.schema_version = str(schema_version or "").strip()
        self.generated_at_utc = str(generated_at_utc or "").strip()

    @classmethod
    def from_path(
        cls,
        path: str | Path,
        *,
        strict_live_gate: bool = True,
    ) -> ThesisStore:
        resolved_path = Path(path)
        payload = _load_payload(resolved_path)
        trust = inspect_artifact_trust("promoted_theses", resolved_path)
        if trust.historical_trust_status == HISTORICAL_TRUST_LEGACY:
            raise CompatibilityRequiredError(
                f"Promoted thesis artifact {resolved_path} is legacy_but_interpretable and cannot be reused on the canonical path"
            )
        if trust.historical_trust_status == HISTORICAL_TRUST_REQUIRES_REVALIDATION:
            raise DataIntegrityError(
                f"Promoted thesis artifact {resolved_path} requires revalidation before reuse on the canonical path"
            )
        _validate_store_payload(payload, path=resolved_path)
        theses = [
            PromotedThesis.model_validate(item)
            for item in payload.get("theses", [])
            if isinstance(item, dict)
        ]
        # Enforce the live approval gate on any thesis in a live-approval-required state.
        # Raises RuntimeError if strict_live_gate=True and violations are found.
        gate = DeploymentGate(strict=strict_live_gate)
        gate.validate_batch(theses)
        return cls(
            theses,
            run_id=str(payload.get("run_id", "")).strip(),
            source_path=resolved_path,
            schema_version=str(payload.get("schema_version", "")).strip(),
            generated_at_utc=str(payload.get("generated_at_utc", "")).strip(),
        )

    @classmethod
    def from_run_id(cls, run_id: str, *, data_root: Path | None = None) -> ThesisStore:
        return cls.from_path(promoted_theses_path(run_id, data_root))

    @classmethod
    def latest(
        cls,
        *,
        data_root: Path | None = None,
        allow_implicit_latest: bool = False,
    ) -> ThesisStore:
        if not allow_implicit_latest:
            raise RuntimeError(
                "Implicit latest thesis resolution is disabled. "
                "Use ThesisStore.from_run_id(...), ThesisStore.from_path(...), "
                "or pass allow_implicit_latest=True for compatibility-only callers."
            )
        index_path = live_thesis_index_path(data_root)
        index = _load_payload(index_path)
        trust = inspect_artifact_trust("live_thesis_index", index_path)
        if trust.historical_trust_status == HISTORICAL_TRUST_LEGACY:
            raise CompatibilityRequiredError(
                f"Thesis index {index_path} is legacy_but_interpretable and cannot be reused on the canonical path"
            )
        if trust.historical_trust_status == HISTORICAL_TRUST_REQUIRES_REVALIDATION:
            raise DataIntegrityError(
                f"Thesis index {index_path} requires revalidation before reuse on the canonical path"
            )
        _validate_index_payload(index, path=index_path)
        latest_run_id = str(index.get("latest_run_id", "")).strip()
        if latest_run_id:
            return cls.from_run_id(latest_run_id, data_root=data_root)
        if index.get("runs"):
            raise AmbiguousLatestResolutionError(
                f"Thesis index {index_path} contains runs metadata but no latest_run_id"
            )
        raise MissingArtifactError("No live thesis index is available.")

    def all(self) -> list[PromotedThesis]:
        return list(self._theses)

    def filter(
        self,
        *,
        status: str | None = None,
        symbol: str | None = None,
        timeframe: str | None = None,
        event_id: str | None = None,
        event_family: str | None = None,
        canonical_regime: str | None = None,
        deployment_state: str | None = None,
        overlap_group_id: str | None = None,
    ) -> list[PromotedThesis]:
        filtered = self._theses
        if status is not None:
            status_token = str(status).strip().lower()
            filtered = [thesis for thesis in filtered if thesis.status == status_token]
        if symbol is not None:
            filtered = [thesis for thesis in filtered if _matches_symbol(thesis, symbol)]
        if timeframe is not None:
            timeframe_token = str(timeframe).strip().lower()
            filtered = [
                thesis for thesis in filtered if thesis.timeframe.strip().lower() == timeframe_token
            ]
        event_id_token = str(event_id or "").strip().upper()
        if event_id_token:
            filtered = [
                thesis for thesis in filtered if event_id_token in _event_ids_for_matching(thesis)
            ]
        event_family_token = str(event_family or "").strip().upper()
        if event_family_token:
            filtered = [
                thesis
                for thesis in filtered
                if event_family_token in _family_tokens_for_matching(thesis)
            ]
        if canonical_regime is not None:
            regime_token = str(canonical_regime).strip().upper()
            filtered = [
                thesis
                for thesis in filtered
                if thesis.canonical_regime.strip().upper() == regime_token
            ]
        if deployment_state is not None:
            deployment_token = str(deployment_state).strip().lower()
            filtered = [
                thesis
                for thesis in filtered
                if thesis.deployment_state.strip().lower() == deployment_token
            ]
        if overlap_group_id is not None:
            overlap_token = str(overlap_group_id).strip()
            filtered = [
                thesis
                for thesis in filtered
                if str(thesis.governance.overlap_group_id or "").strip() == overlap_token
            ]
        return list(filtered)

    def active_theses(
        self,
        *,
        symbol: str | None = None,
        timeframe: str | None = None,
        event_id: str | None = None,
        event_family: str | None = None,
        canonical_regime: str | None = None,
        deployment_state: str | None = None,
        overlap_group_id: str | None = None,
    ) -> list[PromotedThesis]:
        return self.filter(
            status="active",
            symbol=symbol,
            timeframe=timeframe,
            event_id=event_id,
            event_family=event_family,
            canonical_regime=canonical_regime,
            deployment_state=deployment_state,
            overlap_group_id=overlap_group_id,
        )
