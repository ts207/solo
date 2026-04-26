from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from project.artifacts import live_thesis_index_path, promoted_theses_path
from project.contracts.schemas import validate_payload_for_schema
from project.core.coercion import safe_float, safe_int
from project.core.config import get_data_root
from project.core.exceptions import (
    DataIntegrityError,
    IncompleteLineageError,
    MissingArtifactError,
    SchemaMismatchError,
)
from project.episodes import load_episode_registry
from project.events.governance import get_event_governance_metadata
from project.io.utils import atomic_write_json, atomic_write_text, ensure_dir, read_parquet
from project.live.contracts import (
    ALL_DEPLOYMENT_STATES,
    PromotedThesis,
    ThesisEvidence,
    ThesisGovernance,
    ThesisLineage,
    ThesisRequirements,
    ThesisSource,
)
from project.live.thesis_specs import resolve_thesis_definition_ids
from project.portfolio.thesis_overlap import overlap_group_id_for_thesis
from project.research.contracts.stat_regime import (
    ARTIFACT_AUDIT_VERSION_PHASE1_V1,
    AUDIT_STATUS_CURRENT,
    STAT_REGIME_POST_AUDIT,
)


@dataclass(frozen=True)
class PromotedThesisExportResult:
    run_id: str
    output_path: Path
    index_path: Path
    thesis_count: int
    active_count: int
    pending_count: int
    contract_json_path: Path | None = None
    contract_md_path: Path | None = None


def _validate_object_payload(payload: Any, *, artifact_name: str) -> None:
    if not isinstance(payload, dict):
        raise SchemaMismatchError(f"{artifact_name} must be a JSON object payload")


def _validate_exported_thesis_payload(payload: Any) -> None:
    validate_payload_for_schema(payload, "promoted_theses_payload")
    _validate_object_payload(payload, artifact_name="promoted_theses.json")
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
            raise SchemaMismatchError(f"promoted_theses.json missing required field {field_name!r}")
        if not isinstance(payload[field_name], field_type):
            raise SchemaMismatchError(
                f"promoted_theses.json field {field_name!r} must be {field_type.__name__}"
            )
    if payload["schema_version"] != "promoted_theses_v1":
        raise SchemaMismatchError(
            f"Unsupported promoted thesis schema_version {payload['schema_version']!r}"
        )
    if payload["thesis_count"] != len(payload["theses"]):
        raise SchemaMismatchError("promoted_theses.json thesis_count does not match theses payload")
    active_count = sum(
        1
        for thesis in payload["theses"]
        if isinstance(thesis, dict) and thesis.get("status") == "active"
    )
    pending_count = sum(
        1
        for thesis in payload["theses"]
        if isinstance(thesis, dict) and thesis.get("status") == "pending_blueprint"
    )
    if payload["active_thesis_count"] != active_count:
        raise SchemaMismatchError(
            "promoted_theses.json active_thesis_count does not match thesis statuses"
        )
    if payload["pending_thesis_count"] != pending_count:
        raise SchemaMismatchError(
            "promoted_theses.json pending_thesis_count does not match thesis statuses"
        )


def _validate_thesis_index_payload(payload: Any) -> None:
    validate_payload_for_schema(payload, "live_thesis_index")
    _validate_object_payload(payload, artifact_name="index.json")
    required = {
        "schema_version": str,
        "latest_run_id": str,
        "default_resolution_disabled": bool,
        "runs": dict,
    }
    for field_name, field_type in required.items():
        if field_name not in payload:
            raise SchemaMismatchError(f"index.json missing required field {field_name!r}")
        if not isinstance(payload[field_name], field_type):
            raise SchemaMismatchError(
                f"index.json field {field_name!r} must be {field_type.__name__}"
            )
    if payload["schema_version"] != "promoted_thesis_index_v1":
        raise SchemaMismatchError(
            f"Unsupported thesis index schema_version {payload['schema_version']!r}"
        )
    latest_run_id = str(payload.get("latest_run_id", "")).strip()
    if latest_run_id and latest_run_id not in payload["runs"]:
        raise SchemaMismatchError("index.json latest_run_id is not present in runs metadata")


def _fallback_authored_definition_for_event(*event_tokens: str):
    from project.domain.compiled_registry import get_domain_registry

    registry = get_domain_registry()
    normalized = [
        str(token or "").strip().upper() for token in event_tokens if str(token or "").strip()
    ]
    if not normalized:
        return None
    for definition in registry.thesis_definitions.values():
        if str(definition.governance.get("operational_role", "")).strip().lower() != "trigger":
            continue
        primary = str(definition.primary_event_id or "").strip().upper()
        family = str(definition.event_family or "").strip().upper()
        triggers = [str(token or "").strip().upper() for token in definition.trigger_events]
        if any(token and token in {primary, family, *triggers} for token in normalized):
            return definition
    return None


def _resolve_detector_lineage(bundle: Mapping[str, Any], promoted_row: Mapping[str, Any]) -> dict[str, str]:
    source = dict(promoted_row or {})
    metadata = dict(bundle.get("metadata", {})) if isinstance(bundle, Mapping) else {}
    return {
        "source_event_name": str(source.get("source_event_name") or source.get("event_type") or metadata.get("event_type") or "").strip(),
        "source_event_version": str(source.get("source_event_version") or metadata.get("source_event_version") or "").strip(),
        "source_detector_class": str(source.get("source_detector_class") or metadata.get("source_detector_class") or "").strip(),
        "source_evidence_mode": str(source.get("source_evidence_mode") or source.get("evidence_mode") or metadata.get("evidence_mode") or "").strip(),
        "source_threshold_version": str(source.get("source_threshold_version") or metadata.get("source_threshold_version") or "").strip(),
        "source_calibration_artifact": str(source.get("source_calibration_artifact") or metadata.get("source_calibration_artifact") or "").strip(),
    }


def _lineage_value(
    *,
    key: str,
    metadata: Mapping[str, Any],
    promoted_row: Mapping[str, Any],
) -> Any:
    value = promoted_row.get(key, "")
    if value not in (None, ""):
        return value
    return metadata.get(key, "")


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _json_load(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DataIntegrityError(f"Failed to read live thesis json artifact {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise DataIntegrityError(
            f"Live thesis json artifact {path} did not contain an object payload"
        )
    return payload


def _load_table(path: Path) -> pd.DataFrame:
    if path.exists():
        try:
            if path.suffix.lower() == ".parquet":
                return read_parquet(path)
            if path.suffix.lower() == ".csv":
                return pd.read_csv(path)
        except Exception as exc:
            raise DataIntegrityError(
                f"Failed to read live thesis tabular artifact {path}: {exc}"
            ) from exc
    return pd.DataFrame()


def _read_jsonl_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError) as exc:
        raise DataIntegrityError(f"Failed to read jsonl artifact {path}: {exc}") from exc
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise DataIntegrityError(
                f"Malformed JSONL record in {path} at line {line_number}: {exc}"
            ) from exc
        if isinstance(payload, dict):
            rows.append(payload)
        else:
            raise DataIntegrityError(
                f"JSONL record in {path} at line {line_number} was not an object payload"
            )
    return rows


def _promotion_dir(run_id: str, data_root: Path) -> Path:
    return data_root / "reports" / "promotions" / str(run_id)


def _blueprint_dir(run_id: str, data_root: Path) -> Path:
    return data_root / "reports" / "strategy_blueprints" / str(run_id)


def _load_evidence_bundles(run_id: str, data_root: Path) -> list[dict[str, Any]]:
    return _read_jsonl_records(_promotion_dir(run_id, data_root) / "evidence_bundles.jsonl")


def _load_promoted_candidates(run_id: str, data_root: Path) -> pd.DataFrame:
    promotion_root = _promotion_dir(run_id, data_root)
    for candidate_path in (
        promotion_root / "promoted_candidates.parquet",
        promotion_root / "promoted_candidates.csv",
    ):
        frame = _load_table(candidate_path)
        if not frame.empty:
            return frame
    return pd.DataFrame()


def _load_blueprints(run_id: str, data_root: Path) -> list[dict[str, Any]]:
    return _read_jsonl_records(_blueprint_dir(run_id, data_root) / "blueprints.jsonl")


def _row_by_candidate_id(frame: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if frame.empty or "candidate_id" not in frame.columns:
        return {}
    rows: dict[str, dict[str, Any]] = {}
    for row in frame.to_dict(orient="records"):
        candidate_id = str(row.get("candidate_id", "")).strip()
        if candidate_id and candidate_id not in rows:
            rows[candidate_id] = dict(row)
    return rows


def _promoted_candidate_ids(frame: pd.DataFrame) -> set[str]:
    if frame.empty:
        return set()
    if "candidate_id" not in frame.columns:
        raise SchemaMismatchError(
            "Promoted candidates artifact missing required candidate_id column."
        )
    promoted_ids: set[str] = set()
    for row in frame.to_dict(orient="records"):
        candidate_id = str(row.get("candidate_id", "")).strip()
        if not candidate_id:
            continue
        status = (
            str(row.get("status", "")).strip()
            or str(row.get("promotion_status", "")).strip()
            or str(row.get("promotion_decision", "")).strip()
            or "PROMOTED"
        )
        if "PROMOT" in status.upper():
            promoted_ids.add(candidate_id)
    return promoted_ids


def _validate_promotion_evidence_alignment(
    *,
    run_id: str,
    bundles: Sequence[Mapping[str, Any]],
    promoted_df: pd.DataFrame,
) -> None:
    promoted_ids = _promoted_candidate_ids(promoted_df)
    if not promoted_ids:
        return
    evidence_ids = {
        str(bundle.get("candidate_id", "")).strip()
        for bundle in bundles
        if str(bundle.get("candidate_id", "")).strip()
    }
    promoted_bundle_ids = {
        str(bundle.get("candidate_id", "")).strip()
        for bundle in bundles
        if str(bundle.get("candidate_id", "")).strip()
        and str(
            (bundle.get("promotion_decision", {}) or {}).get("promotion_status", "")
            if isinstance(bundle.get("promotion_decision", {}), Mapping)
            else ""
        )
        .strip()
        .lower()
        == "promoted"
    }
    missing_evidence = sorted(promoted_ids - evidence_ids)
    unselected_promoted_evidence = sorted(promoted_bundle_ids - promoted_ids)
    if missing_evidence or unselected_promoted_evidence:
        details = []
        if missing_evidence:
            details.append("promoted candidates missing evidence: " + ", ".join(missing_evidence))
        if unselected_promoted_evidence:
            details.append(
                "evidence bundles marked promoted but absent from promoted candidates: "
                + ", ".join(unselected_promoted_evidence)
            )
        raise IncompleteLineageError(
            f"Promotion/evidence lineage mismatch for run {run_id}: " + "; ".join(details)
        )


def _blueprint_by_candidate_id(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for row in rows:
        candidate_id = str(row.get("candidate_id", "")).strip()
        if candidate_id and candidate_id not in indexed:
            indexed[candidate_id] = dict(row)
    return indexed


def _timeframe_from_minutes(value: Any) -> str:
    minutes = int(safe_int(value, 0))
    if minutes <= 0:
        return ""
    if minutes % 1440 == 0:
        return f"{minutes // 1440}d"
    if minutes % 60 == 0:
        return f"{minutes // 60}h"
    return f"{minutes}m"


def _finite_or_none(value: Any) -> float | None:
    out = safe_float(value, np.nan)
    return None if not np.isfinite(out) else float(out)


def _normalize_tokens(values: Any) -> list[str]:
    if values in (None, ""):
        return []
    if isinstance(values, str):
        values = [values]
    if not isinstance(values, (list, tuple, set)):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in values:
        token = str(item or "").strip().upper()
        if token and token not in seen:
            out.append(token)
            seen.add(token)
    return out


def _coerce_symbol_scope(symbol: str, blueprint: Mapping[str, Any] | None) -> dict[str, Any]:
    if isinstance(blueprint, Mapping):
        scope = blueprint.get("symbol_scope", {})
        if isinstance(scope, Mapping) and scope:
            return dict(scope)
    clean_symbol = str(symbol or "").strip().upper()
    if not clean_symbol:
        return {}
    return {
        "mode": "single_symbol",
        "symbols": [clean_symbol],
        "candidate_symbol": clean_symbol,
    }


def _resolve_event_side(bundle: Mapping[str, Any], blueprint: Mapping[str, Any] | None) -> str:
    if isinstance(blueprint, Mapping):
        direction = str(blueprint.get("direction", "")).strip().lower()
        if direction in {"long", "short", "both", "conditional"}:
            return direction
    estimate_bps = _finite_or_none(bundle.get("effect_estimates", {}).get("estimate_bps"))
    if estimate_bps is None:
        estimate_bps = _finite_or_none(bundle.get("cost_robustness", {}).get("net_expectancy_bps"))
    if estimate_bps is None:
        return "unknown"
    if estimate_bps > 0:
        return "long"
    if estimate_bps < 0:
        return "short"
    return "unknown"


def _promotion_track(bundle: Mapping[str, Any], promoted_row: Mapping[str, Any]) -> str:
    track = str(bundle.get("promotion_decision", {}).get("promotion_track", "")).strip()
    if track:
        return track
    return str(promoted_row.get("promotion_track", "")).strip()


def _build_required_context(
    *,
    symbol: str,
    timeframe: str,
    bundle: Mapping[str, Any],
) -> dict[str, Any]:
    sample = bundle.get("sample_definition", {})
    split = bundle.get("split_definition", {})
    metadata = bundle.get("metadata", {})
    return {
        "symbol": symbol,
        "event_type": str(bundle.get("event_type", "")).strip(),
        "timeframe": timeframe,
        "split_scheme_id": str(split.get("split_scheme_id", "")).strip(),
        "bar_duration_minutes": int(safe_int(split.get("bar_duration_minutes", 0), 0)),
        "event_is_trade_trigger": bool(metadata.get("event_is_trade_trigger", True)),
        "sample_symbol": str(sample.get("symbol", "")).strip(),
    }


def _build_supportive_context(
    *,
    bundle: Mapping[str, Any],
    promoted_row: Mapping[str, Any],
) -> dict[str, Any]:
    metadata = bundle.get("metadata", {})
    return {
        "canonical_regime": str(promoted_row.get("canonical_regime", "")).strip(),
        "subtype": str(promoted_row.get("subtype", "")).strip(),
        "phase": str(promoted_row.get("phase", "")).strip(),
        "evidence_mode": str(promoted_row.get("evidence_mode", "")).strip(),
        "recommended_bucket": str(promoted_row.get("recommended_bucket", "")).strip(),
        "regime_bucket": str(promoted_row.get("regime_bucket", "")).strip(),
        "routing_profile_id": str(promoted_row.get("routing_profile_id", "")).strip(),
        "promotion_track": _promotion_track(bundle, promoted_row),
        "bridge_certified": bool(metadata.get("bridge_certified", False)),
        "has_realized_oos_path": bool(metadata.get("has_realized_oos_path", False)),
    }


def _build_expected_response(
    *,
    bundle: Mapping[str, Any],
    blueprint: Mapping[str, Any] | None,
    event_side: str,
) -> dict[str, Any]:
    effect = bundle.get("effect_estimates", {})
    cost = bundle.get("cost_robustness", {})
    response = {
        "direction": event_side,
        "estimate_bps": _finite_or_none(effect.get("estimate_bps")),
        "net_expectancy_bps": _finite_or_none(cost.get("net_expectancy_bps")),
    }
    if isinstance(blueprint, Mapping):
        exit_spec = blueprint.get("exit", {})
        if isinstance(exit_spec, Mapping):
            response.update(
                {
                    "time_stop_bars": int(safe_int(exit_spec.get("time_stop_bars", 0), 0)),
                    "stop_type": str(exit_spec.get("stop_type", "")).strip(),
                    "stop_value": _finite_or_none(exit_spec.get("stop_value")),
                    "target_type": str(exit_spec.get("target_type", "")).strip(),
                    "target_value": _finite_or_none(exit_spec.get("target_value")),
                }
            )
    return response


def _build_governance(
    bundle: Mapping[str, Any],
    promoted_row: Mapping[str, Any],
    *,
    overlap_group_id: str = "",
    authored_def: Any | None = None,
    primary_event_id: str = "",
) -> ThesisGovernance:
    event_id = str(
        primary_event_id or bundle.get("event_type", "") or promoted_row.get("event_type", "")
    ).strip()
    meta = get_event_governance_metadata(event_id) if event_id else {}
    authored_governance = authored_def.governance if authored_def is not None else {}
    if not isinstance(authored_governance, Mapping):
        authored_governance = {}
    return ThesisGovernance(
        tier=str(authored_governance.get("tier", meta.get("tier", ""))).strip(),
        operational_role=str(
            authored_governance.get("operational_role", meta.get("operational_role", ""))
        ).strip(),
        deployment_disposition=str(
            authored_governance.get(
                "deployment_disposition", meta.get("deployment_disposition", "")
            )
        ).strip(),
        evidence_mode=str(
            authored_governance.get("evidence_mode", meta.get("evidence_mode", ""))
        ).strip(),
        overlap_group_id=str(
            overlap_group_id or authored_governance.get("overlap_group_id", "")
        ).strip(),
        trade_trigger_eligible=bool(
            authored_governance.get(
                "trade_trigger_eligible", meta.get("trade_trigger_eligible", False)
            )
        ),
        requires_stronger_evidence=bool(
            authored_governance.get(
                "requires_stronger_evidence", meta.get("requires_stronger_evidence", False)
            )
        ),
    )


def _episode_ids_from_metadata(
    bundle: Mapping[str, Any], promoted_row: Mapping[str, Any], metadata: Mapping[str, Any]
) -> list[str]:
    payloads = [metadata, bundle, promoted_row]
    out: list[str] = []
    seen: set[str] = set()
    known = set(load_episode_registry().keys())
    for payload in payloads:
        if not isinstance(payload, Mapping):
            continue
        values = payload.get("episode_ids") or payload.get("episodes") or []
        if isinstance(values, str):
            values = [values]
        if not isinstance(values, (list, tuple, set)):
            continue
        for item in values:
            token = str(item or "").strip().upper()
            if token and token not in seen and (not known or token in known):
                out.append(token)
                seen.add(token)
    return out


def _build_requirements(
    bundle: Mapping[str, Any], promoted_row: Mapping[str, Any]
) -> ThesisRequirements:
    return _build_requirements_from_contract(
        bundle=bundle,
        promoted_row=promoted_row,
        metadata=bundle.get("metadata", {})
        if isinstance(bundle.get("metadata", {}), Mapping)
        else {},
        authored_def=None,
        event_contract_ids=None,
        episode_contract_ids=None,
    )


def _build_source(
    bundle: Mapping[str, Any], promoted_row: Mapping[str, Any], metadata: Mapping[str, Any]
) -> ThesisSource:
    event_id = str(bundle.get("event_type", "") or promoted_row.get("event_type", "")).strip()
    campaign_id = str(
        metadata.get("campaign_id", "") or promoted_row.get("campaign_id", "")
    ).strip()
    program_id = str(metadata.get("program_id", "") or promoted_row.get("program_id", "")).strip()
    source_run_mode = str(
        metadata.get("source_run_mode", "") or promoted_row.get("source_run_mode", "")
    ).strip()
    objective_name = str(
        metadata.get("objective_name", "") or promoted_row.get("objective_name", "")
    ).strip()
    return ThesisSource(
        source_program_id=program_id,
        source_campaign_id=campaign_id,
        source_run_mode=source_run_mode,
        objective_name=objective_name,
        event_contract_ids=[event_id] if event_id else [],
        episode_contract_ids=_episode_ids_from_metadata(bundle, promoted_row, metadata),
    )


def _resolve_authored_thesis_definition(
    candidate_id: str,
    metadata: Mapping[str, Any],
    promoted_row: Mapping[str, Any],
):
    return resolve_thesis_definition_ids(
        candidate_id,
        str(metadata.get("thesis_id", "")).strip(),
        str(metadata.get("hypothesis_id", "")).strip(),
        str(promoted_row.get("hypothesis_id", "")).strip(),
    )


def _contract_ids(
    *,
    metadata: Mapping[str, Any],
    promoted_row: Mapping[str, Any],
    bundle: Mapping[str, Any],
    event_id: str,
    authored_def: Any,
) -> tuple[list[str], list[str]]:
    if authored_def is not None:
        return (
            [
                str(token).strip().upper()
                for token in authored_def.source_event_contract_ids
                if str(token).strip()
            ],
            [
                str(token).strip().upper()
                for token in authored_def.source_episode_contract_ids
                if str(token).strip()
            ],
        )
    event_contract_ids = (
        _normalize_tokens(metadata.get("event_contract_ids"))
        or _normalize_tokens(promoted_row.get("event_contract_ids"))
        or _normalize_tokens(bundle.get("event_contract_ids"))
    )
    if not event_contract_ids and event_id:
        event_contract_ids = [event_id.strip().upper()]
    episode_contract_ids = _episode_ids_from_metadata(bundle, promoted_row, metadata)
    return event_contract_ids, episode_contract_ids


def _infer_sequence_mode(
    *,
    metadata: Mapping[str, Any],
    promoted_row: Mapping[str, Any],
    authored_def: Any,
    event_contract_ids: Sequence[str],
    episode_contract_ids: Sequence[str],
) -> str:
    if authored_def is not None and str(getattr(authored_def, "thesis_kind", "")).strip():
        return str(authored_def.thesis_kind).strip()
    explicit = (
        str(metadata.get("sequence_mode", "")).strip()
        or str(metadata.get("source_type", "")).strip()
        or str(promoted_row.get("source_type", "")).strip()
    )
    if explicit:
        return explicit
    if event_contract_ids and len(event_contract_ids) > 1:
        return "event_plus_confirm"
    if episode_contract_ids and not event_contract_ids:
        return "episode"
    return "standalone_event"


def _build_requirements_from_contract(
    *,
    bundle: Mapping[str, Any],
    promoted_row: Mapping[str, Any],
    metadata: Mapping[str, Any],
    authored_def: Any,
    event_contract_ids: Sequence[str] | None,
    episode_contract_ids: Sequence[str] | None,
) -> ThesisRequirements:
    event_id = str(bundle.get("event_type", "") or promoted_row.get("event_type", "")).strip()
    primary_event_id = str(
        (event_contract_ids or [event_id])[:1][0] if (event_contract_ids or [event_id]) else ""
    ).strip()
    meta = get_event_governance_metadata(primary_event_id) if primary_event_id else {}
    disallowed = bundle.get("disabled_regimes") or promoted_row.get("disabled_regimes") or []
    if isinstance(disallowed, str):
        disallowed = [disallowed]
    resolved_event_contract_ids = list(event_contract_ids or [])
    resolved_episode_contract_ids = list(episode_contract_ids or [])
    sequence_mode = _infer_sequence_mode(
        metadata=metadata,
        promoted_row=promoted_row,
        authored_def=authored_def,
        event_contract_ids=resolved_event_contract_ids,
        episode_contract_ids=resolved_episode_contract_ids,
    )
    if authored_def is not None:
        trigger_events = [
            str(token).strip().upper()
            for token in authored_def.trigger_events
            if str(token).strip()
        ]
        confirmation_events = [
            str(token).strip().upper()
            for token in authored_def.confirmation_events
            if str(token).strip()
        ]
        required_episodes = [
            str(token).strip().upper()
            for token in authored_def.required_episodes
            if str(token).strip()
        ]
        disallowed_regimes = [
            str(token).strip().upper()
            for token in authored_def.disallowed_regimes
            if str(token).strip()
        ]
    else:
        if sequence_mode == "event_plus_confirm":
            trigger_events = resolved_event_contract_ids[:1]
            confirmation_events = resolved_event_contract_ids[1:]
        elif sequence_mode == "episode":
            trigger_events = []
            confirmation_events = []
        else:
            trigger_events = resolved_event_contract_ids
            confirmation_events = []
        required_episodes = resolved_episode_contract_ids
        disallowed_regimes = [
            str(value).strip().upper() for value in disallowed if str(value).strip()
        ]
    return ThesisRequirements(
        trigger_events=trigger_events,
        confirmation_events=confirmation_events,
        required_episodes=required_episodes,
        disallowed_regimes=disallowed_regimes,
        deployment_gate=str(meta.get("promotion_block_reason", "")).strip(),
        sequence_mode=sequence_mode,
        minimum_episode_confidence=float(metadata.get("minimum_episode_confidence", 0.0) or 0.0),
    )


def _build_source_from_contract(
    *,
    promoted_row: Mapping[str, Any],
    metadata: Mapping[str, Any],
    event_contract_ids: Sequence[str],
    episode_contract_ids: Sequence[str],
) -> ThesisSource:
    campaign_id = str(
        metadata.get("campaign_id", "") or promoted_row.get("campaign_id", "")
    ).strip()
    program_id = str(metadata.get("program_id", "") or promoted_row.get("program_id", "")).strip()
    source_run_mode = str(
        metadata.get("source_run_mode", "") or promoted_row.get("source_run_mode", "")
    ).strip()
    objective_name = str(
        metadata.get("objective_name", "") or promoted_row.get("objective_name", "")
    ).strip()
    return ThesisSource(
        source_program_id=program_id,
        source_campaign_id=campaign_id,
        source_run_mode=source_run_mode,
        objective_name=objective_name,
        event_contract_ids=[
            str(token).strip().upper() for token in event_contract_ids if str(token).strip()
        ],
        episode_contract_ids=[
            str(token).strip().upper() for token in episode_contract_ids if str(token).strip()
        ],
    )


def _build_risk_notes(
    *,
    bundle: Mapping[str, Any],
    blueprint: Mapping[str, Any] | None,
    status: str,
) -> list[str]:
    metadata = bundle.get("metadata", {})
    cost = bundle.get("cost_robustness", {})
    notes: list[str] = []
    if status == "pending_blueprint":
        notes.append("missing_blueprint_invalidation")
    if bool(metadata.get("is_reduced_evidence", False)):
        notes.append("reduced_evidence")
    if not bool(metadata.get("has_realized_oos_path", False)):
        notes.append("limited_realized_oos_path")
    if cost.get("retail_net_expectancy_pass") is False:
        notes.append("retail_net_expectancy_gate_failed")
    if isinstance(blueprint, Mapping):
        direction = str(blueprint.get("direction", "")).strip()
        if direction:
            notes.append(f"direction:{direction}")
    return notes


def _contract_row(thesis: PromotedThesis) -> dict[str, Any]:
    authored_def = resolve_thesis_definition_ids(
        thesis.thesis_id,
        thesis.lineage.candidate_id,
        thesis.lineage.hypothesis_id,
    )
    authored_contract_id = str(authored_def.thesis_id).strip() if authored_def is not None else ""
    return {
        "thesis_id": thesis.thesis_id,
        "candidate_id": str(thesis.lineage.candidate_id or "").strip(),
        "authored_contract_id": authored_contract_id,
        "authored_contract_linked": bool(authored_contract_id),
        "status": thesis.status,
        "promotion_class": thesis.promotion_class,
        "deployment_state": thesis.deployment_state,
        "primary_event_id": str(thesis.primary_event_id or thesis.event_family or "")
        .strip()
        .upper(),
        "compat_event_family": str(thesis.event_family or "").strip().upper(),
        "timeframe": str(thesis.timeframe or "").strip(),
        "trigger_events": list(thesis.requirements.trigger_events),
        "confirmation_events": list(thesis.requirements.confirmation_events),
        "required_episodes": list(thesis.requirements.required_episodes),
        "disallowed_regimes": list(thesis.requirements.disallowed_regimes),
        "sequence_mode": str(thesis.requirements.sequence_mode or "").strip(),
        "source_event_contract_ids": list(thesis.source.event_contract_ids),
        "source_episode_contract_ids": list(thesis.source.episode_contract_ids),
        "governance_tier": str(thesis.governance.tier or "").strip(),
        "operational_role": str(thesis.governance.operational_role or "").strip(),
        "deployment_disposition": str(thesis.governance.deployment_disposition or "").strip(),
        "evidence_mode": str(thesis.governance.evidence_mode or "").strip(),
        "trade_trigger_eligible": bool(thesis.governance.trade_trigger_eligible),
        "requires_stronger_evidence": bool(thesis.governance.requires_stronger_evidence),
        "overlap_group_id": str(thesis.governance.overlap_group_id or "").strip(),
        "source_program_id": str(thesis.source.source_program_id or "").strip(),
        "source_campaign_id": str(thesis.source.source_campaign_id or "").strip(),
        "source_run_mode": str(thesis.source.source_run_mode or "").strip(),
        "objective_name": str(thesis.source.objective_name or "").strip(),
        "source_discovery_mode": str(thesis.lineage.source_discovery_mode or "").strip(),
        "source_cell_id": str(thesis.lineage.source_cell_id or "").strip(),
        "source_scoreboard_run_id": str(thesis.lineage.source_scoreboard_run_id or "").strip(),
        "source_event_atom": str(thesis.lineage.source_event_atom or "").strip(),
        "source_context_cell": str(thesis.lineage.source_context_cell or "").strip(),
        "source_contrast_lift_bps": thesis.lineage.source_contrast_lift_bps,
    }


def _render_contract_markdown(*, run_id: str, rows: Sequence[Mapping[str, Any]]) -> str:
    lines = [
        "# Promoted Thesis Contracts",
        "",
        f"- run_id: `{run_id}`",
        f"- thesis_count: `{len(rows)}`",
        "",
    ]
    if not rows:
        lines.append("_No promoted theses exported._")
        return "\n".join(lines).rstrip() + "\n"
    lines.extend(
        [
            "| thesis_id | authored_contract | primary_event_id | triggers | confirmations | episodes | overlap_group |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for row in rows:
        lines.append(
            "| {thesis_id} | {authored} | {primary_event_id} | {triggers} | {confirmations} | {episodes} | {overlap} |".format(
                thesis_id=f"`{row.get('thesis_id', '')}`",
                authored=(
                    f"`{row.get('authored_contract_id', '')}`"
                    if row.get("authored_contract_id")
                    else "`unlinked`"
                ),
                primary_event_id=f"`{row.get('primary_event_id', '')}`",
                triggers=f"`{', '.join(row.get('trigger_events', []))}`",
                confirmations=f"`{', '.join(row.get('confirmation_events', []))}`",
                episodes=f"`{', '.join(row.get('required_episodes', []))}`",
                overlap=f"`{row.get('overlap_group_id', '')}`",
            )
        )
    return "\n".join(lines).rstrip() + "\n"


def _write_contract_artifacts(
    *,
    run_id: str,
    theses: Sequence[PromotedThesis],
    data_root: Path,
) -> tuple[Path, Path]:
    out_dir = _promotion_dir(run_id, data_root)
    ensure_dir(out_dir)
    rows = [_contract_row(thesis) for thesis in theses]
    json_path = out_dir / "promoted_thesis_contracts.json"
    md_path = out_dir / "promoted_thesis_contracts.md"
    payload = {
        "schema_version": "promoted_thesis_contracts_v1",
        "run_id": run_id,
        "generated_at_utc": _utc_now(),
        "thesis_count": len(rows),
        "contracts": rows,
    }
    atomic_write_json(
        json_path,
        payload,
        validator=lambda data: _validate_object_payload(
            data, artifact_name="promoted_thesis_contracts.json"
        ),
    )
    atomic_write_text(md_path, _render_contract_markdown(run_id=run_id, rows=rows))
    return json_path, md_path


def _status_for_blueprint(blueprint: Mapping[str, Any] | None) -> str:
    if not isinstance(blueprint, Mapping):
        return "pending_blueprint"
    invalidation = blueprint.get("exit", {})
    if isinstance(invalidation, Mapping):
        invalidation = invalidation.get("invalidation", {})
    if isinstance(invalidation, Mapping) and invalidation:
        return "active"
    return "pending_blueprint"


def _build_thesis(
    *,
    run_id: str,
    bundle: Mapping[str, Any],
    promoted_row: Mapping[str, Any],
    blueprint: Mapping[str, Any] | None,
    validation_metadata: Mapping[str, Mapping[str, Any]] | None = None,
    require_validation_lineage: bool = True,
) -> PromotedThesis:
    sample = bundle.get("sample_definition", {})
    split = bundle.get("split_definition", {})
    metadata = bundle.get("metadata", {})
    decision = bundle.get("promotion_decision", {})
    effect = bundle.get("effect_estimates", {})
    uncertainty = bundle.get("uncertainty_estimates", {})
    stability = bundle.get("stability_tests", {})
    cost = bundle.get("cost_robustness", {})

    candidate_id = str(bundle.get("candidate_id", "")).strip()
    if not candidate_id:
        raise DataIntegrityError("Missing candidate_id in bundle")
    authored_def = _resolve_authored_thesis_definition(
        candidate_id,
        metadata if isinstance(metadata, Mapping) else {},
        promoted_row,
    )
    metadata_mapping = metadata if isinstance(metadata, Mapping) else {}
    explicit_contract_shape = bool(
        metadata_mapping.get("event_contract_ids")
        or metadata_mapping.get("episode_ids")
        or metadata_mapping.get("episodes")
        or str(metadata_mapping.get("source_type", "")).strip()
    )
    if authored_def is None and not explicit_contract_shape:
        authored_def = _fallback_authored_definition_for_event(
            bundle.get("event_type", ""),
            bundle.get("event_family", ""),
            promoted_row.get("event_type", ""),
        )
    symbol = str(sample.get("symbol", "") or promoted_row.get("symbol", "")).strip().upper()
    timeframe = _timeframe_from_minutes(split.get("bar_duration_minutes", 0))
    event_family = str(bundle.get("event_family", "") or bundle.get("event_type", "")).strip()
    sample_size = int(safe_int(sample.get("n_events", 0), 0))
    net_expectancy_bps = _finite_or_none(cost.get("net_expectancy_bps"))

    missing_fields = []
    if not symbol:
        missing_fields.append("symbol")
    if not timeframe:
        missing_fields.append("timeframe/bar_duration_minutes")
    if not event_family:
        missing_fields.append("event_family")
    if sample_size <= 0:
        missing_fields.append("sample_size")
    if net_expectancy_bps is None:
        missing_fields.append("net_expectancy_bps")

    if missing_fields:
        raise DataIntegrityError(
            f"Candidate {candidate_id} missing required fields: {', '.join(missing_fields)}"
        )

    status = _status_for_blueprint(blueprint)
    event_side = _resolve_event_side(bundle, blueprint)
    event_id = str(bundle.get("event_type", "") or promoted_row.get("event_type", "")).strip()
    event_contract_ids, episode_contract_ids = _contract_ids(
        metadata=metadata if isinstance(metadata, Mapping) else {},
        promoted_row=promoted_row,
        bundle=bundle,
        event_id=event_id,
        authored_def=authored_def,
    )
    invalidation = {}
    proposal_id = ""
    blueprint_id = ""
    if isinstance(blueprint, Mapping):
        blueprint_id = str(blueprint.get("id", "")).strip()
        exit_spec = blueprint.get("exit", {})
        if isinstance(exit_spec, Mapping):
            invalidation = dict(exit_spec.get("invalidation", {}))
        lineage = blueprint.get("lineage", {})
        if isinstance(lineage, Mapping):
            proposal_id = str(lineage.get("proposal_id", "")).strip()

    track = _promotion_track(bundle, promoted_row)

    validation_run_id = ""
    validation_status = ""
    validation_reasons = []
    validation_artifacts = {}

    if validation_metadata:
        val_meta = validation_metadata.get(candidate_id)
        if val_meta:
            validation_run_id = str(val_meta.get("validation_run_id", "")).strip()
            validation_status = str(val_meta.get("validation_status", "")).strip()
            validation_reasons = list(val_meta.get("validation_reason_codes", []))
            validation_artifacts = dict(val_meta.get("validation_artifact_paths", {}))
    if require_validation_lineage:
        missing_lineage = []
        if not validation_run_id:
            missing_lineage.append("validation_run_id")
        if not validation_status:
            missing_lineage.append("validation_status")
        if missing_lineage:
            raise IncompleteLineageError(
                f"Candidate {candidate_id} missing required validation lineage: "
                + ", ".join(missing_lineage)
            )

    # Sprint 4: Extract explicit promotion fields
    promo_class = str(promoted_row.get("promotion_class") or "paper_promoted").lower()
    if promo_class not in {"paper_promoted", "production_promoted"}:
        promo_class = "paper_promoted"

    deploy_state = str(
        promoted_row.get("deployment_state_default")
        or promoted_row.get("deployment_state")
        or "paper_only"
    ).lower()
    if deploy_state not in ALL_DEPLOYMENT_STATES:
        deploy_state = "paper_only"

    # Compute batch timestamp once for consistency
    generated_at = _utc_now()
    batch_id = f"batch::{run_id}::{generated_at}"

    detector_lineage = _resolve_detector_lineage(bundle, promoted_row)

    thesis = PromotedThesis(
        thesis_id=f"thesis::{run_id}::{candidate_id}",
        promotion_class=promo_class,
        deployment_state=deploy_state,
        evidence_gaps=[],
        status=status,
        symbol_scope=_coerce_symbol_scope(symbol, blueprint),
        timeframe=str(getattr(authored_def, "timeframe", "")).strip() or timeframe,
        primary_event_id=(
            str(getattr(authored_def, "primary_event_id", "")).strip()
            or str((event_contract_ids or [event_id or event_family])[:1][0]).strip()
            or event_family
        ),
        event_family=str(getattr(authored_def, "event_family", "")).strip() or event_family,
        canonical_regime=(
            str(
                getattr(authored_def, "supportive_context", {}).get("canonical_regime", "")
                if authored_def is not None
                else promoted_row.get("canonical_regime", "")
            )
            .strip()
            .upper()
        ),
        event_side=event_side,
        required_context=_build_required_context(symbol=symbol, timeframe=timeframe, bundle=bundle),
        supportive_context=_build_supportive_context(bundle=bundle, promoted_row=promoted_row),
        expected_response=_build_expected_response(
            bundle=bundle,
            blueprint=blueprint,
            event_side=event_side,
        ),
        invalidation=invalidation,
        risk_notes=_build_risk_notes(bundle=bundle, blueprint=blueprint, status=status),
        evidence=ThesisEvidence(
            sample_size=sample_size,
            validation_samples=int(safe_int(sample.get("validation_samples", 0), 0)),
            test_samples=int(safe_int(sample.get("test_samples", 0), 0)),
            estimate_bps=_finite_or_none(effect.get("estimate_bps")),
            net_expectancy_bps=net_expectancy_bps,
            q_value=_finite_or_none(uncertainty.get("q_value")),
            stability_score=_finite_or_none(stability.get("stability_score")),
            cost_survival_ratio=_finite_or_none(cost.get("cost_survival_ratio")),
            tob_coverage=_finite_or_none(cost.get("tob_coverage")),
            rank_score=_finite_or_none(
                decision.get("rank_score", promoted_row.get("selection_score"))
            ),
            promotion_track=track,
            policy_version=str(bundle.get("policy_version", "")).strip(),
            bundle_version=str(bundle.get("bundle_version", "")).strip(),
            stat_regime=STAT_REGIME_POST_AUDIT,
            audit_status=AUDIT_STATUS_CURRENT,
            artifact_audit_version=ARTIFACT_AUDIT_VERSION_PHASE1_V1,
        ),
        lineage=ThesisLineage(
            run_id=run_id,
            candidate_id=candidate_id,
            hypothesis_id=str(
                metadata.get("hypothesis_id", promoted_row.get("hypothesis_id", ""))
            ).strip(),
            plan_row_id=str(metadata.get("plan_row_id", "")).strip(),
            blueprint_id=blueprint_id,
            proposal_id=proposal_id,
            validation_run_id=validation_run_id,
            validation_status=validation_status,
            validation_reason_codes=validation_reasons,
            validation_artifact_paths=validation_artifacts,
            # Batch Identity
            export_batch_id=batch_id,
            export_generated_at=generated_at,
            source_run_id=run_id,
            thesis_version="1.0.0",
            source_event_name=detector_lineage.get("source_event_name", ""),
            source_event_version=detector_lineage.get("source_event_version", ""),
            source_detector_class=detector_lineage.get("source_detector_class", ""),
            source_evidence_mode=detector_lineage.get("source_evidence_mode", ""),
            source_threshold_version=detector_lineage.get("source_threshold_version", ""),
            source_calibration_artifact=detector_lineage.get("source_calibration_artifact", ""),
            source_discovery_mode=str(
                _lineage_value(
                    key="source_discovery_mode",
                    metadata=metadata if isinstance(metadata, Mapping) else {},
                    promoted_row=promoted_row,
                )
            ).strip(),
            source_cell_id=str(
                _lineage_value(
                    key="source_cell_id",
                    metadata=metadata if isinstance(metadata, Mapping) else {},
                    promoted_row=promoted_row,
                )
            ).strip(),
            source_scoreboard_run_id=str(
                _lineage_value(
                    key="source_scoreboard_run_id",
                    metadata=metadata if isinstance(metadata, Mapping) else {},
                    promoted_row=promoted_row,
                )
            ).strip(),
            source_event_atom=str(
                _lineage_value(
                    key="source_event_atom",
                    metadata=metadata if isinstance(metadata, Mapping) else {},
                    promoted_row=promoted_row,
                )
            ).strip(),
            source_context_cell=str(
                _lineage_value(
                    key="source_context_cell",
                    metadata=metadata if isinstance(metadata, Mapping) else {},
                    promoted_row=promoted_row,
                )
            ).strip(),
            source_contrast_lift_bps=_finite_or_none(
                _lineage_value(
                    key="source_contrast_lift_bps",
                    metadata=metadata if isinstance(metadata, Mapping) else {},
                    promoted_row=promoted_row,
                )
            ),
        ),
        governance=ThesisGovernance(
            readiness_status=str(promoted_row.get("readiness_status", "")),
            inventory_reason_code=str(promoted_row.get("inventory_reason_code", "")),
        ),
        requirements=_build_requirements_from_contract(
            bundle=bundle,
            promoted_row=promoted_row,
            metadata=metadata if isinstance(metadata, Mapping) else {},
            authored_def=authored_def,
            event_contract_ids=event_contract_ids,
            episode_contract_ids=episode_contract_ids,
        ),
        source=_build_source_from_contract(
            promoted_row=promoted_row,
            metadata=metadata if isinstance(metadata, Mapping) else {},
            event_contract_ids=event_contract_ids,
            episode_contract_ids=episode_contract_ids,
        ),
    )
    overlap_group_id = overlap_group_id_for_thesis(thesis)
    thesis = thesis.model_copy(
        update={
            "governance": _build_governance(
                bundle,
                promoted_row,
                overlap_group_id=overlap_group_id,
                authored_def=authored_def,
                primary_event_id=thesis.primary_event_id,
            )
        }
    )
    return thesis


def build_promoted_theses(
    *,
    run_id: str,
    bundles: Sequence[Mapping[str, Any]],
    promoted_df: pd.DataFrame | None = None,
    blueprints: Sequence[Mapping[str, Any]] | None = None,
    validation_metadata: Mapping[str, Mapping[str, Any]] | None = None,
    require_validation_lineage: bool = True,
) -> list[PromotedThesis]:
    promoted_frame = promoted_df.copy() if promoted_df is not None else pd.DataFrame()
    promoted_rows = _row_by_candidate_id(promoted_frame)
    promoted_ids = _promoted_candidate_ids(promoted_frame)
    blueprint_rows = _blueprint_by_candidate_id(blueprints or [])
    theses: list[PromotedThesis] = []
    failures: list[str] = []
    for bundle in bundles:
        candidate_id = str(bundle.get("candidate_id", "")).strip()
        if promoted_ids and candidate_id not in promoted_ids:
            continue
        promoted_row = promoted_rows.get(candidate_id, {})
        try:
            thesis = _build_thesis(
                run_id=run_id,
                bundle=bundle,
                promoted_row=promoted_row,
                blueprint=blueprint_rows.get(candidate_id),
                validation_metadata=validation_metadata,
                require_validation_lineage=require_validation_lineage,
            )
            theses.append(thesis)
        except DataIntegrityError as exc:
            failures.append(f"Candidate {candidate_id}: {exc}")
    if failures:
        raise DataIntegrityError(
            f"Failed to build {len(failures)} promoted theses: " + "; ".join(failures)
        )
    theses.sort(key=lambda item: item.thesis_id)
    return theses


def _write_thesis_payload(
    *,
    run_id: str,
    theses: Sequence[PromotedThesis],
    output_path: Path,
) -> None:
    ensure_dir(output_path.parent)
    payload = {
        "schema_version": "promoted_theses_v1",
        "run_id": run_id,
        "generated_at_utc": _utc_now(),
        "thesis_count": len(theses),
        "active_thesis_count": sum(1 for thesis in theses if thesis.status == "active"),
        "pending_thesis_count": sum(1 for thesis in theses if thesis.status == "pending_blueprint"),
        "theses": [thesis.model_dump() for thesis in theses],
    }
    atomic_write_json(output_path, payload, validator=_validate_exported_thesis_payload)


def _deployment_state_counts(theses: Sequence[PromotedThesis]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for thesis in theses:
        token = str(thesis.deployment_state or "").strip().lower()
        if not token:
            continue
        counts[token] = counts.get(token, 0) + 1
    return counts


def _apply_deployment_state_overrides(
    theses: Sequence[PromotedThesis],
    overrides: Mapping[str, str] | None,
) -> list[PromotedThesis]:
    if not overrides:
        return list(theses)
    selector_to_thesis_id: dict[str, str] = {}
    for thesis in theses:
        thesis_id = str(thesis.thesis_id or "").strip()
        candidate_id = str(thesis.lineage.candidate_id or "").strip()
        if thesis_id:
            selector_to_thesis_id[thesis_id] = thesis_id
        if candidate_id:
            selector_to_thesis_id.setdefault(candidate_id, thesis_id)

    updated: dict[str, PromotedThesis] = {thesis.thesis_id: thesis for thesis in theses}
    for selector, deployment_state in overrides.items():
        clean_selector = str(selector or "").strip()
        state_token = str(deployment_state or "").strip().lower()
        if not clean_selector:
            raise ValueError("Deployment-state override selector must not be empty.")
        if state_token not in ALL_DEPLOYMENT_STATES:
            raise ValueError(
                f"Unsupported deployment state override {deployment_state!r}. "
                f"Allowed values: {sorted(ALL_DEPLOYMENT_STATES)}"
            )
        thesis_id = selector_to_thesis_id.get(clean_selector)
        if not thesis_id:
            raise ValueError(
                f"Deployment-state override selector {clean_selector!r} did not match any exported thesis."
            )
        updated[thesis_id] = updated[thesis_id].model_copy(update={"deployment_state": state_token})
    return [updated[thesis.thesis_id] for thesis in theses]


def _update_thesis_index(
    *,
    run_id: str,
    output_path: Path,
    index_path: Path,
    theses: Sequence[PromotedThesis],
    register_runtime_name: str | None = None,
) -> None:
    ensure_dir(index_path.parent)
    index = _json_load(index_path)
    runs = index.get("runs", {})
    if not isinstance(runs, dict):
        runs = {}
    runs[run_id] = {
        "output_path": str(output_path),
        "thesis_count": len(theses),
        "active_thesis_count": sum(1 for thesis in theses if thesis.status == "active"),
        "pending_thesis_count": sum(1 for thesis in theses if thesis.status == "pending_blueprint"),
        "updated_at_utc": _utc_now(),
    }
    runtime_registrations = index.get("runtime_registrations", {})
    if not isinstance(runtime_registrations, dict):
        runtime_registrations = {}
    registration_name = str(register_runtime_name or "").strip()
    if registration_name:
        runtime_registrations[registration_name] = {
            "run_id": run_id,
            "output_path": str(output_path),
            "registered_at_utc": _utc_now(),
            "thesis_count": len(theses),
            "deployment_state_counts": _deployment_state_counts(theses),
        }
    payload = {
        "schema_version": "promoted_thesis_index_v1",
        "latest_run_id": run_id,
        "default_resolution_disabled": True,
        "runs": runs,
    }
    if runtime_registrations:
        payload["runtime_registrations"] = runtime_registrations
    atomic_write_json(index_path, payload, validator=_validate_thesis_index_payload)


def export_promoted_theses_for_run(
    run_id: str,
    *,
    data_root: Path | None = None,
    bundles: Sequence[Mapping[str, Any]] | None = None,
    promoted_df: pd.DataFrame | None = None,
    blueprints: Sequence[Mapping[str, Any]] | None = None,
    deployment_state_overrides: Mapping[str, str] | None = None,
    register_runtime_name: str | None = None,
    allow_bundle_only_export: bool = False,
    compatibility_mode: bool = False,
) -> PromotedThesisExportResult:
    resolved_root = Path(data_root) if data_root is not None else get_data_root()
    effective_bundles = (
        list(bundles) if bundles is not None else _load_evidence_bundles(run_id, resolved_root)
    )
    effective_promoted = (
        promoted_df.copy()
        if promoted_df is not None
        else _load_promoted_candidates(run_id, resolved_root)
    )
    effective_blueprints = (
        list(blueprints) if blueprints is not None else _load_blueprints(run_id, resolved_root)
    )

    validation_metadata: dict[str, dict[str, Any]] = {}
    from project.research.validation.result_writer import load_validation_bundle

    val_bundle = None
    bundle_only_export_allowed = bool(allow_bundle_only_export)
    if effective_promoted.empty and not bundle_only_export_allowed:
        val_bundle = load_validation_bundle(
            run_id,
            resolved_root / "reports" / "validation",
            strict=False,
            compatibility_mode=compatibility_mode,
        )
        if val_bundle is not None and not list(getattr(val_bundle, "validated_candidates", []) or []):
            bundle_only_export_allowed = True

    if effective_promoted.empty and not bundle_only_export_allowed:
        raise DataIntegrityError(
            f"Promoted candidates DataFrame is empty for run {run_id}. "
            "Set allow_bundle_only_export=True to proceed with bundle-only export."
        )
    skip_validation_lineage = (
        bundle_only_export_allowed and effective_promoted.empty and not effective_bundles
    )
    if val_bundle is None and not skip_validation_lineage:
        val_bundle = load_validation_bundle(
            run_id,
            resolved_root / "reports" / "validation",
            strict=not compatibility_mode,
            compatibility_mode=compatibility_mode,
        )
    _validate_promotion_evidence_alignment(
        run_id=run_id,
        bundles=effective_bundles,
        promoted_df=effective_promoted,
    )

    if not skip_validation_lineage:
        if val_bundle:
            all_candidates = (
                val_bundle.validated_candidates
                + val_bundle.rejected_candidates
                + val_bundle.inconclusive_candidates
            )
            for c in all_candidates:
                validation_metadata[c.candidate_id] = {
                    "validation_run_id": val_bundle.run_id,
                    "validation_status": c.decision.status,
                    "validation_reason_codes": list(c.decision.reason_codes),
                    "validation_artifact_paths": {a.artifact_type: a.path for a in c.artifact_refs},
                }
        elif not compatibility_mode:
            raise MissingArtifactError(
                f"Canonical export for run {run_id} requires validation lineage metadata."
            )

    theses = build_promoted_theses(
        run_id=run_id,
        bundles=effective_bundles,
        promoted_df=effective_promoted,
        blueprints=effective_blueprints,
        validation_metadata=validation_metadata,
        require_validation_lineage=not compatibility_mode and not skip_validation_lineage,
    )
    theses = _apply_deployment_state_overrides(theses, deployment_state_overrides)
    contract_json_path, contract_md_path = _write_contract_artifacts(
        run_id=run_id,
        theses=theses,
        data_root=resolved_root,
    )
    output_path = promoted_theses_path(run_id, resolved_root)
    index_path = live_thesis_index_path(resolved_root)
    _write_thesis_payload(run_id=run_id, theses=theses, output_path=output_path)
    _update_thesis_index(
        run_id=run_id,
        output_path=output_path,
        index_path=index_path,
        theses=theses,
        register_runtime_name=register_runtime_name,
    )
    from project.live.thesis_store import ThesisStore

    ThesisStore.from_path(output_path, strict_live_gate=True)
    return PromotedThesisExportResult(
        run_id=run_id,
        output_path=output_path,
        index_path=index_path,
        thesis_count=len(theses),
        active_count=sum(1 for thesis in theses if thesis.status == "active"),
        pending_count=sum(1 for thesis in theses if thesis.status == "pending_blueprint"),
        contract_json_path=contract_json_path,
        contract_md_path=contract_md_path,
    )
