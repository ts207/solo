from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from project.events.detector_contract import (
    DetectorLogicContract,
    NormalizedDetectorMetadata,
    detector_metadata_from_class,
)
from project.events.detectors.base import BaseEventDetector
from project.events.detectors.catalog import load_detector_family_modules

_DETECTORS: dict[str, type[Any]] = {}
_METADATA_ADAPTERS: dict[tuple[str, type[Any], object], type[Any]] = {}


def register_detector(event_type: str, detector_cls: type[Any]) -> None:
    _DETECTORS[event_type.upper()] = detector_cls


def _walk_subclasses(root: type[Any]) -> Iterable[type[Any]]:
    for subclass in root.__subclasses__():
        yield subclass
        yield from _walk_subclasses(subclass)


def _candidate_detector_classes(event_type: str) -> list[type[Any]]:
    token = str(event_type).strip().upper()
    candidates: list[type[Any]] = []
    seen: set[type[Any]] = set()
    for root in (BaseEventDetector, DetectorLogicContract):
        for detector_cls in _walk_subclasses(root):
            if detector_cls in seen:
                continue
            seen.add(detector_cls)
            declared = str(
                getattr(detector_cls, "event_name", "")
                or getattr(detector_cls, "event_type", "")
            ).strip().upper()
            if declared == token:
                candidates.append(detector_cls)
    return candidates


def get_detector_class(event_type: str) -> type[Any] | None:
    load_all_detectors()
    token = str(event_type).strip().upper()
    candidates = _candidate_detector_classes(token)
    if candidates:
        return sorted(
            candidates,
            key=lambda cls: (
                str(getattr(cls, "event_version", "v1")).strip().lower() != "v2",
                not str(cls.__name__).endswith("V2"),
                str(cls.__name__),
            ),
        )[0]
    return _DETECTORS.get(token)


def get_detector(event_type: str) -> Any | None:
    cls = get_detector_class(event_type)
    return cls() if cls else None


def _normalize_columns(values: object) -> tuple[str, ...]:
    if isinstance(values, (list, tuple)):
        return tuple(str(item).strip() for item in values if str(item).strip())
    return ()


def _coerce_bool(value: object, default: bool) -> bool:
    return default if value is None else bool(value)


def _freeze_cache_value(value: object) -> object:
    if isinstance(value, dict):
        return tuple(
            sorted((str(key), _freeze_cache_value(item)) for key, item in value.items())
        )
    if isinstance(value, (list, tuple, set)):
        return tuple(_freeze_cache_value(item) for item in value)
    return value


def _resolve_role(row: dict[str, Any], default: str) -> str:
    role = str(row.get("operational_role") or row.get("role") or default).strip().lower()
    return {
        "sequence_component": "composite",
        "filter": "context",
    }.get(role, role)


def _resolve_maturity(row: dict[str, Any], default: str) -> str:
    explicit = str(row.get("maturity") or "").strip().lower()
    if explicit:
        return explicit
    tier = str(row.get("tier", "")).strip().upper()
    if str(row.get("deprecated", False)).strip().lower() == "true":
        return "deprecated"
    if tier == "A":
        return "production"
    if tier == "B":
        return "specialized"
    return default or "standard"


def _resolve_detector_band(
    row: dict[str, Any],
    *,
    event_name: str,
    role: str,
    default: str,
) -> str:
    raw = str(row.get("detector_band") or "").strip().lower()
    if raw in {"deployable_core", "research_trigger", "context_only", "composite_or_fragile"}:
        return raw
    if default and default != "research_trigger":
        return default
    if role == "context" or bool(row.get("context_only")) or bool(row.get("is_context_tag")):
        return "context_only"
    if (
        role in {"composite", "research_only", "sequence_component"}
        or bool(row.get("composite"))
        or bool(row.get("is_composite"))
        or event_name.startswith("SEQ_")
        or "PROXY" in event_name
    ):
        return "composite_or_fragile"
    return "research_trigger"


def get_detector_metadata_adapter_class(
    event_type: str, governance_row: dict[str, Any] | None = None
) -> type[Any] | None:
    detector_cls = get_detector_class(event_type)
    if detector_cls is None:
        return None
    if not governance_row:
        return detector_cls

    token = str(event_type).strip().upper()
    cache_key = (token, detector_cls, _freeze_cache_value(governance_row))
    if cache_key in _METADATA_ADAPTERS:
        return _METADATA_ADAPTERS[cache_key]

    class_metadata = (
        detector_cls.detector_metadata(event_name=token)
        if hasattr(detector_cls, "detector_metadata")
        else detector_metadata_from_class(detector_cls, event_name=token)
    )
    row_detector = (
        governance_row.get("detector", {})
        if isinstance(governance_row.get("detector"), dict)
        else {}
    )
    role = _resolve_role(governance_row, class_metadata.role)
    adapted_required_columns = (
        _normalize_columns(governance_row.get("required_columns") or row_detector.get("required_columns"))
        or class_metadata.required_columns
        or ("timestamp",)
    )
    attrs = {
        "event_name": token,
        "event_type": token,
        "event_version": str(
            governance_row.get("version")
            or governance_row.get("event_version")
            or class_metadata.event_version
            or "v1"
        ).strip().lower()
        or "v1",
        "required_columns": list(adapted_required_columns),
        "supports_confidence": _coerce_bool(
            governance_row.get("supports_confidence"), class_metadata.supports_confidence
        ),
        "supports_severity": _coerce_bool(
            governance_row.get("supports_severity"), class_metadata.supports_severity
        ),
        "supports_quality_flag": _coerce_bool(
            governance_row.get("supports_quality_flag", governance_row.get("emits_quality_flag")),
            class_metadata.supports_quality_flag,
        ),
        "cooldown_semantics": str(
            governance_row.get("cooldown_semantics") or class_metadata.cooldown_semantics or "none"
        ).strip()
        or "none",
        "merge_key_strategy": str(
            governance_row.get("merge_key_strategy") or class_metadata.merge_key_strategy or "none"
        ).strip()
        or "none",
        "role": role,
        "evidence_mode": str(
            governance_row.get("evidence_mode") or class_metadata.evidence_mode or "direct"
        ).strip().lower()
        or "direct",
        "maturity": _resolve_maturity(governance_row, class_metadata.maturity),
        "detector_band": _resolve_detector_band(
            governance_row,
            event_name=token,
            role=role,
            default=class_metadata.detector_band,
        ),
        "planning_default": _coerce_bool(
            governance_row.get("planning_default", governance_row.get("planning_eligible")),
            class_metadata.planning_default,
        ),
        "promotion_eligible": _coerce_bool(
            governance_row.get("promotion_eligible"), class_metadata.promotion_eligible
        ),
        "runtime_default": _coerce_bool(
            governance_row.get("runtime_default", governance_row.get("runtime_eligible")),
            class_metadata.runtime_default,
        ),
        "primary_anchor_eligible": _coerce_bool(
            governance_row.get("primary_anchor_eligible"),
            class_metadata.primary_anchor_eligible,
        ),
    }
    adapter_name = f"{detector_cls.__name__}MetadataAdapter"
    adapter_cls = type(adapter_name, (detector_cls,), attrs)
    adapter_cls.__module__ = detector_cls.__module__
    _METADATA_ADAPTERS[cache_key] = adapter_cls
    return adapter_cls


def get_detector_metadata(
    event_type: str, governance_row: dict[str, Any] | None = None
) -> tuple[type[Any] | None, NormalizedDetectorMetadata | None]:
    detector_cls = get_detector_metadata_adapter_class(event_type, governance_row)
    if detector_cls is None:
        return None, None
    metadata = (
        detector_cls.detector_metadata(event_name=str(event_type).strip().upper())
        if hasattr(detector_cls, "detector_metadata")
        else detector_metadata_from_class(detector_cls, event_name=str(event_type).strip().upper())
    )
    return detector_cls, metadata


def list_registered_event_types() -> list[str]:
    load_all_detectors()
    return sorted(_DETECTORS.keys())


def load_all_detectors() -> None:
    """Import detector family modules from the explicit catalog."""
    load_detector_family_modules()


# --- Auto-registration helpers ---
def register_family_detectors(detectors: dict[str, type[Any]]) -> None:
    for et, cls in detectors.items():
        register_detector(et, cls)
