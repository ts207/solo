from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml

from project import PROJECT_ROOT
from project.events.canonical_registry_sidecars import (
    ALLOWED_DISPOSITION_VALUES,
    ALLOWED_EVIDENCE_MODE_VALUES,
    ALLOWED_LAYER_VALUES,
)

CORE_KEYS = {"event_type", "reports_dir", "events_file", "signal_column", "parameters"}
META_KEYS = {
    "active",
    "status",
    "description",
    "provenance",
    "deprecated",
    "kind",
    "version",
}
SECTION_KEYS = {
    "identity",
    "governance",
    "runtime",
    "semantics",
    "interaction",
    "routing",
}
_MISSING = object()
_ALLOWED_LAYERS = set(ALLOWED_LAYER_VALUES)
_ALLOWED_DISPOSITIONS = set(ALLOWED_DISPOSITION_VALUES)
_ALLOWED_EVIDENCE_MODES = set(ALLOWED_EVIDENCE_MODE_VALUES)


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _load_detector_ownership(repo_root: Path) -> Dict[str, str]:
    payload = _load_yaml(repo_root / "project" / "configs" / "registries" / "detectors.yaml")
    raw = payload.get("detector_ownership", {}) if isinstance(payload, dict) else {}
    if not isinstance(raw, dict):
        return {}
    return {
        str(event_type).strip().upper(): str(detector_name).strip()
        for event_type, detector_name in raw.items()
        if str(event_type).strip() and str(detector_name).strip()
    }


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _present(mapping: Dict[str, Any], key: str) -> Any:
    return mapping[key] if key in mapping else _MISSING


def _first_value(*values: Any, default: Any = None) -> Any:
    for value in values:
        if value is not _MISSING:
            return value
    return default


def _first_text(*values: Any, default: str = "") -> str:
    for value in values:
        if value is _MISSING:
            continue
        text = str(value or "").strip()
        if text:
            return text
    return default


def _bool_value(*values: Any, default: bool = False) -> bool:
    value = _first_value(*values, default=default)
    return bool(value)


def _list_value(*values: Any) -> list[Any]:
    value = _first_value(*values, default=[])
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    return []


def _int_value(*values: Any, default: int = 0) -> int:
    value = _first_value(*values, default=default)
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return default


def _event_kind_from_row(identity: Dict[str, Any], governance: Dict[str, Any], ontology: Dict[str, Any]) -> str:
    explicit = _first_text(
        _present(governance, "event_kind"),
        _present(identity, "event_kind"),
        default="",
    )
    if explicit:
        return explicit

    layer = str(ontology.get("layer", "")).strip()
    if layer == "composite":
        return "composite_event"
    if layer == "context_tag":
        return "context_tag"
    if layer == "strategy_construct":
        return "strategy_construct"
    return "market_event"


def _infer_layer(identity: Dict[str, Any], governance: Dict[str, Any], payload: Dict[str, Any]) -> str:
    explicit = _first_text(
        _present(identity, "layer"),
        _present(governance, "layer"),
        payload.get("layer", _MISSING),
        default="",
    )
    if explicit:
        return explicit
    if _bool_value(
        _present(governance, "strategy_only"),
        _present(governance, "is_strategy_construct"),
        payload.get("strategy_only", _MISSING),
        payload.get("is_strategy_construct", _MISSING),
        default=False,
    ):
        return "strategy_construct"
    if _bool_value(
        _present(governance, "context_tag"),
        _present(governance, "is_context_tag"),
        payload.get("is_context_tag", _MISSING),
        default=False,
    ):
        return "context_tag"
    if _bool_value(
        _present(governance, "is_composite"),
        payload.get("is_composite", _MISSING),
        default=False,
    ):
        return "composite"
    return "canonical"


def _default_disposition(layer: str) -> str:
    if layer in {"context_tag", "strategy_construct"}:
        return "demote"
    return "keep"


def _normalized_ontology_row(
    event_type: str,
    payload: Dict[str, Any],
    *,
    identity: Dict[str, Any],
    governance: Dict[str, Any],
    semantics: Dict[str, Any],
) -> Dict[str, Any]:
    layer = _infer_layer(identity, governance, payload)
    is_composite = _bool_value(
        _present(governance, "is_composite"),
        payload.get("is_composite", _MISSING),
        default=layer == "composite",
    )
    is_context_tag = _bool_value(
        _present(governance, "context_tag"),
        _present(governance, "is_context_tag"),
        payload.get("is_context_tag", _MISSING),
        default=layer == "context_tag",
    )
    is_strategy_construct = _bool_value(
        _present(governance, "strategy_only"),
        _present(governance, "is_strategy_construct"),
        payload.get("strategy_only", _MISSING),
        payload.get("is_strategy_construct", _MISSING),
        default=layer == "strategy_construct",
    )
    return {
        "event_type": event_type,
        "canonical_regime": _first_text(_present(identity, "canonical_regime")).upper(),
        "subtype": _first_text(_present(identity, "subtype")),
        "phase": _first_text(_present(identity, "phase")),
        "evidence_mode": _first_text(_present(identity, "evidence_mode")),
        "layer": layer,
        "disposition": _first_text(
            _present(identity, "disposition"),
            _present(governance, "disposition"),
            default=_default_disposition(layer),
        ),
        "asset_scope": _first_text(
            _present(identity, "asset_scope"),
            default="single_asset",
        ),
        "venue_scope": _first_text(
            _present(identity, "venue_scope"),
            default="single_venue",
        ),
        "deconflict_priority": _int_value(
            _present(semantics, "deconflict_priority"),
            default=0,
        ),
        "research_only": _bool_value(
            _present(governance, "research_only"),
            default=False,
        ),
        "strategy_only": _bool_value(
            _present(governance, "strategy_only"),
            default=False,
        ),
        "notes": _first_text(
            _present(semantics, "notes"),
            payload.get("description", _MISSING),
        ),
        "is_composite": is_composite,
        "is_context_tag": is_context_tag,
        "is_strategy_construct": is_strategy_construct,
    }


def _validate_mapping_rows(rows: Dict[str, Dict[str, Any]]) -> list[str]:
    issues: list[str] = []
    for event_type, row in rows.items():
        if not row["canonical_regime"]:
            issues.append(f"{event_type}: missing canonical_regime")
        if not row["subtype"]:
            issues.append(f"{event_type}: missing subtype")
        if not row["phase"]:
            issues.append(f"{event_type}: missing phase")
        if row["layer"] not in _ALLOWED_LAYERS:
            issues.append(f"{event_type}: invalid layer={row['layer']}")
        if row["disposition"] not in _ALLOWED_DISPOSITIONS:
            issues.append(f"{event_type}: invalid disposition={row['disposition']}")
        if row["evidence_mode"] not in _ALLOWED_EVIDENCE_MODES:
            issues.append(f"{event_type}: invalid evidence_mode={row['evidence_mode']}")
        layer_flags = [
            row["is_composite"],
            row["is_context_tag"],
            row["is_strategy_construct"],
        ]
        if sum(1 for flag in layer_flags if flag) > 1:
            issues.append(f"{event_type}: multiple ontology layer flags enabled")
        if row["strategy_only"] and not row["is_strategy_construct"]:
            issues.append(f"{event_type}: strategy_only requires strategy_construct layer")
    return issues


def _canonical_regime_fanout(rows: Dict[str, Dict[str, Any]]) -> Dict[str, tuple[str, ...]]:
    groups: Dict[str, list[str]] = {}
    for event_type, row in rows.items():
        regime = str(row.get("canonical_regime", "")).strip().upper()
        if not regime:
            continue
        groups.setdefault(regime, []).append(event_type)
    return {regime: tuple(sorted(event_types)) for regime, event_types in sorted(groups.items())}


def build_unified_registry(repo_root: Path) -> Dict[str, Any]:
    spec_root = repo_root / "spec"
    events_root = spec_root / "events"
    detector_ownership = _load_detector_ownership(repo_root)

    event_defaults = _load_yaml(events_root / "_defaults.yaml")
    event_family_defaults = _load_yaml(events_root / "_families.yaml")
    template_registry = _load_yaml(spec_root / "templates" / "registry.yaml")

    # Build event rows from per-event specs first.
    event_rows: Dict[str, Dict[str, Any]] = {}
    ontology_rows: Dict[str, Dict[str, Any]] = {}
    for spec_path in sorted(events_root.glob("*.yaml")):
        if spec_path.name.startswith("_") or spec_path.name in {
            "canonical_event_registry.yaml",
            "event_ontology_mapping.yaml",
            "event_contract_overrides.yaml",
            "event_registry_unified.yaml",
        }:
            continue
        payload = _load_yaml(spec_path)
        if not payload:
            continue
        if bool(payload.get("deprecated", False)) or not bool(payload.get("active", True)):
            continue
        if payload.get("kind") in {
            "canonical_event_registry",
            "event_config_defaults",
            "event_family_defaults",
            "event_unified_registry",
        }:
            continue

        event_type = str(payload.get("event_type", "")).strip().upper()
        if not event_type:
            continue

        identity = _mapping(payload.get("identity"))
        governance = _mapping(payload.get("governance"))
        runtime = _mapping(payload.get("runtime"))
        semantics = _mapping(payload.get("semantics"))
        interaction = _mapping(payload.get("interaction"))
        routing = _mapping(payload.get("routing"))

        params = payload.get("parameters", {})
        if not isinstance(params, dict):
            params = {}
        ontology = _normalized_ontology_row(
            event_type,
            payload,
            identity=identity,
            governance=governance,
            semantics=semantics,
        )
        ontology_rows[event_type] = ontology

        legacy_top_level = {
            str(k): v
            for k, v in payload.items()
            if k not in CORE_KEYS and k not in META_KEYS and k not in SECTION_KEYS
        }
        merged_event_params = dict(legacy_top_level)
        merged_event_params.update(params)

        is_composite = _bool_value(
            _present(governance, "is_composite"),
            ontology.get("is_composite", _MISSING),
            default=False,
        )
        is_context_tag = _bool_value(
            _present(governance, "context_tag"),
            _present(governance, "is_context_tag"),
            ontology.get("is_context_tag", _MISSING),
            default=False,
        )
        is_strategy_construct = _bool_value(
            _present(governance, "strategy_only"),
            _present(governance, "is_strategy_construct"),
            ontology.get("is_strategy_construct", _MISSING),
            default=False,
        )
        default_executable = _bool_value(
            _present(governance, "default_executable"),
            default=not (is_composite or is_context_tag or is_strategy_construct),
        )
        trade_runtime = payload.get("trade_runtime", {})
        if not isinstance(trade_runtime, dict):
            trade_runtime = {}

        event_rows[event_type] = {
            "research_family": _first_text(
                payload.get("research_family", _MISSING),
                payload.get("canonical_family", _MISSING),
                default=ontology["canonical_regime"],
            ),
            "canonical_family": _first_text(
                payload.get("research_family", _MISSING),
                payload.get("canonical_family", _MISSING),
                default=ontology["canonical_regime"],
            ),
            "canonical_regime": _first_text(
                _present(identity, "canonical_regime"),
                ontology["canonical_regime"],
            ),
            "event_kind": _event_kind_from_row(identity, governance, ontology),
            "subtype": _first_text(_present(identity, "subtype"), ontology["subtype"]),
            "phase": _first_text(_present(identity, "phase"), ontology["phase"]),
            "evidence_mode": _first_text(_present(identity, "evidence_mode"), ontology["evidence_mode"]),
            "layer": ontology["layer"],
            "disposition": ontology["disposition"],
            "asset_scope": ontology["asset_scope"],
            "venue_scope": ontology["venue_scope"],
            "is_composite": is_composite,
            "is_context_tag": is_context_tag,
            "is_strategy_construct": is_strategy_construct,
            "research_only": _bool_value(
                _present(governance, "research_only"),
                ontology["research_only"],
                default=False,
            ),
            "strategy_only": _bool_value(
                _present(governance, "strategy_only"),
                ontology["strategy_only"],
                default=False,
            ),
            "default_executable": default_executable,
            "deconflict_priority": _int_value(
                _present(semantics, "deconflict_priority"),
                ontology["deconflict_priority"],
                default=0,
            ),
            "notes": _first_text(_present(semantics, "notes"), ontology["notes"]),
            "maturity": _first_text(
                _present(governance, "maturity"),
                default="",
            ),
            "tier": _first_text(
                _present(governance, "tier"),
                default="",
            ),
            "operational_role": _first_text(
                _present(governance, "operational_role"),
                default="",
            ),
            "deployment_disposition": _first_text(
                _present(governance, "deployment_disposition"),
                default="",
            ),
            "runtime_category": _first_text(
                _present(governance, "runtime_category"),
                default="active_runtime_event",
            ),
            "detector_band": _first_text(
                _present(governance, "detector_band"),
                default="",
            ),
            "planning_eligible": _bool_value(
                _present(governance, "planning_eligible"),
                _present(governance, "research_eligible"),
                default=False,
            ),
            "runtime_eligible": _bool_value(
                _present(trade_runtime, "eligible"),
                _present(governance, "runtime_trade_eligible"),
                default=False,
            ),
            "promotion_eligible": _bool_value(
                _present(governance, "promotion_eligible"),
                default=False,
            ),
            "primary_anchor_eligible": _bool_value(
                _present(governance, "primary_anchor_eligible"),
                default=False,
            ),
            "enabled": _bool_value(
                _present(runtime, "enabled"),
                default=True,
            ),
            "detector_name": _first_text(
                _present(runtime, "detector"),
                _present(runtime, "detector_name"),
                _present(runtime, "detector_class"),
                detector_ownership.get(event_type, _MISSING),
                default="",
            ),
            "runtime_tags": _list_value(
                _present(runtime, "runtime_tags"),
                _present(runtime, "tags"),
            ),
            "instrument_classes": _list_value(
                _present(runtime, "instrument_classes"),
            ),
            "requires_features": _list_value(
                _present(runtime, "requires_features"),
            ),
            "sequence_eligible": _bool_value(
                _present(runtime, "sequence_eligible"),
                default=True,
            ),
            "reports_dir": _first_text(_present(runtime, "reports_dir"), payload["reports_dir"]),
            "events_file": _first_text(_present(runtime, "events_file"), payload["events_file"]),
            "signal_column": _first_text(_present(runtime, "signal_column"), payload["signal_column"]),
            "routing_profile_ref": _first_text(
                _present(routing, "routing_profile_ref"),
                default="",
            ),
            "cluster_id": _first_text(
                _present(semantics, "cluster_id"),
                default="",
            ),
            "collapse_target": _first_text(
                _present(semantics, "collapse_target"),
                default="",
            ),
            "overlap_group": _first_text(
                _present(semantics, "overlap_group"),
                default="",
            ),
            "precedence_rank": _int_value(
                _present(semantics, "precedence_rank"),
                default=0,
            ),
            "suppresses": _list_value(
                _present(interaction, "suppresses"),
            ),
            "suppressed_by": _list_value(
                _present(interaction, "suppressed_by"),
            ),
            "parameters": merged_event_params,
        }

    ontology_issues = _validate_mapping_rows(ontology_rows)
    if ontology_issues:
        raise ValueError(
            "Invalid event ontology mapping:\n" + "\n".join(f"- {issue}" for issue in ontology_issues)
        )

    template_defaults = template_registry.get("defaults", {})
    if not isinstance(template_defaults, dict):
        template_defaults = {}
    template_families = template_registry.get("families", {})
    if not isinstance(template_families, dict):
        template_families = {}
    template_events = template_registry.get("events", {})
    if not isinstance(template_events, dict):
        template_events = {}

    for event_type, row in template_events.items():
        token = str(event_type).strip().upper()
        if not token or not isinstance(row, dict):
            continue
        if not bool(row.get("active", True)) or bool(row.get("deprecated", False)):
            if token in event_rows:
                del event_rows[token]
            continue
        base = event_rows.setdefault(
            token,
            {
                "research_family": "",
                "canonical_family": "",
                "canonical_regime": "",
                "subtype": "",
                "phase": "",
                "evidence_mode": "",
                "layer": "",
                "disposition": "",
                "asset_scope": "",
                "venue_scope": "",
                "is_composite": False,
                "is_context_tag": False,
                "is_strategy_construct": False,
                "research_only": False,
                "strategy_only": False,
                "default_executable": True,
                "deconflict_priority": 0,
                "notes": "",
                "maturity": "",
                "tier": "",
                "operational_role": "",
                "deployment_disposition": "",
                "runtime_category": "active_runtime_event",
                "enabled": True,
                "detector_name": "",
                "runtime_tags": [],
                "instrument_classes": [],
                "requires_features": [],
                "sequence_eligible": True,
                "routing_profile_ref": "",
                "cluster_id": "",
                "collapse_target": "",
                "overlap_group": "",
                "precedence_rank": 0,
                "suppresses": [],
                "suppressed_by": [],
                "reports_dir": "",
                "events_file": "",
                "signal_column": "",
                "parameters": {},
            },
        )
        ontology = dict(ontology_rows.get(token, {}))
        if not ontology:
            if token in event_rows:
                raise ValueError(f"Active event_type {token} missing from ontology mapping")
            continue
        research_family = _first_text(
            row.get("research_family", _MISSING),
            row.get("canonical_family", _MISSING),
            base.get("research_family", _MISSING),
            base.get("canonical_family", _MISSING),
            default=ontology["canonical_regime"],
        )
        base["research_family"] = research_family
        base["canonical_family"] = research_family
        base["canonical_regime"] = ontology["canonical_regime"]
        for key in (
            "subtype",
            "phase",
            "evidence_mode",
            "layer",
            "disposition",
            "asset_scope",
            "venue_scope",
            "is_composite",
            "is_context_tag",
            "is_strategy_construct",
            "research_only",
            "strategy_only",
            "deconflict_priority",
            "notes",
        ):
            base[key] = ontology[key]
        for key in (
            "templates",
            "horizons",
            "conditioning_cols",
            "max_candidates_per_run",
            "state_overrides",
        ):
            if key in row:
                base[key] = row.get(key)
        if "parameters" in row and isinstance(row["parameters"], dict):
            base.setdefault("parameters", {}).update(row["parameters"])
        if "synthetic_coverage" in row:
            base.setdefault("parameters", {})["synthetic_coverage"] = row["synthetic_coverage"]

    family_rows: Dict[str, Dict[str, Any]] = {}
    family_default_rows = event_family_defaults.get("families", {})
    if not isinstance(family_default_rows, dict):
        family_default_rows = {}

    all_families = set()
    all_families.update(str(k).strip().upper() for k in family_default_rows.keys())
    all_families.update(str(k).strip().upper() for k in template_families.keys())
    all_families.update(str(row.get("research_family", "")).strip().upper() for row in event_rows.values())
    all_families.discard("")

    for family in sorted(all_families):
        legacy_row = family_default_rows.get(family, {})
        params = {}
        if isinstance(legacy_row, dict):
            raw = legacy_row.get("parameters", {})
            if isinstance(raw, dict):
                params = dict(raw)

        out: Dict[str, Any] = {"parameters": params}
        template_row = template_families.get(family, {})
        if isinstance(template_row, dict):
            for key in (
                "templates",
                "horizons",
                "conditioning_cols",
                "max_candidates_per_run",
            ):
                if key in template_row:
                    out[key] = template_row.get(key)
        family_rows[family] = out

    defaults = {
        "parameters": dict(
            event_defaults.get("parameters", {})
            if isinstance(event_defaults.get("parameters", {}), dict)
            else {}
        ),
        "templates": template_defaults.get("templates", []),
        "horizons": template_defaults.get("horizons", []),
        "conditioning_cols": template_defaults.get("conditioning_cols", []),
        "max_candidates_per_run": template_defaults.get("max_candidates_per_run", 1000),
    }

    return {
        "version": 1,
        "kind": "event_unified_registry",
        "metadata": {
            "status": "authoritative",
            "legacy_sources": {
                "event_defaults": "spec/events/_defaults.yaml",
                "event_family_defaults": "spec/events/_families.yaml",
                "event_specs_dir": "spec/events",
                "template_registry": "spec/templates/registry.yaml",
            },
            "notes": (
                "Single event-centric schema for phase1+phase2 composition. "
                "Generated compatibility sidecars are downstream outputs only. "
                "research_family is the authoritative coarse search/template grouping; "
                "canonical_family is a generated compatibility alias of research_family; "
                "family-level defaults are applied directly to research_family buckets."
            ),
        },
        "defaults": defaults,
        "families": family_rows,
        "canonical_regimes": {
            regime: {
                "event_types": list(event_types),
                "default_executable_event_types": [
                    event_type
                    for event_type in event_types
                    if not event_rows.get(event_type, {}).get("is_composite", False)
                    and not event_rows.get(event_type, {}).get("is_context_tag", False)
                    and not event_rows.get(event_type, {}).get("is_strategy_construct", False)
                ],
            }
            for regime, event_types in _canonical_regime_fanout(event_rows).items()
        },
        "events": {k: event_rows[k] for k in sorted(event_rows)},
    }


def main() -> int:
    repo_root = PROJECT_ROOT.parent
    unified = build_unified_registry(repo_root)
    out_path = repo_root / "spec" / "events" / "event_registry_unified.yaml"
    out_path.write_text(yaml.safe_dump(unified, sort_keys=False), encoding="utf-8")
    print(f"Wrote {out_path} with {len(unified.get('events', {}))} events")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
