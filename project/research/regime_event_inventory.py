from __future__ import annotations

import importlib
import json
import pkgutil
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_EVENT_REGISTRY_PATH = REPO_ROOT / "spec" / "events" / "event_registry_unified.yaml"
DEFAULT_STATE_REGISTRY_PATH = REPO_ROOT / "spec" / "states" / "state_registry.yaml"
DEFAULT_CONTEXT_REGISTRY_PATH = REPO_ROOT / "spec" / "contexts" / "context_dimension_registry.yaml"
DEFAULT_TEMPLATE_REGISTRY_PATH = REPO_ROOT / "spec" / "templates" / "registry.yaml"
DEFAULT_MECHANISM_REGISTRY_PATH = REPO_ROOT / "spec" / "mechanisms" / "registry.yaml"
DEFAULT_REPORT_DIR = REPO_ROOT / "data" / "reports" / "regime_event_inventory"

EVENT_INVENTORY_COLUMNS = [
    "id",
    "kind",
    "family",
    "registered_unified",
    "enabled",
    "default_executable",
    "python_detector_registered",
    "runtime_eligible",
    "planning_eligible",
    "promotion_eligible",
    "research_only",
    "strategy_only",
    "materialized_event_file_known",
    "features_required",
    "known_data_risk",
    "active_candidate_event",
    "conditional_registered_event",
    "draft_event",
    "active_invalid_event_count",
    "conditional_maybe_not_materialized_event_count",
    "tested_count",
    "surviving_candidate_count",
    "parked_count",
    "killed_count",
    "last_decision",
    "classification",
    "recommended_action",
]

CLASSIFICATIONS = {
    "registered_executable",
    "registered_research_only",
    "registered_not_runtime_executable",
    "registered_maybe_not_materialized",
    "invalid_unregistered",
    "baseline_first",
    "eligible_for_event_lift_test",
    "eligible_for_funding_squeeze",
    "historical_negative",
    "paused_mechanism",
    "draft_mechanism",
    "data_quality_blocked",
    "materialization_unknown",
}


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a YAML object")
    return payload


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple | set):
        return list(value)
    return [value]


def _as_str_list(value: Any) -> list[str]:
    return [str(item).strip() for item in _as_list(value) if str(item).strip()]


def _event_token(value: Any) -> str:
    return str(value or "").strip().upper()


def _context_token(value: Any) -> str:
    return str(value or "").strip().lower()


def _load_registered_detector_ids() -> set[str]:
    import project.events.registries as registries_pkg
    from project.events.detectors.registry import list_registered_event_types

    for module_info in pkgutil.iter_modules(registries_pkg.__path__):
        if module_info.name.startswith("_"):
            continue
        module = importlib.import_module(f"{registries_pkg.__name__}.{module_info.name}")
        for attr_name in dir(module):
            if attr_name.startswith("ensure_") and attr_name.endswith("_registered"):
                getattr(module, attr_name)()
    return set(list_registered_event_types())


def load_authoritative_event_registry(
    path: Path = DEFAULT_EVENT_REGISTRY_PATH,
) -> dict[str, dict[str, Any]]:
    payload = _load_yaml(path)
    events = payload.get("events") or {}
    if not isinstance(events, dict):
        raise ValueError("event_registry_unified.yaml events must be an object")
    return {_event_token(event_id): dict(raw or {}) for event_id, raw in events.items()}


def load_template_ids(path: Path = DEFAULT_TEMPLATE_REGISTRY_PATH) -> set[str]:
    payload = _load_yaml(path)
    ids: set[str] = set()
    for section in ("expression_templates", "filter_templates", "operators"):
        raw = payload.get(section) or {}
        if isinstance(raw, dict | list):
            ids.update(str(item).strip() for item in raw if str(item).strip())
    families = payload.get("families") or {}
    if isinstance(families, dict):
        for raw_family in families.values():
            if isinstance(raw_family, dict):
                ids.update(_as_str_list(raw_family.get("templates")))
    ids.update(_as_str_list((payload.get("defaults") or {}).get("templates")))
    return ids


@dataclass(frozen=True)
class ContextCanonicalization:
    raw_dimension: str
    raw_value: str
    dimension: str
    value: str

    @property
    def changed(self) -> bool:
        return self.raw_dimension != self.dimension or self.raw_value != self.value

    @property
    def raw_label(self) -> str:
        return f"{self.raw_dimension}={self.raw_value}"

    @property
    def canonical_label(self) -> str:
        return f"{self.dimension}={self.value}"


@dataclass(frozen=True)
class ContextRegistry:
    values_by_dimension: dict[str, set[str]]
    materializable_dimensions: set[str]

    def canonicalize(self, dimension: str, value: str) -> ContextCanonicalization:
        raw_dimension = str(dimension or "").strip()
        raw_value = str(value or "").strip()
        return ContextCanonicalization(
            raw_dimension=raw_dimension,
            raw_value=raw_value,
            dimension=_context_token(raw_dimension),
            value=_context_token(raw_value),
        )

    def has_dimension(self, dimension: str) -> bool:
        return _context_token(dimension) in self.values_by_dimension

    def allowed_values(self, dimension: str) -> set[str]:
        return self.values_by_dimension.get(_context_token(dimension), set())

    def is_value_allowed(self, dimension: str, value: str) -> bool:
        return _context_token(value) in self.allowed_values(dimension)

    def is_materializable(self, dimension: str) -> bool:
        return _context_token(dimension) in self.materializable_dimensions


def load_context_registry(
    *,
    state_registry_path: Path = DEFAULT_STATE_REGISTRY_PATH,
    context_registry_path: Path = DEFAULT_CONTEXT_REGISTRY_PATH,
) -> ContextRegistry:
    values: dict[str, set[str]] = defaultdict(set)
    materializable: set[str] = set()

    state_payload = _load_yaml(state_registry_path)
    for dimension, raw in (state_payload.get("context_dimensions") or {}).items():
        key = _context_token(dimension)
        if not key or not isinstance(raw, dict):
            continue
        values[key].update(_context_token(item) for item in _as_str_list(raw.get("allowed_values")))
        materializable.add(key)

    context_payload = _load_yaml(context_registry_path)
    for dimension, raw in (context_payload.get("dimensions") or {}).items():
        key = _context_token(dimension)
        if not key or not isinstance(raw, dict):
            continue
        raw_values = raw.get("values") or {}
        if isinstance(raw_values, dict):
            values[key].update(_context_token(item) for item in raw_values)
        else:
            values[key].update(_context_token(item) for item in _as_str_list(raw_values))

    return ContextRegistry(
        values_by_dimension={key: set(vals) for key, vals in values.items()},
        materializable_dimensions=materializable,
    )


def _empty_inventory_row(item_id: str, kind: str) -> dict[str, Any]:
    return {
        "id": item_id,
        "kind": kind,
        "family": "",
        "registered_unified": False,
        "enabled": False,
        "default_executable": False,
        "python_detector_registered": False,
        "runtime_eligible": False,
        "planning_eligible": False,
        "promotion_eligible": False,
        "research_only": False,
        "strategy_only": False,
        "materialized_event_file_known": False,
        "features_required": [],
        "known_data_risk": "",
        "active_candidate_event": False,
        "conditional_registered_event": False,
        "draft_event": False,
        "active_invalid_event_count": 0,
        "conditional_maybe_not_materialized_event_count": 0,
        "tested_count": 0,
        "surviving_candidate_count": 0,
        "parked_count": 0,
        "killed_count": 0,
        "last_decision": "",
        "classification": "",
        "recommended_action": "",
    }


def _features_required(raw: dict[str, Any]) -> list[str]:
    params = raw.get("parameters") if isinstance(raw.get("parameters"), dict) else {}
    detector = params.get("detector") if isinstance(params.get("detector"), dict) else {}
    values = (
        _as_str_list(raw.get("features_required"))
        or _as_str_list(raw.get("requires_features"))
        or _as_str_list(raw.get("required_columns"))
        or _as_str_list(detector.get("required_columns"))
    )
    return sorted({item for item in values if item != "timestamp"})


def _known_data_risk(raw: dict[str, Any]) -> str:
    params = raw.get("parameters") if isinstance(raw.get("parameters"), dict) else {}
    coverage = str(params.get("synthetic_coverage") or "").strip().lower()
    if coverage in {"synthetic", "missing", "uncovered"}:
        return coverage
    if bool(raw.get("strategy_only")):
        return "strategy_only"
    return ""


def _materialized_file_known(raw: dict[str, Any]) -> bool:
    return bool(str(raw.get("events_file") or "").strip() and str(raw.get("signal_column") or "").strip())


def classify_event_row(
    event_id: str,
    raw: dict[str, Any] | None,
    *,
    python_detector_registered: bool,
) -> tuple[str, str]:
    if raw is None:
        return "invalid_unregistered", "replace_with_registered_event"
    if _known_data_risk(raw) in {"missing", "synthetic", "uncovered"}:
        return "data_quality_blocked", "repair_or_replace_observable_before_research"
    if bool(raw.get("research_only")):
        return "registered_research_only", "eligible_for_research_only_preflight"
    if not _materialized_file_known(raw):
        return "materialization_unknown", "verify_detector_or_materialization_before_active_proposal"
    if bool(raw.get("default_executable")) and bool(raw.get("enabled", True)):
        if event_id == "FUNDING_EXTREME_ONSET":
            return "registered_executable", "eligible_for_baseline_or_event_lift"
        return "registered_executable", "eligible_for_baseline_or_event_lift"
    params = raw.get("parameters") if isinstance(raw.get("parameters"), dict) else {}
    if raw.get("collapse_target") or params.get("source_event_type"):
        return (
            "registered_maybe_not_materialized",
            "verify_detector_or_materialization_before_active_proposal",
        )
    if python_detector_registered:
        return "registered_not_runtime_executable", "verify_runtime_eligibility_before_proposal"
    return "registered_not_runtime_executable", "verify_detector_or_materialization_before_active_proposal"


def _load_ledger_rows(root: Path) -> list[dict[str, Any]]:
    path = root / "data" / "reports" / "search_ledger" / "search_burden.json"
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    rows = payload.get("rows") if isinstance(payload, dict) else []
    return [dict(row) for row in rows if isinstance(row, dict)]


def _ledger_stats_by_event(root: Path) -> dict[str, dict[str, Any]]:
    counts: dict[str, Counter[str]] = defaultdict(Counter)
    last_decision: dict[str, str] = {}
    for row in _load_ledger_rows(root):
        event_id = _event_token(row.get("event_id"))
        if not event_id:
            continue
        counts[event_id]["tested"] += 1
        evidence_class = str(row.get("evidence_class") or "").strip().lower()
        decision = str(row.get("decision") or "").strip().lower()
        if evidence_class == "parked_candidate" or decision == "park":
            counts[event_id]["parked"] += 1
        if evidence_class == "killed_candidate" or decision in {"kill", "reject"}:
            counts[event_id]["killed"] += 1
        if bool(row.get("active_research_candidate")) or evidence_class in {
            "candidate_signal",
            "surviving_candidate",
        }:
            counts[event_id]["surviving"] += 1
        if decision:
            last_decision[event_id] = decision
    return {
        event_id: {
            "tested_count": int(counter["tested"]),
            "surviving_candidate_count": int(counter["surviving"]),
            "parked_count": int(counter["parked"]),
            "killed_count": int(counter["killed"]),
            "last_decision": last_decision.get(event_id, ""),
        }
        for event_id, counter in counts.items()
    }


def _mechanism_event_roles(root: Path) -> dict[str, dict[str, set[str]]]:
    roles: dict[str, dict[str, set[str]]] = defaultdict(
        lambda: {
            "active_candidate_event": set(),
            "conditional_registered_event": set(),
            "draft_event": set(),
        }
    )
    for path in (root / "spec" / "mechanisms").glob("*.yaml"):
        if path.name == "registry.yaml":
            continue
        try:
            payload = _load_yaml(path)
        except (OSError, ValueError, yaml.YAMLError):
            continue
        mechanism_id = str(payload.get("mechanism_id") or path.stem).strip()
        for event_id in _as_str_list(payload.get("candidate_events")):
            roles[_event_token(event_id)]["active_candidate_event"].add(mechanism_id)
        for event_id in _as_str_list(payload.get("conditional_registered_events")):
            roles[_event_token(event_id)]["conditional_registered_event"].add(mechanism_id)
        for event_id in _as_str_list(payload.get("draft_events")):
            roles[_event_token(event_id)]["draft_event"].add(mechanism_id)
    return roles


def build_event_inventory(root: Path = REPO_ROOT) -> list[dict[str, Any]]:
    events = load_authoritative_event_registry(root / "spec" / "events" / "event_registry_unified.yaml")
    detector_ids = _load_registered_detector_ids()
    ledger_stats = _ledger_stats_by_event(root)
    event_roles = _mechanism_event_roles(root)

    ids = set(events)
    for path in (root / "spec" / "mechanisms").glob("*.yaml"):
        if path.name == "registry.yaml":
            continue
        try:
            payload = _load_yaml(path)
        except (OSError, ValueError, yaml.YAMLError):
            continue
        ids.update(_event_token(item) for item in _as_str_list(payload.get("candidate_events")))
        ids.update(_event_token(item) for item in _as_str_list(payload.get("conditional_registered_events")))
        ids.update(_event_token(item) for item in _as_str_list(payload.get("draft_events")))
    ids.update(ledger_stats)

    rows: list[dict[str, Any]] = []
    for event_id in sorted(item for item in ids if item):
        raw = events.get(event_id)
        row = _empty_inventory_row(event_id, "event")
        roles = event_roles.get(event_id, {})
        row.update(
            {
                "active_candidate_event": bool(roles.get("active_candidate_event")),
                "conditional_registered_event": bool(roles.get("conditional_registered_event")),
                "draft_event": bool(roles.get("draft_event")),
            }
        )
        row.update(ledger_stats.get(event_id, {}))
        if raw is not None:
            row.update(
                {
                    "family": str(
                        raw.get("canonical_regime")
                        or raw.get("research_family")
                        or raw.get("canonical_family")
                        or ""
                    ),
                    "registered_unified": True,
                    "enabled": bool(raw.get("enabled", True)),
                    "default_executable": bool(raw.get("default_executable", False)),
                    "python_detector_registered": event_id in detector_ids,
                    "runtime_eligible": bool(raw.get("runtime_eligible", False)),
                    "planning_eligible": bool(raw.get("planning_eligible", False)),
                    "promotion_eligible": bool(raw.get("promotion_eligible", False)),
                    "research_only": bool(raw.get("research_only", False)),
                    "strategy_only": bool(raw.get("strategy_only", False)),
                    "materialized_event_file_known": _materialized_file_known(raw),
                    "features_required": _features_required(raw),
                    "known_data_risk": _known_data_risk(raw),
                }
            )
        classification, action = classify_event_row(
            event_id,
            raw,
            python_detector_registered=event_id in detector_ids,
        )
        row["classification"] = classification
        row["recommended_action"] = action
        rows.append(row)
    return rows


def build_context_dimension_inventory(root: Path = REPO_ROOT) -> list[dict[str, Any]]:
    registry = load_context_registry(
        state_registry_path=root / "spec" / "states" / "state_registry.yaml",
        context_registry_path=root / "spec" / "contexts" / "context_dimension_registry.yaml",
    )
    rows: list[dict[str, Any]] = []
    for dimension in sorted(registry.values_by_dimension):
        row = _empty_inventory_row(dimension, "context_dimension")
        row.update(
            {
                "enabled": True,
                "materialized_event_file_known": registry.is_materializable(dimension),
                "features_required": sorted(registry.allowed_values(dimension)),
                "classification": "baseline_first",
                "recommended_action": "eligible_for_regime_baseline_if_ex_ante",
            }
        )
        rows.append(row)
    return rows


def build_state_inventory(root: Path = REPO_ROOT) -> list[dict[str, Any]]:
    payload = _load_yaml(root / "spec" / "states" / "state_registry.yaml")
    rows: list[dict[str, Any]] = []
    for raw in payload.get("states") or []:
        if not isinstance(raw, dict):
            continue
        state_id = str(raw.get("state_id") or "").strip()
        if not state_id:
            continue
        row = _empty_inventory_row(state_id, "state")
        row.update(
            {
                "family": str(raw.get("family") or ""),
                "enabled": True,
                "features_required": _as_str_list(raw.get("features_required")),
                "classification": "baseline_first",
                "recommended_action": "eligible_for_regime_baseline_if_ex_ante",
            }
        )
        rows.append(row)
    return rows


def build_mechanism_inventory(root: Path = REPO_ROOT) -> list[dict[str, Any]]:
    registry = _load_yaml(root / "spec" / "mechanisms" / "registry.yaml")
    events = load_authoritative_event_registry(root / "spec" / "events" / "event_registry_unified.yaml")
    rows: list[dict[str, Any]] = []
    for mechanism_id, raw_entry in (registry.get("mechanisms") or {}).items():
        if not isinstance(raw_entry, dict):
            continue
        path = root / str(raw_entry.get("path") or "")
        payload = _load_yaml(path) if path.exists() else {}
        status = str(raw_entry.get("status") or payload.get("status") or "").strip().lower()
        candidate_events = [_event_token(item) for item in _as_str_list(payload.get("candidate_events"))]
        conditional_events = [
            _event_token(item)
            for item in _as_str_list(payload.get("conditional_registered_events"))
        ]
        active_invalid_count = sum(1 for event_id in candidate_events if event_id not in events)
        conditional_maybe_count = 0
        for event_id in conditional_events:
            event_row = events.get(event_id)
            if event_row is None:
                continue
            classification, _action = classify_event_row(
                event_id,
                event_row,
                python_detector_registered=False,
            )
            if classification == "registered_maybe_not_materialized":
                conditional_maybe_count += 1
        row = _empty_inventory_row(str(mechanism_id), "mechanism")
        row.update(
            {
                "enabled": status == "active",
                "features_required": _as_str_list(
                    (payload.get("observable_pressure") or {}).get("required")
                    if isinstance(payload.get("observable_pressure"), dict)
                    else []
                ),
                "classification": "eligible_for_event_lift_test"
                if status == "active"
                else "paused_mechanism"
                if status in {"paused", "pause"}
                else "draft_mechanism",
                "active_invalid_event_count": active_invalid_count,
                "conditional_maybe_not_materialized_event_count": conditional_maybe_count,
                "recommended_action": "baseline_and_event_lift_before_proposal"
                if str(mechanism_id) == "funding_squeeze" and status == "active"
                else "require_registry_baseline_lift_before_proposal"
                if status == "active"
                else "do_not_compile_active_proposals",
            }
        )
        rows.append(row)
    return rows


def build_regime_event_inventory(root: Path = REPO_ROOT) -> pd.DataFrame:
    rows = (
        build_context_dimension_inventory(root)
        + build_state_inventory(root)
        + build_event_inventory(root)
        + build_mechanism_inventory(root)
    )
    df = pd.DataFrame(rows, columns=EVENT_INVENTORY_COLUMNS)
    return df.sort_values(["kind", "id"]).reset_index(drop=True)


def _json_records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{column: row.get(column) for column in EVENT_INVENTORY_COLUMNS} for row in rows]


def write_inventory_outputs(
    *,
    root: Path = REPO_ROOT,
    output_dir: Path | None = None,
) -> pd.DataFrame:
    out_dir = output_dir or root / "data" / "reports" / "regime_event_inventory"
    out_dir.mkdir(parents=True, exist_ok=True)

    sections = {
        "context_dimensions": build_context_dimension_inventory(root),
        "state_inventory": build_state_inventory(root),
        "event_inventory": build_event_inventory(root),
        "mechanism_inventory": build_mechanism_inventory(root),
    }
    for name, rows in sections.items():
        payload = {
            "schema_version": "regime_event_inventory_v1",
            "row_count": len(rows),
            "rows": _json_records(rows),
        }
        (out_dir / f"{name}.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    df = pd.DataFrame(
        [row for rows in sections.values() for row in rows],
        columns=EVENT_INVENTORY_COLUMNS,
    ).sort_values(["kind", "id"])
    df.to_parquet(out_dir / "regime_event_inventory.parquet", index=False)
    return df.reset_index(drop=True)
