from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from project.domain.compiled_registry import get_domain_registry
from project.research.cell_discovery.models import (
    ContextCell,
    ContrastRule,
    DiscoveryRegistry,
    EventAtom,
    HorizonSet,
    RankingPolicy,
)
from project.research.context_labels import canonicalize_context_label
from project.spec_registry.loaders import load_yaml_relative

REQUIRED_SPEC_FILES = (
    "event_atoms.yaml",
    "context_cells.yaml",
    "horizons.yaml",
    "contrast_rules.yaml",
    "ranking_policy.yaml",
)


def _read_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Discovery spec must be a mapping: {path}")
    return payload


def _tuple_of_str(values: Any, *, field: str) -> tuple[str, ...]:
    if isinstance(values, str):
        raw = [values]
    else:
        raw = list(values or [])
    out = tuple(str(item).strip() for item in raw if str(item).strip())
    if not out:
        raise ValueError(f"{field} must contain at least one value")
    return out


def _validate_unique(ids: list[str], *, label: str) -> None:
    duplicates = sorted({item for item in ids if ids.count(item) > 1})
    if duplicates:
        raise ValueError(f"Duplicate {label} ids: {', '.join(duplicates)}")


def _authoritative_context_dimensions() -> dict[str, dict[str, Any]]:
    payload = load_yaml_relative("project/configs/registries/contexts.yaml")
    raw = payload.get("context_dimensions", {}) if isinstance(payload, dict) else {}
    return dict(raw) if isinstance(raw, dict) else {}


def load_registry(spec_dir: str | Path = "spec/discovery") -> DiscoveryRegistry:
    base = Path(spec_dir)
    for filename in REQUIRED_SPEC_FILES:
        if not (base / filename).exists():
            raise FileNotFoundError(f"Missing discovery spec: {base / filename}")

    events_doc = _read_yaml(base / "event_atoms.yaml")
    contexts_doc = _read_yaml(base / "context_cells.yaml")
    horizons_doc = _read_yaml(base / "horizons.yaml")
    contrast_doc = _read_yaml(base / "contrast_rules.yaml")
    ranking_doc = _read_yaml(base / "ranking_policy.yaml")

    registry = get_domain_registry()
    authoritative_contexts = _authoritative_context_dimensions()
    event_atoms: list[EventAtom] = []
    for item in list(events_doc.get("event_atoms", []) or []):
        if not isinstance(item, dict):
            raise ValueError("event_atoms entries must be mappings")
        atom = EventAtom(
            atom_id=str(item.get("id", "")).strip(),
            event_family=str(item.get("event_family", "")).strip(),
            event_type=str(item.get("event_type", "")).strip().upper(),
            directions=_tuple_of_str(item.get("directions", []), field="event directions"),
            templates=_tuple_of_str(item.get("templates", []), field="event templates"),
            horizons=_tuple_of_str(item.get("horizons", []), field="event horizons"),
            required_feature_keys=tuple(
                str(value).strip()
                for value in item.get("required_feature_keys", [])
                if str(value).strip()
            ),
            thesis_eligible=bool(item.get("thesis_eligible", True)),
            research_only=bool(item.get("research_only", False)),
            search_role=str(item.get("search_role", "")).strip(),
            promotion_role=str(item.get("promotion_role", "")).strip(),
            runtime_role=str(item.get("runtime_role", "")).strip(),
        )
        if not atom.atom_id:
            raise ValueError("event atom id is required")
        if not registry.has_event(atom.event_type):
            raise ValueError(
                f"Unknown event_type in discovery atom {atom.atom_id}: {atom.event_type}"
            )
        
        event_row = registry.event_row(atom.event_type) or {}
        
        # Validation rules:
        if atom.thesis_eligible or atom.search_role == "primary_trigger":
            if not event_row.get("promotion_eligible", False):
                raise ValueError(f"Atom {atom.atom_id} is thesis_eligible but generated governance has promotion_eligible=False")
        
        detector_band = event_row.get("detector_band", "")
        if detector_band == "context_only":
            if atom.thesis_eligible or atom.search_role == "primary_trigger":
                raise ValueError(f"Atom {atom.atom_id} has detector_band=context_only but is primary thesis trigger")
        
        if detector_band == "composite_or_fragile":
            if (atom.thesis_eligible or atom.search_role == "primary_trigger") and atom.promotion_role != "requires_composite":
                raise ValueError(f"Atom {atom.atom_id} has detector_band=composite_or_fragile but is directly thesis-eligible without promotion_role=requires_composite")
        
        if not event_row.get("runtime_eligible", False):
            if atom.runtime_role == "trade_trigger":
                raise ValueError(f"Atom {atom.atom_id} claims runtime_role=trade_trigger but generated runtime_eligible=False")

        event_atoms.append(atom)
    _validate_unique([item.atom_id for item in event_atoms], label="event atom")

    context_cells: list[ContextCell] = []
    for item in list(contexts_doc.get("context_cells", []) or []):
        if not isinstance(item, dict):
            raise ValueError("context_cells entries must be mappings")
        executability = str(item.get("executability_class", "research_only")).strip()
        if executability not in {"runtime", "research_only", "supportive_only"}:
            raise ValueError(f"Unsupported context executability_class: {executability}")
        cell = ContextCell(
            cell_id=str(item.get("id", "")).strip(),
            dimension=str(item.get("dimension", "")).strip(),
            values=_tuple_of_str(item.get("values", []), field="context values"),
            required_feature_key=str(item.get("required_feature_key", "")).strip(),
            executability_class=executability,  # type: ignore[arg-type]
            max_conjunction_depth=int(item.get("max_conjunction_depth", 1)),
            supportive_context=dict(item.get("supportive_context", {}) or {}),
        )
        if not cell.cell_id:
            raise ValueError("context cell id is required")
        if cell.max_conjunction_depth > 1:
            raise ValueError("edge-cell v1 only supports max_conjunction_depth <= 1")
        allowed_meta = authoritative_contexts.get(cell.dimension)
        if not isinstance(allowed_meta, dict):
            raise ValueError(f"Unknown context dimension in discovery cell {cell.cell_id}: {cell.dimension}")
        allowed_values = {
            str(item).strip()
            for item in list(allowed_meta.get("allowed_values", []) or [])
            if str(item).strip()
        }
        invalid_values = [
            value
            for value in cell.values
            if canonicalize_context_label(cell.dimension, value) not in allowed_values
        ]
        if invalid_values:
            raise ValueError(
                f"Context cell {cell.cell_id} has invalid values for {cell.dimension}: "
                + ", ".join(invalid_values)
            )
        context_cells.append(cell)
    _validate_unique([item.cell_id for item in context_cells], label="context cell")

    horizons = HorizonSet(
        horizons=_tuple_of_str(horizons_doc.get("horizons", []), field="horizons")
    )
    contrast_rules: list[ContrastRule] = []
    for item in list(contrast_doc.get("contrast_rules", []) or []):
        if not isinstance(item, dict):
            raise ValueError("contrast_rules entries must be mappings")
        rule_type = str(item.get("type", "")).strip()
        if rule_type not in {"in_bucket_vs_unconditional"}:
            raise ValueError(f"Unsupported contrast rule type: {rule_type}")
        rule = ContrastRule(
            rule_id=str(item.get("id", "")).strip(),
            rule_type=rule_type,
            required=bool(item.get("required", True)),
            min_lift_bps=(
                float(item["min_lift_bps"]) if item.get("min_lift_bps") is not None else None
            ),
        )
        if not rule.rule_id:
            raise ValueError("contrast rule id is required")
        contrast_rules.append(rule)
    _validate_unique([item.rule_id for item in contrast_rules], label="contrast rule")
    if not contrast_rules:
        raise ValueError("at least one contrast rule is required")

    ranking_payload = dict(ranking_doc.get("ranking_policy", {}) or {})
    policy = RankingPolicy(
        min_support=int(ranking_payload.get("min_support", 30)),
        min_forward_valid_folds=int(ranking_payload.get("min_forward_valid_folds", 3)),
        min_forward_support=int(ranking_payload.get("min_forward_support", 30)),
        min_forward_support_fraction=float(
            ranking_payload.get("min_forward_support_fraction", 0.10)
        ),
        min_forward_net_mean_bps=float(ranking_payload.get("min_forward_net_mean_bps", 0.0)),
        min_contrast_lift_bps=float(ranking_payload.get("min_contrast_lift_bps", 5.0)),
        max_search_hypotheses=int(ranking_payload.get("max_search_hypotheses", 1000)),
        forward_weight=float(ranking_payload.get("forward_weight", 0.40)),
        expectancy_weight=float(ranking_payload.get("expectancy_weight", 0.25)),
        stability_weight=float(ranking_payload.get("stability_weight", 0.15)),
        contrast_weight=float(ranking_payload.get("contrast_weight", 0.15)),
        simplicity_weight=float(ranking_payload.get("simplicity_weight", 0.05)),
    )
    if policy.min_support < 1:
        raise ValueError("ranking_policy.min_support must be >= 1")
    if policy.min_forward_valid_folds < 1:
        raise ValueError("ranking_policy.min_forward_valid_folds must be >= 1")
    if policy.min_forward_support < 1:
        raise ValueError("ranking_policy.min_forward_support must be >= 1")
    if policy.min_forward_support_fraction < 0:
        raise ValueError("ranking_policy.min_forward_support_fraction must be >= 0")
    return DiscoveryRegistry(
        event_atoms=tuple(event_atoms),
        context_cells=tuple(context_cells),
        horizons=horizons,
        ranking_policy=policy,
        contrast_rules=tuple(contrast_rules),
        spec_version=str(events_doc.get("version", "edge_cells_v1")),
    )
