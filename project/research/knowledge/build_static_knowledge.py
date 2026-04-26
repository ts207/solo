from __future__ import annotations

import argparse
import json
import os
from collections.abc import Iterable
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pandas as pd

from project.core.config import get_data_root
from project.domain.registry_loader import compile_domain_registry_from_sources
from project.io.utils import write_parquet
from project.research.knowledge.knobs import build_agent_knob_rows
from project.research.knowledge.schemas import (
    KNOB_COLUMNS,
    STATIC_DOCUMENT_COLUMNS,
    STATIC_ENTITY_COLUMNS,
    STATIC_RELATION_COLUMNS,
    canonical_json,
    entity_id,
    relation_id,
)
from project.research.semantic_registry_views import build_canonical_semantic_registry_views
from project.spec_registry import load_yaml_path


def _registry_path(registry_root: Path, name: str) -> Path:
    return registry_root / f"{name}.yaml"


def _document_row(
    *,
    entity_type: str,
    name: str,
    title: str,
    content: str,
    source_path: str,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    eid = entity_id(entity_type, name)
    return {
        "document_id": f"doc::{eid}",
        "entity_id": eid,
        "entity_type": entity_type,
        "title": title,
        "content": content,
        "source_path": source_path,
        "metadata_json": canonical_json(metadata),
    }


def _append_relation(
    rows: list[dict[str, Any]],
    *,
    from_type: str,
    from_name: str,
    relation_type: str,
    to_type: str,
    to_name: str,
    source_path: str,
    attributes: dict[str, Any] | None = None,
) -> None:
    from_id = entity_id(from_type, from_name)
    to_id = entity_id(to_type, to_name)
    rows.append(
        {
            "relation_id": relation_id(from_id, relation_type, to_id),
            "from_entity_id": from_id,
            "relation_type": relation_type,
            "to_entity_id": to_id,
            "source_path": source_path,
            "attributes_json": canonical_json(attributes or {}),
        }
    )


def _to_df(rows: list[dict[str, Any]], columns: list[str]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows).reindex(columns=columns)


@contextmanager
def _canonical_parquet_write_mode() -> Iterable[None]:
    original = os.environ.get("BACKTEST_FORCE_CSV_FALLBACK")
    os.environ["BACKTEST_FORCE_CSV_FALLBACK"] = "0"
    try:
        yield
    finally:
        if original is None:
            os.environ.pop("BACKTEST_FORCE_CSV_FALLBACK", None)
        else:
            os.environ["BACKTEST_FORCE_CSV_FALLBACK"] = original


def _write_knowledge_frame(df: pd.DataFrame, path: Path) -> Path:
    with _canonical_parquet_write_mode():
        written_path, _ = write_parquet(df, path)
    return written_path


def build_static_knowledge(
    *,
    data_root: Path | None = None,
    registry_root: Path | None = None,
    domain_registry: Any | None = None,
) -> dict[str, Any]:
    resolved_data_root = Path(data_root) if data_root is not None else get_data_root()
    resolved_registry_root = (
        Path(registry_root) if registry_root is not None else (Path("project/configs/registries"))
    )
    if not resolved_registry_root.is_absolute():
        resolved_registry_root = (Path.cwd() / resolved_registry_root).resolve()
    domain = domain_registry or compile_domain_registry_from_sources()

    out_root = resolved_data_root / "knowledge" / "static"
    out_root.mkdir(parents=True, exist_ok=True)

    semantic_payloads = build_canonical_semantic_registry_views(domain_registry=domain)
    events_payload = semantic_payloads["events"]
    states_payload = semantic_payloads["states"]
    templates_payload = semantic_payloads["templates"]
    contexts_payload = load_yaml_path(_registry_path(resolved_registry_root, "contexts"))
    detectors_payload = load_yaml_path(_registry_path(resolved_registry_root, "detectors"))
    features_payload = load_yaml_path(_registry_path(resolved_registry_root, "features"))

    entities: list[dict[str, Any]] = []
    relations: list[dict[str, Any]] = []
    documents: list[dict[str, Any]] = []

    event_rows = events_payload.get("events", {}) if isinstance(events_payload, dict) else {}
    detector_ownership = (
        detectors_payload.get("detector_ownership", {})
        if isinstance(detectors_payload, dict)
        else {}
    )
    seen_families: set[str] = set()
    seen_trigger_types: set[str] = set()

    for event_type, row in sorted(event_rows.items()):
        if not isinstance(row, dict):
            continue
        family = str(row.get("family", "")).strip().upper()
        detector_name = str(row.get("detector") or detector_ownership.get(event_type, "")).strip()
        enabled = bool(row.get("enabled", True))
        source_path = str(row.get("source_path", "spec/events/*.yaml"))
        entities.append(
            {
                "entity_id": entity_id("event", event_type),
                "entity_type": "event",
                "name": str(event_type),
                "title": str(event_type),
                "family": family,
                "enabled": enabled,
                "source_path": source_path,
                "description": str(row.get("description", "")),
                "attributes_json": canonical_json(row),
            }
        )
        documents.append(
            _document_row(
                entity_type="event",
                name=str(event_type),
                title=f"Event {event_type}",
                content=f"Event {event_type} in family {family or 'UNKNOWN'} detected by {detector_name or 'UNKNOWN'}.",
                source_path=source_path,
                metadata=row,
            )
        )
        if family:
            seen_families.add(family)
            _append_relation(
                relations,
                from_type="event",
                from_name=str(event_type),
                relation_type="belongs_to_family",
                to_type="event_family",
                to_name=family,
                source_path=source_path,
            )
        if detector_name:
            _append_relation(
                relations,
                from_type="detector",
                from_name=detector_name,
                relation_type="detects",
                to_type="event",
                to_name=str(event_type),
                source_path=source_path,
            )
        for feature_name in row.get("requires_features", []) or []:
            _append_relation(
                relations,
                from_type="event",
                from_name=str(event_type),
                relation_type="requires_feature",
                to_type="feature",
                to_name=str(feature_name),
                source_path=source_path,
            )

    for state_id, row in sorted(
        (states_payload.get("states", {}) if isinstance(states_payload, dict) else {}).items()
    ):
        if not isinstance(row, dict):
            continue
        family = str(row.get("family", "")).strip().upper()
        source_path = str(row.get("source_path", f"spec/states/{state_id}.yaml"))
        entities.append(
            {
                "entity_id": entity_id("state", state_id),
                "entity_type": "state",
                "name": str(state_id),
                "title": str(state_id),
                "family": family,
                "enabled": bool(row.get("enabled", True)),
                "source_path": source_path,
                "description": str(row.get("description", "")),
                "attributes_json": canonical_json(row),
            }
        )
        documents.append(
            _document_row(
                entity_type="state",
                name=str(state_id),
                title=f"State {state_id}",
                content=str(
                    row.get("description", f"State {state_id} in family {family or 'UNKNOWN'}.")
                ),
                source_path=source_path,
                metadata=row,
            )
        )
        if family:
            seen_families.add(family)
            _append_relation(
                relations,
                from_type="state",
                from_name=str(state_id),
                relation_type="belongs_to_family",
                to_type="event_family",
                to_name=family,
                source_path=source_path,
            )

    for template_id, row in sorted(
        (
            templates_payload.get("templates", {}) if isinstance(templates_payload, dict) else {}
        ).items()
    ):
        if not isinstance(row, dict):
            continue
        source_path = str(row.get("source_path", "spec/templates/registry.yaml"))
        trigger_types = tuple(
            str(v).strip().upper()
            for v in row.get("supports_trigger_types", []) or []
            if str(v).strip()
        )
        entities.append(
            {
                "entity_id": entity_id("template", template_id),
                "entity_type": "template",
                "name": str(template_id),
                "title": str(template_id),
                "family": "",
                "enabled": bool(row.get("enabled", True)),
                "source_path": source_path,
                "description": "",
                "attributes_json": canonical_json(row),
            }
        )
        documents.append(
            _document_row(
                entity_type="template",
                name=str(template_id),
                title=f"Template {template_id}",
                content=f"Template {template_id} supports trigger types: {', '.join(trigger_types) or 'none'}.",
                source_path=source_path,
                metadata=row,
            )
        )
        for trigger_type in trigger_types:
            seen_trigger_types.add(trigger_type)
            _append_relation(
                relations,
                from_type="template",
                from_name=str(template_id),
                relation_type="supports_trigger_type",
                to_type="trigger_type",
                to_name=trigger_type,
                source_path=source_path,
            )

    for detector_name in sorted(
        {
            *(str(v).strip() for v in detector_ownership.values()),
            *(
                str(row.get("detector", "")).strip()
                for row in event_rows.values()
                if isinstance(row, dict)
            ),
        }
    ):
        if not detector_name:
            continue
        source_path = str(_registry_path(resolved_registry_root, "detectors"))
        entities.append(
            {
                "entity_id": entity_id("detector", detector_name),
                "entity_type": "detector",
                "name": detector_name,
                "title": detector_name,
                "family": "",
                "enabled": True,
                "source_path": source_path,
                "description": "",
                "attributes_json": canonical_json({"detector": detector_name}),
            }
        )

    for family_name in sorted(seen_families):
        entities.append(
            {
                "entity_id": entity_id("event_family", family_name),
                "entity_type": "event_family",
                "name": family_name,
                "title": family_name,
                "family": family_name,
                "enabled": True,
                "source_path": "spec/templates/registry.yaml",
                "description": "",
                "attributes_json": canonical_json({"family": family_name}),
            }
        )

    context_dimensions = (
        contexts_payload.get("context_dimensions", {}) if isinstance(contexts_payload, dict) else {}
    )
    for family_name, row in sorted(context_dimensions.items()):
        if not isinstance(row, dict):
            continue
        source_path = str(_registry_path(resolved_registry_root, "contexts"))
        entities.append(
            {
                "entity_id": entity_id("context_family", family_name),
                "entity_type": "context_family",
                "name": str(family_name),
                "title": str(family_name),
                "family": "",
                "enabled": True,
                "source_path": source_path,
                "description": "",
                "attributes_json": canonical_json(row),
            }
        )
        for label in row.get("allowed_values", []) or []:
            label_name = f"{family_name}={label}"
            entities.append(
                {
                    "entity_id": entity_id("context_label", label_name),
                    "entity_type": "context_label",
                    "name": label_name,
                    "title": label_name,
                    "family": str(family_name),
                    "enabled": True,
                    "source_path": source_path,
                    "description": "",
                    "attributes_json": canonical_json({"family": family_name, "label": label}),
                }
            )
            mapped_state = domain.resolve_context_state(str(family_name), str(label))
            if mapped_state:
                _append_relation(
                    relations,
                    from_type="context_label",
                    from_name=label_name,
                    relation_type="maps_to_state",
                    to_type="state",
                    to_name=mapped_state,
                    source_path=source_path,
                )

    for feature_name, row in sorted(
        (features_payload.get("features", {}) if isinstance(features_payload, dict) else {}).items()
    ):
        if not isinstance(row, dict):
            continue
        source_path = str(_registry_path(resolved_registry_root, "features"))
        entities.append(
            {
                "entity_id": entity_id("feature", feature_name),
                "entity_type": "feature",
                "name": str(feature_name),
                "title": str(feature_name),
                "family": str(row.get("type", "")).strip(),
                "enabled": True,
                "source_path": source_path,
                "description": str(row.get("description", "")),
                "attributes_json": canonical_json(row),
            }
        )

    for trigger_type in sorted(seen_trigger_types):
        entities.append(
            {
                "entity_id": entity_id("trigger_type", trigger_type),
                "entity_type": "trigger_type",
                "name": trigger_type,
                "title": trigger_type,
                "family": "",
                "enabled": True,
                "source_path": str(_registry_path(resolved_registry_root, "templates")),
                "description": "",
                "attributes_json": canonical_json({"trigger_type": trigger_type}),
            }
        )

    for event_type, spec in domain.event_definitions.items():
        event_meta = event_rows.get(event_type, {})
        family_name = str(
            event_meta.get("family", spec.research_family or spec.canonical_family)
        ).strip().upper()
        for template_id in domain.family_templates(family_name):
            _append_relation(
                relations,
                from_type="event",
                from_name=event_type,
                relation_type="compatible_with_template",
                to_type="template",
                to_name=template_id,
                source_path="spec/templates/registry.yaml",
            )

    entities_df = (
        _to_df(entities, STATIC_ENTITY_COLUMNS)
        .drop_duplicates(subset=["entity_id"])
        .reset_index(drop=True)
    )
    relations_df = (
        _to_df(relations, STATIC_RELATION_COLUMNS)
        .drop_duplicates(subset=["relation_id"])
        .reset_index(drop=True)
    )
    documents_df = (
        _to_df(documents, STATIC_DOCUMENT_COLUMNS)
        .drop_duplicates(subset=["document_id"])
        .reset_index(drop=True)
    )

    entities_path = out_root / "entities.parquet"
    relations_path = out_root / "relations.parquet"
    documents_path = out_root / "documents.parquet"
    knobs_path = out_root / "agent_knobs.parquet"
    index_path = out_root / "index.json"

    _write_knowledge_frame(entities_df, entities_path)
    _write_knowledge_frame(relations_df, relations_path)
    _write_knowledge_frame(documents_df, documents_path)
    _write_knowledge_frame(_to_df(build_agent_knob_rows(), KNOB_COLUMNS), knobs_path)

    index_payload = {
        "entity_count": len(entities_df),
        "relation_count": len(relations_df),
        "document_count": len(documents_df),
        "knob_count": len(build_agent_knob_rows()),
        "entity_types": sorted(entities_df["entity_type"].dropna().astype(str).unique().tolist()),
        "relation_types": sorted(
            relations_df["relation_type"].dropna().astype(str).unique().tolist()
        ),
        "sources": {
            "registry_root": str(resolved_registry_root),
            "semantic": {
                "events": "spec/events/*.yaml",
                "states": "spec/states/*.yaml",
                "templates": "spec/templates/registry.yaml",
            },
            "runtime_config": {
                "contexts": str(_registry_path(resolved_registry_root, "contexts")),
                "detectors": str(_registry_path(resolved_registry_root, "detectors")),
                "features": str(_registry_path(resolved_registry_root, "features")),
            },
        },
    }
    index_path.write_text(
        json.dumps(index_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return {
        "out_root": out_root,
        "entities_path": entities_path,
        "relations_path": relations_path,
        "documents_path": documents_path,
        "knobs_path": knobs_path,
        "index_path": index_path,
        "index": index_payload,
    }


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build static research knowledge artifacts.")
    parser.add_argument("--data_root", default=None)
    parser.add_argument("--registry_root", default="project/configs/registries")
    args = parser.parse_args(list(argv) if argv is not None else None)
    result = build_static_knowledge(
        data_root=Path(args.data_root) if args.data_root else None,
        registry_root=Path(args.registry_root),
    )
    print(json.dumps({"out_root": str(result["out_root"]), **result["index"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
