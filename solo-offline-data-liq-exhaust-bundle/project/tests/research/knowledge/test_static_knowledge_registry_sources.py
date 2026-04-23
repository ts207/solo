from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml

from project.research.knowledge.build_static_knowledge import build_static_knowledge


def test_static_knowledge_uses_canonical_semantic_sources_when_runtime_mirrors_conflict(
    tmp_path: Path,
) -> None:
    registry_root = tmp_path / "registries"
    registry_root.mkdir()
    (registry_root / "events.yaml").write_text(
        yaml.safe_dump(
            {
                "events": {
                    "BASIS_DISLOC": {
                        "enabled": False,
                        "instrument_classes": ["equities"],
                        "requires_features": ["poison_feature"],
                        "detector": "PoisonDetector",
                    }
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (registry_root / "states.yaml").write_text(
        yaml.safe_dump(
            {"states": {"HIGH_VOL_REGIME": {"enabled": False, "instrument_classes": ["equities"]}}},
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (registry_root / "templates.yaml").write_text(
        yaml.safe_dump(
            {
                "templates": {
                    "continuation": {
                        "enabled": False,
                        "supports_trigger_types": ["FEATURE_PREDICATE"],
                    }
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (registry_root / "contexts.yaml").write_text(
        yaml.safe_dump({"context_dimensions": {"session": {"allowed_values": ["open", "close"]}}}),
        encoding="utf-8",
    )
    (registry_root / "detectors.yaml").write_text(
        yaml.safe_dump({"detector_ownership": {"BASIS_DISLOC": "BasisDislocDetector"}}),
        encoding="utf-8",
    )
    (registry_root / "features.yaml").write_text(
        yaml.safe_dump({"features": {"ret_1": {"allowed_operators": [">", "<"]}}}),
        encoding="utf-8",
    )

    result = build_static_knowledge(data_root=tmp_path / "data", registry_root=registry_root)

    entities = pd.read_parquet(result["entities_path"])
    basis_event = entities.loc[entities["entity_id"] == "event::BASIS_DISLOC"].iloc[0]
    continuation = entities.loc[entities["entity_id"] == "template::continuation"].iloc[0]

    basis_attrs = json.loads(basis_event["attributes_json"])
    continuation_attrs = json.loads(continuation["attributes_json"])

    assert basis_event["source_path"] == "spec/events/BASIS_DISLOC.yaml"
    assert basis_attrs["enabled"] is True
    assert basis_attrs["instrument_classes"] == ["crypto", "futures"]
    assert "poison_feature" not in basis_attrs["requires_features"]

    assert continuation["source_path"] == "spec/templates/registry.yaml"
    assert continuation_attrs["enabled"] is True
    assert "EVENT" in continuation_attrs["supports_trigger_types"]

    assert result["index"]["sources"]["semantic"]["events"] == "spec/events/*.yaml"
    assert result["index"]["sources"]["semantic"]["states"] == "spec/states/*.yaml"
    assert result["index"]["sources"]["semantic"]["templates"] == "spec/templates/registry.yaml"
