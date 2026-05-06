from __future__ import annotations

import json
from pathlib import Path

from project.artifacts.preview import build_artifact_preview


def test_preview_json_dict(tmp_path: Path) -> None:
    path = tmp_path / "payload.json"
    path.write_text(json.dumps({"a": 1, "b": [1, 2, 3]}), encoding="utf-8")

    payload = build_artifact_preview(path)

    assert payload["status"] == "pass"
    assert payload["format"] == "json"
    assert payload["top_level_type"] == "dict"
    assert payload["keys"] == ["a", "b"]


def test_preview_jsonl_counts_rows(tmp_path: Path) -> None:
    path = tmp_path / "rows.jsonl"
    path.write_text('{"x": 1}\n{"x": 2}\n', encoding="utf-8")

    payload = build_artifact_preview(path, limit=1)

    assert payload["status"] == "pass"
    assert payload["format"] == "jsonl"
    assert payload["row_count"] == 2
    assert payload["preview"] == [{"x": 1}]


def test_preview_missing_file(tmp_path: Path) -> None:
    payload = build_artifact_preview(tmp_path / "missing.json")

    assert payload["status"] == "missing"
