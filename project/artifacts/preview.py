from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


def _coerce_preview(value: Any, *, max_chars: int = 500) -> Any:
    if isinstance(value, dict):
        return {str(k): _coerce_preview(v, max_chars=max_chars) for k, v in list(value.items())[:50]}
    if isinstance(value, list):
        return [_coerce_preview(v, max_chars=max_chars) for v in value[:20]]
    text = str(value)
    if len(text) > max_chars:
        return text[: max_chars - 3] + "..."
    return value


def _preview_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return {"format": "json", "top_level_type": "list", "row_count": len(payload), "preview": _coerce_preview(payload[:5])}
    if isinstance(payload, dict):
        return {"format": "json", "top_level_type": "dict", "keys": list(payload.keys())[:50], "preview": _coerce_preview(payload)}
    return {"format": "json", "top_level_type": type(payload).__name__, "preview": _coerce_preview(payload)}


def _preview_jsonl(path: Path, *, limit: int) -> dict[str, Any]:
    rows: list[Any] = []
    total = 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            total += 1
            if len(rows) < limit:
                rows.append(json.loads(line))
    return {"format": "jsonl", "row_count": total, "preview": _coerce_preview(rows)}


def _preview_csv(path: Path, *, limit: int) -> dict[str, Any]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows: list[dict[str, Any]] = []
        total = 0
        for row in reader:
            total += 1
            if len(rows) < limit:
                rows.append(dict(row))
    return {"format": "csv", "row_count": total, "columns": reader.fieldnames or [], "preview": _coerce_preview(rows)}


def _preview_parquet(path: Path, *, limit: int) -> dict[str, Any]:
    try:
        import pandas as pd
    except Exception as exc:  # pragma: no cover
        return {"format": "parquet", "status": "unavailable", "message": f"pandas/pyarrow parquet support is unavailable: {exc}"}
    try:
        frame = pd.read_parquet(path)
    except Exception as exc:  # pragma: no cover
        return {"format": "parquet", "status": "error", "message": str(exc)}
    return {"format": "parquet", "row_count": int(len(frame)), "columns": [str(c) for c in frame.columns], "preview": _coerce_preview(frame.head(limit).to_dict(orient="records"))}


def build_artifact_preview(path: str | Path, *, limit: int = 5) -> dict[str, Any]:
    artifact_path = Path(path)
    if not artifact_path.exists():
        return {"status": "missing", "path": str(artifact_path)}
    suffix = artifact_path.suffix.lower()
    try:
        if suffix == ".json":
            payload = _preview_json(artifact_path)
        elif suffix == ".jsonl":
            payload = _preview_jsonl(artifact_path, limit=limit)
        elif suffix == ".csv":
            payload = _preview_csv(artifact_path, limit=limit)
        elif suffix == ".parquet":
            payload = _preview_parquet(artifact_path, limit=limit)
        else:
            return {"status": "unsupported", "path": str(artifact_path), "suffix": suffix}
    except Exception as exc:
        return {"status": "error", "path": str(artifact_path), "message": str(exc)}
    payload.setdefault("status", "pass")
    payload["path"] = str(artifact_path)
    return payload
