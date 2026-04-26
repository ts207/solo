from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from project.io.utils import ensure_dir


def append_selection_log(
    data_root: Path, run_id: str, stage: str, details: dict[str, object]
) -> Path:
    out_dir = Path(data_root) / "reports" / "eval" / str(run_id)
    ensure_dir(out_dir)
    path = out_dir / "selection_log.json"

    payload: dict[str, object] = {"run_id": str(run_id), "entries": []}
    if path.exists():
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                payload = raw
        except Exception:
            payload = {"run_id": str(run_id), "entries": []}

    entries = payload.get("entries", [])
    if not isinstance(entries, list):
        entries = []
    entries.append(
        {
            "logged_at_utc": datetime.now(UTC).isoformat(),
            "stage": str(stage),
            **dict(details),
        }
    )
    payload["run_id"] = str(run_id)
    payload["entries"] = entries
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path
