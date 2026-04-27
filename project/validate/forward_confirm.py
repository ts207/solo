from __future__ import annotations

from pathlib import Path
from typing import Any

from project.core.config import get_data_root
from project.io.utils import atomic_write_json, ensure_dir


def _load_frozen_thesis():
    """Placeholder for oos_frozen_thesis_replay_v1."""
    raise NotImplementedError("implement oos_frozen_thesis_replay_v1")


def build_forward_confirmation_payload(
    *,
    run_id: str,
    window: str,
    data_root: Path | None = None,
) -> dict[str, Any]:
    raise RuntimeError(
        "forward-confirm currently cannot use phase2 candidate snapshots; "
        "implement oos_frozen_thesis_replay_v1"
    )


def forward_confirm(
    *,
    run_id: str,
    window: str,
    data_root: Path | None = None,
) -> dict[str, Any]:
    root = Path(data_root) if data_root is not None else get_data_root()
    payload = build_forward_confirmation_payload(run_id=run_id, window=window, data_root=root)
    out_dir = root / "reports" / "validation" / str(run_id)
    ensure_dir(out_dir)
    out_path = out_dir / "forward_confirmation.json"
    atomic_write_json(out_path, payload)
    payload["path"] = str(out_path)
    return payload


__all__ = ["build_forward_confirmation_payload", "forward_confirm"]
