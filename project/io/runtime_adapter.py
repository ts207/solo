from __future__ import annotations

from pathlib import Path
from typing import Any

from project.io.utils import read_table_auto


def events_path_candidates(data_root: Path, run_id: str) -> list[Path]:
    root = Path(data_root) / "events" / str(run_id)
    return [root / "events.parquet", root / "events.csv"]


def read_raw_event_rows(
    *,
    data_root: Path,
    run_id: str,
) -> tuple[list[dict[str, Any]], str]:
    for candidate in events_path_candidates(Path(data_root), run_id):
        df = read_table_auto(candidate)
        if df is not None and not df.empty:
            return list(df.to_dict(orient="records")), str(candidate)
    return [], ""
