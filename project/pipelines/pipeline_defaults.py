from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from project import PROJECT_ROOT
from project.io.utils import read_table_auto as read_table_auto_compat


def get_data_root() -> Path:

    return Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))


def __getattr__(name: str) -> Any:
    if name == "DATA_ROOT":
        return get_data_root()
    raise AttributeError(f"module {__name__} has no attribute {name}")


DEFAULT_FEATURE_SCHEMA_VERSION = "v2"
DEFAULT_HASH_SCHEMA_VERSION = "v1"


def utc_now_iso() -> str:
    """Returns the current UTC time in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


def run_id_default() -> str:
    """Generates a default run ID based on the current timestamp."""
    return f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"


def cli_flag_present(flag: str) -> bool:
    """Checks if a given CLI flag is present in sys.argv."""
    return flag in sys.argv


def as_flag(val: Any) -> str:
    """Converts a value to a '1' or '0' flag string."""
    try:
        return "1" if bool(int(val)) else "0"
    except (ValueError, TypeError):
        return "1" if bool(val) else "0"


def read_table_auto(path: Path) -> Any:
    """Automatically reads a Parquet or CSV file into a pandas DataFrame."""
    if not path.exists():
        return None
    return read_table_auto_compat(path)


def build_timing_map(timings: List[Tuple[str, float]]) -> Dict[str, float]:
    """Builds a map of stage names to total duration."""
    out: Dict[str, float] = {}
    for name, duration in timings:
        out[name] = round(float(out.get(name, 0.0) + float(duration)), 3)
    return out


def script_supports_flag(script_path: Path, flag: str) -> bool:
    """Checks if a script (by path) contains a reference to a given flag string."""
    try:
        return flag in script_path.read_text(encoding="utf-8")
    except OSError:
        return False
