from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from project.io.utils import ensure_dir

_STDOUT_CAPTURE_ENV = "BACKTEST_STAGE_STDOUT_CAPTURED"
_TRUE_VALUES = {"1", "true", "TRUE", "yes", "YES"}


def build_stage_log_handlers(log_path: str | None) -> list[logging.Handler]:
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if not log_path:
        return handlers
    if str(os.environ.get(_STDOUT_CAPTURE_ENV, "")).strip() in _TRUE_VALUES:
        return handlers
    path = Path(log_path)
    ensure_dir(path.parent)
    handlers.append(logging.FileHandler(path))
    return handlers
