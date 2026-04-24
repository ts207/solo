from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def dump_yaml(path: Path | str, data: Any) -> None:
    """A simple wrapper for yaml.dump."""
    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)
