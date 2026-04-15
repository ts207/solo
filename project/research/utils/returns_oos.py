from __future__ import annotations

import ast
import json
import re
from typing import Any

import numpy as np
import pandas as pd

_NUMPY_SCALAR_WRAPPER_RE = re.compile(
    r"\bnp\.(?:float(?:16|32|64)|int(?:8|16|32|64)|bool_)\(([^()]+)\)"
)


def _strip_numpy_scalar_wrappers(text: str) -> str:
    normalized = text
    while True:
        updated = _NUMPY_SCALAR_WRAPPER_RE.sub(r"\1", normalized)
        if updated == normalized:
            return updated
        normalized = updated


def normalize_returns_oos_combined(value: Any) -> list[float]:
    """Normalize persisted OOS return vectors into plain Python floats.

    Accepted inputs:
    - list/tuple/ndarray/Series/Index of numeric-like values
    - JSON-serialized arrays
    - legacy Python repr arrays, including numpy scalar wrappers such as
      ``[np.float64(0.1), np.float64(-0.2)]``

    Rejected inputs:
    - mapping/object payloads
    - scalar numeric values
    - malformed serialized text
    """

    if value is None:
        return []
    if isinstance(value, (float, np.floating)) and not np.isfinite(value):
        return []
    if isinstance(value, dict):
        raise ValueError('returns_oos_combined must be array-like, not an object')
    if isinstance(value, (int, np.integer, bool)):
        raise ValueError('returns_oos_combined must be array-like, not a scalar')

    parsed = value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except Exception:
            legacy_text = _strip_numpy_scalar_wrappers(text)
            try:
                parsed = ast.literal_eval(legacy_text)
            except Exception as exc:
                raise ValueError(
                    'returns_oos_combined must be a JSON array or legacy Python array when serialized as text'
                ) from exc

    if isinstance(parsed, dict):
        raise ValueError('returns_oos_combined must be array-like, not an object')
    if isinstance(parsed, (list, tuple, np.ndarray, pd.Series, pd.Index)):
        vector = pd.Series(list(parsed), dtype='object')
        numeric = pd.to_numeric(vector, errors='coerce')
        return [float(item) for item in numeric[np.isfinite(numeric)].tolist()]
    if isinstance(parsed, (int, np.integer, bool)):
        raise ValueError('returns_oos_combined must be array-like, not a scalar')
    return []
