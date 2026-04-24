from __future__ import annotations

import logging
from typing import Any, Optional

import numpy as np

# Create module-level logger.  When type coercion fails this logger
# will emit warnings including contextual information passed by the caller.
logger = logging.getLogger(__name__)


def _is_non_finite_number(value: Any) -> bool:
    try:
        return bool(isinstance(value, (int, float, np.number)) and not np.isfinite(float(value)))
    except (TypeError, ValueError):
        return False


def _should_suppress_missing_value_warning(val: Any, default: Any, context: str | None) -> bool:
    if context:
        return False
    val_is_missing = val is None or _is_non_finite_number(val)
    default_is_missing = default is None or _is_non_finite_number(default)
    return bool(val_is_missing and default_is_missing)


def safe_float(
    val: Any, default: Optional[float] = None, *, context: str | None = None
) -> Optional[float]:
    """
    Attempt to coerce ``val`` to a finite ``float``.

    Returns the coerced float on success or ``default`` on failure.  The
    default value is now ``None``; callers should explicitly pass ``0.0``
    or another sentinel to mask conversion errors.  When conversion fails
    a warning is logged including the offending value and optional
    ``context`` provided by the caller (e.g. run ID, symbol, or file name).
    """
    try:
        if val is None:
            raise ValueError("value is None")
        f = float(val)
        if not np.isfinite(f):
            raise ValueError(f"non-finite value {val!r}")
        return f
    except (ValueError, TypeError) as exc:
        if _should_suppress_missing_value_warning(val, default, context):
            return default
        logger.warning(
            "safe_float: failed to convert %r to float; returning %r. Context: %s. Error: %s",
            val,
            default,
            context,
            exc,
        )
        return default


def safe_int(
    val: Any, default: Optional[int] = None, *, context: str | None = None
) -> Optional[int]:
    """
    Attempt to coerce ``val`` to an integer via float.

    Returns the coerced integer on success or ``default`` on failure.  The
    default value is now ``None``; callers should explicitly pass ``0`` or
    another sentinel to mask conversion errors.  When conversion fails a
    warning is logged including the offending value and optional
    ``context`` provided by the caller.
    """
    try:
        if val is None:
            raise ValueError("value is None")
        return int(float(val))
    except (ValueError, TypeError) as exc:
        if _should_suppress_missing_value_warning(val, default, context):
            return default
        logger.warning(
            "safe_int: failed to convert %r to int; returning %r. Context: %s. Error: %s",
            val,
            default,
            context,
            exc,
        )
        return default


def as_bool(val: Any) -> bool:
    if isinstance(val, bool):
        return val
    if val is None:
        return False
    if isinstance(val, (int, float)):
        try:
            return bool(int(float(val)))
        except (ValueError, TypeError):
            return False
    s = str(val).strip().lower()
    return s in {"1", "true", "t", "yes", "y", "on", "pass"}
