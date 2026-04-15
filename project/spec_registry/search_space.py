"""
spec_registry.search_space — Authoritative loader for spec/search_space.yaml.

Phase 2.2: Provides a single, importable source for event priority weights
derived from QUALITY annotations and raw IG values embedded in
search_space.yaml comment lines.

Both the campaign controller and search_intelligence consume this module so
that quality-weighted ordering is consistent across the two systems.

Weight computation
------------------
  base     = QUALITY label tier  →  HIGH = 3.0, MODERATE = 2.0, LOW = 1.0
  ig_bonus = raw IG float × IG_SCALE_FACTOR when present in the comment
  weight   = base + ig_bonus

The IG_SCALE_FACTOR (1 000) normalises typical IG magnitudes (~1e-4) into a
fractional bonus in the range 0.1–0.5, preserving tier ordering while
breaking ties within a tier using empirical information gain.

Unannotated events are absent from the returned dict; callers should apply
DEFAULT_EVENT_PRIORITY_WEIGHT (= 1.5) as the fallback.

Canonical annotation syntax (must appear on the YAML list-item line):
    - EVENT_ID  # [QUALITY: HIGH] - High IG (0.000467)
    - EVENT_ID  # [QUALITY: MODERATE] - Moderate IG (0.000226)
    - EVENT_ID  # [QUALITY: LOW] - Marginal IG (0.000134)
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Dict, Optional, Tuple

from project.spec_registry.loaders import REPO_ROOT

LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Public constants — imported by campaign_controller and search_intelligence
# ---------------------------------------------------------------------------

QUALITY_SCORES: Dict[str, float] = {
    "HIGH": 3.0,
    "MODERATE": 2.0,
    "LOW": 1.0,
}

DEFAULT_EVENT_PRIORITY_WEIGHT: float = 1.5

# Multiplier applied to raw IG floats before adding to the tier base weight.
# IG values are typically in the range 1e-4 – 5e-4; scaling by 1 000 gives a
# fractional bonus of 0.1 – 0.5, safely within a single tier band.
IG_SCALE_FACTOR: float = 1_000.0

# ---------------------------------------------------------------------------
# Internal regex patterns
# ---------------------------------------------------------------------------

_QUALITY_RE = re.compile(
    r"\[QUALITY:\s*(HIGH|MODERATE|LOW)\]",
    re.IGNORECASE,
)

# Matches "IG (0.000467)" or "IG(0.000467)" with optional exponent notation.
_IG_RE = re.compile(
    r"\bIG\s*\(\s*([0-9]+(?:\.[0-9]+)?(?:[eE][+-]?\d+)?)\s*\)",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Canonical search_space.yaml path resolution
# ---------------------------------------------------------------------------

_DEFAULT_SEARCH_SPACE_PATH = REPO_ROOT / "spec" / "search_space.yaml"


def _resolve_path(path: Optional[Path]) -> Path:
    """Return *path* if provided, else fall back to the repo-canonical location."""
    if path is not None:
        return path
    # Allow running from arbitrary working directories (e.g. tests).
    candidates = [
        _DEFAULT_SEARCH_SPACE_PATH,
        Path("spec/search_space.yaml"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return _DEFAULT_SEARCH_SPACE_PATH  # Caller handles missing file


# ---------------------------------------------------------------------------
# Line-level parser
# ---------------------------------------------------------------------------


def _parse_annotation_line(line: str) -> Optional[Tuple[str, float]]:
    """Parse one YAML list line and return ``(event_id, weight)`` or ``None``.

    Returns ``None`` for:
    - Lines that are not YAML list items (``- ...``).
    - Lines without a ``#`` comment (logged as warning if they look like events).
    - Lines whose event ID cannot be extracted.
    - Lines without a ``[QUALITY: …]`` annotation (logged as warning).
    """
    stripped = line.strip()

    if not stripped.startswith("- "):
        return None

    # Strip "- "
    content = stripped[2:].strip()
    
    # Split comment
    if "#" in content:
        code_part, comment_part = content.split("#", 1)
    else:
        # Valid event but no comment -> no annotation.
        # We might want to warn if we expect ALL events to be annotated, 
        # but for now, we just skip it (default weight applies).
        return None
        
    code_part = code_part.strip()
    comment_part = comment_part.strip()
    
    # Extract ID: take the first token, strip trailing colon
    tokens = code_part.split()
    if not tokens:
        return None
        
    event_id = tokens[0].rstrip(":")
    
    # Verify ID format (alphanumeric + underscore) to avoid garbage
    if not re.match(r"^[A-Z0-9_]+$", event_id):
        # Could be a complex structure or invalid ID
        return None

    quality_match = _QUALITY_RE.search(comment_part)
    if not quality_match:
        # Found a comment on an event line, but no [QUALITY: ...] tag.
        # This is likely a typo or a forgotten annotation.
        # We check if "QUALITY" appears in the comment to be more sure it's a typo.
        if "QUALITY" in comment_part.upper():
             LOGGER.warning(f"Malformed QUALITY annotation for event '{event_id}': {comment_part}")
        return None

    label = quality_match.group(1).upper()
    base_weight = QUALITY_SCORES.get(label, DEFAULT_EVENT_PRIORITY_WEIGHT)

    ig_bonus = 0.0
    ig_match = _IG_RE.search(comment_part)
    if ig_match:
        try:
            ig_value = float(ig_match.group(1))
            ig_bonus = ig_value * IG_SCALE_FACTOR
        except ValueError:
            pass

    return event_id, base_weight + ig_bonus


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_event_priority_weights(
    search_space_path: Optional[Path] = None,
) -> Dict[str, float]:
    """Parse QUALITY and IG annotations from *search_space.yaml*.

    Parameters
    ----------
    search_space_path:
        Explicit path to the YAML file.  When *None* the function resolves
        the canonical ``spec/search_space.yaml`` relative to the repository
        root (via ``REPO_ROOT`` from ``spec_registry.loaders``).

    Returns
    -------
    dict[str, float]
        Maps annotated ``event_id → priority_weight``.  Unannotated events
        are **absent**; callers should apply
        ``DEFAULT_EVENT_PRIORITY_WEIGHT`` for those.

    Examples
    --------
    >>> weights = load_event_priority_weights()
    >>> weights["LIQUIDATION_CASCADE"]   # HIGH + IG 0.000467
    3.467
    >>> weights["OVERSHOOT_AFTER_SHOCK"] # MODERATE + IG 0.000226
    2.226
    >>> weights["LIQUIDITY_VACUUM"]      # LOW + IG 0.000134
    1.134
    """
    resolved = _resolve_path(search_space_path)

    if not resolved.exists():
        return {}

    text = resolved.read_text(encoding="utf-8")
    weights: Dict[str, float] = {}

    for line in text.splitlines():
        result = _parse_annotation_line(line)
        if result is not None:
            event_id, weight = result
            weights[event_id] = weight

    return weights
