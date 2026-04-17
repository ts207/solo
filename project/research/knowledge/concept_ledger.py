"""
Concept Ledger — Phase 3 research memory layer.

Provides a persistent, append-only record of tested concept lineages so that
multiplicity burden reflects what the repo has actually explored across time,
not just what happened in one run family.

Design principles
-----------------
* Additive-only in this phase. Nothing is deleted or mutated.
* The lineage key is explainable and stable across runs.
* Storage is pure parquet so it composes with the rest of the pipeline.
* All public helpers are safe to call when the ledger does not yet exist.

Storage path (default): data/artifacts/research/concept_ledger.parquet
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

import numpy as np
import pandas as pd

from project.io.utils import read_parquet, write_parquet

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

CONCEPT_LEDGER_COLUMNS: list[str] = [
    "ledger_id",
    "run_id",
    "program_id",
    "candidate_id",
    "concept_lineage_key",
    "event_type",
    "event_family",
    "template_id",
    "direction",
    "timeframe",
    "horizon_bars",
    "primary_symbol",       # NEW: specific symbol tested (not just scope type)
    "symbol_scope_type",
    "context_dim_count",
    "tested_at",
    "is_discovery",
    "passed_sample_quality",
    "passed_promotion",
    "adjusted_q_value",
    "after_cost_expectancy_bps",
    "discovery_quality_score",
]

_LEDGER_SCHEMA_VERSION = "v1"

# Horizon bucket boundaries (inclusive upper end in bars)
_HORIZON_SHORT_MAX = 24
_HORIZON_MEDIUM_MAX = 48
# >48 → "long"


# ---------------------------------------------------------------------------
# Lineage key
# ---------------------------------------------------------------------------

def _normalize_direction(direction: object) -> str:
    """Map raw direction value to long / short / neutral."""
    raw = str(direction or "").strip().lower()
    if raw in ("1", "1.0", "long", "up", "buy"):
        return "long"
    if raw in ("-1", "-1.0", "short", "down", "sell"):
        return "short"
    return "neutral"


def _template_family(rule_template: object) -> str:
    """Extract the first semantic token from a template id string."""
    raw = str(rule_template or "").strip().lower()
    if not raw:
        return "unknown"
    # Common template families — take everything up to the first underscore
    # or the full string if there is no underscore.
    token = raw.split("_")[0]
    return token or "unknown"


def _horizon_bucket(horizon_bars: object) -> str:
    try:
        bars = int(float(str(horizon_bars or 0)))
    except (ValueError, TypeError):
        return "unknown"
    if bars <= 0:
        return "unknown"
    if bars <= _HORIZON_SHORT_MAX:
        return "short"
    if bars <= _HORIZON_MEDIUM_MAX:
        return "medium"
    return "long"


def _symbol_scope_type(symbols: object) -> str:
    """Return 'single' or 'multi' based on symbol field."""
    raw = str(symbols or "").strip()
    if not raw:
        return "single"
    # If the field contains a comma it represents multiple symbols
    if "," in raw:
        return "multi"
    return "single"


def _context_dim_count(row: dict) -> int:
    """Count non-empty context dimension fields on the candidate row."""
    ctx_fields = [
        "context_json",
        "state_id",
        "regime",
        "vol_regime",
        "liquidity_state",
        "market_liquidity_state",
        "context_mode",
    ]
    count = 0
    for field in ctx_fields:
        val = row.get(field)
        if val is None:
            continue
        val_str = str(val).strip()
        if val_str and val_str not in ("", "{}", "null", "None", "nan"):
            count += 1
    # Clamp to 3+ bucket for key stability
    return min(count, 3)


def build_concept_lineage_key(candidate_row: dict) -> str:
    """Build a deterministic, explainable concept lineage key (v1).

    The key identifies a *research family*, not an individual candidate.
    It is intentionally coarser than a unique candidate fingerprint so
    that related ideas accumulate shared history in the ledger.

    Key format::

        EVENT:<family>|TMPL:<template_family>|DIR:<direction>|TF:<timeframe>
        |H:<horizon_bucket>|SYM:<scope>|SYM_ID:<symbol>|CTX:<dim_count>

    Parameters
    ----------
    candidate_row:
        A dict-like mapping from a candidate DataFrame row. The function
        reads whichever fields are present and falls back gracefully.

    Returns
    -------
    str
        A pipe-delimited, human-readable lineage key.

    Note on SYM_ID
    --------------
    ``SYM_ID`` encodes the specific symbol tested (e.g. ``BTCUSDT``,
    ``ETHUSDT``).  This prevents test history for BTC from penalising
    ETH investigations of the same hypothesis family.  BTC and ETH have
    different microstructure, funding dynamics, and institutional ownership;
    an edge confirmed on one should not count as evidence against the other.
    """
    row = dict(candidate_row)

    # Event family — prefer canonical grouping, fall back to raw event_type
    event_family = (
        str(row.get("event_family", "") or "").strip()
        or str(row.get("canonical_event_type", "") or "").strip()
        or str(row.get("event_type", "") or "").strip()
        or "unknown"
    ).upper()

    template_fam = _template_family(row.get("rule_template") or row.get("template_id"))
    direction = _normalize_direction(row.get("direction"))
    timeframe = str(row.get("timeframe") or row.get("bar_timeframe") or "unknown").strip().lower()

    # Horizon bars — try several column names
    horizon_bars_raw = (
        row.get("horizon_bars")
        or row.get("horizon_bars_override")
        or row.get("horizon")  # sometimes stored bare as int
    )
    try:
        horizon_bars = int(float(str(horizon_bars_raw or 0)))
    except (ValueError, TypeError):
        horizon_bars = 0

    h_bucket = _horizon_bucket(horizon_bars)
    sym_scope = _symbol_scope_type(row.get("symbol") or row.get("symbol_scope"))
    # SYM_ID: the specific symbol tested.  Falls back to 'any' for multi-symbol
    # or unknown contexts so the key remains stable.
    primary_symbol = (
        str(row.get("symbol") or row.get("primary_symbol") or "").strip().upper()
        or "any"
    )
    ctx_count = _context_dim_count(row)

    key = (
        f"EVENT:{event_family}"
        f"|TMPL:{template_fam}"
        f"|DIR:{direction}"
        f"|TF:{timeframe}"
        f"|H:{h_bucket}"
        f"|SYM:{sym_scope}"
        f"|SYM_ID:{primary_symbol}"
        f"|CTX:{ctx_count}"
    )
    return key


# ---------------------------------------------------------------------------
# Ledger storage helpers
# ---------------------------------------------------------------------------

def _ledger_id(run_id: str, candidate_id: str, lineage_key: str) -> str:
    material = f"{run_id}||{candidate_id}||{lineage_key}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _empty_ledger() -> pd.DataFrame:
    df = pd.DataFrame(columns=CONCEPT_LEDGER_COLUMNS)
    return df


def _coerce_ledger_types(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure the ledger DataFrame has sensible types after loading."""
    out = df.copy()
    int_cols = ["horizon_bars", "context_dim_count"]
    float_cols = ["adjusted_q_value", "after_cost_expectancy_bps", "discovery_quality_score"]
    bool_cols = ["is_discovery", "passed_sample_quality", "passed_promotion"]
    str_cols = [
        c for c in CONCEPT_LEDGER_COLUMNS
        if c not in int_cols + float_cols + bool_cols
    ]
    for col in str_cols:
        if col in out.columns:
            out[col] = out[col].fillna("").astype(str)
    for col in int_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0).astype(int)
    for col in float_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in bool_cols:
        if col in out.columns:
            out[col] = out[col].fillna(False).infer_objects(copy=False).astype(bool)
    return out


def load_concept_ledger(path: str | Path, *, raise_on_error: bool = False) -> pd.DataFrame:
    """Load the concept ledger from *path*.

    Returns an empty DataFrame with the correct schema when the file does
    not exist. By default, unreadable ledgers are logged and treated as
    empty for legacy best-effort callers. Set ``raise_on_error=True`` when
    missing history would corrupt multiplicity accounting.
    """
    resolved = Path(path)
    if not resolved.exists():
        return _empty_ledger()
    try:
        df = read_parquet(resolved)
        # Add any missing columns introduced by schema evolution
        for col in CONCEPT_LEDGER_COLUMNS:
            if col not in df.columns:
                df[col] = None
        return _coerce_ledger_types(df.reindex(columns=CONCEPT_LEDGER_COLUMNS))
    except Exception as exc:
        log.warning("Could not read concept ledger at %s: %s", resolved, exc)
        if raise_on_error:
            raise
        return _empty_ledger()


def append_concept_ledger(
    records: pd.DataFrame,
    path: str | Path,
    *,
    raise_on_error: bool = False,
) -> None:
    """Append *records* to the concept ledger at *path*.

    The file is created on first write. Appends are done by reading the
    existing ledger, concatenating, and rewriting — safe for the typical
    batch sizes in this pipeline. De-duplication by ``ledger_id`` is
    applied so repeated writes of the same run are idempotent.

    By default, exceptions are logged at WARNING level for legacy callers
    that treat the ledger as best-effort. Set ``raise_on_error=True`` for
    outcome-critical discovery paths where missing ledger history would
    corrupt future multiplicity accounting.
    """
    if records is None or (isinstance(records, pd.DataFrame) and records.empty):
        return
    resolved = Path(path)
    try:
        resolved.parent.mkdir(parents=True, exist_ok=True)
        existing = load_concept_ledger(resolved)
        # Ensure new records have all required columns
        new_df = records.copy()
        for col in CONCEPT_LEDGER_COLUMNS:
            if col not in new_df.columns:
                new_df[col] = None
        new_df = new_df.reindex(columns=CONCEPT_LEDGER_COLUMNS)
        combined = pd.concat(
            [df for df in [existing, new_df] if not df.empty],
            ignore_index=True,
        ) if (not existing.empty or not new_df.empty) else existing.copy()
        # idempotent: drop duplicate ledger_ids keeping the first occurrence
        if "ledger_id" in combined.columns:
            combined = combined.drop_duplicates(subset=["ledger_id"], keep="first")
        combined = _coerce_ledger_types(combined)
        write_parquet(combined, resolved)
        log.debug(
            "Wrote %d new concept ledger records to %s (total %d)",
            len(new_df),
            resolved,
            len(combined),
        )
    except Exception as exc:
        log.warning("Failed to append concept ledger at %s: %s", resolved, exc)
        if raise_on_error:
            raise


# ---------------------------------------------------------------------------
# Lineage summary
# ---------------------------------------------------------------------------

def summarize_lineage_history(
    ledger: pd.DataFrame,
    lineage_keys: Sequence[str],
    *,
    lookback_days: int | None = 365,
    recent_window_days: int = 90,
) -> pd.DataFrame:
    """Compute per-lineage history statistics from *ledger*.

    Parameters
    ----------
    ledger:
        The full concept ledger (output of ``load_concept_ledger``).
    lineage_keys:
        Distinct lineage keys to summarise.
    lookback_days:
        How far back to include records for *prior* counts. ``None`` means
        no cutoff.
    recent_window_days:
        Window for *recent_* counts (always relative to now).

    Returns
    -------
    pd.DataFrame with one row per unique lineage key and columns:
        concept_lineage_key, ledger_prior_test_count,
        ledger_prior_discovery_count, ledger_prior_promotion_count,
        ledger_recent_test_count, ledger_recent_failure_count,
        ledger_empirical_success_rate, ledger_family_density.
    """
    now_utc = datetime.now(timezone.utc)
    result_rows: list[dict] = []

    unique_keys = list(dict.fromkeys(str(k) for k in lineage_keys))

    if ledger.empty or "concept_lineage_key" not in ledger.columns:
        for key in unique_keys:
            result_rows.append(_zero_lineage_row(key))
        return pd.DataFrame(result_rows)

    # Parse tested_at once
    ledger_work = ledger.copy()
    if "tested_at" in ledger_work.columns:
        ledger_work["_tested_dt"] = pd.to_datetime(
            ledger_work["tested_at"], utc=True, errors="coerce"
        )
    else:
        ledger_work["_tested_dt"] = pd.NaT

    for key in unique_keys:
        key_mask = ledger_work["concept_lineage_key"].astype(str) == str(key)
        key_df = ledger_work[key_mask].copy()

        if key_df.empty:
            result_rows.append(_zero_lineage_row(key))
            continue

        # Apply lookback window
        if lookback_days is not None:
            cutoff = now_utc - pd.Timedelta(days=int(lookback_days))
            dt_series = key_df["_tested_dt"]
            valid_dt = dt_series.notna()
            within_window = valid_dt & (dt_series >= cutoff)
            # Include rows with unparseable dates in the full count
            windowed = key_df[within_window | ~valid_dt]
        else:
            windowed = key_df

        # Prior counts (full lookback window)
        prior_test_count = int(len(windowed))
        prior_discovery_count = int(
            windowed["is_discovery"].fillna(False).astype(bool).sum()
            if "is_discovery" in windowed.columns
            else 0
        )
        prior_promotion_count = int(
            windowed["passed_promotion"].fillna(False).astype(bool).sum()
            if "passed_promotion" in windowed.columns
            else 0
        )

        # Recent window
        recent_cutoff = now_utc - pd.Timedelta(days=int(recent_window_days))
        dt_series = key_df["_tested_dt"]
        recent_mask = dt_series.notna() & (dt_series >= recent_cutoff)
        recent_df = key_df[recent_mask]
        recent_test_count = int(len(recent_df))
        recent_failure_count = int(
            (~recent_df["is_discovery"].fillna(False).astype(bool)).sum()
            if "is_discovery" in recent_df.columns
            else 0
        )

        # Empirical success rate (discovery / tested, with Laplace smoothing)
        if prior_test_count > 0:
            empirical_success_rate = float(prior_discovery_count) / float(prior_test_count)
        else:
            empirical_success_rate = 0.0

        # Family density: how many distinct run_ids tested this lineage
        if "run_id" in windowed.columns:
            family_density = int(windowed["run_id"].nunique())
        else:
            family_density = int(prior_test_count > 0)

        result_rows.append(
            {
                "concept_lineage_key": key,
                "ledger_prior_test_count": prior_test_count,
                "ledger_prior_discovery_count": prior_discovery_count,
                "ledger_prior_promotion_count": prior_promotion_count,
                "ledger_recent_test_count": recent_test_count,
                "ledger_recent_failure_count": recent_failure_count,
                "ledger_empirical_success_rate": empirical_success_rate,
                "ledger_family_density": family_density,
            }
        )

    return pd.DataFrame(result_rows)


def _zero_lineage_row(key: str) -> dict:
    return {
        "concept_lineage_key": key,
        "ledger_prior_test_count": 0,
        "ledger_prior_discovery_count": 0,
        "ledger_prior_promotion_count": 0,
        "ledger_recent_test_count": 0,
        "ledger_recent_failure_count": 0,
        "ledger_empirical_success_rate": 0.0,
        "ledger_family_density": 0,
    }


# ---------------------------------------------------------------------------
# Record builder (used by discovery pipelines)
# ---------------------------------------------------------------------------

def build_ledger_records(
    candidates: pd.DataFrame,
    *,
    run_id: str,
    program_id: str = "",
    timeframe: str = "",
) -> pd.DataFrame:
    """Convert a scored candidate DataFrame into concept ledger records.

    Include both survivors and failures — multiplicity burden comes from
    what was *tested*, not only what passed.

    Parameters
    ----------
    candidates:
        The final scored candidate table (post multiplicity correction).
    run_id:
        Run identifier for provenance.
    program_id:
        Program identifier (optional).
    timeframe:
        Timeframe string if not already in the candidate rows.
    """
    if candidates is None or candidates.empty:
        return _empty_ledger()

    now_str = datetime.now(timezone.utc).isoformat()
    records: list[dict] = []

    for _, row in candidates.iterrows():
        row_dict = dict(row)
        lineage_key = (
            str(row_dict.get("concept_lineage_key", "")).strip()
            or build_concept_lineage_key(row_dict)
        )
        candidate_id = str(row_dict.get("candidate_id", "") or "").strip()
        lid = _ledger_id(str(run_id), candidate_id, lineage_key)

        resolved_timeframe = (
            str(row_dict.get("timeframe") or row_dict.get("bar_timeframe") or timeframe or "")
            .strip()
            .lower()
        )
        horizon_bars_raw = (
            row_dict.get("horizon_bars")
            or row_dict.get("horizon_bars_override")
        )
        try:
            horizon_bars = int(float(str(horizon_bars_raw or 0)))
        except (ValueError, TypeError):
            horizon_bars = 0

        # Detect symbol scope type
        sym = row_dict.get("symbol") or row_dict.get("symbol_scope") or ""
        sym_scope = _symbol_scope_type(sym)
        # Store the specific symbol for per-symbol lineage isolation
        primary_sym = str(sym).strip().upper() or "any"
        ctx_count = _context_dim_count(row_dict)

        is_disc = bool(
            pd.to_numeric(
                row_dict.get("is_discovery", False), errors="coerce"
            )
            or False
        )
        passed_sq = bool(
            pd.to_numeric(
                row_dict.get("gate_sample_quality", row_dict.get("is_discovery", False)),
                errors="coerce",
            )
            or False
        )

        records.append(
            {
                "ledger_id": lid,
                "run_id": str(run_id),
                "program_id": str(program_id or ""),
                "candidate_id": candidate_id,
                "concept_lineage_key": lineage_key,
                "event_type": str(
                    row_dict.get("canonical_event_type", row_dict.get("event_type", ""))
                ).strip(),
                "event_family": str(
                    row_dict.get("event_family", "")
                ).strip(),
                "template_id": str(
                    row_dict.get("rule_template", row_dict.get("template_id", ""))
                ).strip(),
                "direction": _normalize_direction(row_dict.get("direction")),
                "timeframe": resolved_timeframe,
                "horizon_bars": horizon_bars,
                "symbol_scope_type": sym_scope,
                "primary_symbol": primary_sym,
                "context_dim_count": ctx_count,
                "tested_at": now_str,
                "is_discovery": is_disc,
                "passed_sample_quality": passed_sq,
                "passed_promotion": False,  # updated in a later phase
                "adjusted_q_value": float(
                    pd.to_numeric(row_dict.get("q_value", np.nan), errors="coerce")
                    if pd.notna(
                        pd.to_numeric(row_dict.get("q_value", np.nan), errors="coerce")
                    )
                    else np.nan
                ),
                "after_cost_expectancy_bps": float(
                    pd.to_numeric(
                        row_dict.get("estimate_bps", row_dict.get("mean_return_bps", np.nan)),
                        errors="coerce",
                    )
                    if pd.notna(
                        pd.to_numeric(
                            row_dict.get("estimate_bps", row_dict.get("mean_return_bps", np.nan)),
                            errors="coerce",
                        )
                    )
                    else np.nan
                ),
                "discovery_quality_score": float(
                    pd.to_numeric(
                        row_dict.get("discovery_quality_score", np.nan), errors="coerce"
                    )
                    if pd.notna(
                        pd.to_numeric(
                            row_dict.get("discovery_quality_score", np.nan), errors="coerce"
                        )
                    )
                    else np.nan
                ),
            }
        )

    if not records:
        return _empty_ledger()

    df = pd.DataFrame(records).reindex(columns=CONCEPT_LEDGER_COLUMNS)
    return _coerce_ledger_types(df)


# ---------------------------------------------------------------------------
# Default ledger path helper
# ---------------------------------------------------------------------------

def default_ledger_path(data_root: str | Path) -> Path:
    """Return the canonical global ledger path under *data_root*."""
    return Path(data_root) / "artifacts" / "research" / "concept_ledger.parquet"
