"""Append-only negative-result registry (T3.3).

Hypothesis fingerprints that failed are persisted here with their failure reason
and a TTL. The pre-search filter skips re-testing within TTL, shrinking the BH
pool and freeing compute.
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

_LOG = logging.getLogger(__name__)

_DEFAULT_TTL_DAYS: dict[str, int] = {
    "insufficient_data": 7,
    "infeasible": 30,
    "below_min_t_stat": 14,
    "below_min_n": 7,
    "cost_gate": 21,
    "default": 14,
}

_SCHEMA = {
    "fingerprint": "string",
    "hypothesis_key": "string",
    "failure_reason": "string",
    "failed_at": "string",
    "expires_at": "string",
    "program_id": "string",
    "run_id": "string",
}


def _registry_path(data_root: Path, program_id: str) -> Path:
    return (
        Path(data_root)
        / "artifacts"
        / "experiments"
        / str(program_id)
        / "memory"
        / "negative_results.parquet"
    )


def _hypothesis_fingerprint(hypothesis_key: str | dict[str, Any]) -> str:
    if isinstance(hypothesis_key, dict):
        canonical = json.dumps(hypothesis_key, sort_keys=True, separators=(",", ":"))
    else:
        canonical = str(hypothesis_key)
    return hashlib.sha1(canonical.encode(), usedforsecurity=False).hexdigest()[:16]


def _ttl_days(failure_reason: str) -> int:
    for prefix, days in _DEFAULT_TTL_DAYS.items():
        if prefix != "default" and failure_reason.startswith(prefix):
            return days
    return _DEFAULT_TTL_DAYS["default"]


def record_negative_result(
    hypothesis_key: str | dict[str, Any],
    failure_reason: str,
    *,
    program_id: str,
    run_id: str,
    data_root: Path,
    ttl_days: int | None = None,
) -> None:
    """Append one failed hypothesis to the negative-result registry."""
    path = _registry_path(data_root, program_id)
    path.parent.mkdir(parents=True, exist_ok=True)

    now = datetime.now(UTC)
    days = ttl_days if ttl_days is not None else _ttl_days(failure_reason)
    row = {
        "fingerprint": _hypothesis_fingerprint(hypothesis_key),
        "hypothesis_key": (
            json.dumps(hypothesis_key, sort_keys=True)
            if isinstance(hypothesis_key, dict)
            else str(hypothesis_key)
        ),
        "failure_reason": str(failure_reason),
        "failed_at": now.isoformat(),
        "expires_at": (now + timedelta(days=days)).isoformat(),
        "program_id": str(program_id),
        "run_id": str(run_id),
    }
    new_df = pd.DataFrame([row])

    if path.exists():
        try:
            existing = pd.read_parquet(path)
            combined = pd.concat([existing, new_df], ignore_index=True)
            combined = combined.drop_duplicates(
                subset=["fingerprint", "failure_reason"], keep="last"
            )
        except Exception as exc:
            _LOG.warning("Could not read negative results registry: %s", exc)
            combined = new_df
    else:
        combined = new_df

    combined.to_parquet(path, index=False)


def load_active_negative_results(
    program_id: str,
    *,
    data_root: Path,
    as_of: datetime | None = None,
) -> pd.DataFrame:
    """Return unexpired negative results as a DataFrame."""
    path = _registry_path(data_root, program_id)
    if not path.exists():
        return pd.DataFrame(list(_SCHEMA.keys())).iloc[0:0]
    try:
        raw = pd.read_parquet(path)
        df = raw if isinstance(raw, pd.DataFrame) else pd.DataFrame(raw)
    except Exception as exc:
        _LOG.warning("Failed to load negative results: %s", exc)
        return pd.DataFrame(list(_SCHEMA.keys())).iloc[0:0]

    now = (as_of or datetime.now(UTC)).isoformat()
    if "expires_at" in df.columns:
        df = df[df["expires_at"] > now]
    return df.reset_index(drop=True)


def is_negative_result(
    hypothesis_key: str | dict[str, Any],
    failure_reason_prefix: str | None = None,
    *,
    program_id: str,
    data_root: Path,
    as_of: datetime | None = None,
) -> bool:
    """Return True when hypothesis has an active negative result."""
    active = load_active_negative_results(program_id, data_root=data_root, as_of=as_of)
    if active.empty:
        return False
    fp = _hypothesis_fingerprint(hypothesis_key)
    matches = active[active["fingerprint"] == fp]
    if matches.empty:
        return False
    if failure_reason_prefix:
        matches = matches[
            matches["failure_reason"].str.startswith(failure_reason_prefix, na=False)
        ]
    return not matches.empty


def purge_expired(program_id: str, *, data_root: Path) -> int:
    """Remove expired entries. Returns number of rows removed."""
    path = _registry_path(data_root, program_id)
    if not path.exists():
        return 0
    df = pd.read_parquet(path)
    now = datetime.now(UTC).isoformat()
    before = len(df)
    df = df[df["expires_at"] > now].reset_index(drop=True)
    removed = before - len(df)
    df.to_parquet(path, index=False)
    return removed
