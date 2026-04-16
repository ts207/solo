from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Set

from project.core.exceptions import (
    DataIntegrityError,
    MalformedReconciliationMetadataError,
    StageExecutionError,
)
from project.io.utils import atomic_write_json
from project.live.contracts import PromotedThesis
from project.live.thesis_store import ThesisStore

_LOG = logging.getLogger(__name__)


class ThesisBatchReconciliationError(StageExecutionError):
    """Raised when thesis-batch reconciliation cannot complete safely."""


RECONCILIATION_DEGRADED_EXCEPTIONS = (
    DataIntegrityError,
    MalformedReconciliationMetadataError,
    OSError,
    ValueError,
)


@dataclass
class ThesisBatchDiff:
    added: List[str] = field(default_factory=list)
    unchanged: List[str] = field(default_factory=list)
    removed: List[str] = field(default_factory=list)
    superseded: List[str] = field(default_factory=list)
    downgraded: List[Dict[str, str]] = field(default_factory=list)


@dataclass
class ReconciliationResult:
    previous_batch_id: str
    current_batch_id: str
    diff: ThesisBatchDiff
    blocked_reasons: List[str] = field(default_factory=list)
    logged_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    safe_to_proceed: bool = False


DEPLOYMENT_STATE_PRECEDENCE = {
    "monitor_only": 0,
    "paper_only": 1,
    "promoted": 2,
    "paper_enabled": 3,
    "paper_approved": 4,
    "live_eligible": 5,
    "live_enabled": 6,
    "live_paused": 7,
    "live_disabled": 8,
    "retired": 9,
}


def _load_previous_batch_metadata(persist_dir: Path) -> Dict[str, str]:
    meta_path = persist_dir / "thesis_batch_metadata.json"
    if not meta_path.exists():
        return {}
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MalformedReconciliationMetadataError(
            f"Failed to read thesis batch metadata {meta_path}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise MalformedReconciliationMetadataError(
            f"Thesis batch metadata {meta_path} must be a JSON object"
        )
    return payload


def _save_current_batch_metadata(persist_dir: Path, store: ThesisStore) -> None:
    persist_dir.mkdir(parents=True, exist_ok=True)
    meta_path = persist_dir / "thesis_batch_metadata.json"
    payload = {
        "run_id": store.run_id,
        "schema_version": store.schema_version,
        "generated_at_utc": store.generated_at_utc,
        "thesis_ids": sorted([t.thesis_id for t in store.all()]),
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }
    atomic_write_json(meta_path, payload)


def _is_downgrade(old_state: str, new_state: str) -> bool:
    old_rank = DEPLOYMENT_STATE_PRECEDENCE.get(old_state, -1)
    new_rank = DEPLOYMENT_STATE_PRECEDENCE.get(new_state, -1)
    if old_rank < 0 or new_rank < 0:
        return False
    return new_rank < old_rank


def _is_supersede(old_thesis: PromotedThesis, new_thesis: PromotedThesis) -> bool:
    if old_thesis.thesis_id != new_thesis.thesis_id:
        return False
    old_version = getattr(old_thesis.lineage, "thesis_version", "1.0.0")
    new_version = getattr(new_thesis.lineage, "thesis_version", "1.0.0")
    try:
        old_parts = [int(x) for x in str(old_version).split(".")]
        new_parts = [int(x) for x in str(new_version).split(".")]
        return new_parts > old_parts
    except (ValueError, AttributeError):
        return False


def classify_thesis_diff(
    previous_store: ThesisStore | None,
    current_store: ThesisStore,
) -> ThesisBatchDiff:
    if previous_store is None:
        return ThesisBatchDiff(
            added=[t.thesis_id for t in current_store.all()],
            unchanged=[],
            removed=[],
            superseded=[],
            downgraded=[],
        )

    prev_by_id = {t.thesis_id: t for t in previous_store.all()}
    curr_by_id = {t.thesis_id: t for t in current_store.all()}

    added = [tid for tid in curr_by_id if tid not in prev_by_id]
    removed = [tid for tid in prev_by_id if tid not in curr_by_id]
    unchanged = []
    superseded = []
    downgraded = []

    for tid in curr_by_id:
        if tid not in prev_by_id:
            continue
        old = prev_by_id[tid]
        cur = curr_by_id[tid]
        if _is_supersede(old, cur):
            superseded.append(tid)
        elif _is_downgrade(old.deployment_state, cur.deployment_state):
            downgraded.append({
                "thesis_id": tid,
                "old_state": old.deployment_state,
                "new_state": cur.deployment_state,
            })
        elif old.deployment_state == cur.deployment_state:
            unchanged.append(tid)

    return ThesisBatchDiff(
        added=added,
        unchanged=unchanged,
        removed=removed,
        superseded=superseded,
        downgraded=downgraded,
    )


def check_reconciliation_safety(
    diff: ThesisBatchDiff,
    previous_store: ThesisStore | None,
    current_store: ThesisStore,
    thesis_manager_state: Dict[str, str],
) -> List[str]:
    reasons: List[str] = []

    if previous_store is None:
        return reasons

    prev_by_id = {t.thesis_id: t for t in previous_store.all()}
    curr_by_id = {t.thesis_id: t for t in current_store.all()}

    for tid in diff.removed:
        runtime_state = thesis_manager_state.get(tid)
        if runtime_state in ("active",):
            prev = prev_by_id.get(tid)
            reasons.append(
                f"Removed thesis {tid} has runtime state '{runtime_state}'"
                f" (deployment_state was '{prev.deployment_state if prev else 'unknown'}')"
            )

    for entry in diff.downgraded:
        tid = entry["thesis_id"]
        old_state = entry["old_state"]
        new_state = entry["new_state"]

        if old_state == "live_enabled":
            reasons.append(
                f"Thesis {tid} downgraded from '{old_state}' to '{new_state}'; "
                f"live_enabled -> paper_only/monitor_only downgrade requires operator approval"
            )

    return reasons


def reconcile_thesis_batch(
    current_store: ThesisStore,
    persist_dir: Path,
    thesis_manager_state: Dict[str, str],
    audit_log_path: Path | None = None,
    data_root: Path | None = None,
) -> ReconciliationResult:
    from project.core.config import get_data_root as _get_data_root

    resolved_data_root = data_root if data_root is not None else _get_data_root()

    prev_meta = _load_previous_batch_metadata(persist_dir)
    prev_run_id = prev_meta.get("run_id", "")
    prev_thesis_ids = set(prev_meta.get("thesis_ids", []))
    previous_store: ThesisStore | None = None

    if prev_run_id and prev_thesis_ids:
        try:
            previous_store = ThesisStore.from_run_id(prev_run_id, data_root=resolved_data_root)
        except FileNotFoundError:
            raise DataIntegrityError(
                f"Previous batch metadata references run_id={prev_run_id} but the thesis store is missing"
            )

    diff = classify_thesis_diff(previous_store, current_store)
    blocked_reasons = check_reconciliation_safety(
        diff, previous_store, current_store, thesis_manager_state
    )

    result = ReconciliationResult(
        previous_batch_id=prev_run_id,
        current_batch_id=current_store.run_id,
        diff=diff,
        blocked_reasons=blocked_reasons,
        safe_to_proceed=len(blocked_reasons) == 0,
    )

    _LOG.info(
        "Thesis batch reconciliation: previous=%s current=%s added=%d unchanged=%d "
        "removed=%d superseded=%d downgraded=%d blocked=%d",
        result.previous_batch_id or "(none)",
        result.current_batch_id,
        len(diff.added),
        len(diff.unchanged),
        len(diff.removed),
        len(diff.superseded),
        len(diff.downgraded),
        len(blocked_reasons),
    )

    if blocked_reasons:
        for reason in blocked_reasons:
            _LOG.error("Reconciliation blocked: %s", reason)

    _save_current_batch_metadata(persist_dir, current_store)

    if audit_log_path is not None:
        _write_audit_log(audit_log_path, result)

    return result


def _write_audit_log(audit_log_path: Path, result: ReconciliationResult) -> None:
    audit_log_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "previous_batch_id": result.previous_batch_id,
        "current_batch_id": result.current_batch_id,
        "added": result.diff.added,
        "unchanged": result.diff.unchanged,
        "removed": result.diff.removed,
        "superseded": result.diff.superseded,
        "downgraded": result.diff.downgraded,
        "blocked_reasons": result.blocked_reasons,
        "safe_to_proceed": result.safe_to_proceed,
        "logged_at": result.logged_at,
    }
    atomic_write_json(audit_log_path, payload)
    _LOG.info("Wrote thesis reconciliation audit log to %s", audit_log_path)
