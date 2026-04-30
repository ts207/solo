from __future__ import annotations

import hashlib
import json
import warnings
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from project.research import results_index

ROOT = Path(__file__).resolve().parents[2]
SEARCH_LEDGER_DIR = ROOT / "data" / "reports" / "search_ledger"
SEARCH_LEDGER_JSON_PATH = SEARCH_LEDGER_DIR / "search_burden.json"
SEARCH_LEDGER_PARQUET_PATH = SEARCH_LEDGER_DIR / "search_burden.parquet"

SEARCH_LEDGER_COLUMNS = [
    "run_id",
    "program_id",
    "proposal_hash",
    "methodology_epoch",
    "mechanism_id",
    "mechanism_version",
    "mechanism_preflight_status",
    "mechanism_classification",
    "active_research_candidate",
    "archive_reason",
    "event_id",
    "template_id",
    "context",
    "direction",
    "horizon_bars",
    "symbol",
    "start",
    "end",
    "source_file",
    "estimated_hypothesis_count",
    "event_count",
    "n_obs",
    "t_stat_net",
    "q_value",
    "robustness_score",
    "evidence_class",
    "decision",
    "decision_reason",
    "required_falsification",
    "nearby_attempt_count",
    "forbidden_rescue_actions",
]


def _is_missing(value: Any) -> bool:
    return results_index._is_missing(value)


def _first_present(row: dict[str, Any] | pd.Series, columns: Iterable[str]) -> Any:
    return results_index._first_present(row, columns)


def _horizon_bars(value: Any) -> int | None:
    return results_index._horizon_bars(value)


def _to_float(value: Any) -> float | None:
    return results_index._to_float(value)


def _to_int(value: Any) -> int | None:
    return results_index._to_int(value)


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _proposal_hash(path_value: Any, fallback: str) -> str:
    if not _is_missing(path_value):
        path = Path(str(path_value))
        try:
            if path.exists() and path.is_file():
                return hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError:
            pass
        return _sha256_text(str(path_value))
    return _sha256_text(fallback)


def _parse_command_window(command_json: Any) -> tuple[str, str]:
    if _is_missing(command_json):
        return "", ""
    try:
        command = json.loads(str(command_json))
    except json.JSONDecodeError:
        return "", ""
    if not isinstance(command, list):
        return "", ""
    start = ""
    end = ""
    for idx, arg in enumerate(command):
        if arg == "--start" and idx + 1 < len(command):
            start = str(command[idx + 1])
        if arg == "--end" and idx + 1 < len(command):
            end = str(command[idx + 1])
    return start, end


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def collect_validated_plan_metadata(root: Path = ROOT) -> dict[str, dict[str, Any]]:
    metadata: dict[str, dict[str, Any]] = {}
    for path in root.glob("data/artifacts/experiments/*/*/validated_plan.json"):
        run_id = path.parent.name
        payload = _load_json(path)
        if not payload:
            continue
        metadata[run_id] = {
            "program_id": str(payload.get("program_id", "") or path.parent.parent.name),
            "estimated_hypothesis_count": _to_int(payload.get("estimated_hypothesis_count")),
        }
    return metadata


def collect_proposal_metadata(root: Path = ROOT) -> dict[str, dict[str, Any]]:
    metadata: dict[str, dict[str, Any]] = {}
    for path in root.glob("data/artifacts/experiments/*/memory/proposals.parquet"):
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            warnings.warn(f"Could not read proposal memory from {path}: {exc}", stacklevel=2)
            continue
        for _, row in df.iterrows():
            run_id = str(row.get("run_id", "") or "")
            if not run_id:
                continue
            start, end = _parse_command_window(row.get("command_json"))
            fallback = f"{row.get('program_id', '')}:{run_id}:{row.get('proposal_path', '')}"
            metadata[run_id] = {
                "proposal_hash": _proposal_hash(row.get("proposal_path"), fallback),
                "start": start,
                "end": end,
                "program_id": str(row.get("program_id", "") or ""),
            }
    return metadata


def _context_from_memory_row(row: pd.Series) -> str:
    context = _first_present(row, ["context_json", "context_signature", "context"])
    if _is_missing(context):
        return ""
    text = str(context)
    return "" if text == "{}" else text


def collect_memory_rows(root: Path = ROOT) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in root.glob("data/artifacts/experiments/*/memory/tested_regions.parquet"):
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            warnings.warn(f"Could not read tested regions from {path}: {exc}", stacklevel=2)
            continue
        for _, row in df.iterrows():
            rows.append(
                {
                    "run_id": str(row.get("run_id", "") or ""),
                    "program_id": str(row.get("program_id", "") or path.parent.parent.name),
                    "candidate_id": str(
                        row.get("candidate_id", "") or row.get("hypothesis_id", "") or ""
                    ),
                    "event_id": str(row.get("event_type", "") or ""),
                    "template_id": str(row.get("template_id", "") or ""),
                    "context": _context_from_memory_row(row),
                    "direction": str(row.get("direction", "") or ""),
                    "horizon_bars": _horizon_bars(row.get("horizon")),
                    "symbol": str(row.get("symbol_scope", "") or ""),
                    "n_obs": _to_int(
                        _first_present(
                            row, ["n_obs", "test_n_obs", "validation_n_obs", "train_n_obs"]
                        )
                    ),
                    "event_count": _to_int(
                        _first_present(row, ["event_count", "test_n_obs", "validation_n_obs"])
                    ),
                    "t_stat_net": _to_float(_first_present(row, ["t_stat_net", "t_stat"])),
                    "q_value": _to_float(row.get("q_value")),
                    "robustness_score": _to_float(row.get("robustness_score")),
                    "evidence_class": "candidate_signal"
                    if str(row.get("eval_status", "")).lower() == "evaluated"
                    else "review_only",
                    "decision": "review",
                    "decision_reason": str(
                        row.get("primary_fail_gate", "") or row.get("eval_status", "") or ""
                    ),
                    "source_file": "memory_tested_regions",
                }
            )
    return rows


def collect_search_rows(root: Path = ROOT) -> list[dict[str, Any]]:
    result_df = results_index.build_results_index(root=root)
    result_rows = result_df.to_dict(orient="records")
    for row in result_rows:
        row["source_file"] = "results_index"
    return result_rows + collect_memory_rows(root)


def _normalize_ledger_row(
    row: dict[str, Any],
    plan_metadata: dict[str, dict[str, Any]],
    proposal_metadata: dict[str, dict[str, Any]],
    mechanism_metadata: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    run_id = str(row.get("run_id", "") or "")
    plan = plan_metadata.get(run_id, {})
    proposal = proposal_metadata.get(run_id, {})
    program_id = str(
        row.get("program_id", "") or proposal.get("program_id") or plan.get("program_id") or ""
    )
    mechanism = mechanism_metadata.get(run_id, {})
    methodology_epoch = str(
        row.get("methodology_epoch", "") or mechanism.get("methodology_epoch") or "pre_mechanism"
    )
    evidence_class = str(row.get("evidence_class", "") or "")
    decision = str(row.get("decision", "") or "")
    active_research_candidate = (
        methodology_epoch == "mechanism_backed"
        and str(mechanism.get("mechanism_preflight_status", "") or "") == "pass"
        and evidence_class not in {"killed_candidate", "parked_candidate", "historical_result"}
        and decision not in {"kill", "park", "archive"}
    )
    return {
        "run_id": run_id,
        "program_id": program_id,
        "proposal_hash": str(
            proposal.get("proposal_hash") or _sha256_text(f"{program_id}:{run_id}")
        ),
        "methodology_epoch": methodology_epoch,
        "mechanism_id": str(row.get("mechanism_id", "") or mechanism.get("mechanism_id", "") or ""),
        "mechanism_version": str(
            row.get("mechanism_version", "") or mechanism.get("mechanism_version", "") or ""
        ),
        "mechanism_preflight_status": str(
            row.get("mechanism_preflight_status", "")
            or mechanism.get("mechanism_preflight_status", "")
            or ""
        ),
        "mechanism_classification": str(
            row.get("mechanism_classification", "")
            or mechanism.get("mechanism_classification", "")
            or ""
        ),
        "active_research_candidate": bool(
            row.get("active_research_candidate", active_research_candidate)
        ),
        "archive_reason": str(
            row.get("archive_reason", "")
            or ("" if methodology_epoch == "mechanism_backed" else "pre_mechanism_methodology")
        ),
        "event_id": str(row.get("event_id", "") or ""),
        "template_id": str(row.get("template_id", "") or ""),
        "context": str(row.get("context", "") or ""),
        "direction": str(row.get("direction", "") or ""),
        "horizon_bars": _to_int(row.get("horizon_bars")),
        "symbol": str(row.get("symbol", "") or ""),
        "start": str(proposal.get("start", "") or ""),
        "end": str(proposal.get("end", "") or ""),
        "source_file": str(row.get("source_file", "") or ""),
        "estimated_hypothesis_count": _to_int(plan.get("estimated_hypothesis_count")),
        "event_count": _to_int(row.get("event_count")),
        "n_obs": _to_int(row.get("n_obs")),
        "t_stat_net": _to_float(row.get("t_stat_net")),
        "q_value": _to_float(row.get("q_value")),
        "robustness_score": _to_float(row.get("robustness_score")),
        "evidence_class": evidence_class,
        "decision": decision,
        "decision_reason": str(row.get("decision_reason", "") or ""),
        "required_falsification": list(
            row.get("required_falsification") or mechanism.get("required_falsification") or []
        ),
        "nearby_attempt_count": 0,
        "forbidden_rescue_actions": list(
            row.get("forbidden_rescue_actions")
            or mechanism.get("forbidden_rescue_actions")
            or []
        ),
    }


def _same_nearby_surface(row: pd.Series, other: pd.Series) -> bool:
    if row.name == other.name:
        return False
    keys = ["event_id", "template_id", "direction", "symbol"]
    if any(str(row.get(key, "") or "") != str(other.get(key, "") or "") for key in keys):
        return False
    h1 = _to_float(row.get("horizon_bars"))
    h2 = _to_float(other.get("horizon_bars"))
    if h1 is None or h2 is None or h1 <= 0 or h2 <= 0:
        return h1 == h2
    return 0.5 <= h2 / h1 <= 2.0


def attach_nearby_attempt_counts(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    counts: list[int] = []
    grouped = out.groupby(["event_id", "template_id", "direction", "symbol"], dropna=False)
    group_lookup = {key: group for key, group in grouped}
    for _idx, row in out.iterrows():
        key = (
            row.get("event_id", ""),
            row.get("template_id", ""),
            row.get("direction", ""),
            row.get("symbol", ""),
        )
        group = group_lookup.get(key, pd.DataFrame())
        counts.append(
            sum(1 for other_idx, other in group.iterrows() if _same_nearby_surface(row, other))
        )
    out["nearby_attempt_count"] = counts
    return out


def build_search_ledger(root: Path = ROOT) -> pd.DataFrame:
    plan_metadata = collect_validated_plan_metadata(root)
    proposal_metadata = collect_proposal_metadata(root)
    mechanism_metadata = results_index.collect_mechanism_metadata(root)
    rows = [
        _normalize_ledger_row(row, plan_metadata, proposal_metadata, mechanism_metadata)
        for row in collect_search_rows(root)
        if row.get("event_id")
    ]
    df = pd.DataFrame(rows, columns=SEARCH_LEDGER_COLUMNS)
    if df.empty:
        return df
    subset = [
        "run_id",
        "program_id",
        "event_id",
        "template_id",
        "context",
        "direction",
        "horizon_bars",
        "symbol",
        "source_file",
    ]
    df = df.drop_duplicates(subset=subset, keep="first")
    df = attach_nearby_attempt_counts(df)
    return df.sort_values(
        ["event_id", "template_id", "direction", "symbol", "horizon_bars", "run_id"],
        na_position="last",
    )


def _records_for_json(df: pd.DataFrame) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for row in df.to_dict(orient="records"):
        item: dict[str, Any] = {}
        for column in SEARCH_LEDGER_COLUMNS:
            value = row.get(column)
            if isinstance(value, float) and pd.isna(value):
                value = None
            item[column] = value
        records.append(item)
    return records


def write_search_ledger_json(df: pd.DataFrame, path: Path = SEARCH_LEDGER_JSON_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "search_burden_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "row_count": len(df),
        "rows": _records_for_json(df),
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_search_ledger_parquet(df: pd.DataFrame, path: Path = SEARCH_LEDGER_PARQUET_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def update_search_ledger(root: Path = ROOT) -> pd.DataFrame:
    df = build_search_ledger(root)
    write_search_ledger_json(df, root / "data" / "reports" / "search_ledger" / "search_burden.json")
    write_search_ledger_parquet(
        df, root / "data" / "reports" / "search_ledger" / "search_burden.parquet"
    )
    return df
