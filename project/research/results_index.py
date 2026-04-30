from __future__ import annotations

import glob
import json
import warnings
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from project.research.mechanisms import (
    CandidateHypothesis,
    load_mechanism,
    validate_candidate_against_mechanism,
)
from project.scripts.discover_doctor import build_discover_doctor_report

warnings.filterwarnings("ignore")

ROOT = Path(__file__).resolve().parents[2]
DECISIONS_PATH = ROOT / "docs" / "research" / "decisions.yaml"
RESULTS_DIR = ROOT / "data" / "reports" / "results"
RESULTS_JSON_PATH = RESULTS_DIR / "results_index.json"
RESULTS_PARQUET_PATH = RESULTS_DIR / "results_index.parquet"
RESULTS_MD_PATH = ROOT / "docs" / "research" / "results.md"
SEARCH_LEDGER_PARQUET_PATH = ROOT / "data" / "reports" / "search_ledger" / "search_burden.parquet"

RESULT_COLUMNS = [
    "run_id",
    "program_id",
    "candidate_id",
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
    "n_obs",
    "event_count",
    "t_stat_net",
    "mean_return_net_bps",
    "q_value",
    "robustness_score",
    "evidence_class",
    "decision",
    "decision_reason",
    "next_safe_command",
    "required_falsification",
    "forbidden_rescue_actions",
    "manual_decision",
    "nearby_attempt_count",
    "governed_reproduction_status",
    "governed_reproduction_decision",
    "governed_reproduction_reason",
    "year_split_status",
    "year_split_classification",
    "year_split_reason",
    "specificity_status",
    "specificity_classification",
    "specificity_reason",
    "specificity_decision",
]

TEMPLATE_HINTS = {
    "exhaustion_reversal": [
        "liq",
        "std_gate",
        "climax",
        "forced_flow",
        "oi_flush",
        "deleveraging",
        "oi_spike",
        "broad_oi",
        "broad_post",
        "broad_forced",
        "broad_climax",
        "broad_liquidation",
    ],
    "mean_reversion": ["mr_", "liqdirect", "direct_highvol", "golden_path", "mean_rev"],
    "continuation": ["cont_", "broad_vol_spike_short"],
    "reversal_or_squeeze": ["reversal_or", "targeted"],
}

PROMOTED_PROGS = {
    "broad_vol_spike_long_mr_24b",
    "campaign_pe_oi-spike-negative",
    "campaign_pe_oi_spike_neg_48b",
    "liquidation_std_gate_2yr",
}

SOURCE_PRIORITY = {
    "eval_results": 0,
    "manual": 1,
    "phase2_hyp": 2,
    "edge_cand": 3,
}


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().lower() in {"", "nan", "none", "null"}
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _first_present(row: dict[str, Any] | pd.Series, columns: Iterable[str]) -> Any:
    for column in columns:
        value = row.get(column)
        if not _is_missing(value):
            return value
    return None


def _to_float(value: Any) -> float | None:
    if _is_missing(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    numeric = _to_float(value)
    return None if numeric is None else int(numeric)


def _round(value: float | None, digits: int) -> float | None:
    return None if value is None else round(float(value), digits)


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError, TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _resolve_path(root: Path, path_value: Any) -> Path:
    path = Path(str(path_value or ""))
    return path if path.is_absolute() else root / path


def _mechanism_id_from_payload(payload: dict[str, Any]) -> str:
    mechanism = payload.get("mechanism")
    if isinstance(mechanism, dict):
        return str(mechanism.get("id") or mechanism.get("mechanism_id") or "").strip()
    artifacts = payload.get("artifacts")
    if isinstance(artifacts, dict):
        return str(artifacts.get("mechanism_id") or "").strip()
    return str(payload.get("mechanism_id") or "").strip()


def _mechanism_version_from_payload(payload: dict[str, Any]) -> str:
    mechanism = payload.get("mechanism")
    if isinstance(mechanism, dict):
        return str(mechanism.get("version") or "").strip()
    artifacts = payload.get("artifacts")
    if isinstance(artifacts, dict):
        return str(artifacts.get("mechanism_version") or "").strip()
    return str(payload.get("mechanism_version") or "").strip()


def _preflight_metadata_from_payload(
    payload: dict[str, Any],
    proposal_path: str = "",
) -> dict[str, Any]:
    mechanism_id = _mechanism_id_from_payload(payload)
    if not mechanism_id:
        return {}
    mechanism_version = _mechanism_version_from_payload(payload)
    try:
        mechanism = load_mechanism(mechanism_id)
        candidate = CandidateHypothesis.from_proposal_payload(payload)
        report = validate_candidate_against_mechanism(
            candidate,
            mechanism,
            proposal_path=proposal_path,
        )
        return {
            "methodology_epoch": "mechanism_backed",
            "mechanism_id": mechanism.mechanism_id,
            "mechanism_version": mechanism_version or mechanism.version,
            "mechanism_preflight_status": report.status,
            "mechanism_classification": report.classification,
            "required_falsification": list(report.required_falsification),
            "forbidden_rescue_actions": list(report.forbidden_rescue_actions),
        }
    except Exception:
        return {
            "methodology_epoch": "mechanism_backed",
            "mechanism_id": mechanism_id,
            "mechanism_version": mechanism_version,
            "mechanism_preflight_status": "fail",
            "mechanism_classification": "invalid_mechanism",
            "required_falsification": list(payload.get("required_falsification") or []),
            "forbidden_rescue_actions": list(payload.get("forbidden_rescue_actions") or []),
        }


def collect_mechanism_metadata(root: Path = ROOT) -> dict[str, dict[str, Any]]:
    metadata: dict[str, dict[str, Any]] = {}
    for path in sorted(root.glob("data/artifacts/experiments/*/memory/proposals.parquet")):
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            warnings.warn(f"Could not read proposal memory from {path}: {exc}", stacklevel=2)
            continue
        for _, row in df.iterrows():
            run_id = str(row.get("run_id", "") or "")
            proposal_path = _resolve_path(root, row.get("proposal_path"))
            if not run_id or not proposal_path.exists():
                continue
            payload = _load_yaml(proposal_path)
            mechanism_meta = _preflight_metadata_from_payload(payload, str(proposal_path))
            if mechanism_meta:
                metadata[run_id] = mechanism_meta
    return metadata


def _horizon_bars(value: Any) -> int | None:
    if _is_missing(value):
        return None
    text = str(value).strip().lower()
    for suffix in ("bars", "bar", "b"):
        if text.endswith(suffix):
            text = text[: -len(suffix)].strip()
            break
    return _to_int(text)


def infer_template(program_id: str, existing: Any) -> str:
    if not _is_missing(existing) and str(existing) != "unknown":
        return str(existing)
    program = str(program_id).lower()
    for template, keys in TEMPLATE_HINTS.items():
        if any(key in program for key in keys):
            return template
    return ""


def _event_from_row(row: dict[str, Any] | pd.Series) -> str:
    explicit = _first_present(row, ["event_id", "event_type", "event"])
    if explicit is not None:
        return str(explicit)

    payload = _first_present(row, ["trigger_payload"])
    if isinstance(payload, str) and payload.strip():
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            parsed = {}
        if isinstance(parsed, dict):
            event = parsed.get("event_id") or parsed.get("event_type")
            if event:
                return str(event)

    trigger_key = _first_present(row, ["trigger_key"])
    if isinstance(trigger_key, str) and trigger_key.startswith("event:"):
        return trigger_key.split(":", 1)[1].strip()
    return ""


def _context_from_row(row: dict[str, Any] | pd.Series) -> str:
    value = _first_present(
        row,
        [
            "context",
            "context_cell",
            "condition",
            "market_context",
            "filter_context",
            "entry_condition",
        ],
    )
    return "" if value is None else str(value)


def _candidate_id(row: dict[str, Any], normalized: dict[str, Any]) -> str:
    explicit = _first_present(row, ["candidate_id", "hypothesis_id", "strategy_id"])
    if explicit is not None:
        return str(explicit)
    parts = [
        normalized.get("run_id"),
        normalized.get("symbol"),
        normalized.get("event_id"),
        normalized.get("context"),
        normalized.get("template_id"),
        normalized.get("direction"),
        normalized.get("horizon_bars"),
    ]
    return ":".join(str(part) for part in parts if not _is_missing(part))


def _mean_return_net_bps(row: dict[str, Any] | pd.Series) -> float | None:
    bps = _first_present(
        row,
        [
            "mean_return_net_bps",
            "after_cost_expectancy_bps",
            "cost_adjusted_return_bps",
            "net_expectancy_bps",
            "mean_return_bps",
        ],
    )
    if bps is not None:
        return _to_float(bps)

    decimal = _first_present(row, ["after_cost_expectancy_per_trade", "after_cost_expectancy"])
    decimal_float = _to_float(decimal)
    return None if decimal_float is None else decimal_float * 10000


def _default_classification(row: dict[str, Any]) -> tuple[str, str, str, str]:
    t_stat = _to_float(row.get("t_stat_net"))
    robustness = _to_float(row.get("robustness_score"))
    q_value = _to_float(row.get("q_value"))
    event_count = _to_int(row.get("event_count")) or _to_int(row.get("n_obs")) or 0

    if event_count <= 0 or t_stat is None:
        return (
            "review_only",
            "review",
            "not_evaluated",
            "Inspect source artifacts before validation or promotion.",
        )
    if t_stat >= 2.0 and (robustness or 0.0) >= 0.70:
        return (
            "validate_ready",
            "validate",
            "local_bridge_gate_passed",
            f"make validate RUN_ID={row.get('run_id')}",
        )
    if t_stat >= 2.0 and (robustness or 0.0) >= 0.60:
        return (
            "candidate_signal",
            "review",
            "phase2_gate_only",
            "Run discover-doctor and candidate autopsy before validation.",
        )
    if q_value is not None and q_value < 0.05:
        return (
            "candidate_signal",
            "review",
            "local_discovery_signal",
            "Run discover-doctor and candidate autopsy before validation.",
        )
    return (
        "review_only",
        "review",
        "below_bridge_gate",
        "Record result and move to the next bounded cell unless mechanism review justifies autopsy.",
    )


def normalize_result_row(row: dict[str, Any]) -> dict[str, Any]:
    program_id = str(row.get("program_id", "") or "")
    horizon = _horizon_bars(_first_present(row, ["horizon_bars", "horizon", "horizon_label"]))
    event_count = _to_int(_first_present(row, ["event_count", "n_events", "n", "sample_size"]))
    n_obs = _to_int(_first_present(row, ["n_obs", "n", "sample_size", "validation_samples"]))
    normalized: dict[str, Any] = {
        "source_file": str(row.get("source_file", "") or ""),
        "run_id": str(row.get("run_id", "") or ""),
        "program_id": program_id,
        "candidate_id": "",
        "methodology_epoch": "pre_mechanism",
        "mechanism_id": "",
        "mechanism_version": "",
        "mechanism_preflight_status": "",
        "mechanism_classification": "",
        "active_research_candidate": False,
        "archive_reason": "pre_mechanism_methodology",
        "event_id": _event_from_row(row),
        "template_id": infer_template(program_id, _first_present(row, ["template_id", "template"])),
        "context": _context_from_row(row),
        "direction": "" if _is_missing(row.get("direction")) else str(row.get("direction")),
        "horizon_bars": horizon,
        "symbol": "" if _is_missing(row.get("symbol")) else str(row.get("symbol")),
        "n_obs": n_obs,
        "event_count": event_count if event_count is not None else n_obs,
        "t_stat_net": _round(
            _to_float(_first_present(row, ["t_stat_net", "t_stat", "t_value"])), 4
        ),
        "mean_return_net_bps": _round(_mean_return_net_bps(row), 4),
        "q_value": _round(_to_float(_first_present(row, ["q_value", "p_value_for_fdr"])), 6),
        "robustness_score": _round(_to_float(row.get("robustness_score")), 4),
        "required_falsification": [],
        "forbidden_rescue_actions": [],
        "manual_decision": False,
        "nearby_attempt_count": 0,
        "governed_reproduction_status": "",
        "governed_reproduction_decision": "",
        "governed_reproduction_reason": "",
        "year_split_status": "",
        "year_split_classification": "",
        "year_split_reason": "",
        "specificity_status": "",
        "specificity_classification": "",
        "specificity_reason": "",
        "specificity_decision": "",
    }
    normalized["candidate_id"] = _candidate_id(row, normalized)
    evidence_class, decision, reason, next_safe = _default_classification(normalized)
    normalized.update(
        {
            "evidence_class": evidence_class,
            "decision": decision,
            "decision_reason": reason,
            "next_safe_command": next_safe,
            "promoted": program_id in PROMOTED_PROGS,
        }
    )
    if (
        normalized["promoted"]
        and normalized["event_count"]
        and normalized["t_stat_net"] is not None
    ):
        normalized["evidence_class"] = "research_edge"
        normalized["decision"] = "monitor"
        normalized["decision_reason"] = "historically_promoted_requires_current_confirmation"
    return normalized


def _extract_rows(
    df: pd.DataFrame, source: str, run_id: str, program_id: str
) -> list[dict[str, Any]]:
    rows = []
    for _, series in df.iterrows():
        row = series.to_dict()
        row.update({"source_file": source, "run_id": run_id, "program_id": program_id})
        rows.append(row)
    return rows


def collect_result_rows(root: Path = ROOT) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for path in sorted(
        glob.glob(str(root / "data/artifacts/experiments/*/*/evaluation_results.parquet"))
    ):
        parts = Path(path).parts
        program_id, run_id = parts[-3], parts[-2]
        try:
            rows.extend(_extract_rows(pd.read_parquet(path), "eval_results", run_id, program_id))
        except Exception as exc:
            warnings.warn(f"Could not read evaluation results from {path}: {exc}", stacklevel=2)
            continue

    for path in sorted(
        glob.glob(str(root / "data/reports/phase2/*/hypotheses/*/evaluated_hypotheses.parquet"))
    ):
        parts = Path(path).parts
        run_id = parts[-4]
        symbol = parts[-2]
        try:
            for row in _extract_rows(pd.read_parquet(path), "phase2_hyp", run_id, run_id):
                row.setdefault("symbol", symbol)
                rows.append(row)
        except Exception as exc:
            warnings.warn(f"Could not read phase2 hypotheses from {path}: {exc}", stacklevel=2)
            continue

    for path in sorted(
        glob.glob(str(root / "data/reports/edge_candidates/*/edge_candidates_normalized.parquet"))
    ):
        run_id = Path(path).parts[-2]
        try:
            rows.extend(_extract_rows(pd.read_parquet(path), "edge_cand", run_id, run_id))
        except Exception as exc:
            warnings.warn(f"Could not read edge candidates from {path}: {exc}", stacklevel=2)
            continue

    return rows


def _norm(value: Any) -> str:
    if _is_missing(value):
        return ""
    return str(value).strip().lower()


def _matches(row: dict[str, Any], match: dict[str, Any]) -> bool:
    for key, value in match.items():
        row_value = row.get(key)
        if key == "event_type":
            row_value = row.get("event_id")
        if _norm(row_value) != _norm(value):
            return False
    return True


def load_manual_decisions(path: Path = DECISIONS_PATH) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    decisions = payload.get("decisions", []) if isinstance(payload, dict) else []
    return [decision for decision in decisions if isinstance(decision, dict)]


def attach_manual_decisions(
    rows: list[dict[str, Any]], decisions: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    out = [dict(row) for row in rows]
    matched_ids: set[int] = set()

    for row in out:
        for idx, decision in enumerate(decisions):
            match = decision.get("match", {})
            if not isinstance(match, dict) or not _matches(row, match):
                continue
            if (
                row.get("methodology_epoch") == "mechanism_backed"
                and "run_id" not in match
                and decision.get("applies_to_methodology_epoch") != "mechanism_backed"
            ):
                continue
            matched_ids.add(idx)
            row["evidence_class"] = str(decision.get("evidence_class", row["evidence_class"]))
            row["decision"] = str(decision.get("decision", row["decision"]))
            row["decision_reason"] = str(decision.get("decision_reason", row["decision_reason"]))
            row["next_safe_command"] = str(
                decision.get("next_safe_command", row["next_safe_command"])
            )
            row["forbidden_rescue_actions"] = list(
                decision.get("forbidden_rescue_actions", row["forbidden_rescue_actions"]) or []
            )
            row["manual_decision"] = True
            break

    for idx, decision in enumerate(decisions):
        if idx in matched_ids:
            continue
        match = decision.get("match", {})
        if not isinstance(match, dict):
            continue
        manual_row = normalize_result_row(
            {
                **match,
                "source_file": "manual",
                "run_id": decision.get("run_id", ""),
                "program_id": decision.get("program_id", "manual_decision"),
            }
        )
        manual_row.update(
            {
                "evidence_class": str(decision.get("evidence_class", manual_row["evidence_class"])),
                "decision": str(decision.get("decision", manual_row["decision"])),
                "decision_reason": str(
                    decision.get("decision_reason", manual_row["decision_reason"])
                ),
                "next_safe_command": str(
                    decision.get("next_safe_command", manual_row["next_safe_command"])
                ),
                "required_falsification": list(decision.get("required_falsification", []) or []),
                "forbidden_rescue_actions": list(
                    decision.get("forbidden_rescue_actions", []) or []
                ),
                "manual_decision": True,
            }
        )
        out.append(manual_row)

    return out


def attach_methodology_provenance(
    rows: list[dict[str, Any]],
    mechanism_metadata: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        updated = dict(row)
        run_id = str(updated.get("run_id", "") or "")
        metadata = mechanism_metadata.get(run_id)
        if metadata:
            updated["methodology_epoch"] = "mechanism_backed"
            updated["mechanism_id"] = str(metadata.get("mechanism_id", "") or "")
            updated["mechanism_version"] = str(metadata.get("mechanism_version", "") or "")
            updated["mechanism_preflight_status"] = str(
                metadata.get("mechanism_preflight_status", "") or ""
            )
            updated["mechanism_classification"] = str(
                metadata.get("mechanism_classification", "") or ""
            )
            updated["archive_reason"] = ""
            updated["required_falsification"] = list(metadata.get("required_falsification") or [])
            if metadata.get("forbidden_rescue_actions"):
                updated["forbidden_rescue_actions"] = list(
                    metadata.get("forbidden_rescue_actions") or []
                )
        else:
            updated.setdefault("methodology_epoch", "pre_mechanism")
            updated.setdefault("archive_reason", "pre_mechanism_methodology")
            if not updated.get("methodology_epoch"):
                updated["methodology_epoch"] = "pre_mechanism"
            if not updated.get("archive_reason"):
                updated["archive_reason"] = "pre_mechanism_methodology"
        out.append(updated)
    return out


def attach_active_research_flags(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    terminal_decisions = {"kill", "park", "archive"}
    terminal_classes = {"killed_candidate", "parked_candidate", "historical_result"}
    out: list[dict[str, Any]] = []
    for row in rows:
        updated = dict(row)
        methodology_epoch = str(updated.get("methodology_epoch", "") or "")
        preflight_status = str(updated.get("mechanism_preflight_status", "") or "")
        decision = str(updated.get("decision", "") or "")
        evidence_class = str(updated.get("evidence_class", "") or "")
        updated["active_research_candidate"] = (
            methodology_epoch == "mechanism_backed"
            and preflight_status == "pass"
            and decision not in terminal_decisions
            and evidence_class not in terminal_classes
        )
        if methodology_epoch == "pre_mechanism":
            updated["archive_reason"] = updated.get("archive_reason") or "pre_mechanism_methodology"
        out.append(updated)
    return out


def _doctor_to_row_fields(report: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    status = str(report.get("status", "") or "")
    if status == "validate_ready":
        if row.get("evidence_class") == "validate_ready":
            evidence_class = "validate_ready"
            decision = "validate"
        else:
            evidence_class = row.get("evidence_class", "candidate_signal")
            decision = row.get("decision", "review")
    elif status == "review_candidate":
        evidence_class = "review_only"
        decision = "review"
    elif status == "rejected":
        evidence_class = "killed_candidate"
        decision = "kill"
    else:
        evidence_class = "review_only"
        decision = "review"
    return {
        "evidence_class": evidence_class,
        "decision": decision,
        "decision_reason": (
            str(report.get("classification", "") or "")
            if evidence_class == "validate_ready" or status != "validate_ready"
            else str(row.get("decision_reason", "") or "")
        ),
        "next_safe_command": (
            str(report.get("next_safe_command", "") or "")
            if evidence_class == "validate_ready" or status != "validate_ready"
            else str(row.get("next_safe_command", "") or "")
        ),
        "forbidden_rescue_actions": list(report.get("forbidden_rescue_actions", []) or []),
    }


def attach_doctor_status(rows: list[dict[str, Any]], root: Path = ROOT) -> list[dict[str, Any]]:
    reports: dict[str, dict[str, Any] | None] = {}
    out: list[dict[str, Any]] = []
    for row in rows:
        updated = dict(row)
        run_id = str(updated.get("run_id", "") or "")
        if run_id and run_id not in reports:
            try:
                reports[run_id] = build_discover_doctor_report(
                    run_id=run_id, data_root=root / "data"
                )
            except Exception:
                reports[run_id] = None
        report = reports.get(run_id)
        if report is not None:
            updated.update(_doctor_to_row_fields(report, updated))
        out.append(updated)
    return out


def _dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    df = pd.DataFrame(rows)
    if df.empty:
        return []
    df["_source_prio"] = df["source_file"].map(SOURCE_PRIORITY).fillna(99)
    df["_t_sort"] = pd.to_numeric(df["t_stat_net"], errors="coerce")
    df = df.sort_values(["_source_prio", "_t_sort"], ascending=[True, False], na_position="last")
    subset = ["event_id", "direction", "horizon_bars", "template_id", "context", "symbol", "run_id"]
    df = df.drop_duplicates(subset=[col for col in subset if col in df.columns], keep="first")
    return df.drop(columns=["_source_prio", "_t_sort"]).to_dict(orient="records")


def attach_search_ledger_counts(
    rows: list[dict[str, Any]], ledger_path: Path = SEARCH_LEDGER_PARQUET_PATH
) -> list[dict[str, Any]]:
    if not rows or not ledger_path.exists():
        return rows
    try:
        ledger = pd.read_parquet(ledger_path)
    except Exception as exc:
        warnings.warn(f"Could not read search ledger from {ledger_path}: {exc}", stacklevel=2)
        return rows
    if ledger.empty or "nearby_attempt_count" not in ledger.columns:
        return rows

    key_columns = [
        "run_id",
        "event_id",
        "template_id",
        "context",
        "direction",
        "horizon_bars",
        "symbol",
    ]
    available = [column for column in key_columns if column in ledger.columns]
    counts: dict[tuple[str, ...], int] = {}
    for _, row in ledger.iterrows():
        key = tuple(_norm(row.get(column)) for column in available)
        counts[key] = max(counts.get(key, 0), _to_int(row.get("nearby_attempt_count")) or 0)

    out: list[dict[str, Any]] = []
    for row in rows:
        updated = dict(row)
        key = tuple(_norm(updated.get(column)) for column in available)
        updated["nearby_attempt_count"] = counts.get(key, 0)
        out.append(updated)
    return out


def attach_governed_reproduction_reports(
    rows: list[dict[str, Any]], reports_root: Path
) -> list[dict[str, Any]]:
    if not rows or not reports_root.exists():
        return rows
    reports: dict[str, dict[str, Any]] = {}
    for path in reports_root.glob("*/governed_reproduction.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
            warnings.warn(
                f"Could not read governed reproduction report {path}: {exc}", stacklevel=2
            )
            continue
        if isinstance(payload, dict):
            run_id = str(payload.get("reproduction_run_id", "") or path.parent.name)
            reports[run_id] = payload

    out: list[dict[str, Any]] = []
    for row in rows:
        updated = dict(row)
        report = reports.get(str(updated.get("run_id", "") or ""))
        if report is not None:
            normalized_reason = _governed_reproduction_decision_reason(report)
            updated["governed_reproduction_status"] = str(report.get("status", "") or "")
            updated["governed_reproduction_decision"] = str(report.get("decision", "") or "")
            updated["governed_reproduction_reason"] = normalized_reason
            if not bool(updated.get("manual_decision")):
                decision = str(report.get("decision", "") or "")
                status = str(report.get("status", "") or "")
                if status == "pass" and decision == "advance":
                    updated["decision"] = "review"
                    updated["decision_reason"] = "governed_reproduction_passed_pending_next_gate"
                elif decision in {"review", "park", "kill"}:
                    updated["decision"] = decision
                    updated["decision_reason"] = normalized_reason
                    if decision == "kill":
                        updated["evidence_class"] = "killed_candidate"
                    elif decision == "park":
                        updated["evidence_class"] = "parked_candidate"
        out.append(updated)
    return out


def _governed_reproduction_decision_reason(report: dict[str, Any]) -> str:
    decision = str(report.get("decision", "") or "")
    status = str(report.get("status", "") or "")
    if status == "fail" and decision == "kill":
        checks = report.get("blocking_checks", [])
        if isinstance(checks, list):
            failed_ids = {
                str(check.get("id", "") or "")
                for check in checks
                if isinstance(check, dict) and str(check.get("status", "") or "") == "fail"
            }
            if "t_stat_above_research_floor" in failed_ids:
                return "governed_reproduction_negative_t_stat"
        reproduction = report.get("reproduction", {})
        if isinstance(reproduction, dict):
            t_stat = _to_float(reproduction.get("t_stat_net"))
            if t_stat is not None and t_stat < 0.0:
                return "governed_reproduction_negative_t_stat"
        return "governed_reproduction_failed"
    return str(report.get("reason", "") or "")


def attach_year_split_reports(
    rows: list[dict[str, Any]], reports_root: Path
) -> list[dict[str, Any]]:
    if not rows or not reports_root.exists():
        return rows
    reports: dict[str, dict[str, Any]] = {}
    for path in reports_root.glob("*/*_year_split.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
            warnings.warn(f"Could not read year split report {path}: {exc}", stacklevel=2)
            continue
        if isinstance(payload, dict):
            run_id = str(payload.get("run_id", "") or path.parent.name)
            reports[run_id] = payload

    out: list[dict[str, Any]] = []
    for row in rows:
        updated = dict(row)
        report = reports.get(str(updated.get("run_id", "") or ""))
        if report is not None:
            updated["year_split_status"] = str(report.get("status", "") or "")
            updated["year_split_classification"] = str(report.get("classification", "") or "")
            updated["year_split_reason"] = str(report.get("reason", "") or "")
            if not bool(updated.get("manual_decision")):
                classification = str(report.get("classification", "") or "")
                decision = str(report.get("decision", "") or "")
                if classification == "year_conditional":
                    updated["decision"] = decision if decision in {"park", "kill"} else "park"
                    updated["decision_reason"] = str(report.get("reason", "") or "")
                elif decision in {"review", "monitor", "park", "kill"}:
                    updated["decision"] = decision
                    updated["decision_reason"] = str(report.get("reason", "") or "")
        out.append(updated)
    return out


def attach_specificity_reports(
    rows: list[dict[str, Any]], reports_root: Path
) -> list[dict[str, Any]]:
    if not rows or not reports_root.exists():
        return rows
    reports: dict[tuple[str, str], dict[str, Any]] = {}
    run_level_reports: dict[str, dict[str, Any]] = {}
    for path in reports_root.glob("*/*_specificity.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
            warnings.warn(f"Could not read specificity report {path}: {exc}", stacklevel=2)
            continue
        if isinstance(payload, dict):
            run_id = str(payload.get("run_id", "") or path.parent.name)
            candidate_id = str(payload.get("candidate_id", "") or "")
            if candidate_id:
                reports[(run_id, candidate_id)] = payload
            run_level_reports[run_id] = payload

    out: list[dict[str, Any]] = []
    for row in rows:
        updated = dict(row)
        run_id = str(updated.get("run_id", "") or "")
        candidate_id = str(updated.get("candidate_id", "") or "")
        report = reports.get((run_id, candidate_id)) or run_level_reports.get(run_id)
        if report is not None:
            updated["specificity_status"] = str(report.get("status", "") or "")
            updated["specificity_classification"] = str(report.get("classification", "") or "")
            updated["specificity_reason"] = str(report.get("reason", "") or "")
            updated["specificity_decision"] = str(report.get("decision", "") or "")
            if not bool(updated.get("manual_decision")):
                decision = str(report.get("decision", "") or "")
                classification = str(report.get("classification", "") or "")
                year_classification = str(updated.get("year_split_classification", "") or "")
                if (
                    classification == "context_proxy"
                    and year_classification == "year_conditional"
                    and updated.get("decision") == "park"
                ):
                    updated["decision_reason"] = "context_proxy_and_year_pnl_concentration_2022"
                    updated["next_safe_command"] = str(report.get("next_safe_command", "") or "")
                elif updated.get("decision") in {"park", "kill"}:
                    pass
                elif classification == "insufficient_trace_data":
                    updated["decision"] = "review"
                    updated["decision_reason"] = "specificity_insufficient_trace_data"
                    updated["next_safe_command"] = str(report.get("next_safe_command", "") or "")
                elif decision in {"advance", "review", "park", "kill"}:
                    updated["decision"] = "review" if decision == "advance" else decision
                    updated["decision_reason"] = str(report.get("reason", "") or "")
                    updated["next_safe_command"] = str(report.get("next_safe_command", "") or "")
        out.append(updated)
    return out


def _clean_records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cleaned: list[dict[str, Any]] = []
    for row in rows:
        item: dict[str, Any] = {}
        for column in RESULT_COLUMNS:
            value = row.get(column)
            if isinstance(value, float) and pd.isna(value):
                value = None
            elif _is_missing(value) and column not in {"forbidden_rescue_actions"}:
                value = None if column in {"horizon_bars", "n_obs", "event_count"} else ""
            if column == "forbidden_rescue_actions":
                value = list(value or [])
            if column == "required_falsification":
                value = list(value or [])
            if column == "manual_decision":
                value = bool(value) if not _is_missing(value) else False
            if column == "active_research_candidate":
                value = bool(value) if not _is_missing(value) else False
            if column == "nearby_attempt_count":
                value = _to_int(value) or 0
            item[column] = value
        cleaned.append(item)
    return cleaned


def build_results_index(root: Path = ROOT, decisions_path: Path = DECISIONS_PATH) -> pd.DataFrame:
    normalized = [normalize_result_row(row) for row in collect_result_rows(root)]
    normalized = [row for row in normalized if row.get("event_id")]
    mechanism_metadata = collect_mechanism_metadata(root)
    with_initial_provenance = attach_methodology_provenance(normalized, mechanism_metadata)
    with_doctor = attach_doctor_status(with_initial_provenance, root=root)
    with_manual = attach_manual_decisions(with_doctor, load_manual_decisions(decisions_path))
    with_search_counts = attach_search_ledger_counts(
        _dedupe_rows(with_manual),
        root / "data" / "reports" / "search_ledger" / "search_burden.parquet",
    )
    with_reproduction = attach_governed_reproduction_reports(
        with_search_counts,
        root / "data" / "reports" / "reproduction",
    )
    with_year_split = attach_year_split_reports(
        with_reproduction,
        root / "data" / "reports" / "regime",
    )
    with_specificity = attach_specificity_reports(
        with_year_split,
        root / "data" / "reports" / "specificity",
    )
    with_final_provenance = attach_methodology_provenance(with_specificity, mechanism_metadata)
    with_active_flags = attach_active_research_flags(with_final_provenance)
    rows = _clean_records(with_active_flags)
    return pd.DataFrame(rows, columns=RESULT_COLUMNS).sort_values(
        ["event_id", "direction", "horizon_bars", "t_stat_net"],
        ascending=[True, True, True, False],
        na_position="last",
    )


def write_results_index_json(df: pd.DataFrame, path: Path = RESULTS_JSON_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "results_index_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "row_count": len(df),
        "rows": _clean_records(df.to_dict(orient="records")),
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_results_index_parquet(df: pd.DataFrame, path: Path = RESULTS_PARQUET_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def _fmt(value: Any, fmt: str) -> str:
    if _is_missing(value):
        return "-"
    try:
        return fmt.format(float(value))
    except Exception:
        return str(value)


def _select_best_row(group: pd.DataFrame) -> pd.Series:
    priority = {
        "confirmed_edge": 0,
        "research_edge": 1,
        "parked_candidate": 2,
        "killed_candidate": 3,
        "validate_ready": 4,
        "candidate_signal": 5,
        "review_only": 6,
    }
    ranked = group.assign(
        _evidence_prio=group["evidence_class"].map(priority).fillna(99),
        _manual_prio=group["manual_decision"].map(lambda value: 0 if value is True else 1),
        _t_sort=pd.to_numeric(group["t_stat_net"], errors="coerce"),
    ).sort_values(
        ["_evidence_prio", "_manual_prio", "_t_sort"],
        ascending=[True, True, False],
        na_position="last",
    )
    return ranked.iloc[0]


def render_results_markdown(df: pd.DataFrame) -> str:
    n_events = int(df["event_id"].nunique()) if not df.empty else 0
    lines = [
        "# All Results - Edge Discovery Project",
        "",
        "*Auto-generated. Do not edit manually - rerun `project/scripts/update_results_index.py`.*",
        f"*{len(df)} indexed rows across {n_events} events.*",
        "*Decision fields come from discover-doctor plus `docs/research/decisions.yaml`.*",
        "*`year_split_event_support_pass` means event support is not dominated by one year; it is not PnL stability unless per-event returns are available.*",
        "",
        "## Summary - Decision Row Per Event",
        "",
        "| Event | Dir | Horizon | Template | t | rob | q | net bps | Evidence | Decision | Reason |",
        "|---|---|---:|---|---:|---:|---:|---:|---|---|---|",
    ]

    if not df.empty:
        for _, row in (
            pd.DataFrame([_select_best_row(group) for _, group in df.groupby("event_id")])
            .sort_values("event_id")
            .iterrows()
        ):
            lines.append(
                f"| {row.get('event_id', '')} | {row.get('direction', '')} | "
                f"{_fmt(row.get('horizon_bars'), '{:.0f}')} | {row.get('template_id', '')} | "
                f"{_fmt(row.get('t_stat_net'), '{:.2f}')} | "
                f"{_fmt(row.get('robustness_score'), '{:.3f}')} | "
                f"{_fmt(row.get('q_value'), '{:.4f}')} | "
                f"{_fmt(row.get('mean_return_net_bps'), '{:.1f}')} | "
                f"{row.get('evidence_class', '')} | {row.get('decision', '')} | "
                f"{row.get('decision_reason', '')} |"
            )

    lines.extend(["", "## Active Mechanism-Backed Candidates", ""])
    lines.extend(
        [
            "| Mechanism | Event | Symbol | Context | Dir | Horizon | Template | Evidence | Decision | Run |",
            "|---|---|---|---|---|---:|---|---|---|---|",
        ]
    )
    active_df = df[df["active_research_candidate"]] if not df.empty else df
    for _, row in active_df.iterrows():
        run_id = str(row.get("run_id", "") or "")
        if len(run_id) > 42:
            run_id = run_id[:42] + "..."
        lines.append(
            f"| {row.get('mechanism_id', '')} | {row.get('event_id', '')} | "
            f"{row.get('symbol', '')} | {row.get('context', '')} | "
            f"{row.get('direction', '')} | {_fmt(row.get('horizon_bars'), '{:.0f}')} | "
            f"{row.get('template_id', '')} | {row.get('evidence_class', '')} | "
            f"{row.get('decision', '')} | `{run_id}` |"
        )

    lines.extend(["", "## Full Index", ""])
    lines.extend(
        [
            "| Epoch | Mechanism | Active | Event | Symbol | Context | Dir | Horizon | Template | n | events | t | net bps | nearby | Evidence | Decision | Run |",
            "|---|---|---|---|---|---|---|---:|---|---:|---:|---:|---:|---:|---|---|---|",
        ]
    )
    for _, row in df.iterrows():
        run_id = str(row.get("run_id", "") or "")
        if len(run_id) > 42:
            run_id = run_id[:42] + "..."
        lines.append(
            f"| {row.get('methodology_epoch', '')} | {row.get('mechanism_id', '')} | "
            f"{row.get('active_research_candidate', '')} | "
            f"{row.get('event_id', '')} | {row.get('symbol', '')} | {row.get('context', '')} | "
            f"{row.get('direction', '')} | {_fmt(row.get('horizon_bars'), '{:.0f}')} | "
            f"{row.get('template_id', '')} | {_fmt(row.get('n_obs'), '{:.0f}')} | "
            f"{_fmt(row.get('event_count'), '{:.0f}')} | {_fmt(row.get('t_stat_net'), '{:.2f}')} | "
            f"{_fmt(row.get('mean_return_net_bps'), '{:.1f}')} | "
            f"{_fmt(row.get('nearby_attempt_count'), '{:.0f}')} | "
            f"{row.get('evidence_class', '')} | {row.get('decision', '')} | `{run_id}` |"
        )
    lines.append("")
    return "\n".join(lines)


def write_results_markdown(df: pd.DataFrame, path: Path = RESULTS_MD_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_results_markdown(df), encoding="utf-8")


def update_results_index(root: Path = ROOT) -> pd.DataFrame:
    df = build_results_index(root=root)
    write_results_index_json(df, root / "data" / "reports" / "results" / "results_index.json")
    write_results_index_parquet(df, root / "data" / "reports" / "results" / "results_index.parquet")
    write_results_markdown(df, root / "docs" / "research" / "results.md")
    return df
