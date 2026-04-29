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

from project.scripts.discover_doctor import build_discover_doctor_report

warnings.filterwarnings("ignore")

ROOT = Path(__file__).resolve().parents[2]
DECISIONS_PATH = ROOT / "docs" / "research" / "decisions.yaml"
RESULTS_DIR = ROOT / "data" / "reports" / "results"
RESULTS_JSON_PATH = RESULTS_DIR / "results_index.json"
RESULTS_PARQUET_PATH = RESULTS_DIR / "results_index.parquet"
RESULTS_MD_PATH = ROOT / "docs" / "research" / "results.md"

RESULT_COLUMNS = [
    "run_id",
    "program_id",
    "candidate_id",
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
    "forbidden_rescue_actions",
    "manual_decision",
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
        "forbidden_rescue_actions": [],
        "manual_decision": False,
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
                "forbidden_rescue_actions": list(
                    decision.get("forbidden_rescue_actions", []) or []
                ),
                "manual_decision": True,
            }
        )
        out.append(manual_row)

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
            if column == "manual_decision":
                value = bool(value) if not _is_missing(value) else False
            item[column] = value
        cleaned.append(item)
    return cleaned


def build_results_index(root: Path = ROOT, decisions_path: Path = DECISIONS_PATH) -> pd.DataFrame:
    normalized = [normalize_result_row(row) for row in collect_result_rows(root)]
    normalized = [row for row in normalized if row.get("event_id")]
    with_doctor = attach_doctor_status(normalized, root=root)
    with_manual = attach_manual_decisions(with_doctor, load_manual_decisions(decisions_path))
    rows = _clean_records(_dedupe_rows(with_manual))
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

    lines.extend(["", "## Full Index", ""])
    lines.extend(
        [
            "| Event | Symbol | Context | Dir | Horizon | Template | n | events | t | net bps | Evidence | Decision | Run |",
            "|---|---|---|---|---:|---|---:|---:|---:|---:|---|---|---|",
        ]
    )
    for _, row in df.iterrows():
        run_id = str(row.get("run_id", "") or "")
        if len(run_id) > 42:
            run_id = run_id[:42] + "..."
        lines.append(
            f"| {row.get('event_id', '')} | {row.get('symbol', '')} | {row.get('context', '')} | "
            f"{row.get('direction', '')} | {_fmt(row.get('horizon_bars'), '{:.0f}')} | "
            f"{row.get('template_id', '')} | {_fmt(row.get('n_obs'), '{:.0f}')} | "
            f"{_fmt(row.get('event_count'), '{:.0f}')} | {_fmt(row.get('t_stat_net'), '{:.2f}')} | "
            f"{_fmt(row.get('mean_return_net_bps'), '{:.1f}')} | "
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
