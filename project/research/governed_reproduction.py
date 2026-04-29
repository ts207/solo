from __future__ import annotations

import json
import warnings
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from project.research import results_index

FORBIDDEN_RESCUE_ACTIONS = [
    "change_horizon",
    "drop_bad_years",
    "loosen_gates",
    "switch_context_without_mechanism",
]

RESEARCH_T_STAT_FLOOR = 1.0


@dataclass
class Evidence:
    event_count: int | None
    context_event_count: int | None
    t_stat_net: float | None
    mean_return_net_bps: float | None
    robustness_score: float | None
    bridge_candidate_written: bool


@dataclass
class ReproductionEvidence(Evidence):
    estimated_hypothesis_count: int | None
    valid_metrics_rows: int | None
    bridge_candidates_rows: int | None


def _data_root(path: Path | str | None) -> Path:
    return Path(path or "data")


def _repo_root(data_root: Path) -> Path:
    return data_root.parent if data_root.name == "data" else Path(".")


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _to_float(value: Any) -> float | None:
    return results_index._to_float(value)


def _to_int(value: Any) -> int | None:
    return results_index._to_int(value)


def _first_present(row: dict[str, Any] | pd.Series, columns: list[str]) -> Any:
    return results_index._first_present(row, columns)


def _round(value: float | None) -> float | None:
    return None if value is None else round(float(value), 4)


def _phase2_dir(data_root: Path, run_id: str) -> Path:
    return data_root / "reports" / "phase2" / run_id


def _validated_plan(data_root: Path, run_id: str) -> dict[str, Any]:
    experiments_root = data_root / "artifacts" / "experiments"
    for path in experiments_root.glob(f"*/{run_id}/validated_plan.json"):
        payload = _load_json(path)
        if payload:
            return payload
    return {}


def _load_diagnostics(data_root: Path, run_id: str) -> dict[str, Any]:
    return _load_json(_phase2_dir(data_root, run_id) / "phase2_diagnostics.json")


def _read_table(path: Path) -> pd.DataFrame:
    try:
        return pd.read_parquet(path)
    except Exception as exc:
        warnings.warn(f"Could not read governed reproduction table {path}: {exc}", stacklevel=2)
        return pd.DataFrame()


def _candidate_frame(data_root: Path, run_id: str) -> pd.DataFrame:
    phase2 = _phase2_dir(data_root, run_id)
    candidates = _read_table(phase2 / "phase2_candidates.parquet")
    if not candidates.empty:
        return candidates

    frames: list[pd.DataFrame] = []
    for path in phase2.glob("hypotheses/*/evaluated_hypotheses.parquet"):
        frame = _read_table(path)
        if frame.empty:
            continue
        if "symbol" not in frame.columns:
            frame = frame.assign(symbol=path.parent.name)
        frames.append(frame)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "pass", "passed", "tradable"}


def _select_candidate(df: pd.DataFrame, candidate_id: str | None) -> dict[str, Any] | None:
    if df.empty:
        return None
    selected = df.copy()
    if candidate_id and "candidate_id" in selected.columns:
        matched = selected[selected["candidate_id"].astype(str) == str(candidate_id)]
        if not matched.empty:
            selected = matched
    if "gate_bridge_tradable" in selected.columns:
        bridge = selected[selected["gate_bridge_tradable"].map(_bool_value)]
        if not bridge.empty:
            selected = bridge
    score_column = "t_stat_net" if "t_stat_net" in selected.columns else "t_stat"
    if score_column in selected.columns:
        selected = selected.assign(_score=pd.to_numeric(selected[score_column], errors="coerce"))
        selected = selected.sort_values("_score", ascending=False, na_position="last")
    return selected.iloc[0].to_dict()


def _metric_event_count(row: dict[str, Any] | None) -> int | None:
    if row is None:
        return None
    return _to_int(_first_present(row, ["event_count", "n_events", "sample_size", "n"]))


def _metric_net_bps(row: dict[str, Any] | None) -> float | None:
    if row is None:
        return None
    return _to_float(
        _first_present(
            row,
            [
                "mean_return_net_bps",
                "after_cost_expectancy_bps",
                "cost_adjusted_return_bps",
                "mean_return_bps",
            ],
        )
    )


def _candidate_bridge_written(row: dict[str, Any] | None, diagnostics: dict[str, Any]) -> bool:
    if int(diagnostics.get("bridge_candidates_rows", 0) or 0) > 0:
        return True
    if row is None:
        return False
    return _bool_value(row.get("gate_bridge_tradable")) or str(
        row.get("bridge_eval_status", "")
    ).lower() in {
        "tradable",
        "bridge",
    }


def load_source_evidence(
    *, run_id: str, data_root: Path, candidate_id: str | None = None
) -> Evidence:
    diagnostics = _load_diagnostics(data_root, run_id)
    row = _select_candidate(_candidate_frame(data_root, run_id), candidate_id)
    return Evidence(
        event_count=_metric_event_count(row),
        context_event_count=None,
        t_stat_net=_round(_to_float(_first_present(row or {}, ["t_stat_net", "t_stat"]))),
        mean_return_net_bps=_round(_metric_net_bps(row)),
        robustness_score=_round(_to_float((row or {}).get("robustness_score"))),
        bridge_candidate_written=_candidate_bridge_written(row, diagnostics),
    )


def load_reproduction_evidence(
    *, run_id: str, data_root: Path, candidate_id: str | None = None
) -> ReproductionEvidence:
    diagnostics = _load_diagnostics(data_root, run_id)
    plan = _validated_plan(data_root, run_id)
    row = _select_candidate(_candidate_frame(data_root, run_id), candidate_id)
    return ReproductionEvidence(
        estimated_hypothesis_count=_to_int(
            plan.get("estimated_hypothesis_count")
            if plan
            else diagnostics.get("hypotheses_generated")
        ),
        valid_metrics_rows=_to_int(diagnostics.get("valid_metrics_rows")),
        bridge_candidates_rows=_to_int(diagnostics.get("bridge_candidates_rows")),
        event_count=_metric_event_count(row),
        context_event_count=None,
        t_stat_net=_round(_to_float(_first_present(row or {}, ["t_stat_net", "t_stat"]))),
        mean_return_net_bps=_round(_metric_net_bps(row)),
        robustness_score=_round(_to_float((row or {}).get("robustness_score"))),
        bridge_candidate_written=_candidate_bridge_written(row, diagnostics),
    )


def _pct_delta(source: int | float | None, reproduction: int | float | None) -> float | None:
    if source in (None, 0) or reproduction is None:
        return None
    return round(((float(reproduction) - float(source)) / abs(float(source))) * 100, 4)


def _delta(source: float | None, reproduction: float | None) -> float | None:
    if source is None or reproduction is None:
        return None
    return round(float(reproduction) - float(source), 4)


def _check(check_id: str, passed: bool | None, detail: str) -> dict[str, str]:
    status = "unknown" if passed is None else "pass" if passed else "fail"
    return {"id": check_id, "status": status, "detail": detail}


def _sign(value: float | None) -> int | None:
    if value is None:
        return None
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def _result_context(
    *, data_root: Path, reproduction_run_id: str, candidate_id: str | None
) -> dict[str, Any]:
    path = data_root / "reports" / "results" / "results_index.json"
    payload = _load_json(path)
    rows = payload.get("rows", []) if isinstance(payload.get("rows"), list) else []
    matches = [
        row
        for row in rows
        if row.get("run_id") == reproduction_run_id
        and (not candidate_id or row.get("candidate_id") == candidate_id)
    ]
    if not matches:
        return {}
    matches = sorted(
        matches,
        key=lambda row: (
            not bool(row.get("manual_decision")),
            int(row.get("nearby_attempt_count") or 0),
            -float(row.get("t_stat_net") or -999),
        ),
    )
    return matches[0]


def _classify(
    *,
    source: Evidence,
    reproduction: ReproductionEvidence,
    deltas: dict[str, float | None],
) -> tuple[str, str, str, list[dict[str, str]]]:
    checks = [
        _check(
            "estimated_hypothesis_count_positive",
            None
            if reproduction.estimated_hypothesis_count is None
            else reproduction.estimated_hypothesis_count > 0,
            f"estimated_hypothesis_count={reproduction.estimated_hypothesis_count}",
        ),
        _check(
            "valid_metrics_rows_positive",
            None
            if reproduction.valid_metrics_rows is None
            else reproduction.valid_metrics_rows > 0,
            f"valid_metrics_rows={reproduction.valid_metrics_rows}",
        ),
        _check(
            "event_count_positive",
            None if reproduction.event_count is None else reproduction.event_count > 0,
            f"event_count={reproduction.event_count}",
        ),
        _check(
            "bridge_candidate_written",
            reproduction.bridge_candidate_written,
            f"bridge_candidates_rows={reproduction.bridge_candidates_rows}",
        ),
        _check(
            "net_sign_preserved",
            None
            if source.mean_return_net_bps is None or reproduction.mean_return_net_bps is None
            else _sign(source.mean_return_net_bps) == _sign(reproduction.mean_return_net_bps),
            f"source_net_bps={source.mean_return_net_bps}; reproduction_net_bps={reproduction.mean_return_net_bps}",
        ),
        _check(
            "event_count_delta_within_30pct",
            None
            if deltas["event_count_delta_pct"] is None
            else abs(float(deltas["event_count_delta_pct"])) <= 30.0,
            f"event_count_delta_pct={deltas['event_count_delta_pct']}",
        ),
        _check(
            "t_stat_above_research_floor",
            None
            if reproduction.t_stat_net is None
            else reproduction.t_stat_net >= RESEARCH_T_STAT_FLOOR,
            f"t_stat_net={reproduction.t_stat_net}; floor={RESEARCH_T_STAT_FLOOR}",
        ),
    ]

    blocking_ids = {
        "estimated_hypothesis_count_positive",
        "valid_metrics_rows_positive",
        "event_count_positive",
    }
    if any(check["id"] in blocking_ids and check["status"] == "fail" for check in checks):
        return "blocked", "review", "current governed evidence is mechanically incomplete", checks

    fail_ids = {
        "bridge_candidate_written",
        "net_sign_preserved",
        "event_count_delta_within_30pct",
        "t_stat_above_research_floor",
    }
    if any(check["id"] in fail_ids and check["status"] == "fail" for check in checks):
        return (
            "fail",
            "kill",
            "current governed reproduction failed one or more falsification checks",
            checks,
        )

    if source.event_count is None or source.t_stat_net is None:
        return (
            "unknown",
            "review",
            "source evidence missing but current governed evidence exists",
            checks,
        )

    if all(check["status"] in {"pass", "unknown"} for check in checks):
        return "pass", "advance", "current governed reproduction passed v1 checks", checks

    return "unknown", "review", "insufficient evidence to classify governed reproduction", checks


def build_governed_reproduction_report(
    *,
    source_run_id: str,
    reproduction_run_id: str,
    candidate_id: str | None = None,
    data_root: Path | str | None = None,
) -> dict[str, Any]:
    resolved_data_root = _data_root(data_root)
    source = load_source_evidence(
        run_id=source_run_id, data_root=resolved_data_root, candidate_id=candidate_id
    )
    reproduction = load_reproduction_evidence(
        run_id=reproduction_run_id, data_root=resolved_data_root, candidate_id=candidate_id
    )
    deltas = {
        "event_count_delta_pct": _pct_delta(source.event_count, reproduction.event_count),
        "context_event_count_delta_pct": _pct_delta(
            source.context_event_count, reproduction.context_event_count
        ),
        "t_stat_delta": _delta(source.t_stat_net, reproduction.t_stat_net),
        "net_bps_delta": _delta(source.mean_return_net_bps, reproduction.mean_return_net_bps),
        "robustness_delta": _delta(source.robustness_score, reproduction.robustness_score),
    }
    status, decision, reason, checks = _classify(
        source=source, reproduction=reproduction, deltas=deltas
    )

    context = _result_context(
        data_root=resolved_data_root,
        reproduction_run_id=reproduction_run_id,
        candidate_id=candidate_id,
    )
    nearby = _to_int(context.get("nearby_attempt_count"))
    decision_reason = str(context.get("decision_reason", "") or "")
    if status == "pass" and (nearby or decision_reason):
        decision = "review"
        parts = ["current governed reproduction passed v1 checks"]
        if nearby is not None:
            parts.append(f"nearby_attempt_count={nearby}")
        if decision_reason:
            parts.append(decision_reason)
        reason = "; ".join(parts)

    next_safe_command = (
        "Run year split before validation or promotion."
        if decision == "review"
        else "Proceed to the next governed falsification gate."
        if decision == "advance"
        else "Write kill/park decision and do not rescue by widening the search surface."
    )

    return {
        "schema_version": "governed_reproduction_v1",
        "source_run_id": source_run_id,
        "reproduction_run_id": reproduction_run_id,
        "candidate_id": candidate_id or str(context.get("candidate_id", "") or ""),
        "status": status,
        "decision": decision,
        "reason": reason,
        "source": asdict(source),
        "reproduction": asdict(reproduction),
        "deltas": deltas,
        "blocking_checks": checks,
        "next_safe_command": next_safe_command,
        "forbidden_rescue_actions": FORBIDDEN_RESCUE_ACTIONS,
    }


def report_path(data_root: Path | str | None, reproduction_run_id: str) -> Path:
    return (
        _data_root(data_root)
        / "reports"
        / "reproduction"
        / reproduction_run_id
        / "governed_reproduction.json"
    )


def write_governed_reproduction_report(report: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_governed_reproduction(
    *,
    source_run_id: str,
    reproduction_run_id: str,
    candidate_id: str | None = None,
    data_root: Path | str | None = None,
) -> dict[str, Any]:
    report = build_governed_reproduction_report(
        source_run_id=source_run_id,
        reproduction_run_id=reproduction_run_id,
        candidate_id=candidate_id,
        data_root=data_root,
    )
    write_governed_reproduction_report(report, report_path(data_root, reproduction_run_id))
    return report
