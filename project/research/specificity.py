from __future__ import annotations

import ast
import glob
import json
import math
import re
import warnings
from pathlib import Path
from typing import Any

import pandas as pd

from project.research import results_index
from project.research.year_split import _safe_name

SCHEMA_VERSION = "specificity_v1"
TRACE_REQUIRED_TESTS = {"base", "event_only", "context_only", "opposite_direction"}
TRACE_RETURN_COLUMNS = (
    "net_return_bps",
    "return_net_bps",
    "forward_return_net_bps",
    "signed_return_bps",
    "mean_return_net_bps",
    "forward_return",
)
FORBIDDEN_RESCUE_ACTIONS = [
    "change_horizon",
    "drop_bad_years",
    "loosen_gates",
    "switch_context_without_mechanism",
]


def _data_root(path: Path | str | None) -> Path:
    return Path(path or "data")


def _phase2_dir(data_root: Path, run_id: str) -> Path:
    return data_root / "reports" / "phase2" / run_id


def _read_table(path: Path) -> pd.DataFrame:
    try:
        return pd.read_parquet(path)
    except Exception as exc:
        warnings.warn(f"Could not read specificity table {path}: {exc}", stacklevel=2)
        return pd.DataFrame()


def _to_float(value: Any) -> float | None:
    return results_index._to_float(value)


def _to_int(value: Any) -> int | None:
    return results_index._to_int(value)


def _round(value: float | None, digits: int = 4) -> float | None:
    return None if value is None else round(float(value), digits)


def _norm(value: Any) -> str:
    if results_index._is_missing(value):
        return ""
    return str(value).strip().lower()


def _candidate_frame(data_root: Path, run_id: str) -> pd.DataFrame:
    return _read_table(_phase2_dir(data_root, run_id) / "phase2_candidates.parquet")


def _evaluated_hypotheses_frame(data_root: Path, run_id: str) -> pd.DataFrame:
    frames = []
    for path in sorted((_phase2_dir(data_root, run_id) / "hypotheses").glob("*/evaluated_hypotheses.parquet")):
        frame = _read_table(path)
        if not frame.empty and "symbol" not in frame.columns:
            frame = frame.assign(symbol=path.parent.name)
        frames.append(frame)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _evaluation_results_frame(data_root: Path, run_id: str) -> pd.DataFrame:
    pattern = str(
        data_root.parent
        / "data"
        / "artifacts"
        / "experiments"
        / "*"
        / run_id
        / "evaluation_results.parquet"
    )
    frames = [_read_table(Path(path)) for path in sorted(glob.glob(pattern))]
    return pd.concat([frame for frame in frames if not frame.empty], ignore_index=True) if frames else pd.DataFrame()


def _parse_context_payload(value: Any) -> str:
    if results_index._is_missing(value):
        return ""
    if isinstance(value, dict):
        items = value.items()
    else:
        text = str(value).strip()
        if not text:
            return ""
        parsed: Any
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            try:
                parsed = ast.literal_eval(text)
            except (SyntaxError, ValueError):
                return text.upper()
        if not isinstance(parsed, dict):
            return text.upper()
        items = parsed.items()
    parts = []
    for key, raw in sorted(items):
        if results_index._is_missing(raw):
            continue
        parts.append(f"{str(key).upper()}={str(raw).upper()}")
    return ",".join(parts)


def _row_event_id(row: dict[str, Any] | pd.Series) -> str:
    event = results_index._event_from_row(row)
    if event:
        return event
    trigger_key = str(row.get("trigger_key", "") or "")
    return trigger_key.split(":", 1)[1] if trigger_key.startswith("event:") else ""


def _row_context(row: dict[str, Any] | pd.Series) -> str:
    key = str(row.get("context_key", "") or "").strip()
    value = str(row.get("context_value", "") or "").strip()
    if key or value:
        return f"{key.upper()}={value.upper()}" if key and value else value.upper()
    for column in ("context", "context_cell", "context_signature", "context_slice"):
        value = row.get(column)
        parsed = _parse_context_payload(value)
        if parsed:
            return parsed
    return results_index._context_from_row(row)


def _horizon_bars(value: Any) -> int | None:
    return results_index._horizon_bars(value)


def _select_row(df: pd.DataFrame, candidate_id: str | None) -> dict[str, Any]:
    if df.empty:
        return {}
    selected = df.copy()
    if candidate_id:
        masks = []
        for column in ("candidate_id", "hypothesis_id", "strategy_id"):
            if column in selected.columns:
                masks.append(selected[column].astype(str) == str(candidate_id))
        if masks:
            mask = masks[0]
            for next_mask in masks[1:]:
                mask = mask | next_mask
            matched = selected[mask].copy()
            if not matched.empty:
                selected = matched
    score_column = "t_stat_net" if "t_stat_net" in selected.columns else "t_stat"
    if score_column in selected.columns:
        selected = selected.assign(_score=pd.to_numeric(selected[score_column], errors="coerce"))
        selected = selected.sort_values("_score", ascending=False, na_position="last")
    return selected.iloc[0].to_dict()


def _candidate_ids(row: dict[str, Any], candidate_id: str | None) -> set[str]:
    ids = {str(candidate_id or "").strip()}
    for column in ("candidate_id", "hypothesis_id", "strategy_id"):
        value = str(row.get(column, "") or "").strip()
        if value:
            ids.add(value)
    return {value for value in ids if value}


def _candidate_profile(
    data_root: Path,
    run_id: str,
    candidate_id: str | None,
) -> tuple[dict[str, Any], set[str]]:
    primary = _select_row(_candidate_frame(data_root, run_id), candidate_id)
    ids = _candidate_ids(primary, candidate_id)
    supplements = [
        _select_row(_evaluated_hypotheses_frame(data_root, run_id), next(iter(ids), candidate_id)),
        _select_row(_evaluation_results_frame(data_root, run_id), next(iter(ids), candidate_id)),
    ]
    merged = dict(primary)
    for row in supplements:
        for key, value in row.items():
            if results_index._is_missing(merged.get(key)) and not results_index._is_missing(value):
                merged[key] = value
    ids = _candidate_ids(merged, candidate_id)
    event_id = _row_event_id(merged)
    context = _row_context(merged)
    template_id = str(
        results_index._first_present(merged, ["template_id", "rule_template", "template"]) or ""
    )
    direction = str(merged.get("direction", "") or "")
    horizon_bars = _horizon_bars(
        results_index._first_present(merged, ["horizon_bars", "horizon", "horizon_label"])
    )
    resolved_candidate_id = (
        str(candidate_id or "")
        or str(merged.get("candidate_id", "") or "")
        or str(merged.get("hypothesis_id", "") or "")
    )
    return (
        {
            "candidate_id": resolved_candidate_id,
            "event_id": event_id,
            "context": context,
            "template_id": template_id,
            "direction": direction,
            "horizon_bars": horizon_bars,
            "entry_lag_bars": _to_int(
                results_index._first_present(merged, ["entry_lag_bars", "entry_lag"])
            ),
            "event_count": _to_int(results_index._first_present(merged, ["event_count", "n_events", "n"])),
            "t_stat_net": _to_float(results_index._first_present(merged, ["t_stat_net", "t_stat"])),
        },
        ids,
    )


def _trace_paths(data_root: Path, run_id: str) -> list[Path]:
    phase2 = _phase2_dir(data_root, run_id)
    paths = [
        data_root
        / "reports"
        / "candidate_traces"
        / run_id
        / "*_traces.parquet",
        phase2 / "edge_cell_pnl_traces.parquet",
        phase2 / "edge_cell_trigger_traces.parquet",
        phase2 / "phase2_candidate_event_timestamps.parquet",
    ]
    expanded: list[Path] = []
    for path in paths:
        if "*" in str(path):
            expanded.extend(Path(item) for item in sorted(glob.glob(str(path))))
        else:
            expanded.append(path)
    expanded.extend(
        Path(path)
        for path in sorted(
            glob.glob(str(data_root / "artifacts" / "experiments" / "*" / run_id / "*trace*.parquet"))
        )
    )
    return expanded


def _load_trace_frame(data_root: Path, run_id: str) -> pd.DataFrame:
    frames = []
    for path in _trace_paths(data_root, run_id):
        if not path.exists():
            continue
        frame = _read_table(path)
        if not frame.empty:
            frame = frame.assign(_source_trace_file=str(path))
            frames.append(frame)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _return_column(frame: pd.DataFrame) -> str | None:
    for column in TRACE_RETURN_COLUMNS:
        if column in frame.columns:
            return column
    return None


def _return_bps(frame: pd.DataFrame, column: str) -> pd.Series:
    values = pd.to_numeric(frame[column], errors="coerce")
    if column == "forward_return":
        return values * 10_000.0
    return values


def _metric(values: pd.Series) -> dict[str, Any]:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    event_count = int(len(clean))
    t_stat = None
    if event_count >= 2:
        std = float(clean.std(ddof=1))
        if std != 0.0 and not math.isnan(std):
            t_stat = float(clean.mean()) / (std / math.sqrt(event_count))
    return {
        "event_count": event_count,
        "mean_return_net_bps": _round(float(clean.mean())) if event_count else None,
        "t_stat_net": _round(t_stat),
    }


def _opposite(direction: str) -> str:
    value = direction.strip().lower()
    if value == "long":
        return "short"
    if value == "short":
        return "long"
    return f"not_{value}" if value else ""


def _column_values(frame: pd.DataFrame, columns: tuple[str, ...]) -> pd.Series:
    for column in columns:
        if column in frame.columns:
            return frame[column].map(lambda value: "" if results_index._is_missing(value) else str(value))
    return pd.Series([""] * len(frame), index=frame.index)


def _annotate_trace_frame(frame: pd.DataFrame, candidate: dict[str, Any], ids: set[str]) -> pd.DataFrame:
    out = frame.copy()
    out["_event_id"] = _column_values(out, ("event_id", "event_type", "event")).map(str)
    if "trigger_key" in out.columns:
        trigger_event = out["trigger_key"].map(
            lambda value: str(value).split(":", 1)[1] if str(value).startswith("event:") else ""
        )
        out["_event_id"] = out["_event_id"].where(out["_event_id"].astype(bool), trigger_event)
    out["_context"] = pd.Series([_row_context(row) for row in out.to_dict("records")], index=out.index)
    out["_template_id"] = _column_values(out, ("template_id", "rule_template", "template"))
    out["_direction"] = _column_values(out, ("direction",))
    horizon = _column_values(out, ("horizon_bars", "horizon", "horizon_label")).map(_horizon_bars)
    out["_horizon_bars"] = horizon
    out["_entry_lag_bars"] = _column_values(out, ("entry_lag_bars", "entry_lag")).map(_to_int)
    out["_specificity_test"] = _column_values(out, ("specificity_test", "test_case", "variant"))
    if "_source_trace_file" in out.columns:
        candidate_trace_source = out["_source_trace_file"].astype(str).str.contains(
            "/candidate_traces/", regex=False
        )
        out.loc[candidate_trace_source & (out["_specificity_test"].map(_norm) == ""), "_specificity_test"] = "base"

    if ids:
        id_masks = []
        for column in ("candidate_id", "hypothesis_id", "strategy_id"):
            if column in out.columns:
                id_masks.append(out[column].astype(str).isin(ids))
        if id_masks:
            mask = id_masks[0]
            for next_mask in id_masks[1:]:
                mask = mask | next_mask
            out["_candidate_match"] = mask
        else:
            out["_candidate_match"] = False
    else:
        out["_candidate_match"] = False

    context = str(candidate.get("context", "") or "")
    base_mask = (
        (out["_event_id"].map(_norm) == _norm(candidate.get("event_id")))
        & (out["_direction"].map(_norm) == _norm(candidate.get("direction")))
        & (out["_horizon_bars"] == candidate.get("horizon_bars"))
    )
    if candidate.get("template_id"):
        base_mask = base_mask & (out["_template_id"].map(_norm) == _norm(candidate.get("template_id")))
    if context:
        base_mask = base_mask & (out["_context"].map(_norm) == _norm(context))
    out["_derived_base"] = base_mask | out["_candidate_match"]
    return out


def _variant_mask(frame: pd.DataFrame, candidate: dict[str, Any], variant: str) -> pd.Series:
    explicit = frame["_specificity_test"].map(_norm) == variant
    if explicit.any():
        return explicit
    event_id = candidate.get("event_id")
    context = candidate.get("context")
    direction = candidate.get("direction")
    template_id = candidate.get("template_id")
    horizon = candidate.get("horizon_bars")
    base_common = frame["_horizon_bars"] == horizon
    if template_id:
        base_common = base_common & (frame["_template_id"].map(_norm) == _norm(template_id))
    if variant == "base":
        return frame["_derived_base"]
    not_base_trace = frame["_specificity_test"].map(_norm) != "base"
    if variant == "event_only":
        return (
            not_base_trace
            &
            base_common
            & (frame["_event_id"].map(_norm) == _norm(event_id))
            & (frame["_direction"].map(_norm) == _norm(direction))
            & (frame["_context"].map(_norm) != _norm(context))
        )
    if variant == "context_only":
        return (
            not_base_trace
            &
            base_common
            & (frame["_event_id"].map(_norm) != _norm(event_id))
            & (frame["_context"].map(_norm) == _norm(context))
            & (frame["_direction"].map(_norm) == _norm(direction))
        )
    if variant == "opposite_direction":
        return (
            not_base_trace
            &
            base_common
            & (frame["_event_id"].map(_norm) == _norm(event_id))
            & (frame["_context"].map(_norm) == _norm(context))
            & (frame["_direction"].map(_norm) == _norm(_opposite(str(direction or ""))))
        )
    return pd.Series([False] * len(frame), index=frame.index)


def _trace_tests(frame: pd.DataFrame, candidate: dict[str, Any], ids: set[str]) -> tuple[dict[str, Any], dict[str, Any]]:
    return_col = _return_column(frame)
    if frame.empty or return_col is None:
        return {}, {"trace_data_available": False, "return_column": return_col}
    annotated = _annotate_trace_frame(frame, candidate, ids)
    annotated["_return_net_bps"] = _return_bps(annotated, return_col)
    tests: dict[str, Any] = {}
    for variant in ("base", "event_only", "context_only", "opposite_direction"):
        tests[variant] = _metric(annotated.loc[_variant_mask(annotated, candidate, variant), "_return_net_bps"])

    lag_rows = []
    for lag in (0, 1, 2, 3):
        explicit = annotated["_specificity_test"].map(_norm).isin({f"lag_{lag}", f"entry_lag_{lag}"})
        mask = explicit if explicit.any() else annotated["_derived_base"] & (annotated["_entry_lag_bars"] == lag)
        row = {"entry_lag_bars": lag, **_metric(annotated.loc[mask, "_return_net_bps"])}
        lag_rows.append(row)
    tests["entry_lag_sensitivity"] = lag_rows
    return tests, {"trace_data_available": True, "return_column": return_col}


def _aggregate_base(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_count": candidate.get("event_count"),
        "mean_return_net_bps": None,
        "t_stat_net": _round(candidate.get("t_stat_net")),
    }


def _complete_trace_tests(tests: dict[str, Any]) -> bool:
    if not tests:
        return False
    for name in TRACE_REQUIRED_TESTS:
        if int(tests.get(name, {}).get("event_count") or 0) <= 0:
            return False
    lag_rows = tests.get("entry_lag_sensitivity", [])
    return isinstance(lag_rows, list) and any(int(row.get("event_count") or 0) > 0 for row in lag_rows)


def _classify(tests: dict[str, Any], meta: dict[str, Any], candidate: dict[str, Any]) -> tuple[str, str, str, str, dict[str, Any]]:
    if not _complete_trace_tests(tests):
        if meta.get("trace_data_available"):
            reason = "specificity controls unavailable; candidate traces contain base returns only"
        else:
            reason = "specificity cannot be computed from aggregate candidate metrics only"
        return (
            "review",
            "insufficient_trace_data",
            "review",
            reason,
            {
                "base_vs_event_only_bps": None,
                "base_vs_context_only_bps": None,
                "pass": None,
            },
        )
    base_mean = tests["base"]["mean_return_net_bps"]
    event_only_mean = tests["event_only"]["mean_return_net_bps"]
    context_only_mean = tests["context_only"]["mean_return_net_bps"]
    opposite_mean = tests["opposite_direction"]["mean_return_net_bps"]
    base_vs_event = None if base_mean is None or event_only_mean is None else base_mean - event_only_mean
    base_vs_context = None if base_mean is None or context_only_mean is None else base_mean - context_only_mean
    lift_pass = (
        base_vs_event is not None
        and base_vs_context is not None
        and base_vs_event > 0.0
        and base_vs_context > 0.0
    )
    specificity_lift = {
        "base_vs_event_only_bps": _round(base_vs_event),
        "base_vs_context_only_bps": _round(base_vs_context),
        "pass": bool(lift_pass),
    }
    if not lift_pass:
        return "fail", "context_proxy", "park", "base does not beat event-only and context-only controls", specificity_lift
    if opposite_mean is not None and base_mean is not None and opposite_mean >= base_mean:
        return "fail", "direction_ambiguous", "park", "opposite direction performs comparably to base", specificity_lift

    base_lag = candidate.get("entry_lag_bars")
    lag_rows = tests.get("entry_lag_sensitivity", [])
    if base_lag is not None:
        lag_means = {
            int(row["entry_lag_bars"]): row.get("mean_return_net_bps")
            for row in lag_rows
            if row.get("mean_return_net_bps") is not None
        }
        if lag_means and base_lag in lag_means:
            best_lag = max(lag_means, key=lambda lag: lag_means[lag])
            if best_lag != int(base_lag) and lag_means[best_lag] >= lag_means[int(base_lag)]:
                return "fail", "timing_ambiguous", "park", "entry-lag sensitivity is not anchored on the candidate lag", specificity_lift

    return "pass", "event_specific", "advance", "base beats event-only, context-only, and opposite-direction controls", specificity_lift


def build_specificity_report(
    *, run_id: str, candidate_id: str, data_root: Path | str | None = None
) -> dict[str, Any]:
    resolved_data_root = _data_root(data_root)
    candidate, ids = _candidate_profile(resolved_data_root, run_id, candidate_id)
    tests, trace_meta = _trace_tests(_load_trace_frame(resolved_data_root, run_id), candidate, ids)
    if not tests:
        tests = {
            "base": _aggregate_base(candidate),
            "event_only": {},
            "context_only": {},
            "opposite_direction": {},
            "entry_lag_sensitivity": [],
        }
    status, classification, decision, reason, specificity_lift = _classify(tests, trace_meta, candidate)
    if decision == "advance":
        next_safe = "Run cost stress v1."
    elif classification == "insufficient_trace_data" and trace_meta.get("trace_data_available"):
        next_safe = "Inspect pipeline trace generation for specificity control traces before validation."
    elif classification == "insufficient_trace_data":
        next_safe = "Implement candidate trace extraction before promotion or validation."
    else:
        next_safe = "Park candidate unless a new bounded mechanism explains the failed control."
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "candidate_id": str(candidate.get("candidate_id") or candidate_id),
        "event_id": str(candidate.get("event_id", "") or ""),
        "context": str(candidate.get("context", "") or ""),
        "template_id": str(candidate.get("template_id", "") or ""),
        "direction": str(candidate.get("direction", "") or ""),
        "horizon_bars": candidate.get("horizon_bars"),
        "status": status,
        "classification": classification,
        "tests": tests,
        "specificity_lift": specificity_lift,
        "decision": decision,
        "reason": reason,
        "next_safe_command": next_safe,
        "trace_data": trace_meta,
        "forbidden_rescue_actions": FORBIDDEN_RESCUE_ACTIONS,
    }


def report_path(data_root: Path | str | None, run_id: str, candidate_id: str) -> Path:
    return (
        _data_root(data_root)
        / "reports"
        / "specificity"
        / run_id
        / f"{_safe_name(candidate_id)}_specificity.json"
    )


def write_specificity_report(report: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_specificity(
    *, run_id: str, candidate_id: str, data_root: Path | str | None = None
) -> dict[str, Any]:
    report = build_specificity_report(
        run_id=run_id,
        candidate_id=candidate_id,
        data_root=data_root,
    )
    write_specificity_report(report, report_path(data_root, run_id, candidate_id))
    return report
