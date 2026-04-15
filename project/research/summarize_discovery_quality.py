from __future__ import annotations
from project.core.config import get_data_root

import argparse
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import pandas as pd

from project.io.utils import read_parquet
from project.specs.manifest import finalize_manifest, start_manifest

def _utc_now_iso() -> str:
    DATA_ROOT = get_data_root()
    return datetime.now(timezone.utc).isoformat()


def _sanitize(value: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", str(value).strip().lower()).strip("_")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize phase2 discovery quality across event families."
    )
    parser.add_argument("--run_id", required=True)
    parser.add_argument(
        "--phase2_root",
        default="",
        help="Optional phase2 root directory (default: data/reports/phase2/<run_id>).",
    )
    parser.add_argument(
        "--out_path",
        default="",
        help="Optional output path (default: <phase2_root>/discovery_quality_summary.json).",
    )
    parser.add_argument(
        "--funnel_out_path",
        default="",
        help="Optional output path for funnel summary (default: data/reports/<run_id>/funnel_summary.json).",
    )
    parser.add_argument("--top_fail_reasons", type=int, default=10)
    parser.add_argument("--log_path", default="")
    return parser.parse_args()


def _load_candidates(path: Path) -> pd.DataFrame:
    path_parquet = path.with_suffix(".parquet")
    if path_parquet.exists():
        try:
            return read_parquet(path_parquet)
        except Exception:
            pass
    if path.exists():
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def _event_type_from_trigger_key(value: object) -> str:
    token = str(value or "").strip()
    if not token:
        return ""
    if ":" in token:
        token = token.split(":", 1)[1]
    return token.strip().upper()


def _phase2_event_roots(phase2_root: Path) -> Dict[str, list[Path]]:
    event_roots: Dict[str, list[Path]] = defaultdict(list)
    if not phase2_root.exists():
        return event_roots
    for event_dir in sorted([p for p in phase2_root.iterdir() if p.is_dir()]):
        if event_dir.name == "search_engine":
            direct_candidate = event_dir / "phase2_candidates.csv"
            direct_candidate_parquet = event_dir / "phase2_candidates.parquet"
            if direct_candidate.exists() or direct_candidate_parquet.exists():
                event_roots[event_dir.name].append(event_dir)
            continue
        direct_candidate = event_dir / "phase2_candidates.csv"
        direct_candidate_parquet = event_dir / "phase2_candidates.parquet"
        if direct_candidate.exists() or direct_candidate_parquet.exists():
            event_roots[event_dir.name].append(event_dir)
            continue
        timeframe_dirs = [
            child
            for child in sorted(event_dir.iterdir())
            if child.is_dir()
            and (
                (child / "phase2_candidates.csv").exists()
                or (child / "phase2_candidates.parquet").exists()
            )
        ]
        if timeframe_dirs:
            event_roots[event_dir.name].extend(timeframe_dirs)
    return event_roots


def _load_json_object(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _external_validation_summary_paths(run_id: str) -> List[Path]:
    DATA_ROOT = get_data_root()
    return [
        DATA_ROOT / "reports" / "external_validation" / run_id / "external_validation_summary.json",
        DATA_ROOT / "reports" / "eval" / run_id / "walkforward_summary.json",
    ]


def _load_jsonl_rows(path: Path) -> List[Dict[str, object]]:
    if not path.exists():
        return []
    rows: List[Dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _split_fail_reasons(series: pd.Series) -> List[str]:
    out: List[str] = []
    for raw in series.fillna("").astype(str):
        for token in raw.split(","):
            reason = token.strip()
            if reason:
                out.append(reason)
    return out


def _build_summary_from_flat_phase2_layout(
    *, run_id: str, phase2_root: Path, top_fail_reasons: int
) -> dict | None:
    candidate_frames: Dict[str, List[pd.DataFrame]] = defaultdict(list)
    evaluated_frames: Dict[str, List[pd.DataFrame]] = defaultdict(list)
    failure_frames: Dict[str, List[pd.DataFrame]] = defaultdict(list)
    source_files: Dict[str, str] = {}

    flat_candidates = _load_candidates(phase2_root / "phase2_candidates.csv")
    if flat_candidates.empty:
        flat_candidates = _load_candidates(phase2_root / "phase2_candidates.parquet")
    if not flat_candidates.empty and "event_type" in flat_candidates.columns:
        for event_type, frame in flat_candidates.groupby(flat_candidates["event_type"].astype(str)):
            family = str(event_type).strip().upper()
            if not family:
                continue
            candidate_frames[family].append(frame.copy())
            source_files[family] = str(phase2_root)

    hypotheses_root = phase2_root / "hypotheses"
    if hypotheses_root.exists():
        for symbol_dir in sorted(child for child in hypotheses_root.iterdir() if child.is_dir()):
            evaluated_path = symbol_dir / "evaluated_hypotheses.parquet"
            if evaluated_path.exists():
                evaluated = read_parquet(evaluated_path)
                if not evaluated.empty and "trigger_key" in evaluated.columns:
                    work = evaluated.copy()
                    work["event_type"] = work["trigger_key"].map(_event_type_from_trigger_key)
                    for event_type, frame in work.groupby(work["event_type"].astype(str)):
                        family = str(event_type).strip().upper()
                        if not family:
                            continue
                        evaluated_frames[family].append(frame.copy())
                        source_files.setdefault(family, str(evaluated_path))

            failures_path = symbol_dir / "gate_failures.parquet"
            if failures_path.exists():
                failures = read_parquet(failures_path)
                if not failures.empty and "trigger_key" in failures.columns:
                    work = failures.copy()
                    work["event_type"] = work["trigger_key"].map(_event_type_from_trigger_key)
                    for event_type, frame in work.groupby(work["event_type"].astype(str)):
                        family = str(event_type).strip().upper()
                        if not family:
                            continue
                        failure_frames[family].append(frame.copy())
                        source_files.setdefault(family, str(failures_path))

    families = set(candidate_frames) | set(evaluated_frames) | set(failure_frames)
    if not families:
        return None

    by_primary_event_id: Dict[str, Dict[str, object]] = {}
    family_fail_counter: Dict[str, Counter[str]] = defaultdict(Counter)
    global_fail_counter: Counter[str] = Counter()

    for family in sorted(families):
        candidate_frame = (
            pd.concat(candidate_frames[family], ignore_index=True)
            if candidate_frames.get(family)
            else pd.DataFrame()
        )
        evaluated_frame = (
            pd.concat(evaluated_frames[family], ignore_index=True)
            if evaluated_frames.get(family)
            else pd.DataFrame()
        )
        failure_frame = (
            pd.concat(failure_frames[family], ignore_index=True)
            if failure_frames.get(family)
            else pd.DataFrame()
        )

        family_row = _family_defaults()
        if not candidate_frame.empty:
            summary = _event_summary(candidate_frame)
            family_row.update(summary)
            family_row["phase2_candidates"] = int(len(candidate_frame))
            family_row["phase2_gate_all_pass"] = int(_gate_pass_series(candidate_frame).sum())
            _apply_bridge_metrics_from_frame(family_row, candidate_frame)
        else:
            total = int(len(evaluated_frame))
            fail_count = int(len(failure_frame))
            pass_count = max(0, total - fail_count)
            family_row["total_candidates"] = total
            family_row["gate_pass_count"] = pass_count
            family_row["gate_pass_rate"] = float(pass_count / total) if total else 0.0

        fail_reasons = _split_fail_reasons(
            failure_frame.get("gate_failure_reason", pd.Series(dtype=str))
        )
        family_fail_counter[family].update(fail_reasons)
        global_fail_counter.update(fail_reasons)
        by_primary_event_id[family] = family_row

    for family, counter in family_fail_counter.items():
        by_primary_event_id.setdefault(family, _family_defaults())
        by_primary_event_id[family]["top_failure_reasons"] = [
            {"reason": reason, "count": int(count)}
            for reason, count in counter.most_common(max(0, int(top_fail_reasons)))
        ]

    primary_event_ids = sorted(by_primary_event_id.keys())
    total_candidates = int(
        sum(int(by_primary_event_id[event_id].get("total_candidates", 0)) for event_id in primary_event_ids)
    )
    gate_pass_count = int(
        sum(int(by_primary_event_id[event_id].get("gate_pass_count", 0)) for event_id in primary_event_ids)
    )

    return {
        "run_id": run_id,
        "generated_at": _utc_now_iso(),
        "phase2_root": str(phase2_root),
        "source_files": source_files,
        "primary_event_ids": primary_event_ids,
        "event_families": primary_event_ids,
        "total_candidates": total_candidates,
        "gate_pass_count": gate_pass_count,
        "gate_pass_rate": float(gate_pass_count / total_candidates) if total_candidates else 0.0,
        "top_fail_reasons": [
            {"reason": reason, "count": int(count)}
            for reason, count in global_fail_counter.most_common(max(0, int(top_fail_reasons)))
        ],
        "by_primary_event_id": by_primary_event_id,
        "by_event_family": by_primary_event_id,
    }


def _gate_pass_series(df: pd.DataFrame) -> pd.Series:
    if "gate_phase2_final" in df.columns:
        return (
            pd.to_numeric(df["gate_phase2_final"], errors="coerce").fillna(0.0).astype(float) > 0.0
        )
    if "gate_pass" in df.columns:
        return pd.to_numeric(df["gate_pass"], errors="coerce").fillna(0.0).astype(float) > 0.0
    if "gate_all" in df.columns:
        return pd.to_numeric(df["gate_all"], errors="coerce").fillna(0.0).astype(float) > 0.0
    search_engine_gate_cols = [
        "gate_oos_validation",
        "gate_after_cost_positive",
        "gate_after_cost_stressed_positive",
        "gate_bridge_tradable",
        "gate_multiplicity",
        "gate_c_regime_stable",
    ]
    if all(col in df.columns for col in search_engine_gate_cols):
        mask = pd.Series(True, index=df.index)
        for col in search_engine_gate_cols:
            mask &= df[col].fillna(False).astype(bool)
        return mask
    return pd.Series(False, index=df.index)


def _is_pass_value(value: object) -> bool:
    text = str(value).lower().strip()
    return text in ("1", "true", "t", "yes", "y", "on", "pass")


def _apply_bridge_metrics_from_frame(family_row: Dict[str, object], frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    if "bridge_eval_status" in frame.columns:
        eval_mask = frame["bridge_eval_status"].astype(str).str.strip().ne("")
        family_row["bridge_evaluable"] = int(eval_mask.sum())
    if "gate_bridge_tradable" in frame.columns:
        family_row["bridge_pass_val"] = int(
            frame["gate_bridge_tradable"].apply(_is_pass_value).sum()
        )
    if "bridge_fail_reasons" in frame.columns:
        missing_base_mask = (
            frame["bridge_fail_reasons"]
            .astype(str)
            .str.contains("gate_bridge_missing_overlay_base", regex=False, na=False)
        )
        family_row["overlay_kill_by_missing_base_count"] = int(missing_base_mask.sum())


def _event_summary(df: pd.DataFrame) -> Dict[str, float | int]:
    total = int(len(df))
    gate_pass = _gate_pass_series(df)
    pass_count = int(gate_pass.sum()) if total else 0
    pass_rate = float(pass_count / total) if total else 0.0
    return {
        "total_candidates": total,
        "gate_pass_count": pass_count,
        "gate_pass_rate": pass_rate,
    }


def _family_defaults() -> Dict[str, object]:
    return {
        "total_candidates": 0,
        "gate_pass_count": 0,
        "gate_pass_rate": 0.0,
        "phase2_candidates": 0,
        "phase2_gate_all_pass": 0,
        "bridge_evaluable": 0,
        "bridge_pass_val": 0,
        "overlay_kill_by_missing_base_count": 0,
        "compiled_bases": 0,
        "compiled_overlays": 0,
        "wf_tested": 0,
        "wf_survivors": 0,
        "top_failure_reasons": [],
    }


def build_summary(*, run_id: str, phase2_root: Path, top_fail_reasons: int) -> dict:
    DATA_ROOT = get_data_root()
    event_roots = _phase2_event_roots(phase2_root)
    if not event_roots:
        flat_summary = _build_summary_from_flat_phase2_layout(
            run_id=run_id,
            phase2_root=phase2_root,
            top_fail_reasons=top_fail_reasons,
        )
        if flat_summary is not None:
            return flat_summary
        print(f"[WARN] No phase2 event directories found for run_id={run_id}: {phase2_root}")
        return {
            "run_id": run_id,
            "generated_at": _utc_now_iso(),
            "phase2_root": str(phase2_root),
            "source_files": {},
            "primary_event_ids": [],
            "event_families": [],
            "total_candidates": 0,
            "gate_pass_count": 0,
            "gate_pass_rate": 0.0,
            "top_fail_reasons": [],
            "by_primary_event_id": {},
            "by_event_family": {},
        }

    by_primary_event_id: Dict[str, Dict[str, object]] = {}
    source_files: Dict[str, str] = {}
    global_fail_counter: Counter[str] = Counter()
    family_fail_counter: Dict[str, Counter[str]] = defaultdict(Counter)

    bridge_root = DATA_ROOT / "reports" / "bridge_eval" / run_id
    for family, roots in sorted(event_roots.items()):
        if family == "search_engine":
            grouped_frames: Dict[str, List[pd.DataFrame]] = defaultdict(list)
            for root in roots:
                frame = _load_candidates(root / "phase2_candidates.csv")
                if frame.empty:
                    frame = _load_candidates(root / "phase2_candidates.parquet")
                if frame.empty or "event_type" not in frame.columns:
                    continue
                for event_type, event_frame in frame.groupby(frame["event_type"].astype(str)):
                    event_type = str(event_type).strip()
                    if not event_type:
                        continue
                    grouped_frames[event_type].append(event_frame.copy())
                    source_files[event_type] = str(root)

            for event_type, event_frames in sorted(grouped_frames.items()):
                frame = pd.concat(event_frames, ignore_index=True)
                summary = _event_summary(frame)
                family_row = _family_defaults()
                family_row.update(summary)
                family_row["phase2_candidates"] = int(len(frame))
                family_row["phase2_gate_all_pass"] = int(_gate_pass_series(frame).sum())
                _apply_bridge_metrics_from_frame(family_row, frame)
                phase2_reasons = _split_fail_reasons(
                    frame.get("fail_reasons", pd.Series(dtype=str))
                )
                family_fail_counter[event_type].update(phase2_reasons)
                global_fail_counter.update(phase2_reasons)
                bridge_reasons = _split_fail_reasons(
                    frame.get("bridge_fail_reasons", pd.Series(dtype=str))
                )
                family_fail_counter[event_type].update(bridge_reasons)
                global_fail_counter.update(bridge_reasons)
                by_primary_event_id[event_type] = family_row
            continue

        frames: List[pd.DataFrame] = []
        source_paths: List[str] = []
        for root in roots:
            candidates_path = root / "phase2_candidates.csv"
            frame = _load_candidates(candidates_path)
            if frame.empty:
                frame = _load_candidates(root / "phase2_candidates.parquet")
            if frame.empty:
                continue
            frame["event_type"] = family
            frames.append(frame)
            source_paths.append(str(root))
        if frames:
            frame = pd.concat(frames, ignore_index=True)
        else:
            frame = pd.DataFrame(
                columns=[
                    "candidate_id",
                    "gate_phase2_final",
                    "gate_pass",
                    "fail_reasons",
                    "gate_all",
                    "gate_bridge_tradable",
                ]
            )
            frame["event_type"] = family
        source_files[family] = ",".join(source_paths)

        summary = _event_summary(frame)
        family_row = _family_defaults()
        family_row.update(summary)

        family_row["phase2_candidates"] = int(len(frame))
        family_row["phase2_gate_all_pass"] = int(_gate_pass_series(frame).sum())

        phase2_reasons = _split_fail_reasons(frame.get("fail_reasons", pd.Series(dtype=str)))
        family_fail_counter[family].update(phase2_reasons)
        global_fail_counter.update(phase2_reasons)

        bridge_df = pd.DataFrame()
        bridge_family_root = bridge_root / family
        if bridge_family_root.exists():
            bridge_frames: List[pd.DataFrame] = []
            for bridge_path in sorted(bridge_family_root.rglob("bridge_candidate_metrics.csv")):
                loaded = _load_candidates(bridge_path)
                if not loaded.empty:
                    bridge_frames.append(loaded)
            for bridge_path in sorted(bridge_family_root.rglob("bridge_evaluation.parquet")):
                loaded = _load_candidates(bridge_path)
                if not loaded.empty:
                    bridge_frames.append(loaded)
            if bridge_frames:
                bridge_df = pd.concat(bridge_frames, ignore_index=True)
        if not bridge_df.empty:
            _apply_bridge_metrics_from_frame(family_row, bridge_df)
            if (
                "bridge_fail_reasons" not in bridge_df.columns
                and "bridge_eval_status" in bridge_df.columns
            ):
                missing_base_mask = (
                    bridge_df["bridge_eval_status"]
                    .astype(str)
                    .str.contains("missing_overlay_base", regex=False, na=False)
                )
                family_row["overlay_kill_by_missing_base_count"] = int(missing_base_mask.sum())
            bridge_reasons = _split_fail_reasons(
                bridge_df.get("bridge_fail_reasons", pd.Series(dtype=str))
            )
            family_fail_counter[family].update(bridge_reasons)
            global_fail_counter.update(bridge_reasons)

        by_primary_event_id[family] = family_row

    blueprints_path = DATA_ROOT / "reports" / "strategy_blueprints" / run_id / "blueprints.jsonl"
    blueprints_rows = _load_jsonl_rows(blueprints_path)
    strategy_to_family: Dict[str, str] = {}
    for row in blueprints_rows:
        family = str(row.get("event_type", "")).strip()
        if not family:
            continue
        by_primary_event_id.setdefault(family, _family_defaults())
        by_primary_event_id[family]["compiled_bases"] = (
            int(by_primary_event_id[family].get("compiled_bases", 0)) + 1
        )
        overlays = row.get("overlays", [])
        overlay_count = len(overlays) if isinstance(overlays, list) else 0
        by_primary_event_id[family]["compiled_overlays"] = int(
            by_primary_event_id[family].get("compiled_overlays", 0)
        ) + int(overlay_count)

        bp_id = str(row.get("id", "")).strip()
        if bp_id:
            strategy_to_family[f"dsl_interpreter_v1__{_sanitize(bp_id)}"] = family

    external_validation_summary = {}
    for candidate_path in _external_validation_summary_paths(run_id):
        external_validation_summary = _load_json_object(candidate_path)
        if external_validation_summary:
            break

    per_strategy_metrics = external_validation_summary.get("per_strategy_split_metrics", {})
    if isinstance(per_strategy_metrics, dict):
        for strategy_id, split_payload in per_strategy_metrics.items():
            if not isinstance(strategy_id, str) or not isinstance(split_payload, dict):
                continue
            family = strategy_to_family.get(strategy_id, "")
            if not family:
                continue
            has_validation = isinstance(split_payload.get("validation"), dict)
            has_test = isinstance(split_payload.get("test"), dict)
            if has_validation and has_test:
                by_primary_event_id.setdefault(family, _family_defaults())
                by_primary_event_id[family]["wf_tested"] = (
                    int(by_primary_event_id[family].get("wf_tested", 0)) + 1
                )

    promotion_report = _load_json_object(
        DATA_ROOT / "reports" / "promotions" / run_id / "promotion_report.json"
    )
    tested_rows = promotion_report.get("tested", [])
    if isinstance(tested_rows, list):
        for row in tested_rows:
            if not isinstance(row, dict):
                continue
            family = str(row.get("family", row.get("event_type", ""))).strip()
            if not family:
                strategy_id = str(row.get("strategy_id", "")).strip()
                family = strategy_to_family.get(strategy_id, "")
            if not family:
                continue
            by_primary_event_id.setdefault(family, _family_defaults())
            if bool(row.get("promoted", False)):
                by_primary_event_id[family]["wf_survivors"] = (
                    int(by_primary_event_id[family].get("wf_survivors", 0)) + 1
                )
            reasons = row.get("fail_reasons", [])
            if isinstance(reasons, list):
                tokens = [str(x).strip() for x in reasons if str(x).strip()]
                family_fail_counter[family].update(tokens)
                global_fail_counter.update(tokens)

    for family, counter in family_fail_counter.items():
        by_primary_event_id.setdefault(family, _family_defaults())
        by_primary_event_id[family]["top_failure_reasons"] = [
            {"reason": reason, "count": int(count)}
            for reason, count in counter.most_common(max(0, int(top_fail_reasons)))
        ]

    primary_event_ids = sorted(by_primary_event_id.keys())
    total_candidates = int(
        sum(int(by_primary_event_id[f].get("total_candidates", 0)) for f in primary_event_ids)
    )
    gate_pass_count = int(
        sum(int(by_primary_event_id[f].get("gate_pass_count", 0)) for f in primary_event_ids)
    )
    gate_pass_rate = float(gate_pass_count / total_candidates) if total_candidates else 0.0

    top_reasons = [
        {"reason": reason, "count": int(count)}
        for reason, count in global_fail_counter.most_common(max(0, int(top_fail_reasons)))
    ]

    return {
        "run_id": run_id,
        "generated_at": _utc_now_iso(),
        "phase2_root": str(phase2_root),
        "source_files": source_files,
        "primary_event_ids": primary_event_ids,
        "event_families": primary_event_ids,
        "total_candidates": total_candidates,
        "gate_pass_count": gate_pass_count,
        "gate_pass_rate": gate_pass_rate,
        "top_fail_reasons": top_reasons,
        "by_primary_event_id": by_primary_event_id,
        "by_event_family": by_primary_event_id,
    }


def _build_funnel_payload(
    summary: Dict[str, object], *, top_fail_reasons: int
) -> Dict[str, object]:
    by_primary_event_id = summary.get("by_primary_event_id", summary.get("by_event_family", {}))
    if not isinstance(by_primary_event_id, dict):
        by_primary_event_id = {}

    families: Dict[str, Dict[str, object]] = {}
    totals = {
        "phase2_candidates": 0,
        "phase2_gate_all_pass": 0,
        "bridge_evaluable": 0,
        "bridge_pass_val": 0,
        "overlay_kill_by_missing_base_count": 0,
        "compiled_bases": 0,
        "compiled_overlays": 0,
        "wf_tested": 0,
        "wf_survivors": 0,
    }
    global_fail_counter: Counter[str] = Counter()

    for event_id in sorted(by_primary_event_id.keys()):
        row = by_primary_event_id.get(event_id, {})
        if not isinstance(row, dict):
            continue
        family_counts = {
            "phase2_candidates": int(row.get("phase2_candidates", 0) or 0),
            "phase2_gate_all_pass": int(row.get("phase2_gate_all_pass", 0) or 0),
            "bridge_evaluable": int(row.get("bridge_evaluable", 0) or 0),
            "bridge_pass_val": int(row.get("bridge_pass_val", 0) or 0),
            "overlay_kill_by_missing_base_count": int(
                row.get("overlay_kill_by_missing_base_count", 0) or 0
            ),
            "compiled_bases": int(row.get("compiled_bases", 0) or 0),
            "compiled_overlays": int(row.get("compiled_overlays", 0) or 0),
            "wf_tested": int(row.get("wf_tested", 0) or 0),
            "wf_survivors": int(row.get("wf_survivors", 0) or 0),
        }
        for key, value in family_counts.items():
            totals[key] += int(value)

        top_family = row.get("top_failure_reasons", [])
        if isinstance(top_family, list):
            for item in top_family:
                if not isinstance(item, dict):
                    continue
                reason = str(item.get("reason", "")).strip()
                count = int(item.get("count", 0) or 0)
                if reason and count > 0:
                    global_fail_counter[reason] += count

        families[event_id] = {
            **family_counts,
            "top_failure_reasons": top_family if isinstance(top_family, list) else [],
        }

    payload = {
        "run_id": str(summary.get("run_id", "")),
        "generated_at": str(summary.get("generated_at", _utc_now_iso())),
        "families": families,
        "totals": totals,
        "top_failure_reasons": [
            {"reason": reason, "count": int(count)}
            for reason, count in global_fail_counter.most_common(max(0, int(top_fail_reasons)))
        ],
        "diagnostic_flags": {
            "all_bridge_pass_zero": bool(totals["bridge_pass_val"] == 0),
        },
    }
    return payload


def main() -> int:
    DATA_ROOT = get_data_root()
    args = _parse_args()
    phase2_root = (
        Path(args.phase2_root)
        if args.phase2_root
        else DATA_ROOT / "reports" / "phase2" / args.run_id
    )
    out_path = (
        Path(args.out_path) if args.out_path else phase2_root / "discovery_quality_summary.json"
    )
    funnel_out_path = (
        Path(args.funnel_out_path)
        if args.funnel_out_path
        else DATA_ROOT / "reports" / args.run_id / "funnel_summary.json"
    )
    outputs = [{"path": str(out_path)}, {"path": str(funnel_out_path)}]
    if args.log_path:
        outputs.append({"path": str(args.log_path)})
    manifest = start_manifest("summarize_discovery_quality", args.run_id, vars(args), [], outputs)

    try:
        payload = build_summary(
            run_id=args.run_id,
            phase2_root=phase2_root,
            top_fail_reasons=int(args.top_fail_reasons),
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

        funnel_payload = _build_funnel_payload(payload, top_fail_reasons=int(args.top_fail_reasons))
        funnel_out_path.parent.mkdir(parents=True, exist_ok=True)
        funnel_out_path.write_text(
            json.dumps(funnel_payload, indent=2, sort_keys=True), encoding="utf-8"
        )

        print(
            json.dumps(
                {
                    "run_id": args.run_id,
                    "out_path": str(out_path),
                    "funnel_out_path": str(funnel_out_path),
                },
                sort_keys=True,
            )
        )
        finalize_manifest(
            manifest,
            "success",
            stats={
                "primary_event_id_count": int(len(payload.get("primary_event_ids", []))),
                "event_family_count": int(len(payload.get("event_families", []))),
                "total_candidates": int(payload.get("total_candidates", 0) or 0),
                "gate_pass_count": int(payload.get("gate_pass_count", 0) or 0),
            },
        )
        return 0
    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        raise


if __name__ == "__main__":
    raise SystemExit(main())
