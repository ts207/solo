from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from project.io.utils import atomic_write_json, read_parquet, write_parquet
from project.research.cell_discovery.contrast import build_contrast_frame
from project.research.cell_discovery.forward_rank import rank_score
from project.research.cell_discovery.models import DiscoveryRegistry
from project.research.cell_discovery.paths import paths_for_run
from project.research.services.pathing import phase2_candidates_path

RAW_COLUMNS = [
    "cell_id",
    "source_cell_id",
    "event_family",
    "event_atom",
    "source_event_atom",
    "source_context_cell",
    "source_context_value",
    "source_discovery_mode",
    "source_scoreboard_run_id",
    "symbol",
    "timeframe",
    "direction",
    "template",
    "horizon",
    "context_cell",
    "context_json",
    "context_dimension_count",
    "n_events",
    "gross_mean_bps",
    "net_mean_bps",
    "median_bps",
    "hit_rate",
    "t_stat",
    "p_value",
    "q_value",
    "robustness_score",
    "stability_score",
    "fold_sign_vector",
    "fold_valid_count",
    "fold_fail_ratio",
    "forward_net_mean_bps",
    "forward_t_stat",
    "forward_pass",
    "runtime_executable",
    "thesis_eligible",
    "executability_class",
    "supportive_context_json",
    "blocked_reason",
]


def _read_optional(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return read_parquet([path])


def _context_json_dim(value: Any) -> int:
    if isinstance(value, dict):
        return len(value)
    try:
        parsed = json.loads(str(value))
    except Exception:
        return 0
    return len(parsed) if isinstance(parsed, dict) else 0


def _series(frame: pd.DataFrame, column: str, default: Any = "") -> pd.Series:
    if column in frame.columns:
        return frame[column]
    return pd.Series([default] * len(frame), index=frame.index)


def _first_series(frame: pd.DataFrame, columns: tuple[str, ...], default: Any = "") -> pd.Series:
    for column in columns:
        if column in frame.columns:
            return frame[column]
    return pd.Series([default] * len(frame), index=frame.index)


def _numeric_series(
    frame: pd.DataFrame,
    columns: tuple[str, ...],
    default: float = 0.0,
) -> pd.Series:
    return pd.to_numeric(_first_series(frame, columns, default), errors="coerce").fillna(default)


def _fold_forward_metrics(folds: pd.DataFrame) -> pd.DataFrame:
    if folds.empty or "hypothesis_id" not in folds.columns:
        return pd.DataFrame(
            columns=[
                "hypothesis_id",
                "fold_sign_vector",
                "fold_valid_count_detail",
                "fold_fail_ratio_detail",
                "fold_median_after_cost_expectancy_detail",
                "fold_median_oos_expectancy_detail",
                "fold_median_t_stat_detail",
            ]
        )
    valid = (
        folds["valid"].fillna(False).astype(bool)
        if "valid" in folds.columns
        else pd.Series(True, index=folds.index)
    )
    value_col = ""
    for candidate in ("after_cost_expectancy_bps", "oos_expectancy_bps", "test_mean_return_bps"):
        if candidate in folds.columns:
            value_col = candidate
            break
    if not value_col:
        return _fold_forward_metrics(pd.DataFrame())
    rows = []
    for hypothesis_id, group in folds.groupby("hypothesis_id"):
        valid_group = group[valid.loc[group.index]].copy()
        values = pd.to_numeric(valid_group[value_col], errors="coerce").dropna()
        t_values = (
            pd.to_numeric(valid_group["t_stat"], errors="coerce").dropna()
            if "t_stat" in valid_group.columns
            else pd.Series(dtype="float64")
        )
        signs = []
        for value in values:
            signs.append("+" if value > 0 else ("-" if value < 0 else "0"))
        valid_count = int(len(values))
        fail_count = int((values <= 0).sum()) if valid_count else 0
        rows.append(
            {
                "hypothesis_id": hypothesis_id,
                "fold_sign_vector": "".join(signs),
                "fold_valid_count_detail": valid_count,
                "fold_fail_ratio_detail": float(fail_count / valid_count) if valid_count else 1.0,
                "fold_median_after_cost_expectancy_detail": float(values.median())
                if valid_count
                else pd.NA,
                "fold_median_oos_expectancy_detail": float(values.median())
                if valid_count
                else pd.NA,
                "fold_median_t_stat_detail": float(t_values.median())
                if not t_values.empty
                else pd.NA,
            }
        )
    return pd.DataFrame(rows)


def build_scoreboard(
    *,
    registry: DiscoveryRegistry,
    run_id: str,
    data_root: Path,
    timeframe: str = "5m",
) -> dict[str, Any]:
    paths = paths_for_run(data_root=data_root, run_id=run_id)
    paths.run_dir.mkdir(parents=True, exist_ok=True)
    candidate_universe = _read_optional(paths.candidate_universe_path)
    final_candidates = _read_optional(phase2_candidates_path(data_root=data_root, run_id=run_id))
    lineage = _read_optional(paths.lineage_path)
    folds = _read_optional(paths.run_dir / "phase2_candidate_fold_metrics.parquet")
    unauthorized_rows_filtered = 0

    source = candidate_universe if not candidate_universe.empty else final_candidates
    if source.empty:
        raw = pd.DataFrame(columns=RAW_COLUMNS)
    else:
        raw = source.copy()
        if not lineage.empty and "hypothesis_id" in raw.columns and "hypothesis_id" in lineage.columns:
            before = len(raw)
            if "symbol" in raw.columns and "symbol" in lineage.columns:
                allowed_pairs = set(
                    zip(
                        lineage["hypothesis_id"].dropna().astype(str),
                        lineage["symbol"].fillna("").astype(str).str.upper(),
                    )
                )
                raw_pairs = zip(
                    raw["hypothesis_id"].fillna("").astype(str),
                    raw["symbol"].fillna("").astype(str).str.upper(),
                )
                raw = raw[[pair in allowed_pairs for pair in raw_pairs]].copy()
            else:
                allowed = set(lineage["hypothesis_id"].dropna().astype(str))
                raw = raw[raw["hypothesis_id"].astype(str).isin(allowed)].copy()
            unauthorized_rows_filtered = int(before - len(raw))
        if "source_cell_id" not in raw.columns and not lineage.empty:
            if "symbol" in raw.columns and "symbol" in lineage.columns:
                left = raw.copy()
                right = lineage.copy()
                left["__edge_cell_symbol_key"] = left["symbol"].fillna("").astype(str).str.upper()
                right["__edge_cell_symbol_key"] = (
                    right["symbol"].fillna("").astype(str).str.upper()
                )
                existing = set(left.columns)
                lineage_cols = [
                    col
                    for col in right.columns
                    if col in {"hypothesis_id", "__edge_cell_symbol_key"} or col not in existing
                ]
                raw = left.merge(
                    right[lineage_cols],
                    on=["hypothesis_id", "__edge_cell_symbol_key"],
                    how="left",
                    suffixes=("", "_lineage"),
                ).drop(columns=["__edge_cell_symbol_key"])
            else:
                raw = raw.merge(lineage, on="hypothesis_id", how="left", suffixes=("", "_lineage"))
        fold_forward = _fold_forward_metrics(folds)
        if not fold_forward.empty and "hypothesis_id" in raw.columns:
            raw = raw.merge(fold_forward, on="hypothesis_id", how="left")
        raw["cell_id"] = _first_series(raw, ("source_cell_id", "hypothesis_id"), "").astype(str)
        raw["source_cell_id"] = _first_series(raw, ("source_cell_id", "cell_id"), "").astype(str)
        raw["event_family"] = _first_series(
            raw,
            ("event_family", "research_family"),
            "",
        ).astype(str)
        raw["event_atom"] = _first_series(raw, ("event_atom", "event_type"), "").astype(str)
        raw["source_event_atom"] = _series(raw, "source_event_atom", "").astype(str)
        raw["source_context_cell"] = _series(raw, "source_context_cell", "").astype(str)
        raw["source_context_value"] = _series(raw, "source_context_value", "").astype(str)
        raw["source_discovery_mode"] = _series(
            raw,
            "source_discovery_mode",
            "edge_cells",
        ).astype(str)
        raw["source_scoreboard_run_id"] = run_id
        raw["symbol"] = _series(raw, "symbol", "").astype(str)
        raw["timeframe"] = timeframe
        raw["direction"] = _series(raw, "direction", "").astype(str)
        raw["horizon"] = _series(raw, "horizon", "").astype(str)
        raw["template"] = _first_series(raw, ("template", "rule_template", "template_id"), "")
        raw["context_cell"] = _first_series(
            raw,
            ("context_cell", "source_context_cell"),
            "",
        ).astype(str)
        raw["context_json"] = _series(raw, "context_json", "").astype(str)
        if "context_dimension_count" not in raw.columns:
            raw["context_dimension_count"] = _series(raw, "context_json", "").map(_context_json_dim)
        raw["n_events"] = _numeric_series(raw, ("n_events", "n"), 0)
        raw["gross_mean_bps"] = _numeric_series(raw, ("mean_return_bps",), 0.0)
        raw["net_mean_bps"] = _numeric_series(
            raw,
            ("cost_adjusted_return_bps", "mean_return_bps"),
            0.0,
        )
        raw["median_bps"] = _numeric_series(raw, ("median_return_bps",), 0.0)
        raw["hit_rate"] = _numeric_series(raw, ("hit_rate",), 0.0)
        raw["t_stat"] = _numeric_series(raw, ("t_stat",), 0.0)
        raw["p_value"] = _numeric_series(raw, ("p_value",), 1.0)
        raw["q_value"] = _numeric_series(raw, ("q_value", "p_value"), 1.0)
        raw["robustness_score"] = _numeric_series(raw, ("robustness_score",), 0.0)
        raw["stability_score"] = _numeric_series(raw, ("stability_score", "robustness_score"), 0.0)
        raw["fold_valid_count"] = _numeric_series(
            raw,
            ("fold_valid_count", "fold_valid_count_detail"),
            0.0,
        )
        raw["fold_fail_ratio"] = _numeric_series(
            raw,
            ("fold_fail_ratio", "fold_fail_ratio_detail"),
            1.0,
        )
        forward_net = pd.to_numeric(
            _first_series(
                raw,
                (
                    "fold_median_after_cost_expectancy",
                    "fold_median_after_cost_expectancy_detail",
                    "fold_median_oos_expectancy",
                    "fold_median_oos_expectancy_detail",
                ),
                pd.NA,
            ),
            errors="coerce",
        )
        forward_t = pd.to_numeric(
            _first_series(raw, ("fold_median_t_stat", "fold_median_t_stat_detail"), pd.NA),
            errors="coerce",
        )
        has_forward_evidence = forward_net.notna() & (raw["fold_valid_count"] > 0)
        raw["forward_net_mean_bps"] = forward_net.fillna(0.0)
        raw["forward_t_stat"] = forward_t.fillna(0.0)
        raw["forward_pass"] = (
            has_forward_evidence
            & (raw["forward_net_mean_bps"] > registry.ranking_policy.min_forward_net_mean_bps)
            & (raw["fold_fail_ratio"] < 1.0)
        )
        raw["runtime_executable"] = (
            _series(raw, "runtime_executable", False).fillna(False).astype(bool)
        )
        raw["thesis_eligible"] = _series(raw, "thesis_eligible", False).fillna(False).astype(bool)
        raw["executability_class"] = _series(raw, "executability_class", "").astype(str)
        raw["supportive_context_json"] = _series(raw, "supportive_context_json", "").astype(str)
        raw["blocked_reason"] = ""
        raw.loc[
            raw["n_events"] < registry.ranking_policy.min_support,
            "blocked_reason",
        ] = "rejected_low_support"
        raw.loc[
            (~has_forward_evidence) & (raw["blocked_reason"].astype(str) == ""),
            "blocked_reason",
        ] = "blocked_missing_forward_window"
        raw.loc[
            has_forward_evidence
            & (~raw["forward_pass"])
            & (raw["blocked_reason"].astype(str) == ""),
            "blocked_reason",
        ] = "rejected_instability"
        raw = raw.reindex(columns=RAW_COLUMNS, fill_value="")

    contrast = build_contrast_frame(
        raw,
        rules=registry.contrast_rules,
        min_lift_bps=registry.ranking_policy.min_contrast_lift_bps,
    )
    scoreboard = raw.merge(contrast, on=["cell_id", "source_cell_id"], how="left")
    if scoreboard.empty:
        scoreboard["rank_score"] = pd.Series(dtype="float64")
        scoreboard["status"] = pd.Series(dtype="object")
    else:
        scoreboard["contrast_lift_bps"] = pd.to_numeric(
            scoreboard.get("contrast_lift_bps", 0), errors="coerce"
        ).fillna(0.0)
        scoreboard["contrast_pass"] = (
            scoreboard.get("contrast_pass", False).fillna(False).astype(bool)
        )
        scoreboard.loc[~scoreboard["contrast_pass"], "blocked_reason"] = scoreboard[
            "blocked_reason"
        ].mask(scoreboard["blocked_reason"].astype(str) == "", "rejected_no_contrast")
        scoreboard["rank_score"] = scoreboard.apply(
            lambda row: 0.0
            if str(row.get("blocked_reason", "")).strip()
            else rank_score(dict(row), registry.ranking_policy),
            axis=1,
        )
        scoreboard["status"] = "rankable_research_only"
        scoreboard.loc[scoreboard["thesis_eligible"], "status"] = "rankable_thesis_eligible"
        scoreboard.loc[scoreboard["runtime_executable"], "status"] = "rankable_runtime_executable"
        scoreboard.loc[scoreboard["blocked_reason"].astype(str) != "", "status"] = scoreboard[
            "blocked_reason"
        ]
        scoreboard = scoreboard.sort_values(["rank_score", "net_mean_bps"], ascending=False)

    write_parquet(raw, paths.raw_cells_path)
    write_parquet(contrast, paths.contrast_path)
    write_parquet(scoreboard, paths.scoreboard_path)
    summary = {
        "schema_version": "edge_scoreboard_summary_v1",
        "run_id": run_id,
        "raw_rows": int(len(raw)),
        "unauthorized_rows_filtered": unauthorized_rows_filtered,
        "scoreboard_rows": int(len(scoreboard)),
        "rankable_rows": int((scoreboard.get("rank_score", pd.Series(dtype=float)) > 0).sum()),
        "top_rank_score": float(scoreboard["rank_score"].max()) if not scoreboard.empty else 0.0,
        "artifacts": {
            "edge_cells_raw": str(paths.raw_cells_path),
            "edge_cells_contrast": str(paths.contrast_path),
            "edge_scoreboard": str(paths.scoreboard_path),
        },
    }
    atomic_write_json(paths.summary_path, summary)
    return summary
