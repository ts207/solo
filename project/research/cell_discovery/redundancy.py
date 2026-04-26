from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from project.io.utils import read_parquet, write_parquet
from project.research.cell_discovery.paths import paths_for_run
from project.research.clustering.pnl_similarity import (
    calculate_similarity_matrix,
    calculate_trigger_overlap,
)
from project.research.services.candidate_diversification import (
    annotate_candidates_with_diversification,
)


def _scoreboard_for_clustering(scoreboard: pd.DataFrame) -> pd.DataFrame:
    out = scoreboard.copy()
    if out.empty:
        return out
    out = out[out.get("rank_score", pd.Series(0.0, index=out.index)).astype(float) > 0].copy()
    if out.empty:
        return out
    out["candidate_id"] = out.get("cell_id", out.index.to_series()).astype(str)
    out["rule_template"] = out.get("template", "").astype(str)
    out["horizon_bars"] = (
        out.get("horizon", "")
        .astype(str)
        .str.replace("b", "", regex=False)
        .str.replace("bars", "", regex=False)
    )
    out["context_dim_count"] = pd.to_numeric(
        out.get("context_dimension_count", 0),
        errors="coerce",
    ).fillna(0)
    out["discovery_quality_score"] = pd.to_numeric(
        out.get("rank_score", 0.0),
        errors="coerce",
    ).fillna(0.0)
    out["concept_lineage_key"] = (
        "EVENT:"
        + out.get("event_atom", "").astype(str)
        + "|CONTEXT:"
        + out.get("context_cell", "").astype(str)
    )
    return out


def _empty_clusters() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "cell_id",
            "redundancy_cluster_id",
            "cluster_size",
            "cluster_rank",
            "is_representative",
            "redundancy_penalty",
            "cluster_basis",
            "max_pnl_similarity",
            "max_trigger_overlap",
        ]
    )


def _trace_id_column(frame: pd.DataFrame) -> str | None:
    for column in ("cell_id", "source_cell_id", "candidate_id", "hypothesis_id"):
        if column in frame.columns:
            return column
    return None


def _pivot_trace(frame: pd.DataFrame, *, value_candidates: tuple[str, ...]) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    id_column = _trace_id_column(frame)
    if not id_column:
        return pd.DataFrame()
    time_column = next(
        (column for column in ("signal_ts", "timestamp", "ts", "bar_ts") if column in frame.columns),
        None,
    )
    value_column = next((column for column in value_candidates if column in frame.columns), None)
    if not time_column or not value_column:
        return pd.DataFrame()
    return frame.pivot_table(
        index=time_column,
        columns=id_column,
        values=value_column,
        aggfunc="sum",
    ).fillna(0.0)


def _load_behavior_matrices(paths) -> tuple[pd.DataFrame, pd.DataFrame]:
    pnl_similarity = pd.DataFrame()
    trigger_overlap = pd.DataFrame()
    if paths.pnl_traces_path.exists():
        pnl_trace = read_parquet([paths.pnl_traces_path])
        pnl_wide = _pivot_trace(
            pnl_trace,
            value_candidates=(
                "pnl_bps",
                "net_pnl_bps",
                "net_return_bps",
                "return_bps",
                "pnl",
            ),
        )
        pnl_similarity = calculate_similarity_matrix(pnl_wide)
    if paths.trigger_traces_path.exists():
        trigger_trace = read_parquet([paths.trigger_traces_path])
        trigger_wide = _pivot_trace(
            trigger_trace,
            value_candidates=("trigger", "active", "signal", "is_triggered"),
        )
        trigger_overlap = calculate_trigger_overlap(trigger_wide)
    return pnl_similarity, trigger_overlap


class _UnionFind:
    def __init__(self, members: list[str]) -> None:
        self.parent = {member: member for member in members}

    def find(self, member: str) -> str:
        parent = self.parent.setdefault(member, member)
        if parent != member:
            self.parent[member] = self.find(parent)
        return self.parent[member]

    def union(self, left: str, right: str) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root != right_root:
            self.parent[right_root] = left_root


def _similarity_pairs(
    matrix: pd.DataFrame,
    *,
    threshold: float,
) -> tuple[list[tuple[str, str, float]], dict[str, float]]:
    pairs: list[tuple[str, str, float]] = []
    max_by_id: dict[str, float] = {}
    if matrix.empty:
        return pairs, max_by_id
    columns = [str(column) for column in matrix.columns]
    normalized = matrix.copy()
    normalized.index = [str(index) for index in normalized.index]
    normalized.columns = columns
    for i, left in enumerate(columns):
        for right in columns[i + 1 :]:
            value = float(normalized.loc[left, right]) if left in normalized.index else 0.0
            max_by_id[left] = max(max_by_id.get(left, 0.0), value)
            max_by_id[right] = max(max_by_id.get(right, 0.0), value)
            if value >= threshold:
                pairs.append((left, right, value))
    return pairs, max_by_id


def _apply_behavioral_cluster_merges(
    annotated: pd.DataFrame,
    *,
    pnl_similarity: pd.DataFrame,
    trigger_overlap: pd.DataFrame,
    pnl_threshold: float = 0.85,
    trigger_threshold: float = 0.65,
) -> pd.DataFrame:
    out = annotated.copy()
    out["cell_id"] = out.get("cell_id", out.get("candidate_id", out.index.to_series())).astype(str)
    out["cluster_basis"] = "structural_only"
    out["max_pnl_similarity"] = 0.0
    out["max_trigger_overlap"] = 0.0

    pnl_pairs, max_pnl = _similarity_pairs(pnl_similarity, threshold=pnl_threshold)
    trigger_pairs, max_trigger = _similarity_pairs(trigger_overlap, threshold=trigger_threshold)
    out["max_pnl_similarity"] = out["cell_id"].map(max_pnl).fillna(0.0).astype(float)
    out["max_trigger_overlap"] = out["cell_id"].map(max_trigger).fillna(0.0).astype(float)

    if not pnl_pairs and not trigger_pairs:
        if pnl_similarity.empty and trigger_overlap.empty:
            out["cluster_basis"] = "structural_only_no_behavior_trace"
        return out

    members = out["cell_id"].astype(str).tolist()
    uf = _UnionFind(members)
    for cluster_id, group in out.groupby("redundancy_cluster_id"):
        ids = group["cell_id"].astype(str).tolist()
        if ids:
            first = ids[0]
            for other in ids[1:]:
                uf.union(first, other)
    for left, right, _ in pnl_pairs + trigger_pairs:
        if left in uf.parent and right in uf.parent:
            uf.union(left, right)

    root_to_id: dict[str, str] = {}
    merged_ids: list[str] = []
    for cell_id in out["cell_id"].astype(str):
        root = uf.find(cell_id)
        root_to_id.setdefault(root, f"edge_behavior_cluster_{len(root_to_id) + 1:04d}")
        merged_ids.append(root_to_id[root])
    out["redundancy_cluster_id"] = merged_ids

    basis_by_id: dict[str, set[str]] = {cell_id: {"structural"} for cell_id in members}
    for left, right, _ in pnl_pairs:
        basis_by_id.setdefault(left, {"structural"}).add("pnl_similarity")
        basis_by_id.setdefault(right, {"structural"}).add("pnl_similarity")
    for left, right, _ in trigger_pairs:
        basis_by_id.setdefault(left, {"structural"}).add("trigger_overlap")
        basis_by_id.setdefault(right, {"structural"}).add("trigger_overlap")
    out["cluster_basis"] = out["cell_id"].map(
        lambda value: "+".join(sorted(basis_by_id.get(str(value), {"structural"})))
    )

    rank_columns = [column for column in ("rank_score", "net_mean_bps") if column in out.columns]
    if rank_columns:
        out = out.sort_values(rank_columns, ascending=False).copy()
    out["cluster_rank"] = out.groupby("redundancy_cluster_id").cumcount() + 1
    out["cluster_size"] = out.groupby("redundancy_cluster_id")["cell_id"].transform("size")
    out["is_representative"] = out["cluster_rank"].astype(int) == 1
    return out


def build_redundancy_clusters(
    *,
    run_id: str,
    data_root: Path,
    shortlist_size: int = 50,
) -> dict[str, Any]:
    paths = paths_for_run(data_root=data_root, run_id=run_id)
    if not paths.scoreboard_path.exists():
        raise FileNotFoundError(f"edge scoreboard not found: {paths.scoreboard_path}")
    scoreboard = read_parquet([paths.scoreboard_path])
    cluster_input = _scoreboard_for_clustering(scoreboard)

    if cluster_input.empty:
        clusters = _empty_clusters()
        representatives = clusters.copy()
    else:
        annotated, _ = annotate_candidates_with_diversification(
            cluster_input,
            {
                "mode": "greedy",
                "overlap": {
                    "structural_weight": 0.55,
                    "fold_weight": 0.30,
                    "lineage_weight": 0.15,
                    "edge_threshold": 0.65,
                },
                "shortlist": {"enabled": False, "size": shortlist_size},
            },
        )
        annotated["redundancy_cluster_id"] = annotated["overlap_cluster_id"].astype(str)
        annotated["is_representative"] = annotated["cluster_rank"].astype(int) == 1
        annotated["redundancy_penalty"] = pd.to_numeric(
            annotated.get("crowding_penalty", 0.0),
            errors="coerce",
        ).fillna(0.0)
        pnl_similarity, trigger_overlap = _load_behavior_matrices(paths)
        annotated = _apply_behavioral_cluster_merges(
            annotated,
            pnl_similarity=pnl_similarity,
            trigger_overlap=trigger_overlap,
        )
        clusters = annotated.copy()
        representatives = annotated[annotated["is_representative"]].copy()
        representatives = representatives.sort_values(
            ["rank_score", "net_mean_bps"],
            ascending=False,
        ).head(shortlist_size)

    write_parquet(clusters, paths.clusters_path)
    write_parquet(representatives, paths.cluster_representatives_path)
    return {
        "run_id": run_id,
        "cluster_rows": len(clusters),
        "representative_rows": len(representatives),
        "edge_clusters": str(paths.clusters_path),
        "edge_cluster_representatives": str(paths.cluster_representatives_path),
    }
