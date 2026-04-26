"""
Phase 5 — Candidate Diversification Module.

Adds discovery-time portfolio-aware diversification on top of the existing
final candidate table.  The full table is never shrunk or replaced; only
new additive columns are appended.

Public API
----------
build_candidate_overlap_signatures(candidates) -> pd.DataFrame
compute_pairwise_similarity(signatures, ...) -> pd.DataFrame
cluster_candidates_by_overlap(candidates, edges, ...) -> pd.DataFrame
compute_novelty_crowding(candidates, edges) -> pd.DataFrame
select_diversified_shortlist(candidates, ...) -> pd.DataFrame
annotate_candidates_with_diversification(candidates, config) -> (candidates, shortlist)

Design principles
-----------------
* Additive-only — no existing column is overwritten.
* No scipy / networkx dependency — union-find clustering is self-contained.
* O(n²) safeguard — pair computation is pre-bucketed when n > max_candidates.
* All similarity inputs come from columns Phase 2/3/4 already produce.
* Quality input is discovery_quality_score_v3 (or v2 / v1 fallback).
* Greedy MMR selector is deterministic (lexicographic tie-breaking).
"""

from __future__ import annotations

import logging
import math
from typing import Any

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_STRUCTURAL_WEIGHT = 0.5
_DEFAULT_FOLD_WEIGHT = 0.3
_DEFAULT_LINEAGE_WEIGHT = 0.2
_DEFAULT_EDGE_THRESHOLD = 0.65
_MAX_CANDIDATES_FULL_PAIRWISE = 500

OVERLAP_COLUMNS = [
    "overlap_cluster_id",
    "cluster_size",
    "cluster_density",
    "is_duplicate_like",
    "novelty_score",
    "crowding_penalty",
    "cluster_rank",
    "selected_into_diversified_shortlist",
    "shortlist_rank",
    "selection_score",
    "selection_reason",
]

SHORTLIST_REQUIRED_COLUMNS = [
    "candidate_id",
    "overlap_cluster_id",
    "shortlist_rank",
    "selection_score",
    "selection_reason",
    "novelty_score",
    "crowding_penalty",
]

# Structural similarity field weights (must sum to 1.0)
_STRUCTURAL_WEIGHTS: list[tuple[str, float]] = [
    ("event_family", 0.30),
    ("template_family", 0.20),
    ("direction", 0.15),
    ("horizon_bucket", 0.15),
    ("symbol_scope_type", 0.10),
    ("context_dim_count_bucket", 0.10),
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def _template_family(rule_template: Any) -> str:
    raw = _safe_str(rule_template)
    if not raw:
        return "unknown"
    return raw.split("_")[0] or "unknown"


def _horizon_bucket(horizon: Any) -> str:
    """Convert a horizon string (e.g. '24b', '48b') or int to short/medium/long."""
    raw = _safe_str(horizon).replace("b", "").replace("bars", "").strip()
    try:
        bars = int(float(raw))
    except (ValueError, TypeError):
        return "unknown"
    if bars <= 0:
        return "unknown"
    if bars <= 24:
        return "short"
    if bars <= 48:
        return "medium"
    return "long"


def _normalize_direction(direction: Any) -> str:
    raw = _safe_str(direction)
    if raw in ("1", "1.0", "long", "up", "buy"):
        return "long"
    if raw in ("-1", "-1.0", "short", "down", "sell"):
        return "short"
    return "neutral"


def _context_dim_bucket(dim_count: Any) -> str:
    try:
        n = int(float(str(dim_count or 0)))
        if n == 0:
            return "none"
        if n == 1:
            return "one"
        return "multi"
    except (ValueError, TypeError):
        return "none"


def _extract_event_family(row: dict) -> str:
    return (
        _safe_str(row.get("event_family"))
        or _safe_str(row.get("canonical_event_type"))
        or _safe_str(row.get("event_type"))
        or "unknown"
    ).upper()


def _lineage_event_prefix(lineage_key: str) -> str:
    """Extract the EVENT:xxx segment from a concept lineage key."""
    raw = str(lineage_key or "").strip()
    if not raw:
        return ""
    for segment in raw.split("|"):
        if segment.startswith("EVENT:"):
            return segment
    return ""


def _fold_sign_vector(row: dict) -> str:
    """Condense fold signs into a compact string for concordance comparison.

    Uses fold_sign_vector if present, otherwise builds from fold_pass_rate
    as a single-character proxy.
    """
    if row.get("fold_sign_vector"):
        return str(row["fold_sign_vector"]).strip()
    # Fallback: synthesize a 1-character sign from direction + robustness
    direction = _normalize_direction(row.get("direction"))
    rob = float(row.get("robustness_score", 0.5) or 0.5)
    if direction == "long":
        return "+" if rob >= 0.5 else "-"
    if direction == "short":
        return "-" if rob >= 0.5 else "+"
    return "0"


def _sign_concordance(v1: str, v2: str) -> float:
    """Return fraction of positions where v1 and v2 agree. Neutral = 0.5."""
    if not v1 or not v2:
        return 0.5
    min_len = min(len(v1), len(v2))
    if min_len == 0:
        return 0.5
    matches = sum(1 for a, b in zip(v1[:min_len], v2[:min_len]) if a == b)
    return float(matches) / float(min_len)


# ---------------------------------------------------------------------------
# Union-Find for connected components
# ---------------------------------------------------------------------------


class _UnionFind:
    def __init__(self, n: int) -> None:
        self.parent = list(range(n))
        self.rank = [0] * n

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, x: int, y: int) -> None:
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        if self.rank[rx] < self.rank[ry]:
            rx, ry = ry, rx
        self.parent[ry] = rx
        if self.rank[rx] == self.rank[ry]:
            self.rank[rx] += 1


# ---------------------------------------------------------------------------
# Step 1 — Overlap signatures
# ---------------------------------------------------------------------------


def build_candidate_overlap_signatures(candidates: pd.DataFrame) -> pd.DataFrame:
    """Build one canonical overlap signature row per candidate.

    Returns a DataFrame indexed the same as *candidates* with signature fields.
    """
    if candidates is None or candidates.empty:
        return pd.DataFrame()

    rows = []
    for idx, row in candidates.iterrows():
        row_dict = dict(row)
        event_fam = _extract_event_family(row_dict)
        tmpl_fam = _template_family(row_dict.get("rule_template") or row_dict.get("template_id"))
        direction = _normalize_direction(row_dict.get("direction"))
        horizon_b = _horizon_bucket(row_dict.get("horizon") or row_dict.get("horizon_bars"))
        sym_scope = _safe_str(row_dict.get("symbol_scope_type", "single")) or "single"
        ctx_count = row_dict.get("context_dim_count", 0) or 0
        ctx_bucket = _context_dim_bucket(ctx_count)
        lineage_key = _safe_str(row_dict.get("concept_lineage_key", ""))
        fold_sv = _fold_sign_vector(row_dict)
        fold_pr = float(
            row_dict.get("fold_stability_score", row_dict.get("fold_pass_rate", 0.5)) or 0.5
        )

        rows.append(
            {
                "_orig_idx": idx,
                "candidate_id": str(row_dict.get("candidate_id", f"cand_{idx}")),
                "primary_event_id": event_fam,
                "event_family": event_fam,
                "template_family": tmpl_fam,
                "direction": direction,
                "horizon_bucket": horizon_b,
                "symbol_scope_type": sym_scope,
                "context_dim_count": int(ctx_count),
                "context_dim_count_bucket": ctx_bucket,
                "concept_lineage_key": lineage_key,
                "fold_sign_vector": fold_sv,
                "fold_pass_rate": fold_pr,
            }
        )

    return pd.DataFrame(rows).set_index("_orig_idx")


# ---------------------------------------------------------------------------
# Step 2 — Pairwise similarity
# ---------------------------------------------------------------------------


def _structural_similarity(sig_a: dict, sig_b: dict) -> float:
    score = 0.0
    for field, weight in _STRUCTURAL_WEIGHTS:
        va = sig_a.get(field, "")
        vb = sig_b.get(field, "")
        if va and vb and va == vb:
            score += weight
    return min(1.0, score)


def _fold_similarity(sig_a: dict, sig_b: dict) -> float:
    return _sign_concordance(
        str(sig_a.get("fold_sign_vector", "")),
        str(sig_b.get("fold_sign_vector", "")),
    )


def _lineage_similarity(sig_a: dict, sig_b: dict) -> float:
    ka = str(sig_a.get("concept_lineage_key", "")).strip()
    kb = str(sig_b.get("concept_lineage_key", "")).strip()
    if not ka or not kb:
        return 0.0
    if ka == kb:
        return 1.0
    pa = _lineage_event_prefix(ka)
    pb = _lineage_event_prefix(kb)
    if pa and pb and pa == pb:
        return 0.5
    return 0.0


def _pairwise_score(
    sig_a: dict,
    sig_b: dict,
    *,
    structural_weight: float,
    fold_weight: float,
    lineage_weight: float,
) -> tuple[float, list[str]]:
    """Return (similarity_score, shared_dimension_labels)."""
    struct_sim = _structural_similarity(sig_a, sig_b)
    fold_sim = _fold_similarity(sig_a, sig_b)
    lin_sim = _lineage_similarity(sig_a, sig_b)
    score = structural_weight * struct_sim + fold_weight * fold_sim + lineage_weight * lin_sim
    score = float(np.clip(score, 0.0, 1.0))

    shared: list[str] = []
    for field, _ in _STRUCTURAL_WEIGHTS:
        va = sig_a.get(field, "")
        vb = sig_b.get(field, "")
        if va and vb and va == vb:
            shared.append(f"{field}:{va}")
    if lin_sim >= 0.5:
        shared.append(f"lineage:{_lineage_event_prefix(sig_a.get('concept_lineage_key', ''))}")

    return score, shared


def compute_pairwise_similarity(
    signatures: pd.DataFrame,
    *,
    structural_weight: float = _DEFAULT_STRUCTURAL_WEIGHT,
    fold_weight: float = _DEFAULT_FOLD_WEIGHT,
    lineage_weight: float = _DEFAULT_LINEAGE_WEIGHT,
    max_candidates: int = _MAX_CANDIDATES_FULL_PAIRWISE,
) -> pd.DataFrame:
    """Compute pairwise similarity. Returns an edge-list DataFrame.

    Columns: source_idx, target_idx, source_id, target_id,
             similarity, shared_dimensions.

    When len(signatures) > max_candidates, uses event_family + direction
    pre-bucketing to reduce comparisons.
    """
    if signatures is None or signatures.empty:
        return pd.DataFrame(
            columns=[
                "source_idx",
                "target_idx",
                "source_id",
                "target_id",
                "similarity",
                "shared_dimensions",
            ]
        )

    sig_list = signatures.reset_index().to_dict(orient="records")
    n = len(sig_list)

    # O(n²) safeguard: pre-bucket for large candidate sets
    use_full = n <= max_candidates
    edges: list[dict] = []

    if use_full:
        pairs = [(i, j) for i in range(n) for j in range(i + 1, n)]
    else:
        log.info(
            "Diversification: %d candidates exceeds limit %d — using bucket similarity",
            n,
            max_candidates,
        )
        # Pre-bucket by event_family × direction
        buckets: dict[str, list[int]] = {}
        for i, sig in enumerate(sig_list):
            key = f"{sig.get('event_family', 'unknown')}::{sig.get('direction', 'neutral')}"
            buckets.setdefault(key, []).append(i)
        pairs = []
        for bucket in buckets.values():
            for ii, i in enumerate(bucket):
                for j in bucket[ii + 1 :]:
                    pairs.append((i, j))

    for i, j in pairs:
        sig_a = sig_list[i]
        sig_b = sig_list[j]
        score, shared = _pairwise_score(
            sig_a,
            sig_b,
            structural_weight=structural_weight,
            fold_weight=fold_weight,
            lineage_weight=lineage_weight,
        )
        if score <= 0.0:
            continue
        edges.append(
            {
                "source_idx": i,
                "target_idx": j,
                "source_id": str(sig_a.get("candidate_id", "")),
                "target_id": str(sig_b.get("candidate_id", "")),
                "similarity": round(score, 6),
                "shared_dimensions": "|".join(shared),
            }
        )

    return (
        pd.DataFrame(
            edges,
            columns=[
                "source_idx",
                "target_idx",
                "source_id",
                "target_id",
                "similarity",
                "shared_dimensions",
            ],
        )
        if edges
        else pd.DataFrame(
            columns=[
                "source_idx",
                "target_idx",
                "source_id",
                "target_id",
                "similarity",
                "shared_dimensions",
            ]
        )
    )


# ---------------------------------------------------------------------------
# Step 3 — Overlap clustering
# ---------------------------------------------------------------------------


def cluster_candidates_by_overlap(
    candidates: pd.DataFrame,
    similarity_edges: pd.DataFrame,
    *,
    edge_threshold: float = _DEFAULT_EDGE_THRESHOLD,
) -> pd.DataFrame:
    """Add cluster columns to *candidates*.

    Returns annotated copy; does not modify in place.
    Columns added: overlap_cluster_id, cluster_size, cluster_density,
                   is_duplicate_like.
    """
    out = candidates.copy()
    n = len(out)
    if n == 0:
        for col in ["overlap_cluster_id", "cluster_size", "cluster_density", "is_duplicate_like"]:
            out[col] = pd.Series(dtype=object if col == "overlap_cluster_id" else float)
        return out

    uf = _UnionFind(n)

    # Per-pair max similarity for duplicate detection
    max_sim: dict[int, float] = {i: 0.0 for i in range(n)}

    if similarity_edges is not None and not similarity_edges.empty:
        for edge in similarity_edges.itertuples(index=False):
            sim = float(getattr(edge, "similarity", 0.0))
            if sim < float(edge_threshold):
                continue
            si = int(edge.source_idx)
            ti = int(edge.target_idx)
            if 0 <= si < n and 0 <= ti < n:
                uf.union(si, ti)
                max_sim[si] = max(max_sim[si], sim)
                max_sim[ti] = max(max_sim[ti], sim)

    # Build components
    root_to_members: dict[int, list[int]] = {}
    for pos in range(n):
        root = uf.find(pos)
        root_to_members.setdefault(root, []).append(pos)

    # Stable cluster id: "c" + zero-padded min-position in component
    cluster_id_map: dict[int, str] = {}
    for root, members in root_to_members.items():
        cid = f"c{min(members):04d}"
        for pos in members:
            cluster_id_map[pos] = cid

    # Compute intra-cluster density
    cluster_sizes: dict[str, int] = {}
    for cid in cluster_id_map.values():
        cluster_sizes[cid] = cluster_sizes.get(cid, 0) + 1

    cluster_density_map: dict[str, float] = {}
    if similarity_edges is not None and not similarity_edges.empty:
        for edge in similarity_edges.itertuples(index=False):
            sim = float(getattr(edge, "similarity", 0.0))
            si = int(edge.source_idx)
            ti = int(edge.target_idx)
            if 0 <= si < n and 0 <= ti < n:
                cid_s = cluster_id_map.get(si, "")
                cid_t = cluster_id_map.get(ti, "")
                if cid_s == cid_t and cid_s:
                    old = cluster_density_map.get(cid_s, (0.0, 0))
                    cluster_density_map[cid_s] = (old[0] + sim, old[1] + 1)  # type: ignore[assignment]

    avg_density: dict[str, float] = {}
    for cid, (total, count) in cluster_density_map.items():  # type: ignore[misc]
        avg_density[cid] = float(total) / float(count) if count > 0 else 0.0

    # Assign columns
    cluster_ids = [cluster_id_map.get(pos, f"c{pos:04d}") for pos in range(n)]
    cluster_sz = [cluster_sizes.get(cid, 1) for cid in cluster_ids]
    cluster_dens = [round(avg_density.get(cid, 0.0), 4) for cid in cluster_ids]
    is_dup = [
        bool(sz > 1 and max_sim.get(pos, 0.0) >= 0.85) for pos, sz in zip(range(n), cluster_sz)
    ]

    out["overlap_cluster_id"] = cluster_ids
    out["cluster_size"] = cluster_sz
    out["cluster_density"] = cluster_dens
    out["is_duplicate_like"] = is_dup

    return out


# ---------------------------------------------------------------------------
# Step 4 — Novelty and crowding
# ---------------------------------------------------------------------------


def compute_novelty_crowding(
    candidates: pd.DataFrame,
    similarity_edges: pd.DataFrame,
) -> pd.DataFrame:
    """Add novelty_score, crowding_penalty, cluster_rank to *candidates*.

    These are purely additive fields.
    """
    out = candidates.copy()
    n = len(out)
    if n == 0:
        out["novelty_score"] = pd.Series(dtype=float)
        out["crowding_penalty"] = pd.Series(dtype=float)
        out["cluster_rank"] = pd.Series(dtype=int)
        return out

    # Max similarity to any other candidate
    max_sim_to_other: dict[int, float] = {i: 0.0 for i in range(n)}
    if similarity_edges is not None and not similarity_edges.empty:
        for edge in similarity_edges.itertuples(index=False):
            sim = float(getattr(edge, "similarity", 0.0))
            si = int(edge.source_idx)
            ti = int(edge.target_idx)
            if 0 <= si < n and 0 <= ti < n:
                max_sim_to_other[si] = max(max_sim_to_other[si], sim)
                max_sim_to_other[ti] = max(max_sim_to_other[ti], sim)

    log_n = math.log1p(max(n, 1))
    cluster_sizes = (
        out["cluster_size"] if "cluster_size" in out.columns else pd.Series(1, index=out.index)
    ).fillna(1)
    ledger_pen = pd.to_numeric(
        out["ledger_multiplicity_penalty"]
        if "ledger_multiplicity_penalty" in out.columns
        else pd.Series(0.0, index=out.index),
        errors="coerce",
    ).fillna(0)

    novelty_scores: list[float] = []
    crowding_penalties: list[float] = []
    for pos in range(n):
        max_sim = max_sim_to_other.get(pos, 0.0)
        c_size = int(cluster_sizes.iloc[pos])
        l_pen = float(ledger_pen.iloc[pos])

        novelty = 1.0 - max_sim - 0.2 * math.log1p(c_size - 1) / log_n
        novelty = float(np.clip(novelty, 0.0, 1.0))
        novelty_scores.append(round(novelty, 6))

        crowding = 0.4 * (c_size - 1) / max(n, 1) + 0.4 * (1.0 - novelty) + 0.2 * (l_pen / 3.0)
        crowding = float(np.clip(crowding, 0.0, 1.0))
        crowding_penalties.append(round(crowding, 6))

    out["novelty_score"] = novelty_scores
    out["crowding_penalty"] = crowding_penalties

    # Cluster rank: rank within cluster by quality score (ascending rank = better)
    quality_col = _best_quality_col(out)
    quality = pd.to_numeric(out.get(quality_col, 0), errors="coerce").fillna(0.0)
    cluster_col = out.get("overlap_cluster_id", pd.Series("c0000", index=out.index)).fillna("c0000")
    out["cluster_rank"] = (
        quality.groupby(cluster_col).rank(method="first", ascending=False).astype(int)
    )

    return out


def _best_quality_col(df: pd.DataFrame) -> str:
    for col in ("discovery_quality_score_v3", "discovery_quality_score", "t_stat"):
        if col in df.columns:
            return col
    return "t_stat"


# ---------------------------------------------------------------------------
# Step 5 — Greedy MMR shortlist selector
# ---------------------------------------------------------------------------


def select_diversified_shortlist(
    candidates: pd.DataFrame,
    *,
    size: int = 20,
    lambda_quality: float = 0.7,
    lambda_overlap: float = 0.2,
    lambda_crowding: float = 0.1,
    max_per_overlap_cluster: int = 2,
    max_per_trigger_family: int = 3,
    max_per_lineage: int = 2,
    quality_col: str | None = None,
) -> pd.DataFrame:
    """Greedy MMR-style diversified shortlist.

    Returns a copy of the selected rows from *candidates*, with these
    columns added / overwritten:
        shortlist_rank, selection_score, selection_reason,
        selected_into_diversified_shortlist.

    Tie-breaking: candidate_id lexicographic (deterministic).
    """
    if candidates is None or candidates.empty:
        return pd.DataFrame()

    qcol = quality_col or _best_quality_col(candidates)

    # Enforce deterministic candidate ordering before MMR selection.
    # When candidates arrive from run_distributed_search the DataFrame index
    # reflects thread-completion order, which is non-deterministic.  Sorting
    # here ensures the lexicographic tie-breaking in pool_order (below) is
    # reproducible across runs with the same proposal set.
    _cid_for_sort = (
        candidates["candidate_id"].astype(str)
        if "candidate_id" in candidates.columns
        else pd.Series(range(len(candidates)), index=candidates.index).astype(str)
    )
    _sort_key = pd.to_numeric(candidates.get(qcol, 0), errors="coerce").fillna(0.0)
    candidates = candidates.assign(_sort_cid=_cid_for_sort, _sort_q=_sort_key)
    candidates = (
        candidates.sort_values(
            ["_sort_q", "_sort_cid"], ascending=[False, True], na_position="last"
        )
        .drop(columns=["_sort_q", "_sort_cid"])
        .reset_index(drop=True)
    )

    quality_raw = pd.to_numeric(candidates.get(qcol, 0), errors="coerce").fillna(0.0)
    q_min, q_max = float(quality_raw.min()), float(quality_raw.max())
    q_range = q_max - q_min
    if q_range < 1e-9:
        quality_norm = pd.Series(0.5, index=candidates.index)
    else:
        quality_norm = (quality_raw - q_min) / q_range

    crowding = pd.to_numeric(
        candidates["crowding_penalty"]
        if "crowding_penalty" in candidates.columns
        else pd.Series(0.0, index=candidates.index),
        errors="coerce",
    ).fillna(0.0)
    cluster_col = (
        candidates["overlap_cluster_id"]
        if "overlap_cluster_id" in candidates.columns
        else pd.Series("c0000", index=candidates.index)
    ).fillna("c0000")
    trig_col = (
        candidates["event_family"]
        if "event_family" in candidates.columns
        else candidates["canonical_event_type"]
        if "canonical_event_type" in candidates.columns
        else candidates["event_type"]
        if "event_type" in candidates.columns
        else pd.Series("unknown", index=candidates.index)
    ).fillna("unknown")
    lineage_col = (
        candidates["concept_lineage_key"]
        if "concept_lineage_key" in candidates.columns
        else pd.Series("", index=candidates.index)
    ).fillna("")
    cid_col = (
        candidates["candidate_id"]
        if "candidate_id" in candidates.columns
        else pd.Series(range(len(candidates)), index=candidates.index)
    ).astype(str)

    # Build similarity lookup: candidate_id → {candidate_id: similarity}
    # We use a simple max_sim_to_selected dict updated greedily
    candidate_ids = cid_col.tolist()

    # Build similarity matrix from overlap edges (if cluster cols available)
    sim_lookup: dict[str, dict[str, float]] = {cid: {} for cid in candidate_ids}
    # Infer from cluster membership: same cluster → density as proxy similarity
    if "overlap_cluster_id" in candidates.columns and "cluster_density" in candidates.columns:
        cluster_density = pd.to_numeric(
            candidates.get("cluster_density", 0), errors="coerce"
        ).fillna(0.0)
        for pos, idx in enumerate(candidates.index):
            cid = candidate_ids[pos]
            clu = str(cluster_col.iloc[pos])
            dens = float(cluster_density.iloc[pos])
            # All same-cluster candidates share the cluster density as proxy
            same_cluster_mask = cluster_col == clu
            for j, jidx in enumerate(candidates.index):
                if same_cluster_mask.iloc[j] and j != pos:
                    other_id = candidate_ids[j]
                    sim_lookup[cid][other_id] = max(sim_lookup[cid].get(other_id, 0.0), dens)

    selected_ids: list[str] = []
    selected_clusters: dict[str, int] = {}
    selected_triggers: dict[str, int] = {}
    selected_lineages: dict[str, int] = {}

    shortlist_rows: list[tuple[int, str, float, str]] = []  # (rank, candidate_id, score, reason)

    # Sort pool by quality desc, then candidate_id (deterministic)
    pool_order = sorted(
        range(len(candidates)),
        key=lambda i: (-float(quality_norm.iloc[i]), candidate_ids[i]),
    )

    for _ in range(size):
        best_pos: int | None = None
        best_score = float("-inf")
        best_reason = ""

        for pos in pool_order:
            cid = candidate_ids[pos]
            if cid in selected_ids:
                continue

            # Hard cap checks
            clu = str(cluster_col.iloc[pos])
            trig = str(trig_col.iloc[pos]).upper()
            lin = str(lineage_col.iloc[pos])

            reasons: list[str] = []
            if selected_clusters.get(clu, 0) >= max_per_overlap_cluster:
                reasons.append("same_lineage_cluster_cap")
            if selected_triggers.get(trig, 0) >= max_per_trigger_family:
                reasons.append("same_trigger_family_cap")
            if selected_lineages.get(lin, 0) >= max_per_lineage and lin:
                reasons.append("same_lineage_cap")
            if reasons:
                continue

            # MMR score
            q = float(quality_norm.iloc[pos])
            max_sim = max(
                (sim_lookup.get(cid, {}).get(sid, 0.0) for sid in selected_ids),
                default=0.0,
            )
            cpen = float(crowding.iloc[pos])

            sel_score = lambda_quality * q - lambda_overlap * max_sim - lambda_crowding * cpen

            if sel_score > best_score or (
                abs(sel_score - best_score) < 1e-9
                and cid < (candidate_ids[best_pos] if best_pos is not None else "~")
            ):
                best_pos = pos
                best_score = sel_score
                best_reason = "selected_for_diversity" if max_sim > 0.0 else "highest_quality"

        if best_pos is None:
            break

        cid = candidate_ids[best_pos]
        selected_ids.append(cid)
        rank = len(selected_ids)
        shortlist_rows.append((rank, cid, round(best_score, 6), best_reason))

        clu = str(cluster_col.iloc[best_pos])
        trig = str(trig_col.iloc[best_pos]).upper()
        lin = str(lineage_col.iloc[best_pos])
        selected_clusters[clu] = selected_clusters.get(clu, 0) + 1
        selected_triggers[trig] = selected_triggers.get(trig, 0) + 1
        if lin:
            selected_lineages[lin] = selected_lineages.get(lin, 0) + 1

    # Build output rows
    selected_id_set = {row[1] for row in shortlist_rows}
    rank_map = {row[1]: row[0] for row in shortlist_rows}
    score_map = {row[1]: row[2] for row in shortlist_rows}
    reason_map = {row[1]: row[3] for row in shortlist_rows}

    # Return selected rows with shortlist columns added
    mask = cid_col.isin(selected_id_set)
    shortlist = candidates[mask].copy()
    if shortlist.empty:
        return pd.DataFrame()

    shortlist["shortlist_rank"] = shortlist.index.map(
        lambda idx: rank_map.get(str(cid_col.loc[idx]), 0)
    )
    shortlist["selection_score"] = shortlist.index.map(
        lambda idx: score_map.get(str(cid_col.loc[idx]), 0.0)
    )
    shortlist["selection_reason"] = shortlist.index.map(
        lambda idx: reason_map.get(str(cid_col.loc[idx]), "")
    )
    shortlist["selected_into_diversified_shortlist"] = True
    shortlist = shortlist.sort_values("shortlist_rank").reset_index(drop=True)
    log.info(
        "Diversification shortlist: %d candidates selected from %d (size=%d)",
        len(shortlist),
        len(candidates),
        size,
    )
    return shortlist


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def annotate_candidates_with_diversification(
    candidates: pd.DataFrame,
    diversification_config: dict,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run all Phase 5 steps on *candidates*.

    Args:
        candidates:             Full scored candidate table (post v2/v3 scoring).
        diversification_config: ``discovery_selection`` config dict from search spec.

    Returns:
        (annotated_candidates, shortlist_df)
        - annotated_candidates: same rows as input, Phase 5 columns appended.
        - shortlist_df: greedy shortlist (empty DataFrame when disabled).
    """
    if candidates is None or candidates.empty:
        return (pd.DataFrame(), pd.DataFrame())

    mode = str(diversification_config.get("mode", "greedy")).strip().lower()
    if mode == "off":
        log.info("Diversification mode=off — skipping Phase 5")
        return candidates.copy(), pd.DataFrame()

    overlap_cfg = diversification_config.get("overlap", {})
    shortlist_cfg = diversification_config.get("shortlist", {})

    struct_w = float(overlap_cfg.get("structural_weight", _DEFAULT_STRUCTURAL_WEIGHT))
    fold_w = float(overlap_cfg.get("fold_weight", _DEFAULT_FOLD_WEIGHT))
    lin_w = float(overlap_cfg.get("lineage_weight", _DEFAULT_LINEAGE_WEIGHT))
    edge_thresh = float(overlap_cfg.get("edge_threshold", _DEFAULT_EDGE_THRESHOLD))

    # Step 1 — Signatures
    try:
        sigs = build_candidate_overlap_signatures(candidates)
    except Exception as exc:
        log.warning("Phase 5: signature build failed: %s", exc)
        _stub_overlap_columns(candidates)
        return candidates.copy(), pd.DataFrame()

    # Step 2 — Similarity
    try:
        edges = compute_pairwise_similarity(
            sigs,
            structural_weight=struct_w,
            fold_weight=fold_w,
            lineage_weight=lin_w,
        )
    except Exception as exc:
        log.warning("Phase 5: similarity computation failed: %s", exc)
        edges = pd.DataFrame()

    # Step 3 — Clustering
    try:
        annotated = cluster_candidates_by_overlap(candidates, edges, edge_threshold=edge_thresh)
    except Exception as exc:
        log.warning("Phase 5: clustering failed: %s", exc)
        annotated = candidates.copy()
        for col in ["overlap_cluster_id", "cluster_size", "cluster_density", "is_duplicate_like"]:
            annotated[col] = None if col == "overlap_cluster_id" else 0

    # Step 4 — Novelty / crowding
    try:
        annotated = compute_novelty_crowding(annotated, edges)
    except Exception as exc:
        log.warning("Phase 5: novelty/crowding failed: %s", exc)
        for col in ["novelty_score", "crowding_penalty", "cluster_rank"]:
            annotated[col] = 0.0

    # Initialise shortlist columns so the full table always has them
    annotated["selected_into_diversified_shortlist"] = False
    annotated["shortlist_rank"] = 0
    annotated["selection_score"] = float("nan")
    annotated["selection_reason"] = ""

    # Step 5 — Shortlist
    shortlist_df = pd.DataFrame()
    shortlist_enabled = bool(shortlist_cfg.get("enabled", False))
    if shortlist_enabled:
        try:
            shortlist_df = select_diversified_shortlist(
                annotated,
                size=int(shortlist_cfg.get("size", 20)),
                lambda_quality=float(shortlist_cfg.get("lambda_quality", 0.7)),
                lambda_overlap=float(shortlist_cfg.get("lambda_overlap", 0.2)),
                lambda_crowding=float(shortlist_cfg.get("lambda_crowding", 0.1)),
                max_per_overlap_cluster=int(shortlist_cfg.get("max_per_overlap_cluster", 2)),
                max_per_trigger_family=int(shortlist_cfg.get("max_per_trigger_family", 3)),
                max_per_lineage=int(shortlist_cfg.get("max_per_lineage", 2)),
            )
            # Back-annotate shortlist columns onto full table
            if not shortlist_df.empty and "candidate_id" in shortlist_df.columns:
                cid_col = annotated.get(
                    "candidate_id", pd.Series("", index=annotated.index)
                ).astype(str)
                shortlist_cids = set(shortlist_df["candidate_id"].astype(str).tolist())
                sl_rank_map = dict(
                    zip(
                        shortlist_df["candidate_id"].astype(str),
                        shortlist_df["shortlist_rank"].astype(int),
                    )
                )
                sl_score_map = dict(
                    zip(
                        shortlist_df["candidate_id"].astype(str),
                        shortlist_df["selection_score"].astype(float),
                    )
                )
                sl_reason_map = dict(
                    zip(
                        shortlist_df["candidate_id"].astype(str),
                        shortlist_df["selection_reason"].astype(str),
                    )
                )
                annotated["selected_into_diversified_shortlist"] = cid_col.isin(shortlist_cids)
                annotated["shortlist_rank"] = cid_col.map(sl_rank_map).fillna(0).astype(int)
                annotated["selection_score"] = cid_col.map(sl_score_map)
                annotated["selection_reason"] = cid_col.map(sl_reason_map).fillna("")
        except Exception as exc:
            log.warning("Phase 5: shortlist selection failed: %s", exc)

    n_clusters = (
        annotated["overlap_cluster_id"].nunique()
        if "overlap_cluster_id" in annotated.columns
        else 0
    )
    n_dup = (
        int(annotated.get("is_duplicate_like", pd.Series(False)).sum())
        if "is_duplicate_like" in annotated.columns
        else 0
    )
    log.info(
        "Phase 5: %d clusters, %d duplicate-like, %d shortlisted",
        n_clusters,
        n_dup,
        len(shortlist_df),
    )
    return annotated, shortlist_df


def _stub_overlap_columns(df: pd.DataFrame) -> None:
    """Add zero/default overlap columns in-place when Phase 5 fails."""
    defaults = {
        "overlap_cluster_id": None,
        "cluster_size": 1,
        "cluster_density": 0.0,
        "is_duplicate_like": False,
        "novelty_score": 1.0,
        "crowding_penalty": 0.0,
        "cluster_rank": 1,
        "selected_into_diversified_shortlist": False,
        "shortlist_rank": 0,
        "selection_score": float("nan"),
        "selection_reason": "",
    }
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
