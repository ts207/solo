"""
Pipeline stage: run the hypothesis search engine.

Sequence:
  1. generate_hypotheses() from the configured search space.
  2. Load feature table for each symbol.
  3. run_distributed_search(hypotheses, features).
  4. [Phase 3.3] cluster_hypotheses() deduplication — mark non-representatives
     as redundant before BH adjustment to improve statistical power.
  5. Write hypothesis_metrics.parquet and hypothesis_search_summary.json.
  6. Optionally write bridge_candidates.parquet (--run_bridge_adapter flag).
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import List

import pandas as pd

from project.core.config import get_data_root
from project.io.utils import write_parquet
from project.research.phase2 import load_features
from project.research.search.bridge_adapter import (
    hypotheses_to_bridge_candidates,
    split_bridge_candidates,
)
from project.research.search.distributed_runner import run_distributed_search
from project.research.search.evaluator import evaluated_records_from_metrics
from project.research.search.generator import generate_hypotheses_with_audit
from project.specs.gates import load_gates_spec, select_phase2_gate_spec

LOG = logging.getLogger(__name__)

DEFAULT_SEARCH_SPACE_PATH = Path(__file__).resolve().parents[2] / "spec" / "search_space.yaml"


def _resolve_search_space_path(explicit_path: str | Path | None) -> Path:
    if explicit_path:
        return Path(explicit_path)
    return DEFAULT_SEARCH_SPACE_PATH



def _resolve_search_min_t_stat(explicit_min_t_stat: float | None) -> float:
    if explicit_min_t_stat is not None:
        return float(explicit_min_t_stat)
    phase2_gates = select_phase2_gate_spec(
        load_gates_spec(Path(__file__).resolve().parents[2]),
        mode="research",
        gate_profile="auto",
    )
    return float(phase2_gates.get("min_t_stat", 1.5))


# ---------------------------------------------------------------------------
# Phase 3.3 — within-run alpha clustering deduplication
# ---------------------------------------------------------------------------


def _cluster_deduplicate(
    metrics: pd.DataFrame,
    *,
    eps: float = 0.3,
    min_samples: int = 1,
) -> pd.DataFrame:
    """Mark redundant hypotheses within a run before BH adjustment.

    Phase 3.3: Runs DBSCAN on pairwise metric-vector distances to identify
    clusters of correlated hypotheses.  For each cluster, only the
    representative (highest Sharpe) is kept; non-representatives are marked
    with ``is_cluster_redundant = True``.

    Since per-bar PnL streams are not available at this pipeline stage, the
    distance metric is computed from a normalised vector of aggregate metrics:
    (mean_return_bps, t_stat, sharpe, hit_rate).  Hypotheses whose aggregate
    profiles are nearly identical — which will generate correlated outcomes —
    are grouped together and deduplicated.

    The ``is_cluster_redundant`` column is written to the metrics parquet so
    downstream stages can filter it before FDR correction.  The full set of
    hypotheses is retained in the file to preserve audit traceability.
    """
    if metrics.empty:
        return metrics

    metrics = metrics.copy()
    metrics["is_cluster_redundant"] = False

    # Metric columns used for similarity proxy
    proxy_cols = [c for c in ["mean_return_bps", "t_stat", "sharpe", "hit_rate"] if c in metrics.columns]
    if not proxy_cols or "hypothesis_id" not in metrics.columns:
        return metrics

    valid = metrics.dropna(subset=proxy_cols)
    if len(valid) < 2:
        return metrics

    # Build normalised feature matrix — each row is one hypothesis
    X = valid[proxy_cols].values.astype(float)
    # Normalise column-wise (z-score); handle zero-std columns
    col_std = X.std(axis=0)
    col_std[col_std == 0] = 1.0
    X_norm = (X - X.mean(axis=0)) / col_std

    # Pairwise Euclidean distance in normalised metric space
    from sklearn.cluster import DBSCAN
    from sklearn.metrics import pairwise_distances

    dist = pairwise_distances(X_norm, metric="euclidean")

    clustering = DBSCAN(eps=eps, min_samples=min_samples, metric="precomputed")
    labels = clustering.fit_predict(dist)

    # Build cluster → hypothesis_id mapping
    h_ids = valid["hypothesis_id"].tolist()
    sharpe_col = "sharpe" if "sharpe" in valid.columns else proxy_cols[0]
    sharpes = valid[sharpe_col].fillna(0.0).tolist()

    cluster_map: dict[int, list[tuple[str, float]]] = {}
    for i, label in enumerate(labels):
        cluster_map.setdefault(int(label), []).append((h_ids[i], sharpes[i]))

    redundant_ids: set[str] = set()
    for label, members in cluster_map.items():
        if label == -1 or len(members) == 1:
            # Noise points or singletons — not redundant
            continue
        # Representative = highest Sharpe in cluster
        best_id = max(members, key=lambda m: m[1])[0]
        for hid, _ in members:
            if hid != best_id:
                redundant_ids.add(hid)

    if redundant_ids:
        mask = metrics["hypothesis_id"].isin(redundant_ids)
        metrics.loc[mask, "is_cluster_redundant"] = True
        LOG.info(
            "Phase 3.3 clustering: %d hypotheses → %d clusters → %d marked redundant",
            len(valid),
            len(cluster_map),
            len(redundant_ids),
        )

    return metrics


def _normalize_audit_frame(rows: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(rows or [])
    if frame.empty:
        return frame
    for column in frame.columns:
        if frame[column].dtype != "object":
            continue
        sample = next(
            (
                value
                for value in frame[column]
                if value is not None and not (isinstance(value, float) and pd.isna(value))
            ),
            None,
        )
        if isinstance(sample, (dict, list, tuple)):
            frame[column] = frame[column].map(
                lambda value: (
                    json.dumps(value, sort_keys=True)
                    if isinstance(value, (dict, list, tuple))
                    else value
                )
            )
    return frame


def _write_hypothesis_audit_artifacts(out_dir: Path, audit: dict) -> None:
    write_parquet(
        _normalize_audit_frame(audit.get("generated_rows", [])),
        out_dir / "generated_hypotheses.parquet",
    )
    write_parquet(
        _normalize_audit_frame(audit.get("rejected_rows", [])),
        out_dir / "rejected_hypotheses.parquet",
    )
    write_parquet(
        _normalize_audit_frame(audit.get("feasible_rows", [])),
        out_dir / "feasible_hypotheses.parquet",
    )


def _write_evaluation_artifacts(
    out_dir: Path, metrics: pd.DataFrame, gate_failures: pd.DataFrame
) -> None:
    write_parquet(evaluated_records_from_metrics(metrics), out_dir / "evaluated_hypotheses.parquet")
    write_parquet(gate_failures, out_dir / "gate_failures.parquet")


def _write_regime_conditional_candidates_from_breakdown(
    metrics: pd.DataFrame,
    regime_breakdown: "pd.DataFrame | None",
    out_dir: Path,
    *,
    weak_overall_threshold: float = 1.5,
    strong_regime_threshold: float = 1.5,
    min_regime_n: int = 20,
) -> None:
    """Phase 4.2 (deep hook) — Write regime_conditional_candidates.parquet using
    the full per-regime breakdown from evaluate_by_regime().

    Identifies hypotheses where:
      - Overall t_stat < weak_overall_threshold (aggregate signal is weak)
      - At least one regime has t_stat > strong_regime_threshold with n >= min_regime_n

    This is the vision-doc description of regime-specific alpha: edges that the
    aggregate evaluator would discard but that are real within a specific market
    regime.  The campaign controller reads this artefact and adds entries to the
    explore_adjacent queue with the strong regime pinned as context.
    """
    rcc_path = out_dir / "regime_conditional_candidates.parquet"
    _RCC_COLS = [
        "hypothesis_id", "trigger_key", "template_id", "direction", "horizon",
        "event_type", "overall_t_stat", "best_regime", "best_regime_t_stat",
        "best_regime_mean_return_bps", "best_regime_n",
    ]
    empty = pd.DataFrame(columns=_RCC_COLS)

    if regime_breakdown is None or regime_breakdown.empty:
        write_parquet(empty, rcc_path)
        return

    if metrics.empty or "t_stat" not in metrics.columns or "hypothesis_id" not in metrics.columns:
        write_parquet(empty, rcc_path)
        return

    overall = metrics[["hypothesis_id", "t_stat", "trigger_key",
                        "template_id", "direction", "horizon"]].copy()
    overall["t_stat"] = pd.to_numeric(overall["t_stat"], errors="coerce").fillna(0.0)

    # Only candidates with weak overall signal
    weak_overall = overall[overall["t_stat"].abs() < weak_overall_threshold]
    if weak_overall.empty:
        write_parquet(empty, rcc_path)
        return

    # Within each weak hypothesis find the best-performing regime
    rb = regime_breakdown.copy()
    rb["t_stat"] = pd.to_numeric(rb["t_stat"], errors="coerce").fillna(0.0)
    rb["n"] = pd.to_numeric(rb["n"], errors="coerce").fillna(0).astype(int)
    rb["mean_return_bps"] = pd.to_numeric(rb["mean_return_bps"], errors="coerce").fillna(0.0)

    strong_regime = rb[
        (rb["t_stat"] >= strong_regime_threshold) & (rb["n"] >= min_regime_n)
    ]
    if strong_regime.empty:
        write_parquet(empty, rcc_path)
        return

    # Pick best regime per hypothesis
    best = (
        strong_regime.sort_values("t_stat", ascending=False)
        .groupby("hypothesis_id", as_index=False)
        .first()
        .rename(columns={
            "regime": "best_regime",
            "t_stat": "best_regime_t_stat",
            "mean_return_bps": "best_regime_mean_return_bps",
            "n": "best_regime_n",
        })
    )

    # Join to weak-overall hypotheses
    merged = weak_overall.merge(best[["hypothesis_id", "best_regime",
                                       "best_regime_t_stat", "best_regime_mean_return_bps",
                                       "best_regime_n"]],
                                on="hypothesis_id", how="inner")
    if merged.empty:
        write_parquet(empty, rcc_path)
        return

    # Derive event_type from trigger_key ("event:NAME" → "NAME")
    def _event_from_trigger(tk: str) -> str:
        tk = str(tk)
        for prefix in ("event:", "state:", "transition:"):
            if tk.startswith(prefix):
                return tk[len(prefix):]
        return tk

    merged["event_type"] = merged["trigger_key"].apply(_event_from_trigger)
    merged = merged.rename(columns={"t_stat": "overall_t_stat"})
    merged = merged.sort_values("best_regime_t_stat", ascending=False)

    out_cols = [c for c in _RCC_COLS if c in merged.columns]
    write_parquet(merged[out_cols].head(30), rcc_path)
    LOG.info(
        "Phase 4.2 deep hook: %d regime_conditional_candidates (weak overall, strong per-regime)",
        len(merged),
    )


def _load_all_features(
    symbols: List[str],
    run_id: str,
    timeframe: str,
    data_root: Path,
) -> pd.DataFrame:
    """Load and concatenate features across all symbols."""
    parts: list[pd.DataFrame] = []
    for sym in symbols:
        df = load_features(data_root, run_id, sym, timeframe=timeframe)
        if not df.empty:
            df = df.copy()
            df["symbol"] = sym
            parts.append(df)
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


def _make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run hypothesis search engine")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument(
        "--n_workers",
        type=int,
        default=0,
        help="0 = auto (cpu_count)",
    )
    parser.add_argument("--chunk_size", type=int, default=256)
    parser.add_argument("--min_t_stat", type=float, default=None)
    parser.add_argument("--min_n", type=int, default=30)
    parser.add_argument("--use_context_quality", type=int, default=1)
    parser.add_argument(
        "--run_bridge_adapter",
        type=int,
        default=0,
        help="1 to emit bridge_candidates.parquet alongside metrics",
    )
    parser.add_argument(
        "--search_space_path",
        default=None,
        help="Optional override for search-space YAML path (defaults to spec/search_space.yaml)",
    )
    parser.add_argument(
        "--cluster_deduplication",
        type=int,
        default=1,
        help="1 (default) to run within-run alpha clustering before writing output",
    )
    parser.add_argument(
        "--cluster_eps",
        type=float,
        default=0.3,
        help="DBSCAN eps (Euclidean distance in normalised metric space, default 0.3)",
    )
    parser.add_argument(
        "--out_dir",
        default=None,
        help="Optional explicit output directory (for tests/local runs)",
    )
    parser.add_argument(
        "--data_root",
        default=None,
        help="Optional override for data root (defaults to configured data root)",
    )
    return parser


def main() -> int:
    parser = _make_parser()
    args = parser.parse_args()

    data_root = Path(args.data_root) if args.data_root else get_data_root()
    out_dir = (
        Path(args.out_dir)
        if args.out_dir
        else (data_root / "reports" / "hypothesis_search" / args.run_id)
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
    n_workers = args.n_workers if args.n_workers > 0 else None
    search_space_path = _resolve_search_space_path(args.search_space_path)
    resolved_min_t_stat = _resolve_search_min_t_stat(args.min_t_stat)

    features = _load_all_features(symbols, args.run_id, args.timeframe, data_root)
    try:
        hypotheses, generation_audit = generate_hypotheses_with_audit(
            search_space_path=search_space_path,
            features=None if features.empty else features,
        )
    except Exception as exc:  # pragma: no cover - defensive
        LOG.error("Failed to generate hypotheses: %s", exc)
        return 1

    _write_hypothesis_audit_artifacts(out_dir, generation_audit)
    LOG.info("Generated %d hypotheses", len(hypotheses))

    if features.empty:
        LOG.warning(
            "No features loaded for symbols=%s run_id=%s; writing empty output.",
            symbols,
            args.run_id,
        )
        metrics = pd.DataFrame()
    else:
        try:
            metrics = run_distributed_search(
                hypotheses,
                features,
                n_workers=n_workers,
                chunk_size=args.chunk_size,
                use_context_quality=bool(int(args.use_context_quality)),
            )
        except Exception as exc:  # pragma: no cover - defensive
            LOG.error("Distributed search failed: %s", exc)
            return 1

    # Phase 3.3 — within-run alpha clustering deduplication
    # Mark near-duplicate hypotheses before writing metrics so downstream
    # BH adjustment operates on a deduplicated family.
    if not metrics.empty and int(args.cluster_deduplication):
        metrics = _cluster_deduplicate(metrics, eps=float(args.cluster_eps))

    metrics_path = out_dir / "hypothesis_metrics.parquet"
    if not metrics.empty:
        write_parquet(metrics, metrics_path)
    else:
        # Preserve schema by writing an empty frame with no rows.
        write_parquet(pd.DataFrame(), metrics_path)

    # Phase 4.2 — Write per-hypothesis regime breakdown.
    # evaluate_hypothesis_batch() accumulates per-regime rows in df.attrs["regime_breakdown"].
    # This parquet is the richer signal surfaced by the campaign controller:
    # hypotheses weak overall (t_stat < 1.5) but strong per-regime (t_stat > 1.5)
    # are regime-specific alpha candidates that aggregate scoring would discard.
    regime_breakdown = metrics.attrs.get("regime_breakdown", None) if not metrics.empty else None
    if regime_breakdown is not None and not regime_breakdown.empty:
        write_parquet(regime_breakdown, out_dir / "regime_breakdown.parquet")
        LOG.info(
            "Phase 4.2: wrote %d per-regime rows to %s",
            len(regime_breakdown), out_dir / "regime_breakdown.parquet",
        )
    # Also write the regime_conditional_candidates artefact used by update_campaign_memory.
    # Identifies hypotheses with weak aggregate t_stat but positive mean_return_bps.
    _write_regime_conditional_candidates_from_breakdown(metrics, regime_breakdown, out_dir)
    _, gate_failures = split_bridge_candidates(
        metrics,
        min_t_stat=resolved_min_t_stat,
        min_n=args.min_n,
    )
    _write_evaluation_artifacts(out_dir, metrics, gate_failures)

    passing = (
        int((metrics["t_stat"].abs() >= resolved_min_t_stat).sum())
        if (not metrics.empty and "t_stat" in metrics.columns)
        else 0
    )
    redundant_count = (
        int(metrics["is_cluster_redundant"].sum())
        if (not metrics.empty and "is_cluster_redundant" in metrics.columns)
        else 0
    )
    filter_overlay_count = (
        int(sum(1 for spec in hypotheses if getattr(spec, "filter_template_id", None)))
        if hypotheses
        else 0
    )
    summary = {
        "run_id": args.run_id,
        "symbols": symbols,
        "timeframe": args.timeframe,
        "search_space_path": str(search_space_path),
        "search_tier": "tier1_default",
        "total_hypotheses": int(
            generation_audit.get("counts", {}).get("generated", len(hypotheses))
        ),
        "feasible_hypotheses": int(
            generation_audit.get("counts", {}).get("feasible", len(hypotheses))
        ),
        "rejected_hypotheses": int(generation_audit.get("counts", {}).get("rejected", 0)),
        "rejection_reason_counts": dict(generation_audit.get("rejection_reason_counts", {})),
        "primary_search_unit": "trigger_x_expression_template",
        "filter_overlay_hypotheses": filter_overlay_count,
        "evaluated": int(len(metrics)) if not metrics.empty else 0,
        "passing_filter": passing,
        "cluster_redundant": redundant_count,
        "use_context_quality": bool(int(args.use_context_quality)),
    }
    (out_dir / "hypothesis_search_summary.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )

    if int(args.run_bridge_adapter) and not metrics.empty:
        candidates = hypotheses_to_bridge_candidates(
            metrics,
            min_t_stat=resolved_min_t_stat,
            min_n=args.min_n,
        )
        write_parquet(candidates, out_dir / "bridge_candidates.parquet")

    LOG.info(
        "Wrote %d evaluated hypotheses (%d passing) to %s",
        len(metrics),
        passing,
        out_dir,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
