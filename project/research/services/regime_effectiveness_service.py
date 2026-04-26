from __future__ import annotations

import argparse
import json
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from project.core.config import get_data_root
from project.events.ontology_deconfliction import deconflict_event_episodes
from project.io.utils import ensure_dir, read_parquet, write_parquet
from project.research.regime_routing import annotate_regime_metadata, recommended_bucket_for_regime
from project.research.services.pathing import resolve_phase2_candidates_path

RETURN_COLUMNS = (
    "after_cost_expectancy",
    "mean_return_bps",
    "expectancy_bps",
    "bridge_validation_after_cost_bps",
)
EXECUTION_COLUMNS = ("resolved_cost_bps", "avg_dynamic_cost_bps", "spread_bps", "slippage_bps", "depth")
DIRECT_PROXY_COLUMNS = (
    "canonical_regime",
    "direct_count",
    "proxy_count",
    "direct_mean_return_bps",
    "proxy_mean_return_bps",
    "stability_gap_bps",
    "stability_ratio",
)


@dataclass(frozen=True)
class RegimeEffectivenessArtifacts:
    output_dir: Path
    main_scorecard: pd.DataFrame
    overlap_matrix: pd.DataFrame
    subtype_breakdown: pd.DataFrame
    direct_proxy_stability: pd.DataFrame
    summary: dict[str, Any]


def _metric_column(frame: pd.DataFrame, candidates: Iterable[str]) -> str | None:
    for column in candidates:
        if column in frame.columns:
            return column
    return None


def _duration_series(frame: pd.DataFrame) -> pd.Series:
    if "duration_bars" in frame.columns:
        return pd.to_numeric(frame["duration_bars"], errors="coerce").fillna(0.0)
    if "episode_duration_bars" in frame.columns:
        return pd.to_numeric(frame["episode_duration_bars"], errors="coerce").fillna(0.0)
    if "horizon" in frame.columns:
        return (
            frame["horizon"]
            .astype(str)
            .str.extract(r"(\d+)", expand=False)
            .pipe(pd.to_numeric, errors="coerce")
            .fillna(0.0)
        )
    return pd.Series(1.0, index=frame.index, dtype=float)


def _continuation_profile(frame: pd.DataFrame, value_column: str | None) -> tuple[float, float]:
    if value_column is None or frame.empty:
        return 0.0, 0.0
    values = pd.to_numeric(frame[value_column], errors="coerce").dropna()
    if values.empty:
        return 0.0, 0.0
    continuation_share = float((values > 0).mean())
    reversal_share = float((values < 0).mean())
    return continuation_share, reversal_share


def _cooccurrence_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "canonical_regime" not in frame.columns:
        return pd.DataFrame(columns=["left_regime", "right_regime", "overlap_count"])
    if {"symbol", "timestamp"}.issubset(frame.columns):
        keys = ["symbol", "timestamp"]
    elif "timestamp" in frame.columns:
        keys = ["timestamp"]
    else:
        counts = frame["canonical_regime"].astype(str).value_counts().sort_index()
        return pd.DataFrame(
            [
                {"left_regime": regime, "right_regime": regime, "overlap_count": int(count)}
                for regime, count in counts.items()
            ]
        )
    pairs: list[dict[str, Any]] = []
    deduped = frame[keys + ["canonical_regime"]].drop_duplicates()
    for _, sub in deduped.groupby(keys, dropna=False):
        regimes = sorted({str(value).strip() for value in sub["canonical_regime"] if str(value).strip()})
        for left in regimes:
            for right in regimes:
                pairs.append({"left_regime": left, "right_regime": right, "overlap_count": 1})
    if not pairs:
        return pd.DataFrame(columns=["left_regime", "right_regime", "overlap_count"])
    out = pd.DataFrame(pairs)
    return (
        out.groupby(["left_regime", "right_regime"], as_index=False)["overlap_count"]
        .sum()
        .sort_values(["left_regime", "right_regime"], kind="stable")
        .reset_index(drop=True)
    )


def _filter_canonical_rows(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    out = annotate_regime_metadata(frame)
    non_canonical_mask = (
        out.get("canonical_regime", pd.Series("", index=out.index)).astype(str).str.strip() != ""
    )
    if "event_type" not in out.columns:
        return out[non_canonical_mask].copy()
    filtered_rows: list[dict[str, Any]] = []
    from project.domain.compiled_registry import get_domain_registry

    registry = get_domain_registry()
    for row in out.to_dict(orient="records"):
        event_type = str(row.get("event_type", "")).strip()
        spec = registry.get_event(event_type)
        if spec is None:
            continue
        if spec.is_composite or spec.is_context_tag or spec.is_strategy_construct:
            continue
        filtered_rows.append(row)
    filtered = pd.DataFrame(filtered_rows)
    if filtered.empty:
        return annotate_regime_metadata(filtered)
    if {"timestamp", "symbol"}.issubset(filtered.columns):
        try:
            filtered = deconflict_event_episodes(filtered)
        except Exception:
            filtered = filtered.copy()
    return annotate_regime_metadata(filtered)


def compute_regime_effectiveness(episodes: pd.DataFrame) -> RegimeEffectivenessArtifacts:
    canonical = _filter_canonical_rows(episodes)
    if canonical.empty:
        empty = pd.DataFrame(
            columns=[
                "canonical_regime",
                "subtype",
                "phase",
                "evidence_mode",
                "episode_count",
                "incidence_rate",
                "average_episode_duration_bars",
                "overlap_rate",
                "forward_return_profile",
                "continuation_share",
                "reversal_share",
                "execution_impact_profile",
                "subtype_outcome_variance",
                "direct_proxy_stability_gap_bps",
                "recommended_bucket",
            ]
        )
        return RegimeEffectivenessArtifacts(
            output_dir=Path(),
            main_scorecard=empty,
            overlap_matrix=pd.DataFrame(columns=["left_regime", "right_regime", "overlap_count"]),
            subtype_breakdown=empty.copy(),
            direct_proxy_stability=pd.DataFrame(columns=["canonical_regime"]),
            summary={"status": "no_data", "regimes_total": 0, "episodes_total": 0},
        )

    metric_column = _metric_column(canonical, RETURN_COLUMNS)
    execution_metric_columns = [column for column in EXECUTION_COLUMNS if column in canonical.columns]
    canonical = canonical.copy()
    canonical["_episode_duration_bars"] = _duration_series(canonical)

    overlap_pairs = _cooccurrence_frame(canonical)
    overlap_totals = (
        overlap_pairs[overlap_pairs["left_regime"] != overlap_pairs["right_regime"]]
        .groupby("left_regime", as_index=False)["overlap_count"]
        .sum()
        .rename(columns={"left_regime": "canonical_regime", "overlap_count": "overlap_episode_count"})
        if not overlap_pairs.empty
        else pd.DataFrame(columns=["canonical_regime", "overlap_episode_count"])
    )

    grouped = canonical.groupby(["canonical_regime", "subtype", "phase", "evidence_mode"], dropna=False)
    rows: list[dict[str, Any]] = []
    total = float(len(canonical))
    for keys, sub in grouped:
        regime, subtype, phase, evidence_mode = (str(item or "").strip() for item in keys)
        continuation_share, reversal_share = _continuation_profile(sub, metric_column)
        return_profile: dict[str, float] = {}
        if metric_column is not None:
            if "horizon" in sub.columns:
                horizon_stats = (
                    sub.assign(_metric=pd.to_numeric(sub[metric_column], errors="coerce"))
                    .groupby(sub["horizon"].astype(str))["_metric"]
                    .mean()
                    .dropna()
                )
                return_profile = {str(horizon): float(value) for horizon, value in horizon_stats.items()}
            else:
                values = pd.to_numeric(sub[metric_column], errors="coerce").dropna()
                if not values.empty:
                    return_profile = {"aggregate": float(values.mean())}
        execution_profile = {
            column: float(pd.to_numeric(sub[column], errors="coerce").dropna().mean())
            for column in execution_metric_columns
            if not pd.to_numeric(sub[column], errors="coerce").dropna().empty
        }
        rows.append(
            {
                "canonical_regime": regime,
                "subtype": subtype,
                "phase": phase,
                "evidence_mode": evidence_mode,
                "episode_count": len(sub),
                "incidence_rate": float(len(sub) / total),
                "average_episode_duration_bars": float(sub["_episode_duration_bars"].mean()),
                "forward_return_profile": json.dumps(return_profile, sort_keys=True),
                "continuation_share": continuation_share,
                "reversal_share": reversal_share,
                "execution_impact_profile": json.dumps(execution_profile, sort_keys=True),
                "recommended_bucket": recommended_bucket_for_regime(regime),
            }
        )
    main = pd.DataFrame(rows)
    if not overlap_totals.empty:
        main = main.merge(overlap_totals, on="canonical_regime", how="left")
    else:
        main["overlap_episode_count"] = 0
    main["overlap_episode_count"] = pd.to_numeric(main["overlap_episode_count"], errors="coerce").fillna(0).astype(int)
    main["overlap_rate"] = main["overlap_episode_count"] / main["episode_count"].clip(lower=1)

    subtype_breakdown = (
        canonical.assign(
            _metric=(
                pd.to_numeric(canonical[metric_column], errors="coerce")
                if metric_column
                else pd.Series(index=canonical.index, dtype=float)
            )
        )
        .groupby(["canonical_regime", "subtype", "phase"], dropna=False)
        .agg(
            episode_count=("event_type", "count"),
            average_return_bps=("_metric", "mean"),
            outcome_variance_bps=("_metric", "var"),
            average_duration_bars=("_episode_duration_bars", "mean"),
        )
        .reset_index()
    )

    variance_by_regime = (
        subtype_breakdown.groupby("canonical_regime", dropna=False)["average_return_bps"]
        .var()
        .reset_index(name="subtype_outcome_variance")
    )
    main = main.merge(variance_by_regime, on="canonical_regime", how="left")
    main["subtype_outcome_variance"] = pd.to_numeric(main["subtype_outcome_variance"], errors="coerce").fillna(0.0)

    direct_proxy_rows: list[dict[str, Any]] = []
    if metric_column is not None:
        for regime, sub in canonical.groupby("canonical_regime", dropna=False):
            evidence_groups = {
                mode: pd.to_numeric(mode_frame[metric_column], errors="coerce").dropna()
                for mode, mode_frame in sub.groupby("evidence_mode", dropna=False)
            }
            direct = evidence_groups.get("direct", pd.Series(dtype=float))
            proxy = evidence_groups.get("proxy", pd.Series(dtype=float))
            direct_mean = float(direct.mean()) if not direct.empty else 0.0
            proxy_mean = float(proxy.mean()) if not proxy.empty else 0.0
            direct_proxy_rows.append(
                {
                    "canonical_regime": str(regime),
                    "direct_count": len(direct),
                    "proxy_count": len(proxy),
                    "direct_mean_return_bps": direct_mean,
                    "proxy_mean_return_bps": proxy_mean,
                    "stability_gap_bps": float(direct_mean - proxy_mean),
                    "stability_ratio": float(proxy_mean / direct_mean) if direct_mean not in (0.0, -0.0) else 0.0,
                }
            )
    direct_proxy = pd.DataFrame(direct_proxy_rows, columns=DIRECT_PROXY_COLUMNS)
    if not direct_proxy.empty:
        main = main.merge(
            direct_proxy[["canonical_regime", "stability_gap_bps"]].rename(
                columns={"stability_gap_bps": "direct_proxy_stability_gap_bps"}
            ),
            on="canonical_regime",
            how="left",
        )
    else:
        main["direct_proxy_stability_gap_bps"] = 0.0
    main["direct_proxy_stability_gap_bps"] = pd.to_numeric(main["direct_proxy_stability_gap_bps"], errors="coerce").fillna(0.0)

    summary = {
        "status": "ok",
        "regimes_total": int(main["canonical_regime"].nunique()),
        "episodes_total": len(canonical),
        "scorecard_rows": len(main),
        "recommended_bucket_counts": main["recommended_bucket"].astype(str).value_counts().to_dict(),
        "top_regimes_by_incidence": (
            main.groupby("canonical_regime", as_index=False)["episode_count"]
            .sum()
            .sort_values(["episode_count", "canonical_regime"], ascending=[False, True], kind="stable")
            .head(10)
            .to_dict(orient="records")
        ),
    }
    return RegimeEffectivenessArtifacts(
        output_dir=Path(),
        main_scorecard=main.sort_values(["canonical_regime", "subtype", "phase", "evidence_mode"], kind="stable").reset_index(drop=True),
        overlap_matrix=overlap_pairs,
        subtype_breakdown=subtype_breakdown.sort_values(["canonical_regime", "subtype", "phase"], kind="stable").reset_index(drop=True),
        direct_proxy_stability=direct_proxy.sort_values(["canonical_regime"], kind="stable").reset_index(drop=True),
        summary=summary,
    )


def write_regime_effectiveness_reports(
    *,
    run_id: str,
    data_root: Path,
    episodes: pd.DataFrame,
) -> RegimeEffectivenessArtifacts:
    artifacts = compute_regime_effectiveness(episodes)
    output_dir = data_root / "reports" / "regime_effectiveness" / run_id
    ensure_dir(output_dir)
    write_parquet(artifacts.main_scorecard, output_dir / "regime_effectiveness.parquet")
    write_parquet(artifacts.overlap_matrix, output_dir / "regime_overlap_matrix.parquet")
    write_parquet(artifacts.subtype_breakdown, output_dir / "regime_subtype_breakdown.parquet")
    write_parquet(artifacts.direct_proxy_stability, output_dir / "regime_direct_proxy_stability.parquet")
    (output_dir / "regime_effectiveness_summary.json").write_text(
        json.dumps(artifacts.summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return RegimeEffectivenessArtifacts(
        output_dir=output_dir,
        main_scorecard=artifacts.main_scorecard,
        overlap_matrix=artifacts.overlap_matrix,
        subtype_breakdown=artifacts.subtype_breakdown,
        direct_proxy_stability=artifacts.direct_proxy_stability,
        summary=artifacts.summary,
    )


def build_reports_for_run(*, run_id: str, data_root: Path) -> RegimeEffectivenessArtifacts:
    phase2_path = resolve_phase2_candidates_path(data_root=data_root, run_id=run_id)
    phase2 = read_parquet(phase2_path)
    return write_regime_effectiveness_reports(run_id=run_id, data_root=data_root, episodes=phase2)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build regime effectiveness artifacts for a run.")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--data_root", default=None)
    args = parser.parse_args(argv)
    data_root = Path(args.data_root) if args.data_root else get_data_root()
    build_reports_for_run(run_id=str(args.run_id), data_root=data_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
