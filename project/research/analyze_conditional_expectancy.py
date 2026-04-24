from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from project.core.config import get_data_root
from project.io.utils import ensure_dir, write_parquet
from project.specs.manifest import finalize_manifest, start_manifest


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    data_root = get_data_root()
    parser = argparse.ArgumentParser(
        description="Analyze conditional expectancy from edge registry history."
    )
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", default="")
    parser.add_argument("--retail_profile", default="capital_constrained")
    parser.add_argument("--reports_root", default=str(data_root / "reports"))
    parser.add_argument("--runs_root", default=str(data_root / "runs"))
    parser.add_argument("--registry_path", default="")
    parser.add_argument("--out_dir", default="")
    parser.add_argument("--top_k", type=int, default=25)
    return parser.parse_args(argv)


def _load_registry(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()


def _safe_numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(0.0, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce")


def _build_payload(
    run_id: str,
    registry_df: pd.DataFrame,
    registry_path: Path,
    registry_exists: bool,
    symbols: list[str],
    retail_profile: str,
    top_k: int,
) -> tuple[Dict[str, Any], pd.DataFrame]:
    if registry_df.empty:
        payload = {
            "run_id": run_id,
            "symbols": symbols,
            "retail_profile": retail_profile,
            "expectancy_exists": False,
            "registry_exists": bool(registry_exists),
            "edge_count": 0,
            "skip_reason": "" if registry_exists else "missing_edge_registry",
            "source_registry_path": str(registry_path),
            "summary": {
                "tested_edges": 0,
                "promoted_edges": 0,
                "avg_median_effect": 0.0,
                "avg_stability_median": 0.0,
                "avg_times_tested": 0.0,
            },
            "expectancy_evidence": [],
        }
        return payload, pd.DataFrame()

    work = registry_df.copy()
    work["median_effect"] = _safe_numeric(work, "median_effect")
    work["stability_median"] = _safe_numeric(work, "stability_median")
    work["times_tested"] = _safe_numeric(work, "times_tested").fillna(0.0)
    work["times_promoted"] = _safe_numeric(work, "times_promoted").fillna(0.0)

    evidence = (
        work.sort_values(
            by=["times_promoted", "median_effect", "times_tested", "stability_median"],
            ascending=[False, False, False, False],
            kind="stable",
        )
        .head(int(max(top_k, 1)))
        .copy()
    )

    keep_cols = [
        col
        for col in [
            "edge_id",
            "candidate_id",
            "event_type",
            "template_id",
            "direction_rule",
            "promotion_decision",
            "times_tested",
            "times_promoted",
            "median_effect",
            "stability_median",
            "first_seen_run",
            "last_seen_run",
        ]
        if col in evidence.columns
    ]
    evidence = evidence[keep_cols].reset_index(drop=True)

    payload = {
        "run_id": run_id,
        "symbols": symbols,
        "retail_profile": retail_profile,
        "expectancy_exists": bool(len(evidence) > 0),
        "registry_exists": bool(registry_exists),
        "skip_reason": "",
        "edge_count": int(len(work)),
        "source_registry_path": str(registry_path),
        "summary": {
            "tested_edges": int(len(work)),
            "promoted_edges": int((_safe_numeric(work, "times_promoted") > 0).sum()),
            "avg_median_effect": float(work["median_effect"].dropna().mean())
            if work["median_effect"].notna().any()
            else 0.0,
            "avg_stability_median": float(work["stability_median"].dropna().mean())
            if work["stability_median"].notna().any()
            else 0.0,
            "avg_times_tested": float(work["times_tested"].mean()) if not work.empty else 0.0,
        },
        "expectancy_evidence": evidence.to_dict(orient="records"),
    }
    return payload, evidence


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    reports_root = Path(args.reports_root)
    runs_root = Path(args.runs_root)
    registry_path = (
        Path(args.registry_path)
        if str(args.registry_path).strip()
        else runs_root / args.run_id / "research" / "edge_registry.parquet"
    )
    out_dir = (
        Path(args.out_dir)
        if str(args.out_dir).strip()
        else reports_root / "expectancy" / args.run_id
    )
    ensure_dir(out_dir)

    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
    registry_exists = registry_path.exists()
    manifest = start_manifest(
        "analyze_conditional_expectancy",
        args.run_id,
        vars(args),
        [{"path": str(registry_path), "artifact": "history.candidate.edge_registry"}],
        [
            {
                "path": str(out_dir / "conditional_expectancy.json"),
                "artifact": "research.expectancy_analysis",
            }
        ],
    )

    try:
        registry_df = _load_registry(registry_path)
        payload, evidence = _build_payload(
            args.run_id,
            registry_df,
            registry_path,
            registry_exists,
            symbols,
            str(args.retail_profile),
            int(args.top_k),
        )

        json_path = out_dir / "conditional_expectancy.json"
        json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        if not evidence.empty:
            write_parquet(evidence, out_dir / "conditional_expectancy_evidence.parquet")

        finalize_manifest(
            manifest,
            "success",
            stats={
                "expectancy_exists": bool(payload["expectancy_exists"]),
                "registry_exists": bool(payload["registry_exists"]),
                "edge_count": int(payload["edge_count"]),
                "evidence_count": int(len(payload["expectancy_evidence"])),
            },
        )
        print(
            json.dumps(
                {"expectancy_exists": payload["expectancy_exists"], "out_dir": str(out_dir)},
                indent=2,
            )
        )
        return 0
    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc))
        raise


if __name__ == "__main__":
    raise SystemExit(main())
