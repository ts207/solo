from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from project.core.config import get_data_root
from project.io.utils import read_parquet
from project.pipelines import stage_registry
from project.reliability.contracts import (
    reconcile_portfolio_to_traces,
    validate_candidate_table,
    validate_manifest,
    validate_portfolio_ledger,
    validate_promotion_artifacts,
    validate_strategy_trace,
)
from project.reliability.regression_checks import (
    assert_bundle_policy_consistency,
    assert_storage_fallback_respected,
)
from project.reliability.smoke_data import (
    build_smoke_dataset,
    build_smoke_summary,
    materialize_smoke_promotion_inputs,
    run_engine_smoke,
    run_promotion_smoke,
    run_research_smoke,
)


def _engine_trace_paths(engine_dir: Path) -> list[Path]:
    return sorted([p for p in engine_dir.glob("strategy_trace_*.*") if p.is_file()])


def _read_first_matching(root: Path, stem: str) -> Path:
    matches = sorted(root.glob(f"{stem}.*"))
    if not matches:
        raise FileNotFoundError(f"missing artifact {stem} under {root}")
    return matches[0]


def _read_first_matching_any(root: Path, stems: tuple[str, ...]) -> Path:
    for stem in stems:
        matches = sorted(root.glob(f"{stem}.*"))
        if matches:
            return matches[0]
    raise FileNotFoundError(f"missing artifacts {stems} under {root}")


def run_smoke_cli(
    mode: str, *, root: Path, seed: int = 20260101, storage_mode: str = "auto"
) -> Dict[str, Any]:
    dataset = None
    if mode != "validate-artifacts":
        dataset = build_smoke_dataset(root, seed=seed, storage_mode=storage_mode)
        summary: Dict[str, Any] = {
            "mode": mode,
            "root": str(root),
            "run_id": dataset.run_id,
            "environment": build_smoke_summary(dataset=dataset, storage_mode=storage_mode),
        }
    else:
        summary = {"mode": mode, "root": str(root)}

    if mode in {"engine", "full"}:
        assert dataset is not None
        engine_result = run_engine_smoke(dataset)
        engine_dir = Path(engine_result["engine_dir"])
        validate_manifest(engine_result["manifest"])
        trace_paths = _engine_trace_paths(engine_dir)
        for path in trace_paths:
            validate_strategy_trace(path)
            assert_storage_fallback_respected(path)
        portfolio_path = _read_first_matching(engine_dir, "portfolio_returns")
        validate_portfolio_ledger(portfolio_path)
        reconcile_portfolio_to_traces(portfolio_path, trace_paths)
        summary["engine"] = {"engine_dir": str(engine_dir), "trace_count": len(trace_paths)}

    research_result = None
    if mode in {"research", "promotion", "full"}:
        assert dataset is not None
        research_result = run_research_smoke(dataset)
        candidate_path = _read_first_matching(
            Path(research_result["output_dir"]), "phase2_candidates"
        )
        validate_candidate_table(candidate_path)
        summary["research"] = {
            "candidate_rows": int(len(research_result["combined_candidates"])),
            "output_dir": str(research_result["output_dir"]),
        }

    if mode in {"promotion", "full"}:
        assert research_result is not None
        assert dataset is not None
        materialize_smoke_promotion_inputs(dataset, research_result)
        promotion_result = run_promotion_smoke(dataset, research_result)
        promo_dir = Path(promotion_result["output_dir"])
        info = validate_promotion_artifacts(promo_dir)
        audit_df = read_parquet(
            _read_first_matching_any(
                promo_dir,
                ("promotion_statistical_audit", "promotion_audit"),
            )
        )
        decisions_df = read_parquet(_read_first_matching(promo_dir, "promotion_decisions"))
        assert_bundle_policy_consistency(audit_df, decisions_df)
        summary["promotion"] = {"output_dir": str(promo_dir), **info}

    if mode == "validate-artifacts":
        if not root.exists():
            raise FileNotFoundError(root)
        result: Dict[str, Any] = {"root": str(root)}
        registry_issues = stage_registry.validate_stage_registry_definitions(Path.cwd() / "project")
        if registry_issues:
            raise AssertionError(
                "stage registry definition issues detected during validate-artifacts smoke: "
                + "; ".join(registry_issues)
            )
        result["structural"] = {"stage_registry_issues": 0}
        promo_dirs = (
            list((root / "reports" / "promotions").glob("*"))
            if (root / "reports" / "promotions").exists()
            else []
        )
        if promo_dirs:
            result["promotion"] = validate_promotion_artifacts(promo_dirs[0])
        engine_roots = (
            list((root / "reports" / "backtests").glob("*"))
            if (root / "reports" / "backtests").exists()
            else []
        )
        if not engine_roots:
            discovered = sorted({p.parent for p in root.rglob("strategy_trace_*.*") if p.is_file()})
            engine_roots = discovered
        if engine_roots:
            engine_dir = engine_roots[0]
            trace_paths = _engine_trace_paths(engine_dir)
            portfolio_path = _read_first_matching(engine_dir, "portfolio_returns")
            for path in trace_paths:
                validate_strategy_trace(path)
            validate_portfolio_ledger(portfolio_path)
            reconcile_portfolio_to_traces(portfolio_path, trace_paths)
            result["engine"] = {"engine_dir": str(engine_dir), "trace_count": len(trace_paths)}
        summary = result

    (root / "reliability").mkdir(parents=True, exist_ok=True)
    (root / "reliability" / "smoke_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8"
    )
    return summary


import argparse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run EDGEE smoke and artifact validation checks.")
    parser.add_argument(
        "--mode",
        choices=["engine", "research", "promotion", "full", "validate-artifacts"],
        default="research",
        help="Smoke mode to execute.",
    )
    parser.add_argument("--root", default=None, help="Output root for smoke artifacts.")
    parser.add_argument("--seed", type=int, default=20260101, help="Synthetic smoke seed.")
    parser.add_argument(
        "--storage_mode",
        default="auto",
        help="Artifact storage mode override for smoke generation.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    root = Path(args.root) if args.root else (get_data_root() / "artifacts" / "smoke")
    summary = run_smoke_cli(
        str(args.mode),
        root=root,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
