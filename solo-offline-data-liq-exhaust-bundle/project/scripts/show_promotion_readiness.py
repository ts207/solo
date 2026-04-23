from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from project.core.config import get_data_root
from project.core.exceptions import DataIntegrityError
from project.research.services.promotion_readiness_service import (
    build_promotion_readiness_report,
    render_promotion_readiness_terminal,
    render_promotion_readiness_markdown,
)


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise DataIntegrityError(f"Failed to read promotion readiness json artifact {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise DataIntegrityError(f"Promotion readiness json artifact {path} did not contain an object payload")
    return payload


def _benchmark_roots(data_root: Path) -> list[Path]:
    return [
        data_root / "reports" / "benchmarks",
        data_root / "reports" / "perf_benchmarks",
    ]


def _benchmark_run_dirs(data_root: Path) -> list[Path]:
    run_dirs: list[Path] = []
    for root in _benchmark_roots(data_root):
        latest = root / "latest"
        if latest.exists():
            run_dirs.append(latest)
        history_dir = root / "history"
        if not history_dir.exists():
            continue
        runs = [d for d in history_dir.iterdir() if d.is_dir()]
        runs.sort(key=lambda path: path.name, reverse=True)
        run_dirs.extend(runs)
    return run_dirs


def main() -> int:
    parser = argparse.ArgumentParser(description="Show combined promotion readiness report.")
    parser.add_argument("--review", help="Path to benchmark_review.json")
    parser.add_argument("--cert", help="Path to benchmark_certification.json")
    parser.add_argument("--conf-plan", help="Path to confirmatory_window_plan.json")
    parser.add_argument(
        "--audit",
        help="Path to promotion_statistical_audit.parquet/.csv or legacy promotion_audit.parquet/.csv",
    )
    parser.add_argument("--out-dir", help="Directory to save JSON and MD reports.")
    args = parser.parse_args()

    # Defaults
    data_root = get_data_root()
    if args.review or args.cert:
        review_path = Path(args.review) if args.review else None
        cert_path = Path(args.cert) if args.cert else None
    else:
        review_path = None
        cert_path = None
        for run_dir in _benchmark_run_dirs(data_root):
            candidate_review = run_dir / "benchmark_review.json"
            candidate_cert = run_dir / "benchmark_certification.json"
            if candidate_review.exists() and candidate_cert.exists():
                review_path = candidate_review
                cert_path = candidate_cert
                break

    if not review_path or not review_path.exists():
        print(f"Error: Review file not found: {review_path}")
        return 1
    if not cert_path or not cert_path.exists():
        print(f"Error: Certification file not found: {cert_path}")
        return 1

    review = _load_json(review_path)
    cert = _load_json(cert_path)

    conf_plan = _load_json(Path(args.conf_plan)) if args.conf_plan else None

    promotion_audit = None
    if args.audit:
        audit_path = Path(args.audit)
        if audit_path.exists():
            import pandas as pd

            if audit_path.suffix == ".parquet":
                promotion_audit = pd.read_parquet(audit_path).to_dict(orient="records")
            else:
                promotion_audit = pd.read_csv(audit_path).to_dict(orient="records")

    report = build_promotion_readiness_report(
        benchmark_review=review,
        benchmark_certification=cert,
        confirmatory_plan=conf_plan,
        promotion_audit=promotion_audit,
    )

    print(render_promotion_readiness_terminal(report))

    if args.out_dir:
        out_dir = Path(args.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "promotion_readiness.json").write_text(
            json.dumps(report, indent=2, sort_keys=True), encoding="utf-8"
        )
        (out_dir / "promotion_readiness.md").write_text(
            render_promotion_readiness_markdown(report), encoding="utf-8"
        )
        print(f"Wrote reports to: {out_dir}")

    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
