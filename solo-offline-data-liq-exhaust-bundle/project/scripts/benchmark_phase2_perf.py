from __future__ import annotations

import argparse
import json
from pathlib import Path
from time import perf_counter
from typing import Any

from project.core.config import get_data_root
from project.io.utils import ensure_dir
from project.research.search.distributed_runner import run_distributed_search
from project.research.search.generator import generate_hypotheses_with_audit
from project.research.search.search_feature_utils import prepare_search_features_for_symbol


def _elapsed(start: float) -> float:
    return round(perf_counter() - start, 6)


def _count_valid(metrics) -> int:
    if metrics is None or metrics.empty or "valid" not in metrics.columns:
        return 0
    return int(metrics["valid"].fillna(False).astype(bool).sum())


def run_probe(args: argparse.Namespace) -> dict[str, Any]:
    data_root = Path(args.data_root) if args.data_root else get_data_root()
    expected_events = [
        item.strip().upper()
        for item in str(args.expected_events or "").split(",")
        if item.strip()
    ]

    started = perf_counter()
    features = prepare_search_features_for_symbol(
        run_id=str(args.run_id),
        symbol=str(args.symbol).upper(),
        timeframe=str(args.timeframe),
        data_root=data_root,
        expected_event_ids=expected_events or None,
    )
    feature_load_sec = _elapsed(started)

    started = perf_counter()
    hypotheses, generation_audit = generate_hypotheses_with_audit(
        str(args.search_spec),
        max_hypotheses=int(args.max_hypotheses) if args.max_hypotheses else None,
        features=features,
    )
    generation_sec = _elapsed(started)

    eval_count = min(int(args.evaluate_count), len(hypotheses))
    selected = hypotheses[:eval_count]

    started = perf_counter()
    metrics = run_distributed_search(
        selected,
        features,
        n_workers=int(args.workers),
        chunk_size=int(args.chunk_size),
        min_sample_size=int(args.min_sample_size),
        use_context_quality=not bool(args.disable_context_quality),
        folds=None,
    )
    evaluation_sec = _elapsed(started)

    result = {
        "run_id": str(args.run_id),
        "symbol": str(args.symbol).upper(),
        "timeframe": str(args.timeframe),
        "search_spec": str(args.search_spec),
        "expected_events": expected_events,
        "feature_rows": int(len(features)),
        "feature_columns": int(len(features.columns)),
        "feature_load_sec": feature_load_sec,
        "hypotheses_generated": int(len(hypotheses)),
        "hypotheses_evaluated": int(eval_count),
        "generation_sec": generation_sec,
        "evaluation_sec": evaluation_sec,
        "evaluation_sec_per_hypothesis": (
            round(evaluation_sec / eval_count, 6) if eval_count else 0.0
        ),
        "metrics_rows": int(len(metrics)),
        "valid_metrics_rows": _count_valid(metrics),
        "generation_counts": dict(generation_audit.get("counts", {})),
    }

    if args.output:
        output = Path(args.output)
        ensure_dir(output.parent)
        output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a bounded Phase 2 performance probe.")
    parser.add_argument("--run-id", default="phase2_perf_probe")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--data-root", default="")
    parser.add_argument("--search-spec", default="spec/search/search_benchmark_vol_shock.yaml")
    parser.add_argument("--expected-events", default="VOL_SHOCK")
    parser.add_argument("--max-hypotheses", type=int, default=64)
    parser.add_argument("--evaluate-count", type=int, default=32)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--chunk-size", type=int, default=64)
    parser.add_argument("--min-sample-size", type=int, default=20)
    parser.add_argument("--disable-context-quality", action="store_true")
    parser.add_argument("--output", default="")
    args = parser.parse_args(argv)

    result = run_probe(args)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
