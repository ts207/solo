#!/usr/bin/env python3
"""Regenerate the structured research results index."""

from __future__ import annotations

from project.research.results_index import ROOT, update_results_index


def main() -> int:
    df = update_results_index(ROOT)
    print(
        "Updated docs/research/results.md, "
        "data/reports/results/results_index.json, and "
        f"data/reports/results/results_index.parquet ({len(df)} rows)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
