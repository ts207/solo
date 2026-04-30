#!/usr/bin/env python3
"""Regenerate the mechanism-level research scorecard."""

from __future__ import annotations

from project.research.mechanism_scorecard import ROOT, update_mechanism_scorecard


def main() -> int:
    df = update_mechanism_scorecard(ROOT)
    print(
        "Updated data/reports/mechanisms/mechanism_scorecard.json, "
        "data/reports/mechanisms/mechanism_scorecard.parquet, "
        f"and docs/research/mechanism_scorecard.md ({len(df)} rows)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
