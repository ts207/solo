#!/usr/bin/env python3
"""Regenerate the report-only search burden ledger."""

from __future__ import annotations

from project.research.search_ledger import ROOT, update_search_ledger


def main() -> int:
    df = update_search_ledger(ROOT)
    print(
        "Updated data/reports/search_ledger/search_burden.json and "
        f"data/reports/search_ledger/search_burden.parquet ({len(df)} rows)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
