"""
Audit historical run artifacts for statistical integrity contamination.

This script is READ-ONLY. It scans Parquet files and reports issues without modifying
any artifacts. Labels runs that may have been affected by legacy p-value behavior, 
missing multiplicity fields, or non-split-aware gate calculations.

SCOPE: This is a FIRST-PASS AUDIT TOOL, NOT A FULL VERIFIER. It uses heuristics to:
  - Detect missing multiplicity fields
  - Flag legacy p-value column usage
  - Identify missing split sample counts

It does NOT verify:
  - Correct DSR n_trials calculations
  - Proper BH/FDR adjustment methodology
  - Cross-campaign multiplicity handling

Output distinguishes "flagged for review" from confirmed contamination.
See docs/92_assurance_and_benchmarks.md for audit status and follow-up actions.

Typical usage:
    python -m project.scripts.audit_historical_stat_integrity --dir data/reports
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Mandatory columns for modern statistical integrity
REQUIRED_MULTIPLICITY_COLS = {
    "num_tests_family",
    "num_tests_campaign",
    "num_tests_effective",
}

def audit_parquet_artifact(path: Path) -> list[dict[str, Any]]:
    try:
        df = pd.read_parquet(path)
    except Exception as e:
        return [{"artifact": str(path), "reason": f"read_error: {e}", "action": "ignore"}]

    findings = []
    cols = set(df.columns)

    # Check for missing multiplicity fields
    missing_mult = REQUIRED_MULTIPLICITY_COLS - cols
    if missing_mult:
        findings.append({
            "artifact": str(path),
            "reason": f"missing_multiplicity_fields: {sorted(list(missing_mult))}",
            "action": "recompute"
        })

    # Check for legacy p-value columns (if they are present but not the new ones)
    if "p_value" in cols and "p_value_for_fdr" not in cols:
        findings.append({
            "artifact": str(path),
            "reason": "legacy_p_value_columns: p_value exists but p_value_for_fdr missing",
            "action": "review"
        })

    # Check for non-split-aware gate indicators (heuristic)
    if "t_stat" in cols and not any(c in cols for c in ["train_n_obs", "validation_n_obs"]):
        findings.append({
            "artifact": str(path),
            "reason": "missing_split_sample_counts: cannot verify holdout independence",
            "action": "recompute"
        })

    return findings

def main():
    parser = argparse.ArgumentParser(description="Audit historical run artifacts for statistical integrity.")
    parser.add_argument("--dir", type=str, default="data/reports", help="Directory to scan for artifacts.")
    parser.add_argument("--output", type=str, default="data/reports/stat_integrity_audit.json", help="Path to save audit report.")
    args = parser.parse_args()

    search_dir = Path(args.dir)
    if not search_dir.exists():
        logger.error(f"Directory not found: {search_dir}")
        return 1

    all_findings = []
    logger.info(f"Scanning {search_dir} for Parquet artifacts...")

    for root, _, files in os.walk(search_dir):
        for file in files:
            if file.endswith(".parquet") and ("candidate" in file or "evaluated" in file):
                path = Path(root) / file
                findings = audit_parquet_artifact(path)
                if findings:
                    # Extract run_id from path if possible
                    run_id = "unknown"
                    parts = path.parts
                    if "reports" in parts:
                        idx = parts.index("reports")
                        if idx + 1 < len(parts):
                            run_id = parts[idx + 1]

                    for f in findings:
                        f["run_id"] = run_id
                        all_findings.append(f)

    if all_findings:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump({
                "generated_at": datetime.now().isoformat(),
                "findings": all_findings
            }, f, indent=2)
        logger.info(f"Audit complete. {len(all_findings)} issues flagged. Report saved to {out_path}")
    else:
        logger.info("Audit complete. No issues flagged.")

    return 0

if __name__ == "__main__":
    exit(main())
