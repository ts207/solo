"""
Finalize Experiment Run.
Collects hypotheses evaluation results and appends to the program's tested ledger.
"""

import argparse
import json
import logging
from pathlib import Path
import sys

import pandas as pd
from project.core.config import get_data_root
from project.core.exceptions import DataIntegrityError
from project.core.logging_utils import build_stage_log_handlers
from project.io.utils import read_parquet, write_parquet
from project.artifacts import phase2_candidates_path
from project.specs.manifest import finalize_manifest, start_manifest

_LOG = logging.getLogger(__name__)


def _load_phase2_results(*, data_root: Path, run_id: str) -> pd.DataFrame:
    path = phase2_candidates_path(run_id, root=data_root)
    if not path.exists():
        return pd.DataFrame()
    try:
        return read_parquet(path)
    except Exception as exc:
        _LOG.warning("Failed to read %s: %s", path, exc)
        return pd.DataFrame()


def _adapt_legacy_results(results_df: pd.DataFrame) -> pd.DataFrame:
    if results_df.empty:
        return results_df.copy()
    out = results_df.copy()
    out["lineage_migrated"] = False
    if "hypothesis_id" not in out.columns:
        out["hypothesis_id"] = pd.NA
    if "candidate_id" in out.columns:
        candidate_ids = out["candidate_id"].astype(str).str.strip()
        migratable = out["hypothesis_id"].isna() & candidate_ids.str.startswith("hyp_")
        out.loc[migratable, "hypothesis_id"] = candidate_ids.loc[migratable]
        out.loc[migratable, "lineage_migrated"] = True
    return out


def finalize_experiment(
    data_root: Path,
    program_id: str,
    run_id: str,
) -> int:
    exp_dir = data_root / "artifacts" / "experiments" / program_id / run_id
    if not exp_dir.exists():
        _LOG.error(f"Experiment directory not found: {exp_dir}")
        return 1

    # Load expanded hypotheses
    hyp_path = exp_dir / "expanded_hypotheses.parquet"
    if not hyp_path.exists():
        _LOG.error(f"Expanded hypotheses not found at: {hyp_path}")
        return 1
    try:
        hyps_df = read_parquet(hyp_path)
    except Exception as exc:
        raise DataIntegrityError(f"Failed to read expanded hypotheses from {hyp_path}: {exc}") from exc

    results_df = _adapt_legacy_results(_load_phase2_results(data_root=data_root, run_id=run_id))

    # Initialize merged_df with hyps
    merged_df = hyps_df.copy()
    merged_df["run_id"] = run_id
    finalized_at = pd.Timestamp.now(tz="UTC").isoformat()
    if "created_at" not in merged_df.columns:
        merged_df["created_at"] = finalized_at
    else:
        merged_df["created_at"] = merged_df["created_at"].fillna(finalized_at)

    if not results_df.empty:
        current_format = {
            "hypothesis_id",
            "candidate_id",
        }.issubset(results_df.columns) and not (
            results_df["candidate_id"].astype(str) == results_df["hypothesis_id"].astype(str)
        ).all()

        eval_map = {}

        for _, row in results_df.iterrows():
            hid = row.get("hypothesis_id")
            if not hid:
                continue

            # Helper to assign terminal status
            def get_status(r):
                # Try multiple names for expectancy
                exp = r.get("expectancy")
                if pd.isna(exp):
                    exp = r.get("mean_return_bps")
                if pd.isna(exp):
                    return "empty_sample"
                if r.get("sample_size", 0) < 5 and r.get("n_obs", 0) < 5:
                    return "insufficient_sample"
                return "evaluated"

            r_data = row.to_dict()
            r_data["eval_status"] = get_status(row)

            if hid and str(hid).startswith("hyp_"):
                eval_map[str(hid)] = r_data

        # Apply to merged_df using record-based updates to prevent fragmentation
        updated_records = []
        for _, row in merged_df.iterrows():
            hid = row["hypothesis_id"]
            r_data = row.to_dict()

            if hid in eval_map:
                res = eval_map[hid]
                for k, v in res.items():
                    if k not in r_data or pd.isna(r_data.get(k)):
                        r_data[k] = v

                # Ensure expectancy is populated from mean_return_bps if missing
                if pd.isna(r_data.get("expectancy")) and not pd.isna(r_data.get("mean_return_bps")):
                    r_data["expectancy"] = float(r_data["mean_return_bps"]) / 10000.0
            elif current_format:
                # Handle unsupported/missing
                t_type = row.get("trigger_type")
                if t_type == "transition":
                    r_data["eval_status"] = "unsupported_trigger_evaluator"
                elif (
                    t_type == "sequence"
                    and len(json.loads(row.get("trigger_payload", "{}")).get("events", [])) > 2
                ):
                    r_data["eval_status"] = "unsupported_trigger_evaluator"
                else:
                    r_data["eval_status"] = "not_executed_or_missing_data"
            else:
                r_data["eval_status"] = "not_executed_or_missing_data"

            updated_records.append(r_data)

        merged_df = pd.DataFrame.from_records(updated_records)
    else:
        merged_df["eval_status"] = "not_executed_or_missing_data"

    # Save evaluation results
    eval_df = merged_df[
        ~merged_df["eval_status"].isin(
            ["not_executed_or_missing_data", "unsupported_trigger_evaluator"]
        )
    ].copy()
    if not eval_df.empty:
        write_parquet(eval_df, exp_dir / "evaluation_results.parquet")

    # Append to tested ledger
    ledger_path = data_root / "artifacts" / "experiments" / program_id / "tested_ledger.parquet"
    if ledger_path.exists():
        try:
            ledger_df = read_parquet(ledger_path)
            ledger_df = pd.concat([ledger_df, merged_df], ignore_index=True)
            ledger_df = ledger_df.drop_duplicates(subset=["hypothesis_id"], keep="last")
            write_parquet(ledger_df, ledger_path)
        except Exception as e:
            raise DataIntegrityError(f"Failed to update ledger at {ledger_path}: {e}") from e
    else:
        ledger_path.parent.mkdir(parents=True, exist_ok=True)
        write_parquet(merged_df, ledger_path)

    # Summary
    summary = {
        "program_id": program_id,
        "run_id": run_id,
        "total_hypotheses": len(hyps_df),
        "evaluated_hypotheses": int((merged_df["eval_status"] == "evaluated").sum()),
        "passed_hypotheses": int(merged_df.get("gate_phase2_final", pd.Series([False])).sum())
        if "gate_phase2_final" in merged_df.columns
        else 0,
    }
    (exp_dir / "summary.json").write_text(json.dumps(summary, indent=2))
    _LOG.info(f"Finalized experiment {program_id}/{run_id}. Ledger updated.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--program_id", required=True)
    parser.add_argument("--data_root", default=None)
    parser.add_argument("--log_path", default=None)
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    data_root = Path(args.data_root) if args.data_root else get_data_root()
    logging.basicConfig(
        level=logging.INFO,
        handlers=build_stage_log_handlers(args.log_path),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    exp_dir = data_root / "artifacts" / "experiments" / args.program_id / args.run_id
    outputs = [{"path": str(exp_dir / "summary.json")}]
    if args.log_path:
        outputs.append({"path": str(args.log_path)})
    manifest = start_manifest("finalize_experiment", args.run_id, vars(args), [], outputs)
    try:
        rc = int(finalize_experiment(data_root, args.program_id, args.run_id))
        stats: dict[str, object] = {}
        summary_path = exp_dir / "summary.json"
        if summary_path.exists():
            try:
                summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
                stats = {
                    key: summary_payload[key]
                    for key in ("total_hypotheses", "evaluated_hypotheses", "passed_hypotheses")
                    if key in summary_payload
                }
            except Exception:
                stats = {}
        finalize_manifest(manifest, "success" if rc == 0 else "failed", stats=stats)
        return rc
    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        raise


if __name__ == "__main__":
    sys.exit(main())
