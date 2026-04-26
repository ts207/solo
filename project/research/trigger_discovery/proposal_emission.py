from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from project.io.utils import write_parquet
from project.research.trigger_discovery.adoption_store import register_proposals

log = logging.getLogger(__name__)

def generate_suggested_registry_payload(row: pd.Series) -> dict[str, Any]:
    """Generates a pseudo-registry YAML dictionary payload for the proposal."""
    name = row.get("suggested_trigger_name", "UNKNOWN_TRIGGER")
    family = row.get("detector_family", "unknown")
    param = row.get("parameterization", {})

    return {
        "event_type": name.upper(),
        "synthetic_coverage": "proposed",
        "detector_contract": True,
        "parameters": param,
        "detector": {
            "signal_definition": f"Automatically mined trigger proposal via {row.get('source_lane', 'mining')} in {family}.",
            "required_columns": row.get("dominant_features", [])
        },
        "identity": {
            "research_family": family.upper(),
            "subtype": "mined_proposal",
            "layer": "proposal"
        },
        "governance": {
            "operational_role": "candidate_trigger",
            "deployment_disposition": "pending_manual_review"
        }
    }


def emit_proposals(
    scored_candidates: pd.DataFrame,
    output_dir: Path
):
    """
    Emits the candidate trigger proposals in isolated structured artifact formats.
    """
    if scored_candidates.empty:
        log.info("No candidate triggers to emit.")
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Expand the registry payload
    out_df = scored_candidates.copy()
    out_df["suggested_registry_payload"] = [
        json.dumps(generate_suggested_registry_payload(r))
        for _, r in out_df.iterrows()
    ]

    # 2. JSONL
    jsonl_path = output_dir / "candidate_trigger_proposals.jsonl"
    out_df.to_json(jsonl_path, orient="records", lines=True)
    log.info(f"Wrote generated proposals: {jsonl_path}")

    # 3. Parquet
    parquet_path = output_dir / "candidate_trigger_scored.parquet"
    write_parquet(out_df, parquet_path)

    # 4. Human Markdown Report
    md_path = output_dir / "candidate_trigger_report.md"
    with md_path.open("w") as f:
        f.write("# Advanced Trigger Discovery Proposals\n\n")
        f.write("> **Notice:** These are *candidate* trigger ideas. They are not live edges and ")
        f.write("must undergo explicit registry adoption before trading.\n\n")

        for idx, row in out_df.iterrows():
            f.write(f"## Proposal: `{row.get('suggested_trigger_name', 'Mined Trigger')}`\n")
            f.write(f"- **Quality Score**: {row.get('trigger_candidate_quality_score', 0.0):.4f}\n")
            f.write(f"- **Lane**: {row.get('source_lane', 'unknown')} | **Detector Family**: {row.get('detector_family', 'unknown')}\n")
            f.write(f"- **Support**: {row.get('support_count', 0)} hits | **Fold Consistency**: {row.get('fold_sign_consistency', 0.0):.2f}\n")
            f.write(f"- **Nearest Canonical Trigger**: `{row.get('nearest_existing_trigger_id', 'NONE')}` (Sim: {row.get('registry_similarity_score', 0.0):.2f})\n")

            warn = row.get('warnings', '').strip()
            if warn:
                f.write(f"- **Warnings**: ⚠️ {warn}\n")

            f.write("### Parameters / Active Features\n```json\n")
            f.write(json.dumps(row.get('parameterization', {}), indent=2))
            f.write("\n```\n")

            f.write("### Suggested YAML Payload snippet\n```yaml\n")
            f.write(row.get('suggested_registry_payload', ''))
            f.write("\n```\n\n---\n\n")

    log.info(f"Wrote proposal markdown report: {md_path}")

    # 5. Register in Adoption Control Plane
    proposals_list = out_df.to_dict(orient="records")
    if proposals_list:
        lane = proposals_list[0].get("source_lane", "unknown")
        register_proposals(proposals_list, output_dir, lane)
