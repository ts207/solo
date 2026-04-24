from __future__ import annotations

from pathlib import Path

from project.research.live_export import export_promoted_theses_for_run as export
from project.research.services.promotion_service import build_promotion_config, execute_promotion


def run(
    run_id: str,
    symbols: str,
    retail_profile: str = "capital_constrained",
    out_dir: Path | None = None,
):
    config = build_promotion_config(
        run_id=run_id,
        symbols=symbols,
        out_dir=out_dir,
        retail_profile=retail_profile,
    )
    return execute_promotion(config)
