from __future__ import annotations

from pathlib import Path
from typing import Any


def run(
    run_id: str,
    symbols: str,
    retail_profile: str = "capital_constrained",
    out_dir: Path | None = None,
    promotion_profile: str = "auto",
    require_forward_confirmation: bool | None = None,
):
    from project.research.services.promotion_service import (
        build_promotion_config,
        execute_promotion,
    )

    overrides: dict[str, Any] = {
        "retail_profile": retail_profile,
        "promotion_profile": promotion_profile,
    }
    if require_forward_confirmation is not None:
        overrides["require_forward_confirmation"] = bool(require_forward_confirmation)
    config = build_promotion_config(
        run_id=run_id,
        symbols=symbols,
        out_dir=out_dir,
        **overrides,
    )
    return execute_promotion(config)


def export(*args: Any, **kwargs: Any):
    from project.research.live_export import export_promoted_theses_for_run

    return export_promoted_theses_for_run(*args, **kwargs)
