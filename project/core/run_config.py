from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class RunConfiguration(BaseModel):
    """
    Unified configuration object for a Project Edge research run.
    Passed through the pipeline to ensure consistent parameters.
    """

    run_id: str
    symbols: list[str]
    timeframes: list[str] = Field(default_factory=lambda: ["5m"])
    data_root: Path

    # Run Mode
    mode: str = "exploratory"  # exploratory, confirmatory, production

    # Cost & Execution Params
    fee_model: str = "binance_vip0"
    maker_fee_bps: float = 2.0
    taker_fee_bps: float = 5.0
    slippage_model: str = "fixed_median"

    # Discovery Params
    horizon_bars: int = 96
    min_events_floor: int = 20
    candidate_promotion_max_q_value: float = 0.20

    # Infrastructure
    max_workers: int = 4
    feature_schema_version: str = "v2"

    model_config = ConfigDict(arbitrary_types_allowed=True)


def load_run_config(run_id: str, data_root: Path) -> RunConfiguration:
    """Load configuration from run manifest if it exists, else use defaults."""
    from project.specs.manifest import load_run_manifest

    manifest = load_run_manifest(run_id)
    if manifest and "parameters" in manifest:
        params = manifest["parameters"]
        # Filter params to match model fields
        valid_fields = RunConfiguration.__fields__.keys()
        filtered = {k: v for k, v in params.items() if k in valid_fields}
        return RunConfiguration(**{**filtered, "run_id": run_id, "data_root": data_root})

    raise ValueError(f"No manifest found for run {run_id}")
