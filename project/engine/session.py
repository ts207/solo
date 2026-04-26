from __future__ import annotations

import json
import logging
from typing import Any

import pandas as pd

from project.core.run_config import RunConfiguration
from project.engine.artifacts import write_engine_dataframe
from project.engine.schema import PORTFOLIO_FRAME_SCHEMA_VERSION, STRATEGY_FRAME_SCHEMA_VERSION
from project.io.repository import ProjectDataRepository
from project.io.utils import ensure_dir

_LOG = logging.getLogger(__name__)


class BacktestSession:
    """
    Encapsulates the state and execution environment of a backtest.
    """

    def __init__(self, config: RunConfiguration):
        self.config = config
        self.repo = ProjectDataRepository(config.data_root, run_id=config.run_id)
        self.engine_dir = config.data_root / "runs" / config.run_id / "engine"
        ensure_dir(self.engine_dir)

        # Results
        self.strategy_results: dict[str, pd.DataFrame] = {}
        self.portfolio_results: pd.DataFrame = pd.DataFrame()
        self.metrics: dict[str, Any] = {"strategies": {}, "portfolio": {}}

    def record_strategy_result(self, strategy_name: str, df: pd.DataFrame, metrics: dict[str, Any]):
        """Store strategy returns and metrics."""
        self.strategy_results[strategy_name] = df
        self.metrics["strategies"][strategy_name] = metrics

        # Auto-flush strategy returns to disk to save memory
        write_engine_dataframe(
            df,
            engine_dir=self.engine_dir,
            artifact_name=f"strategy_returns_{strategy_name}",
            schema_name="strategy_frame",
            schema_version=STRATEGY_FRAME_SCHEMA_VERSION,
        )

    def finalize(self):
        """Save final portfolio results and metrics."""
        if not self.portfolio_results.empty:
            write_engine_dataframe(
                self.portfolio_results,
                engine_dir=self.engine_dir,
                artifact_name="portfolio_returns",
                schema_name="portfolio_frame",
                schema_version=PORTFOLIO_FRAME_SCHEMA_VERSION,
            )

        metrics_path = self.engine_dir / "metrics.json"
        metrics_path.write_text(json.dumps(self.metrics, indent=2, sort_keys=True))
        _LOG.info(f"Session finalized. Results at {self.engine_dir}")
