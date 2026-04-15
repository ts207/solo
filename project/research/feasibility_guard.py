from __future__ import annotations

import logging
import yaml
from pathlib import Path
from typing import Dict, List, Tuple

from project.core.datasets import dataset_path_candidates, resolve_dataset_id


class FeasibilityGuard:
    """Fail-closed dataset feasibility checks for spec-driven research plans."""

    def __init__(self, project_root: Path, data_root: Path, run_id: str):
        self.project_root = project_root
        self.data_root = data_root
        self.run_id = run_id
        self.log = logging.getLogger(__name__)

    def check_feasibility(self, spec_path_str: str, symbol: str) -> Tuple[bool, str]:
        path = self.project_root / spec_path_str
        if not path.exists():
            return False, f"Spec file missing at {spec_path_str}"
        try:
            with open(path, "r", encoding="utf-8") as f:
                spec = yaml.safe_load(f)
        except Exception as e:
            return False, f"Failed to parse spec YAML: {e}"
        if spec is None:
            return False, f"Empty spec at {spec_path_str}"
        inputs = spec.get("inputs", [])
        if not isinstance(inputs, list):
            inputs = [inputs]
        for inp in inputs:
            if not isinstance(inp, dict):
                continue
            dataset_id = inp.get("dataset")
            if not dataset_id:
                continue
            if not self._check_dataset_exists(str(dataset_id), symbol):
                return False, f"Required dataset '{dataset_id}' not found in lake for {symbol}"
        return True, "ready"

    def _check_dataset_exists(self, dataset_id: str, symbol: str) -> bool:
        alias_map = {
            "ohlcv_perp_5m": "perp_ohlcv_5m",
            "ohlcv_spot_5m": "spot_ohlcv_5m",
            "ohlcv_perp_1m": "perp_ohlcv_1m",
            "ohlcv_spot_1m": "spot_ohlcv_1m",
            "ohlcv_perp_15m": "perp_ohlcv_15m",
            "ohlcv_spot_15m": "spot_ohlcv_15m",
        }
        resolved_id = alias_map.get(
            str(dataset_id).strip().lower(), str(dataset_id).strip().lower()
        )
        try:
            candidates = dataset_path_candidates(
                data_root=self.data_root,
                run_id=self.run_id,
                symbol=symbol,
                dataset_id=resolved_id,
            )
        except Exception:
            self.log.warning(
                "Unknown dataset mapping in feasibility guard: dataset_id=%s symbol=%s",
                dataset_id,
                symbol,
            )
            return False
        return any(path.exists() for path in candidates)
