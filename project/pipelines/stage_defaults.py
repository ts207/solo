from __future__ import annotations

from typing import Any, Dict

STAGE_DEFAULT_PARAMS: Dict[str, Dict[str, Any]] = {
    "ingest_binance_um_ohlcv_5m": {"period": "5m"},
    "build_features": {"version": "v2"},
}


def get_stage_defaults(stage_name: str) -> Dict[str, Any]:
    return STAGE_DEFAULT_PARAMS.get(stage_name, {})
