from __future__ import annotations

import os

from project.core.exceptions import ContractViolationError
from project.core.timeframes import normalize_timeframe

_CANONICAL_FEATURE_SCHEMA_VERSION = "feature_schema_v2"


def normalize_feature_schema_version(version: str | None = None) -> str:
    if version is None:
        version = os.getenv("BACKTEST_FEATURE_SCHEMA_VERSION", _CANONICAL_FEATURE_SCHEMA_VERSION)
    token = str(version).strip().lower()
    if token == "":
        token = _CANONICAL_FEATURE_SCHEMA_VERSION
    # Support both "v2" and "feature_schema_v2"
    if token == "v2":
        token = _CANONICAL_FEATURE_SCHEMA_VERSION
    if token != _CANONICAL_FEATURE_SCHEMA_VERSION:
        raise ContractViolationError(
            f"Unsupported feature schema version: '{token}'. Supported: ['v2', '{_CANONICAL_FEATURE_SCHEMA_VERSION}']"
        )
    return token


def feature_dataset_dir_name(version: str | None = None) -> str:
    return f"features_{normalize_feature_schema_version(version)}"


def feature_dataset_key(timeframe: str, version: str | None = None) -> str:
    return f"{feature_dataset_dir_name(version)}_{normalize_timeframe(timeframe)}"
