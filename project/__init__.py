from __future__ import annotations

from pathlib import Path

from project.io.parquet_compat import patch_pandas_parquet_fallback

PROJECT_ROOT = Path(__file__).resolve().parent

patch_pandas_parquet_fallback()

__all__ = ["PROJECT_ROOT"]
