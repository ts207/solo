from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any, Callable

import pandas as pd

try:  # pragma: no cover - optional dependency
    import pyarrow  # type: ignore  # noqa: F401
    _HAS_NATIVE_PARQUET = True
except Exception:  # pragma: no cover - optional dependency
    _HAS_NATIVE_PARQUET = False


def _coerce_path(path: Any) -> Path:
    return Path(path)


def _filter_pickle_kwargs(writer: Callable[..., Any], kwargs: dict[str, Any]) -> dict[str, Any]:
    allowed = set(inspect.signature(writer).parameters)
    filtered = {key: value for key, value in kwargs.items() if key in allowed}
    filtered.pop("index", None)
    filtered.pop("engine", None)
    filtered.pop("compression", None)
    return filtered


def write_parquet_compat(
    df: pd.DataFrame,
    path: Any,
    *,
    pickle_writer: Callable[..., Any] | None = None,
    **kwargs: Any,
) -> Path:
    """Write a logical parquet artifact using the repository fallback contract.

    The on-disk filename remains `.parquet`, but the bytes are pickle-backed when
    a native parquet engine is unavailable or intentionally bypassed.
    """

    writer = pickle_writer or pd.DataFrame.to_pickle
    target = _coerce_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    writer_kwargs = _filter_pickle_kwargs(writer, dict(kwargs))
    writer(df, target, **writer_kwargs)
    return target


def read_parquet_compat(
    path: Any,
    *,
    columns: list[str] | None = None,
    pickle_reader: Callable[..., Any] | None = None,
    **kwargs: Any,
) -> pd.DataFrame:
    """Read a logical parquet artifact through the repository fallback contract."""

    reader = pickle_reader or pd.read_pickle
    target = _coerce_path(path)
    allowed = set(inspect.signature(reader).parameters)
    reader_kwargs = {key: value for key, value in kwargs.items() if key in allowed}
    try:
        frame = reader(target, **reader_kwargs)
    except Exception:
        if not _HAS_NATIVE_PARQUET:
            raise
        import pyarrow.parquet as pq

        frame = pq.ParquetFile(target).read(columns=columns).to_pandas()
        if columns is None:
            return frame
        cols = [column for column in columns if column in frame.columns]
        return frame.loc[:, cols]
    if columns is not None:
        cols = [column for column in columns if column in frame.columns]
        frame = frame.loc[:, cols]
    return frame


def patch_pandas_parquet_fallback() -> None:
    """Patch pandas parquet helpers to use pickle storage when native parquet engines
    are not installed.

    This keeps existing code paths working in minimal environments while preserving
    the on-disk `.parquet` filename contract expected by the repository.
    """

    if _HAS_NATIVE_PARQUET:
        return

    if getattr(pd.DataFrame.to_parquet, "_edge_fallback_patched", False):
        return

    original_to_pickle = pd.DataFrame.to_pickle
    original_read_pickle = pd.read_pickle

    def _to_parquet_fallback(self: pd.DataFrame, path, *args, **kwargs):
        if len(args) > 1:
            raise TypeError("parquet fallback accepts at most one positional argument after path")
        if args:
            kwargs.setdefault("engine", args[0])
        return write_parquet_compat(
            self,
            path,
            pickle_writer=original_to_pickle,
            **kwargs,
        )

    def _read_parquet_fallback(path, *args, columns=None, **kwargs):
        if len(args) > 2:
            raise TypeError("parquet fallback accepts at most engine and columns positionally")
        if len(args) >= 1:
            kwargs.setdefault("engine", args[0])
        if len(args) == 2 and columns is None:
            columns = args[1]
        return read_parquet_compat(
            path,
            columns=columns,
            pickle_reader=original_read_pickle,
            **kwargs,
        )

    _to_parquet_fallback._edge_fallback_patched = True  # type: ignore[attr-defined]
    _read_parquet_fallback._edge_fallback_patched = True  # type: ignore[attr-defined]

    pd.DataFrame.to_parquet = _to_parquet_fallback  # type: ignore[assignment]
    pd.read_parquet = _read_parquet_fallback  # type: ignore[assignment]

    # Keep module-level helpers aligned for callers that import them directly.
    try:  # pragma: no cover - optional import surface
        import pandas.io.parquet as pq_mod

        pq_mod.to_parquet = _to_parquet_fallback  # type: ignore[assignment]
        pq_mod.read_parquet = _read_parquet_fallback  # type: ignore[assignment]
    except Exception:
        pass
