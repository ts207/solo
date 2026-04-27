from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import Any

import pandas as pd

from project.io.parquet_compat import read_parquet_compat, write_parquet_compat

try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    HAS_PYARROW = True
except ImportError:  # pragma: no cover - optional dependency
    import types

    pa = types.SimpleNamespace(Table=None)
    pq = types.SimpleNamespace(read_table=None, ParquetFile=None, write_table=None)
    HAS_PYARROW = False


def ensure_dir(path: Path) -> None:
    """
    Ensure a directory exists.
    """
    path.mkdir(parents=True, exist_ok=True)


def atomic_write_text(path: Path, text: str, *, encoding: str = "utf-8") -> Path:
    """
    Atomically replace ``path`` with ``text`` using a sibling temp file.

    The final payload is deterministic and retry-safe for identical inputs.
    """
    ensure_dir(path.parent)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
        text=True,
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding=encoding) as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        temp_path.replace(path)
    finally:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass
    return path


def atomic_write_json(
    path: Path,
    payload: Any,
    *,
    indent: int = 2,
    sort_keys: bool = True,
    default: Callable[[Any], Any] | None = None,
    trailing_newline: bool = True,
    validator: Callable[[Any], None] | None = None,
) -> Path:
    """
    Serialize and atomically replace a JSON payload.

    Optional ``validator`` runs before and after serialization so canonical
    control-plane writers can fail closed on malformed payloads.
    """
    if validator is not None:
        validator(payload)
    serialized = json.dumps(payload, indent=indent, sort_keys=sort_keys, default=default)
    if trailing_newline:
        serialized += "\n"
    target = atomic_write_text(path, serialized)
    if validator is not None:
        validator(json.loads(serialized))
    return target


def read_json(path: Path) -> Any:
    """
    Read a JSON payload from a file.
    """
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def run_scoped_lake_path(data_root: Path, run_id: str, *parts: str) -> Path:
    """
    Build a run-scoped lake path under ``data/lake/runs/<run_id>/...``.
    """
    return Path(data_root) / "lake" / "runs" / str(run_id) / Path(*parts)


def _force_parquet_fallback_enabled() -> bool:
    return str(os.getenv("BACKTEST_FORCE_CSV_FALLBACK", "0")).strip() in {
        "1",
        "true",
        "TRUE",
        "yes",
        "YES",
    }


def _strict_run_scoped_reads_enabled() -> bool:
    """
    Return True when read resolution must remain run-scoped only.

    Controlled via BACKTEST_STRICT_RUN_SCOPED_READS=1 and intended for
    certification/repro runs where cross-run fallback is not allowed.
    """
    return str(os.getenv("BACKTEST_STRICT_RUN_SCOPED_READS", "0")).strip() in {
        "1",
        "true",
        "TRUE",
        "yes",
        "YES",
    }


def _looks_like_native_parquet_bytes(path: Path) -> bool:
    try:
        with Path(path).open("rb") as handle:
            return handle.read(4) == b"PAR1"
    except OSError:
        return False


def parquet_path_runtime_readable(path: Path | str) -> bool:
    """Return True when the current runtime can read ``path`` through the canonical IO surface.

    Native parquet bytes require a parquet engine unless a CSV sidecar exists.
    Pickle-backed logical parquet remains readable through the repository fallback.
    """
    target = Path(path)
    if not target.exists():
        return False
    if target.suffix.lower() == ".csv":
        return True
    csv_sidecar = target.with_suffix(".csv")
    if csv_sidecar.exists():
        return True
    if HAS_PYARROW and not _force_parquet_fallback_enabled():
        return True
    if target.suffix.lower() != ".parquet":
        return True
    return not _looks_like_native_parquet_bytes(target)


def unreadable_parquet_samples(root: Path | str, *, limit: int = 8) -> list[Path]:
    """Collect representative logical parquet paths that are not readable in this runtime."""
    base = Path(root)
    if not base.exists():
        return []
    unreadable: list[Path] = []
    for path in sorted(base.rglob("*.parquet")):
        if parquet_path_runtime_readable(path):
            continue
        unreadable.append(path)
        if len(unreadable) >= max(1, int(limit)):
            break
    return unreadable


def choose_partition_dir(candidates: Sequence[Path]) -> Path | None:
    """
    Pick the best available partition directory from ordered candidates.

    Selection order (default):
    1) first existing directory containing parquet/csv files (recursive)
    2) first existing non-empty directory
    3) first existing directory

    Strict mode:
    - when BACKTEST_STRICT_RUN_SCOPED_READS=1, only the first candidate is
      eligible. This prevents cross-run/global fallback during certification.
    """
    normalized = [Path(p) for p in candidates if p is not None]
    if not normalized:
        return None

    if _strict_run_scoped_reads_enabled():
        first = normalized[0]
        if first.exists() and first.is_dir():
            return first
        return None

    for candidate in normalized:
        if not candidate.exists() or not candidate.is_dir():
            continue
        if any(candidate.rglob("*.parquet")) or any(candidate.rglob("*.csv")):
            return candidate

    for candidate in normalized:
        if not candidate.exists() or not candidate.is_dir():
            continue
        try:
            next(candidate.iterdir())
            return candidate
        except StopIteration:
            continue

    for candidate in normalized:
        if candidate.exists() and candidate.is_dir():
            return candidate

    return None



def _normalize_candidate_path(value: str | Path | None) -> Path | None:
    raw = str(value).strip() if value is not None else ""
    if not raw:
        return None
    path = Path(raw).expanduser().resolve()
    return path


def discover_external_cleaned_root(
    data_root: Path,
    *,
    explicit_root: str | Path | None = None,
) -> Path | None:
    """Locate an external offline cleaned-bars root.

    Supported roots are directories containing the canonical structure:
    ``<root>/<market>/<symbol>/bars_<timeframe>/year=*/month=*/*.parquet``.

    Search order:
    1. explicit_root
    2. EDGE_OFFLINE_CLEANED_BARS_ROOT / BACKTEST_OFFLINE_CLEANED_BARS_ROOT
    3. EDGE_OFFLINE_DATA_ROOT / BACKTEST_OFFLINE_DATA_ROOT joined with ``cleaned_bars``
    4. common local bundle paths near the repo or data root
    5. sibling directories under nearby parents containing ``offline-data/cleaned_bars``
    """
    from project import PROJECT_ROOT

    candidate_roots: list[Path] = []
    seen: set[str] = set()

    def _add(path_like: str | Path | None, *, append_cleaned: bool = False) -> None:
        path = _normalize_candidate_path(path_like)
        if path is None:
            return
        if append_cleaned:
            path = path / "cleaned_bars"
        key = str(path)
        if key in seen:
            return
        seen.add(key)
        candidate_roots.append(path)

    _add(explicit_root)
    _add(os.getenv("EDGE_OFFLINE_CLEANED_BARS_ROOT"))
    _add(os.getenv("BACKTEST_OFFLINE_CLEANED_BARS_ROOT"))
    _add(os.getenv("EDGE_OFFLINE_DATA_ROOT"), append_cleaned=True)
    _add(os.getenv("BACKTEST_OFFLINE_DATA_ROOT"), append_cleaned=True)

    _add(Path(data_root) / "offline-data" / "cleaned_bars")
    _add(Path(data_root).parent / "offline-data" / "cleaned_bars")
    _add(PROJECT_ROOT.parent / "offline-data" / "cleaned_bars")
    _add(PROJECT_ROOT.parent.parent / "offline-data" / "cleaned_bars")
    _add(PROJECT_ROOT.parent.parent.parent / "offline-data" / "cleaned_bars")

    scan_roots = [
        Path(data_root).parent,
        PROJECT_ROOT.parent.parent,
        PROJECT_ROOT.parent.parent.parent,
    ]
    for scan_root in scan_roots:
        try:
            if not scan_root.exists() or not scan_root.is_dir():
                continue
            for child in scan_root.iterdir():
                if not child.is_dir():
                    continue
                _add(child / "offline-data" / "cleaned_bars")
                try:
                    for grandchild in child.iterdir():
                        if not grandchild.is_dir():
                            continue
                        _add(grandchild / "offline-data" / "cleaned_bars")
                except OSError:
                    pass
        except OSError:
            continue

    for candidate in candidate_roots:
        if not candidate.exists() or not candidate.is_dir():
            continue
        if any(candidate.rglob("*.parquet")):
            return candidate
    return None


def _requested_month_keys(start: str, end: str) -> list[str]:
    start_ts = pd.Timestamp(str(start))
    end_ts = pd.Timestamp(str(end))
    if start_ts.tzinfo is None:
        start_ts = start_ts.tz_localize("UTC")
    else:
        start_ts = start_ts.tz_convert("UTC")
    if end_ts.tzinfo is None:
        end_ts = end_ts.tz_localize("UTC")
    else:
        end_ts = end_ts.tz_convert("UTC")
    end_text = str(end).strip()
    if len(end_text) == 10 and "T" not in end_text:
        end_ts = end_ts + pd.Timedelta(days=1)
    if end_ts <= start_ts:
        return []
    last_inclusive = end_ts - pd.Timedelta(minutes=1)
    cursor = pd.Timestamp(start_ts.year, start_ts.month, 1, tz="UTC")
    last_month = pd.Timestamp(last_inclusive.year, last_inclusive.month, 1, tz="UTC")
    keys: list[str] = []
    while cursor <= last_month:
        keys.append(f"{int(cursor.year):04d}-{int(cursor.month):02d}")
        cursor = cursor + pd.offsets.MonthBegin(1)
    return keys


def cleaned_dataset_dir(data_root: Path, *, market: str, symbol: str, timeframe: str) -> Path:
    return Path(data_root) / "lake" / "cleaned" / str(market) / str(symbol) / f"bars_{timeframe}"


def external_cleaned_dataset_dir(external_root: Path, *, market: str, symbol: str, timeframe: str) -> Path:
    return Path(external_root) / str(market) / str(symbol) / f"bars_{timeframe}"


def cleaned_dataset_covers_window(
    dataset_root: Path,
    *,
    start: str,
    end: str,
) -> bool:
    if not dataset_root.exists() or not dataset_root.is_dir():
        return False
    required = set(_requested_month_keys(start, end))
    if not required:
        return False
    available: set[str] = set()
    for year_dir in dataset_root.glob("year=*"):
        if not year_dir.is_dir():
            continue
        year = year_dir.name.split("=", 1)[-1]
        for month_dir in year_dir.glob("month=*"):
            if not month_dir.is_dir():
                continue
            month = month_dir.name.split("=", 1)[-1]
            if any(month_dir.glob("*.parquet")) or any(month_dir.glob("*.csv")):
                available.add(f"{year}-{month}")
    return required.issubset(available)


def materialize_external_cleaned_dataset(
    data_root: Path,
    external_root: Path,
    *,
    market: str,
    symbol: str,
    timeframe: str,
) -> Path | None:
    src = external_cleaned_dataset_dir(external_root, market=market, symbol=symbol, timeframe=timeframe)
    if not src.exists() or not src.is_dir():
        return None
    dst = cleaned_dataset_dir(data_root, market=market, symbol=symbol, timeframe=timeframe)
    if dst.exists():
        return dst
    ensure_dir(dst.parent)
    try:
        os.symlink(src, dst, target_is_directory=True)
        return dst
    except OSError:
        pass
    import shutil
    shutil.copytree(src, dst, dirs_exist_ok=True)
    return dst



def raw_dataset_dir_candidates(
    data_root: Path,
    *,
    market: str,
    symbol: str,
    dataset: str,
    run_id: str | None = None,
    venue: str = "bybit",
    aliases: Sequence[str] = (),
) -> list[Path]:
    """
    Build ordered raw-data candidate directories.

    Order is deliberate:
    1) run-scoped vendor-qualified
    2) global vendor-qualified
    3) run-scoped vendorless
    4) global vendorless

    This keeps the canonical vendor-qualified layout primary while explicitly
    supporting local vendorless archives such as ``data/lake/raw/perp/...``.
    """
    datasets = [str(dataset).strip(), *[str(alias).strip() for alias in aliases if str(alias).strip()]]
    candidates: list[Path] = []
    seen: set[str] = set()
    for dataset_name in datasets:
        ordered = []
        if run_id:
            ordered.append(run_scoped_lake_path(data_root, run_id, "raw", venue, market, symbol, dataset_name))
        ordered.append(Path(data_root) / "lake" / "raw" / venue / market / symbol / dataset_name)
        if run_id:
            ordered.append(run_scoped_lake_path(data_root, run_id, "raw", market, symbol, dataset_name))
        ordered.append(Path(data_root) / "lake" / "raw" / market / symbol / dataset_name)
        for candidate in ordered:
            key = str(candidate)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(candidate)
    return candidates


def resolve_raw_dataset_dir(
    data_root: Path,
    *,
    market: str,
    symbol: str,
    dataset: str,
    run_id: str | None = None,
    venue: str = "bybit",
    aliases: Sequence[str] = (),
) -> Path | None:
    return choose_partition_dir(
        raw_dataset_dir_candidates(
            data_root,
            market=market,
            symbol=symbol,
            dataset=dataset,
            run_id=run_id,
            venue=venue,
            aliases=aliases,
        )
    )


def list_parquet_files(path: Path) -> list[Path]:
    """
    Recursively list all parquet files under a directory.
    If parquet exists in some partitions, include parquet files plus CSV-only
    partitions that have no parquet in the same directory.
    """
    if not path.exists():
        return []
    parquet_files = sorted([p for p in path.rglob("*.parquet") if p.is_file()])
    csv_files = sorted([p for p in path.rglob("*.csv") if p.is_file()])
    if not parquet_files:
        return csv_files

    parquet_dirs = {p.parent for p in parquet_files}
    csv_only_partitions = [p for p in csv_files if p.parent not in parquet_dirs]
    return sorted(parquet_files + csv_only_partitions)


def read_parquet(
    files: Iterable[Path] | Path | str, columns: list[str] | None = None
) -> pd.DataFrame:
    """
    Read multiple logical parquet artifacts into a single DataFrame.

    `.csv` reads remain supported for backward compatibility with legacy artifacts,
    but logical parquet paths are canonical and may resolve to native parquet bytes
    or the repository's pickle-backed fallback.
    """
    if isinstance(files, (str, Path)):
        files = [Path(files)]
    else:
        files = [Path(file_path) for file_path in files]

    if not files:
        return pd.DataFrame()

    force_fallback = _force_parquet_fallback_enabled()
    if HAS_PYARROW and not force_fallback and len(files) > 1 and all(path.suffix == ".parquet" for path in files):
        try:
            return pq.read_table([str(path) for path in files], columns=columns).to_pandas()
        except Exception:
            pass

    frames = []
    for file_path in files:
        if file_path.suffix == ".csv":
            use_cols = columns if columns else None
            try:
                frames.append(pd.read_csv(file_path, usecols=use_cols))
            except ValueError:
                # If some columns are missing in CSV, fallback or handled by pandas
                frames.append(pd.read_csv(file_path))
        else:
            resolved_path = file_path
            if not resolved_path.exists():
                csv_fallback = resolved_path.with_suffix(".csv")
                if csv_fallback.exists():
                    use_cols = columns if columns else None
                    try:
                        frames.append(pd.read_csv(csv_fallback, usecols=use_cols))
                    except ValueError:
                        frames.append(pd.read_csv(csv_fallback))
                    continue
            if HAS_PYARROW and not force_fallback:
                try:
                    # Use ParquetFile for single-file reads to avoid memory usage
                    # when only subset of columns is needed.
                    pf = pq.ParquetFile(resolved_path)
                    frames.append(pf.read(columns=columns).to_pandas())
                    continue
                except Exception:
                    pass
            if not parquet_path_runtime_readable(resolved_path):
                raise RuntimeError(
                    f"Native parquet engine unavailable for real parquet artifact: {resolved_path}. "
                    "Install pyarrow/fastparquet or materialize a CSV sidecar."
                )
            frame = read_parquet_compat(resolved_path, columns=columns)
            frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def read_table_auto(path: Path | str, columns: list[str] | None = None) -> pd.DataFrame:
    """
    Read a single CSV/parquet path through the canonical project IO surface.

    Returns an empty DataFrame when the path is missing or unreadable so callers
    can use best-effort probing without duplicating csv/parquet branches.
    """
    target = Path(path)
    candidates = [target]
    if target.suffix != ".csv":
        candidates.append(target.with_suffix(".csv"))

    for candidate in candidates:
        if not candidate.exists():
            continue
        try:
            if candidate.suffix == ".csv":
                use_cols = columns if columns else None
                try:
                    return pd.read_csv(candidate, usecols=use_cols)
                except ValueError:
                    return pd.read_csv(candidate)
            return read_parquet(candidate, columns=columns)
        except Exception:
            continue
    return pd.DataFrame()


def write_parquet(df: pd.DataFrame, path: Path, skip_lock: bool = False) -> tuple[Path, str]:
    """
    Write a DataFrame to a logical parquet artifact.

    The returned path always preserves the requested `.parquet` contract. When a
    native parquet engine is unavailable, or when the compatibility toggle is set,
    the file is written via the repository's pickle-backed parquet fallback.

    Uses file locking to prevent race conditions during parallel writes unless skip_lock=True.
    Returns the requested path and the logical storage format (`"parquet"`).
    """
    ensure_dir(path.parent)

    if skip_lock:
        return _write_parquet_impl(df, path)

    import fcntl

    lock_path = path.with_suffix(path.suffix + ".lock")
    with open(lock_path, "w") as lock_file:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX)
            return _write_parquet_impl(df, path)
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)
            try:
                os.remove(lock_path)
            except Exception:
                pass


def _write_parquet_impl(df: pd.DataFrame, path: Path) -> tuple[Path, str]:
    if HAS_PYARROW and not _force_parquet_fallback_enabled():
        temp_path = path.with_suffix(path.suffix + ".tmp")
        table = pa.Table.from_pandas(df)
        pq.write_table(table, temp_path)
        temp_path.replace(path)
        return path, "parquet"

    temp_path = path.with_suffix(path.suffix + ".tmp")
    write_parquet_compat(df, temp_path, index=False)
    temp_path.replace(path)

    # Compatibility sidecar for environments without a native parquet engine.
    # Some lightweight scripts only probe ``.csv`` siblings when ``HAS_PYARROW``
    # is false; writing this sidecar keeps those paths interoperable while the
    # canonical logical artifact remains the ``.parquet`` file above.
    try:
        csv_path = path.with_suffix('.csv')
        df.to_csv(csv_path, index=False)
    except Exception:
        pass
    return path, "parquet"


def sorted_glob(paths):
    import glob

    return sorted(glob.glob(paths))


def lake_cache_key(
    symbol: str,
    market: str,
    timeframe: str,
    year: int,
    month: int,
    input_paths: Sequence[Path],
    **params: str,
) -> str:
    """
    Compute a deterministic cache key for a shared-lake artifact.

    Returns an MD5 hex digest encoding:
    - identity: symbol, market, timeframe, year, month
    - provenance: mtime of each raw input file (order-independent via sort)
    - params: any keyword args (e.g. funding_scale)

    Returns "" if any input_path is missing, forcing a cache miss (safe default).
    """
    mtime_parts = []
    for p in sorted(str(x) for x in input_paths):
        p_path = Path(p)
        if not p_path.exists():
            return ""
        mtime_parts.append(f"{p}:{p_path.stat().st_mtime:.1f}")
    identity = f"{symbol}|{market}|{timeframe}|{year}|{month:02d}"
    param_str = "|".join(f"{k}={v}" for k, v in sorted(params.items()))
    key_data = "|".join([identity] + mtime_parts + [param_str])
    return hashlib.md5(key_data.encode()).hexdigest()


def read_cache_key(path: Path) -> str:
    """Read the cache key sidecar for a shared-lake artifact. Returns "" if missing."""
    key_path = path.with_suffix(".cache_key")
    return key_path.read_text().strip() if key_path.exists() else ""


def write_cache_key(path: Path, key: str) -> None:
    """Write the cache key sidecar alongside a shared-lake artifact."""
    path.with_suffix(".cache_key").write_text(key)
