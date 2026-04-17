from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pandas as pd

from project import PROJECT_ROOT
from project.events.event_specs import _load_event_specs

REPO_ROOT = PROJECT_ROOT.parent
BASELINE_ROOT = REPO_ROOT / "artifacts" / "baseline"


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json(path: Path, payload: Any) -> Path:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    return path


def get_git_value(*args: str) -> str:
    try:
        result = subprocess.run(
            ["git", *args], cwd=REPO_ROOT, check=True, capture_output=True, text=True
        )
        return result.stdout.strip()
    except Exception:
        return ""


def collect_spec_hashes() -> dict[str, str]:
    roots = [
        REPO_ROOT / "spec" / "events",
        REPO_ROOT / "spec" / "gates.yaml",
        REPO_ROOT / "spec" / "objectives",
        REPO_ROOT / "spec" / "states",
        REPO_ROOT / "spec" / "runtime",
    ]
    out: dict[str, str] = {}
    for item in roots:
        if item.is_file():
            out[str(item.relative_to(REPO_ROOT))] = sha256_path(item)
        elif item.is_dir():
            for path in sorted(item.rglob("*.yaml")):
                out[str(path.relative_to(REPO_ROOT))] = sha256_path(path)
    return out


def snapshot_metadata() -> Path:
    import importlib

    modules = ["numpy", "pandas", "pyarrow", "yaml", "pydantic"]
    dependencies: dict[str, str] = {}
    for name in modules:
        try:
            mod = importlib.import_module(name)
            dependencies[name] = getattr(mod, "__version__", "unknown")
        except Exception:
            dependencies[name] = "unavailable"

    payload = {
        "git_commit": get_git_value("rev-parse", "HEAD"),
        "git_branch": get_git_value("rev-parse", "--abbrev-ref", "HEAD"),
        "timestamp_utc": pd.Timestamp.utcnow().isoformat(),
        "python_version": sys.version,
        "dependencies": dependencies,
        "spec_hashes": collect_spec_hashes(),
    }
    return write_json(BASELINE_ROOT / "metadata" / "system_snapshot.json", payload)


def snapshot_specs() -> list[Path]:
    targets = [
        REPO_ROOT / "spec" / "events",
        REPO_ROOT / "spec" / "gates.yaml",
        REPO_ROOT / "spec" / "objectives",
        REPO_ROOT / "spec" / "states",
        REPO_ROOT / "spec" / "runtime",
    ]
    copied: list[Path] = []
    dest_root = ensure_dir(BASELINE_ROOT / "specs")
    for source in targets:
        if not source.exists():
            continue
        dest = dest_root / source.name
        if dest.exists():
            if dest.is_dir():
                shutil.rmtree(dest)
            else:
                dest.unlink()
        if source.is_dir():
            shutil.copytree(source, dest)
        else:
            shutil.copy2(source, dest)
        copied.append(dest)
    return copied


def _candidate_event_paths() -> list[tuple[str, Path]]:
    candidates: list[tuple[str, Path]] = []
    data_root = REPO_ROOT / "data"
    for spec in _load_event_specs().values():
        family = str(spec.event_type).strip().lower()
        reports_dir = str(spec.reports_dir).strip()
        events_file = str(spec.events_file).strip()
        paths = [
            data_root / "research" / reports_dir / events_file,
            data_root / "research" / reports_dir / "events.parquet",
            data_root / "events" / events_file,
            data_root / events_file,
        ]
        seen = set()
        for path in paths:
            key = str(path)
            if key not in seen:
                seen.add(key)
                candidates.append((family, path))
    return candidates


def snapshot_event_outputs(*, strict: bool = False) -> Path:
    dest = ensure_dir(BASELINE_ROOT / "events")
    manifest: dict[str, Any] = {"copied": {}, "missing": {}}
    for family, path in _candidate_event_paths():
        if path.exists():
            target = dest / f"{family}{path.suffix or '.parquet'}"
            shutil.copy2(path, target)
            try:
                rel = path.relative_to(REPO_ROOT)
                manifest["copied"][family] = str(rel)
            except Exception:
                manifest["copied"][family] = str(path)
        else:
            manifest["missing"].setdefault(family, []).append(str(path))
    write_json(dest / "manifest.json", manifest)
    if strict and manifest["missing"]:
        raise FileNotFoundError("Missing event outputs for baseline snapshot")
    return dest


def snapshot_analyzer_outputs(*, strict: bool = False) -> Path:
    dest = ensure_dir(BASELINE_ROOT / "analyzers")
    manifest: dict[str, Any] = {"copied_files": [], "missing_roots": []}
    candidates = [
        REPO_ROOT / "data" / "research",
        REPO_ROOT / "data" / "artifacts",
        REPO_ROOT / "data" / "analysis",
    ]
    found_any = False
    for source in candidates:
        if not source.exists():
            manifest["missing_roots"].append(str(source))
            continue
        found_any = True
        for path in source.rglob("*"):
            if not path.is_file():
                continue
            lower = path.name.lower()
            if lower.endswith((".json", ".parquet", ".csv")) and (
                "summary" in lower or "analysis" in lower or "forward" in lower or "report" in lower
            ):
                rel = path.relative_to(source)
                target = dest / source.name / rel
                ensure_dir(target.parent)
                shutil.copy2(path, target)
                manifest["copied_files"].append(
                    str(target.relative_to(REPO_ROOT))
                    if str(target).startswith(str(REPO_ROOT))
                    else str(target)
                )
    write_json(dest / "manifest.json", manifest)
    if strict and not found_any:
        raise FileNotFoundError("No analyzer output roots found for baseline snapshot")
    return dest


def build_baseline(*, strict: bool = False) -> dict[str, str]:
    metadata = snapshot_metadata()
    snapshot_specs()
    events = snapshot_event_outputs(strict=strict)
    analyzers = snapshot_analyzer_outputs(strict=strict)
    return {"metadata": str(metadata), "events": str(events), "analyzers": str(analyzers)}


def _safe_read_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() == ".json":
        return pd.read_json(path)
    raise ValueError(f"Unsupported snapshot file format: {path}")


def compare_snapshot_dirs(baseline_dir: Path, candidate_dir: Path) -> dict[str, Any]:
    report: dict[str, Any] = {
        "event_counts": {},
        "missing_in_candidate": [],
        "missing_in_baseline": [],
    }
    baseline_files = {path.name: path for path in baseline_dir.glob("*.parquet")}
    candidate_files = {path.name: path for path in candidate_dir.glob("*.parquet")}
    for name in sorted(set(baseline_files) | set(candidate_files)):
        b = baseline_files.get(name)
        c = candidate_files.get(name)
        if b is None:
            report["missing_in_baseline"].append(name)
            continue
        if c is None:
            report["missing_in_candidate"].append(name)
            continue
        bdf = _safe_read_table(b)
        cdf = _safe_read_table(c)
        report["event_counts"][name] = {
            "baseline": int(len(bdf)),
            "candidate": int(len(cdf)),
            "delta": int(len(cdf) - len(bdf)),
        }
    return report
