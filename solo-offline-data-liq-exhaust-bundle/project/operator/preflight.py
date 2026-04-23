from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from project.core.config import get_data_root
from project.io.utils import list_parquet_files, resolve_raw_dataset_dir
from project.research.agent_io.proposal_schema import load_operator_proposal
from project.research.agent_io.proposal_to_experiment import translate_and_validate_proposal
from project.research.feature_surface_viability import analyze_feature_surface_viability

_PARTITION_YEAR_RE = re.compile(r"year=(\d{4})")
_PARTITION_MONTH_RE = re.compile(r"month=(\d{2})")


@dataclass(frozen=True)
class DatasetExpectation:
    name: str
    dataset: str
    required: bool
    aliases: tuple[str, ...] = ()


def _bool_from_int(raw: int | bool) -> bool:
    return bool(int(raw)) if isinstance(raw, int) else bool(raw)


def _partition_month_key(path: Path) -> tuple[int, int] | None:
    text = str(path)
    year_match = _PARTITION_YEAR_RE.search(text)
    month_match = _PARTITION_MONTH_RE.search(text)
    if not year_match or not month_match:
        return None
    return int(year_match.group(1)), int(month_match.group(1))


def _sample_boundary_files(files: list[Path]) -> list[Path]:
    if len(files) <= 2:
        return files
    keyed = [(path, _partition_month_key(path)) for path in files]
    with_keys = [item for item in keyed if item[1] is not None]
    if not with_keys:
        return [files[0], files[-1]]
    ordered = sorted(with_keys, key=lambda item: (item[1][0], item[1][1], str(item[0])))
    selected = [ordered[0][0]]
    if ordered[-1][0] != ordered[0][0]:
        selected.append(ordered[-1][0])
    return selected


def _ts_bounds(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {"start": None, "end": None, "file_count": 0, "unreadable_file_count": 0}
    files = list_parquet_files(path)
    if not files:
        return {"start": None, "end": None, "file_count": 0, "unreadable_file_count": 0}
    sampled_files = _sample_boundary_files(files)
    starts: list[pd.Timestamp] = []
    ends: list[pd.Timestamp] = []
    unreadable_file_count = 0
    for file_path in sampled_files:
        try:
            frame = pd.read_csv(file_path, usecols=["timestamp"]) if file_path.suffix == ".csv" else None
            if frame is None:
                from project.io.utils import read_parquet

                frame = read_parquet(file_path, columns=["timestamp"])
        except Exception:
            unreadable_file_count += 1
            continue
        if frame.empty or "timestamp" not in frame.columns:
            continue
        ts = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce").dropna()
        if ts.empty:
            continue
        starts.append(ts.min())
        ends.append(ts.max())
    if not starts or not ends:
        return {
            "start": None,
            "end": None,
            "file_count": len(files),
            "sampled_file_count": len(sampled_files),
            "unreadable_file_count": unreadable_file_count,
        }
    return {
        "start": min(starts).isoformat(),
        "end": max(ends).isoformat(),
        "file_count": len(files),
        "sampled_file_count": len(sampled_files),
        "unreadable_file_count": unreadable_file_count,
    }


def _date_window_payload(start: str, end: str) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    start_ts = pd.to_datetime(start, utc=True, errors="coerce")
    end_ts = pd.to_datetime(end, utc=True, errors="coerce")
    if pd.isna(start_ts) or pd.isna(end_ts):
        return None, None
    if len(str(end).strip()) == 10 and "T" not in str(end):
        end_ts = end_ts + pd.Timedelta(days=1)
    return start_ts, end_ts


def _coverage_status(*, found_start: str | None, found_end: str | None, start: str, end: str) -> str:
    req_start, req_end = _date_window_payload(start, end)
    if req_start is None or req_end is None or not found_start or not found_end:
        return "unknown"
    avail_start = pd.Timestamp(found_start)
    avail_end = pd.Timestamp(found_end)
    if avail_start <= req_start and avail_end >= req_end:
        return "full"
    if avail_end >= req_start and avail_start <= req_end:
        return "partial"
    return "none"


def _proposal_preflight_checks(
    *,
    proposal_path: str | Path,
    registry_root: Path,
    data_root: Path,
    out_dir: Path,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    proposal = load_operator_proposal(proposal_path)
    translation = translate_and_validate_proposal(
        proposal_path,
        registry_root=registry_root,
        out_dir=out_dir,
        config_path=out_dir / "experiment.yaml",
    )
    checks.append(
        {
            "name": "proposal_valid",
            "status": "pass",
            "details": {
                "program_id": proposal.program_id,
                "symbols": list(proposal.symbols),
                "timeframe": proposal.timeframe,
                "templates": list(proposal.templates),
            },
        }
    )
    checks.append(
        {
            "name": "validated_plan",
            "status": "pass",
            "details": translation["validated_plan"],
        }
    )
    search_spec = Path(proposal.search_spec)
    if not search_spec.is_absolute():
        search_spec = Path.cwd() / search_spec
    checks.append(
        {
            "name": "search_spec_exists",
            "status": "pass" if search_spec.exists() else "block",
            "details": {"path": str(search_spec), "exists": bool(search_spec.exists())},
        }
    )

    expectations = [
        DatasetExpectation("ohlcv", f"ohlcv_{proposal.timeframe}", True),
        DatasetExpectation("funding", "funding", False, aliases=(f"funding_{proposal.timeframe}", "fundingRate")),
        DatasetExpectation("open_interest", "open_interest", False, aliases=(proposal.timeframe,)),
    ]
    per_symbol: dict[str, Any] = {}
    for symbol in proposal.symbols:
        symbol_payload: dict[str, Any] = {}
        for expectation in expectations:
            resolved = resolve_raw_dataset_dir(
                data_root,
                market="perp",
                symbol=symbol,
                dataset=expectation.dataset,
                run_id=None,
                aliases=expectation.aliases,
            )
            bounds = _ts_bounds(resolved)
            coverage = _coverage_status(
                found_start=bounds["start"],
                found_end=bounds["end"],
                start=proposal.start,
                end=proposal.end,
            )
            status = "pass"
            if resolved is None or coverage == "none":
                status = "block" if expectation.required else "warn"
            elif coverage == "partial":
                status = "warn" if expectation.required else "warn"
            elif int(bounds.get("unreadable_file_count", 0) or 0) > 0:
                status = "warn"
            symbol_payload[expectation.name] = {
                "status": status,
                "required": expectation.required,
                "resolved_path": str(resolved) if resolved else None,
                "coverage": coverage,
                **bounds,
            }
        per_symbol[symbol] = symbol_payload
    data_statuses = [
        dataset_payload["status"]
        for symbol_payload in per_symbol.values()
        for dataset_payload in symbol_payload.values()
    ]
    local_data_status = (
        "block" if "block" in data_statuses else "warn" if "warn" in data_statuses else "pass"
    )
    checks.append({"name": "local_data_resolution", "status": local_data_status, "details": per_symbol})

    required_detectors = [
        str(item).strip().upper()
        for item in translation.get("validated_plan", {}).get("required_detectors", [])
        if str(item).strip()
    ]
    viability = analyze_feature_surface_viability(
        data_root=data_root,
        run_id="",
        symbols=list(proposal.symbols),
        timeframe=str(proposal.timeframe),
        start=str(proposal.start),
        end=str(proposal.end),
        event_types=required_detectors,
        market="perp",
    )
    viability_status = str(viability.get("status", "unknown") or "unknown").strip().lower()
    if viability_status == "unknown":
        viability_check_status = "warn"
    else:
        viability_check_status = viability_status
    checks.append(
        {
            "name": "feature_surface_viability",
            "status": viability_check_status,
            "details": viability,
        }
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    writable_status = "pass"
    writable_detail: dict[str, Any] = {"path": str(out_dir), "exists": True}
    try:
        probe = out_dir / ".preflight_write_probe"
        probe.write_text("ok\n", encoding="utf-8")
        probe.unlink(missing_ok=True)
    except Exception as exc:  # pragma: no cover - permissions/platform dependent
        writable_status = "block"
        writable_detail["error"] = str(exc)
    checks.append({"name": "artifact_root_writable", "status": writable_status, "details": writable_detail})

    statuses = [item["status"] for item in checks]
    overall = "block" if "block" in statuses else "warn" if "warn" in statuses else "pass"
    return {
        "schema_version": "operator_preflight_v2",
        "status": overall,
        "proposal_path": str(proposal_path),
        "registry_root": str(registry_root),
        "data_root": str(data_root),
        "out_dir": str(out_dir),
        "checks": checks,
    }


def run_preflight(
    *,
    proposal_path: str | Path,
    registry_root: str | Path = "project/configs/registries",
    data_root: str | Path | None = None,
    out_dir: str | Path | None = None,
    json_output: str | Path | None = None,
) -> dict[str, Any]:
    resolved_data_root = Path(data_root) if data_root is not None else get_data_root()
    resolved_registry_root = Path(registry_root)
    resolved_out_dir = Path(out_dir) if out_dir is not None else (resolved_data_root / "reports" / "operator_preflight")
    result = _proposal_preflight_checks(
        proposal_path=proposal_path,
        registry_root=resolved_registry_root,
        data_root=resolved_data_root,
        out_dir=resolved_out_dir,
    )
    if json_output is not None:
        output_path = Path(json_output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run operator preflight for a proposal.")
    parser.add_argument("--proposal", required=True)
    parser.add_argument("--registry_root", default="project/configs/registries")
    parser.add_argument("--data_root", default=None)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--json_output", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = run_preflight(
        proposal_path=args.proposal,
        registry_root=args.registry_root,
        data_root=args.data_root,
        out_dir=args.out_dir,
        json_output=args.json_output,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] != "block" else 1


if __name__ == "__main__":
    raise SystemExit(main())
