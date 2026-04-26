from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from project.core.feature_schema import normalize_feature_schema_version


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def load_foundation_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Foundation config not found: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Foundation config must be a YAML mapping.")
    return payload


def _default_report_paths(
    *,
    data_root: Path,
    run_id: str,
    market: str,
    symbol: str,
    timeframe: str,
    feature_schema_version: str = "v2",
) -> dict[str, Path]:
    schema_version = normalize_feature_schema_version(feature_schema_version)
    return {
        "cleaned_quality": data_root
        / "reports"
        / "data_quality"
        / run_id
        / "cleaned"
        / market
        / symbol
        / f"bars_{timeframe}_quality.json",
        "data_validation": data_root
        / "reports"
        / "data_quality"
        / run_id
        / "validation"
        / f"validate_data_coverage_{timeframe}.json",
        "feature_quality": data_root
        / "reports"
        / "feature_quality"
        / run_id
        / market
        / symbol
        / timeframe
        / f"feature_quality_{schema_version}.json",
        "feature_validation": data_root
        / "reports"
        / "feature_quality"
        / run_id
        / "validation"
        / f"validate_feature_integrity_{timeframe}.json",
        "context_quality": data_root
        / "reports"
        / "context_quality"
        / run_id
        / market
        / symbol
        / timeframe
        / "context_quality_report_v1.json",
    }


def build_live_data_foundation_payload(
    *,
    data_root: Path,
    run_id: str,
    symbol: str,
    timeframe: str,
    market: str = "perp",
    feature_schema_version: str = "v2",
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = dict(config or {})
    paths = _default_report_paths(
        data_root=data_root,
        run_id=run_id,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        feature_schema_version=feature_schema_version,
    )
    payloads = {name: _read_json(path) for name, path in paths.items()}

    missing = [name for name, path in paths.items() if not path.exists()]
    data_validation = payloads.get("data_validation", {})
    feature_validation = payloads.get("feature_validation", {})
    data_failures = int(data_validation.get("failure_count", 0) or 0)
    data_warnings = int(data_validation.get("warning_count", 0) or 0)
    feature_status = str(feature_validation.get("status", "")).strip().lower()
    feature_symbols = dict(feature_validation.get("symbols", {}))
    feature_issues = feature_symbols.get(symbol, {})
    feature_issue_count = 0
    if isinstance(feature_issues, dict):
        for key, value in feature_issues.items():
            if key == "feature_quality_summary":
                continue
            if isinstance(value, list):
                feature_issue_count += len(value)

    if missing or data_failures > 0 or feature_status == "failed":
        readiness = "blocked"
    elif data_warnings > 0 or feature_status == "warning" or feature_issue_count > 0:
        readiness = "warn"
    else:
        readiness = "ready"

    return {
        "schema_version": "live_data_foundation_report_v1",
        "foundation_id": str(cfg.get("foundation_id", "btc_live_foundation")).strip()
        or "btc_live_foundation",
        "description": str(cfg.get("description", "")).strip(),
        "run_id": run_id,
        "market": market,
        "symbol": symbol,
        "timeframe": timeframe,
        "feature_schema_version": feature_schema_version,
        "readiness": readiness,
        "missing_reports": missing,
        "report_paths": {name: str(path) for name, path in paths.items()},
        "data_quality": {
            "cleaned_overall": dict(payloads.get("cleaned_quality", {}).get("overall", {})),
            "validation_failure_count": data_failures,
            "validation_warning_count": data_warnings,
        },
        "feature_quality": {
            "summary": dict(payloads.get("feature_quality", {}).get("quality", {})),
            "validation_status": feature_status or "missing",
            "issue_count": feature_issue_count,
        },
        "context_quality": dict(payloads.get("context_quality", {}).get("summary", {})),
    }


def write_live_data_foundation_report(
    *,
    data_root: Path,
    run_id: str,
    symbol: str,
    timeframe: str,
    market: str = "perp",
    feature_schema_version: str = "v2",
    config_path: Path | None = None,
    out_dir: Path | None = None,
) -> Path:
    config = load_foundation_config(config_path) if config_path is not None else {}
    payload = build_live_data_foundation_payload(
        data_root=data_root,
        run_id=run_id,
        symbol=symbol,
        timeframe=timeframe,
        market=market,
        feature_schema_version=feature_schema_version,
        config=config,
    )
    report_dir = (
        out_dir
        if out_dir is not None
        else data_root / "reports" / "live_foundation" / run_id / market / symbol / timeframe
    )
    report_dir.mkdir(parents=True, exist_ok=True)
    out_path = report_dir / "live_data_foundation_report.json"
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return out_path
