from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from project import PROJECT_ROOT


@dataclass(frozen=True)
class PreflightIssue:
    code: str
    message: str
    severity: str = "error"
    path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {"code": self.code, "message": self.message, "severity": self.severity}
        if self.path is not None:
            payload["path"] = self.path
        return payload


def resolve_data_root(data_root: str | Path | None = None) -> Path:
    if data_root:
        return Path(data_root)
    return PROJECT_ROOT.parent / "data"


def load_proposal_payload(proposal_path: str | Path) -> dict[str, Any]:
    path = Path(proposal_path)
    with path.open("r", encoding="utf-8") as fh:
        payload = yaml.safe_load(fh) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"proposal must be a mapping: {path}")
    return payload


def _extract_primary_anchor(payload: dict[str, Any]) -> dict[str, Any]:
    hypothesis = payload.get("hypothesis", {})
    if not isinstance(hypothesis, dict):
        return {}
    anchor = hypothesis.get("anchor", hypothesis.get("trigger", {}))
    return anchor if isinstance(anchor, dict) else {}


def _required_feature_names(payload: dict[str, Any]) -> list[str]:
    anchor = _extract_primary_anchor(payload)
    out: list[str] = []
    if str(anchor.get("type", "")).strip().lower() == "feature_predicate":
        feature = str(anchor.get("feature", "") or "").strip()
        if feature:
            out.append(feature)
    return out


def _event_required_surface(event_id: str) -> dict[str, list[str]]:
    token = event_id.strip().upper()
    if token in {"VOL_SHOCK", "VOL_SPIKE", "BREAKOUT_TRIGGER", "RANGE_COMPRESSION_END"}:
        return {
            "bars": ["timestamp", "close", "high", "low"],
            "features": ["rv_96", "range_96", "range_med_2880"],
        }
    if token in {"LIQUIDATION_CASCADE", "OI_SPIKE_POSITIVE", "OI_SPIKE_NEGATIVE", "OI_FLUSH"}:
        return {"bars": ["timestamp", "close", "high", "low"], "features": ["oi_notional"]}
    if token in {"FND_DISLOC", "FUNDING_EXTREME_ONSET", "FUNDING_PERSISTENCE_TRIGGER"}:
        return {"bars": ["timestamp", "close"], "features": ["funding_rate", "funding_rate_scaled"]}
    if token in {"BASIS_DISLOC", "SPOT_PERP_BASIS_SHOCK"}:
        return {"bars": ["timestamp", "close"], "features": ["close_perp", "close_spot"]}
    return {"bars": ["timestamp", "close"], "features": []}


def _try_parquet_columns(root: Path) -> set[str] | None:
    if not root.exists():
        return None
    parquet = next(root.rglob("*.parquet"), None)
    if parquet is None:
        return None
    try:
        import pyarrow.parquet as pq

        return set(pq.read_schema(parquet).names)
    except Exception:
        return None


def _check_root_exists(root: Path, *, code: str, label: str) -> PreflightIssue | None:
    if root.exists():
        return None
    return PreflightIssue(code=code, message=f"missing {label}: {root}", path=str(root))


def build_data_preflight_report(
    *,
    proposal_path: str | Path,
    data_root: str | Path | None = None,
    strict_columns: bool = False,
) -> dict[str, Any]:
    proposal = load_proposal_payload(proposal_path)
    root = resolve_data_root(data_root)
    symbols_raw = proposal.get("symbols", [])
    if isinstance(symbols_raw, str):
        symbols_raw = [symbols_raw]
    symbols = [str(symbol).upper() for symbol in symbols_raw if str(symbol).strip()]
    timeframe = str(proposal.get("timeframe", "5m") or "5m")
    start = str(proposal.get("start", "") or "")
    end = str(proposal.get("end", "") or "")
    anchor = _extract_primary_anchor(proposal)
    event_id = str(anchor.get("event_id", "") or "").strip().upper()
    anchor_type = str(anchor.get("type", "") or "").strip().lower()
    required_features = set(_required_feature_names(proposal))
    if event_id:
        required_features.update(_event_required_surface(event_id).get("features", []))

    issues: list[PreflightIssue] = []
    warnings: list[PreflightIssue] = []
    checked: dict[str, Any] = {
        "symbols": symbols,
        "timeframe": timeframe,
        "start": start,
        "end": end,
        "anchor_type": anchor_type,
        "event_id": event_id,
        "required_features": sorted(required_features),
        "symbol_checks": {},
    }

    if not symbols:
        issues.append(PreflightIssue("missing_symbols", "proposal does not declare any symbols"))

    for symbol in symbols:
        bars_root = root / "lake" / "cleaned" / "perp" / symbol / f"bars_{timeframe}"
        features_root = root / "lake" / "features" / "perp" / symbol / timeframe / "features_feature_schema_v2"
        context_root = root / "lake" / "features" / "perp" / symbol / timeframe / "market_context"
        symbol_payload: dict[str, Any] = {
            "bars_root": str(bars_root),
            "features_root": str(features_root),
            "market_context_root": str(context_root),
            "bars_exists": bars_root.exists(),
            "features_exists": features_root.exists(),
            "market_context_exists": context_root.exists(),
        }
        checked["symbol_checks"][symbol] = symbol_payload

        for issue in (
            _check_root_exists(bars_root, code="missing_bars", label=f"cleaned bars for {symbol}"),
            _check_root_exists(features_root, code="missing_features", label=f"features for {symbol}"),
        ):
            if issue is not None:
                issues.append(issue)
        if not context_root.exists():
            warnings.append(
                PreflightIssue(
                    "missing_market_context",
                    f"missing market context for {symbol}; context/regime audits may default or degrade",
                    severity="warning",
                    path=str(context_root),
                )
            )

        if features_root.exists() and required_features:
            columns = _try_parquet_columns(features_root)
            if columns is None:
                warnings.append(
                    PreflightIssue(
                        "unknown_feature_columns",
                        f"could not inspect feature parquet columns for {symbol}",
                        severity="warning",
                        path=str(features_root),
                    )
                )
            else:
                missing = sorted(feature for feature in required_features if feature not in columns)
                symbol_payload["feature_columns_sample"] = sorted(columns)[:100]
                symbol_payload["missing_required_features"] = missing
                issue = PreflightIssue(
                    "missing_required_features",
                    f"missing required feature columns for {symbol}: {missing}",
                    severity="error" if strict_columns else "warning",
                    path=str(features_root),
                )
                if missing and strict_columns:
                    issues.append(issue)
                elif missing:
                    warnings.append(issue)

    status = "pass" if not issues else "fail"
    return {
        "kind": "data_preflight",
        "status": status,
        "proposal_path": str(proposal_path),
        "data_root": str(root),
        "checked": checked,
        "issues": [issue.to_dict() for issue in issues],
        "warnings": [warning.to_dict() for warning in warnings],
        "next_safe_command": (
            "Run discovery only after preflight passes."
            if status == "pass"
            else "Fix data coverage or proposal inputs before discovery."
        ),
    }


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Check proposal data coverage before discovery.")
    parser.add_argument("--proposal", required=True)
    parser.add_argument("--data_root")
    parser.add_argument("--strict_columns", action="store_true")
    args = parser.parse_args(argv)
    report = build_data_preflight_report(
        proposal_path=args.proposal,
        data_root=args.data_root,
        strict_columns=args.strict_columns,
    )
    print(json.dumps(report, indent=2, sort_keys=True, default=str))
    return 0 if report.get("status") == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
