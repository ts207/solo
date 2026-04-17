from __future__ import annotations

import importlib
import json
import logging
from pathlib import Path
from typing import Any, Callable, Dict, List

import pandas as pd

from project import PROJECT_ROOT
from project.core.config import get_data_root
from project.core.coercion import as_bool, safe_int
from project.core.exceptions import ArtifactReadError
from project.io.parquet_compat import read_parquet_compat
from project.io.utils import ensure_dir, read_parquet, write_parquet
from project.specs.ontology import ontology_spec_hash


def _read_csv_or_parquet(path: Path) -> pd.DataFrame:
    if path.suffix.lower() != ".parquet":
        return pd.read_csv(path)
    try:
        return pd.read_parquet(path)
    except RuntimeError:
        raise
    except (ImportError, OSError, ValueError):
        csv_fallback = path.with_suffix(".csv")
        if csv_fallback.exists():
            return pd.read_csv(csv_fallback)
        return read_parquet_compat(path)


def _normalize_statuses(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
        return [token.strip() for token in raw.split(",") if token.strip()]
    return []


def _canonicalize_candidate_audit_keys(candidates_df: pd.DataFrame) -> pd.DataFrame:
    if candidates_df.empty:
        return candidates_df.copy()
    out = candidates_df.copy()
    if "plan_row_id" not in out.columns:
        out["plan_row_id"] = ""
    if "hypothesis_id" not in out.columns:
        out["hypothesis_id"] = ""

    plan_row_ids = out["plan_row_id"].astype(str).str.strip()
    hypothesis_ids = out["hypothesis_id"].astype(str).str.strip()
    out["plan_row_id"] = plan_row_ids.where(plan_row_ids != "", hypothesis_ids)
    return out


def _load_hypothesis_index(
    *,
    run_id: str,
    data_root: Path,
    diagnostics: Dict[str, Any] | None = None,
    read_csv_or_parquet_fn: Callable[[Path], pd.DataFrame],
    record_degraded_state_fn: Callable[..., None],
) -> Dict[str, Dict[str, Any]]:
    phase2_root = data_root / "reports" / "phase2" / run_id
    if not phase2_root.exists():
        return {}

    candidate_paths: List[Path] = []
    for direct_name in ("hypothesis_registry.parquet", "hypothesis_registry.csv"):
        direct_path = phase2_root / direct_name
        if direct_path.exists():
            candidate_paths.append(direct_path)
    for pattern in ("*/*/hypothesis_registry.parquet", "*/*/hypothesis_registry.csv"):
        candidate_paths.extend(sorted(phase2_root.glob(pattern)))

    index: Dict[str, Dict[str, Any]] = {}
    seen_paths: set[Path] = set()
    for registry_path in candidate_paths:
        if registry_path in seen_paths or not registry_path.exists():
            continue
        seen_paths.add(registry_path)
        try:
            registry_df = read_csv_or_parquet_fn(registry_path)
        except (
            ArtifactReadError,
            ImportError,
            OSError,
            UnicodeDecodeError,
            ValueError,
            pd.errors.ParserError,
        ) as exc:
            wrapped = (
                exc
                if isinstance(exc, ArtifactReadError)
                else ArtifactReadError(f"Failed loading hypothesis registry {registry_path}: {exc}")
            )
            logging.warning("%s", wrapped)
            if diagnostics is not None:
                record_degraded_state_fn(
                    diagnostics,
                    code="hypothesis_registry_unreadable",
                    message=str(wrapped),
                    details={"path": str(registry_path)},
                )
            continue
        if registry_df.empty:
            continue

        for _, row in registry_df.iterrows():
            record = row.to_dict()
            hypothesis_id = str(record.get("hypothesis_id", "")).strip()
            if not hypothesis_id:
                continue
            plan_row_id = str(record.get("plan_row_id", "")).strip() or hypothesis_id
            statuses = _normalize_statuses(record.get("statuses"))
            normalized = dict(record)
            normalized["hypothesis_id"] = hypothesis_id
            normalized["plan_row_id"] = plan_row_id
            normalized["statuses"] = statuses or ["candidate_discovery"]
            normalized["executed"] = bool(record.get("executed", True))
            index.setdefault(hypothesis_id, normalized)
            index.setdefault(plan_row_id, normalized)
    return index


def _load_bridge_metrics(bridge_root: Path, symbol: str | None = None) -> pd.DataFrame:
    del symbol
    versioned_files = list(bridge_root.rglob("*_v1.csv"))
    parquet_files = list(bridge_root.rglob("bridge_evaluation.parquet"))
    fallback_csv_files = [
        path for path in bridge_root.rglob("*.csv") if path not in versioned_files
    ]
    ordered_files = [*versioned_files, *parquet_files, *fallback_csv_files]
    if not ordered_files:
        return pd.DataFrame()
    frames = [read_parquet(path) for path in ordered_files]
    out = pd.concat(frames, ignore_index=True)
    dedupe_cols = [col for col in ("candidate_id", "event_type", "symbol") if col in out.columns]
    if dedupe_cols:
        out = out.drop_duplicates(subset=dedupe_cols, keep="first").reset_index(drop=True)
    return out


def _merge_bridge_metrics(phase2_df: pd.DataFrame, bridge_df: pd.DataFrame) -> pd.DataFrame:
    if bridge_df.empty:
        return phase2_df
    out = pd.merge(
        phase2_df,
        bridge_df[
            [
                "candidate_id",
                "event_type",
                "gate_bridge_tradable",
                "bridge_validation_after_cost_bps",
            ]
        ],
        on=["candidate_id", "event_type"],
        how="left",
        suffixes=("", "_bridge"),
    )
    if "gate_bridge_tradable_bridge" in out.columns:
        out["gate_bridge_tradable"] = out["gate_bridge_tradable_bridge"].combine_first(
            out["gate_bridge_tradable"]
        )
        out = out.drop(columns=["gate_bridge_tradable_bridge"])
    return out


def _parse_run_symbols(raw_symbols: Any) -> List[str]:
    if isinstance(raw_symbols, (list, tuple, set)):
        values = raw_symbols
    else:
        values = str(raw_symbols or "").split(",")
    ordered: List[str] = []
    seen: set[str] = set()
    for value in values:
        symbol = str(value).strip().upper()
        if not symbol or symbol in seen:
            continue
        ordered.append(symbol)
        seen.add(symbol)
    return ordered


def _hydrate_edge_candidates_from_phase2(
    *,
    run_id: str,
    run_symbols: List[str],
    source_run_mode: str,
    data_root: Path,
) -> pd.DataFrame:
    if not run_symbols:
        return pd.DataFrame()
    export_module = importlib.import_module("project.research.export_edge_candidates")
    rows = export_module._collect_phase2_candidates(run_id, run_symbols=run_symbols)
    candidates_df = pd.DataFrame(rows)
    if candidates_df.empty:
        return candidates_df

    from project.research.helpers.shrinkage import _apply_hierarchical_shrinkage

    candidates_df = _apply_hierarchical_shrinkage(
        candidates_df,
        train_only_lambda=True,
        split_col="split_label",
        run_mode=source_run_mode,
    )
    is_confirmatory = bool(export_module._is_confirmatory_run_mode(source_run_mode))
    current_spec_hash = ontology_spec_hash(PROJECT_ROOT.parent)
    candidates_df = export_module._normalize_edge_candidates_df(
        candidates_df,
        run_mode=source_run_mode,
        is_confirmatory=is_confirmatory,
        current_spec_hash=current_spec_hash,
    )

    out_dir = data_root / "reports" / "edge_candidates" / run_id
    ensure_dir(out_dir)
    write_parquet(candidates_df, out_dir / "edge_candidates_normalized.parquet")
    (out_dir / "edge_candidates_normalized.json").write_text(
        candidates_df.to_json(orient="records", indent=2),
        encoding="utf-8",
    )
    return candidates_df


def _load_negative_control_summary(run_id: str) -> Dict[str, Any]:
    data_root = get_data_root()
    path = data_root / "reports" / "negative_control" / run_id / "negative_control_summary.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {}


REQUIRED_PROMOTION_FIELDS = frozenset(
    {
        "candidate_id",
        "family",
        "event_type",
        "net_expectancy_bps",
        "stability_score",
        "sign_consistency",
        "cost_survival_ratio",
        "q_value",
        "n_events",
    }
)


def _missing_or_blank_mask(series: pd.Series) -> pd.Series:
    mask = series.isna()
    if pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
        normalized = series.astype(str).str.strip().str.lower()
        mask = mask | normalized.isin({"", "nan", "none", "null", "<na>"})
    return mask


def _coalesce_column(out: pd.DataFrame, target: str, sources: list[str]) -> None:
    if target not in out.columns:
        out[target] = pd.NA
    target_missing = _missing_or_blank_mask(out[target])
    for source in sources:
        if source not in out.columns:
            continue
        source_values = out[source]
        source_present = ~_missing_or_blank_mask(source_values)
        fill_mask = target_missing & source_present
        if bool(fill_mask.any()):
            out.loc[fill_mask, target] = source_values.loc[fill_mask]
            target_missing = _missing_or_blank_mask(out[target])
        if not bool(target_missing.any()):
            break


def _fill_numeric_column_from_scaled_sources(
    out: pd.DataFrame,
    target: str,
    sources: list[tuple[str, float]],
) -> None:
    if target not in out.columns:
        out[target] = pd.NA
    target_numeric = pd.to_numeric(out[target], errors="coerce")
    target_missing = target_numeric.isna()
    for source, scale in sources:
        if source not in out.columns:
            continue
        source_numeric = pd.to_numeric(out[source], errors="coerce") * float(scale)
        fill_mask = target_missing & source_numeric.notna()
        if bool(fill_mask.any()):
            out.loc[fill_mask, target] = source_numeric.loc[fill_mask]
            target_numeric = pd.to_numeric(out[target], errors="coerce")
            target_missing = target_numeric.isna()
        if not bool(target_missing.any()):
            break


def _derive_cost_survival_ratio_from_bridge_flags(out: pd.DataFrame) -> pd.Series:
    scenario_keys = [
        "gate_after_cost_positive",
        "gate_after_cost_stressed_positive",
        "gate_bridge_after_cost_positive_validation",
        "gate_bridge_after_cost_stressed_positive_validation",
    ]
    present = pd.Series(0, index=out.index, dtype="int64")
    passed = pd.Series(0, index=out.index, dtype="int64")
    for key in scenario_keys:
        if key not in out.columns:
            continue
        values = out[key]
        key_present = ~_missing_or_blank_mask(values)
        normalized = values.astype(str).str.strip().str.lower()
        key_passed = key_present & (
            values.eq(True) | normalized.isin({"pass", "true", "1", "passed"})
        )
        present = present + key_present.astype("int64")
        passed = passed + key_passed.astype("int64")
    return passed.where(present > 0).astype("float64") / present.where(present > 0)


def _hydrate_canonical_promotion_aliases(candidates_df: pd.DataFrame) -> pd.DataFrame:
    if candidates_df.empty:
        return candidates_df.copy()

    out = candidates_df.copy()
    _coalesce_column(
        out,
        "family",
        ["family_id", "event_family", "research_family", "canonical_family"],
    )
    _fill_numeric_column_from_scaled_sources(
        out,
        "net_expectancy_bps",
        [
            ("bridge_validation_stressed_after_cost_bps", 1.0),
            ("bridge_validation_after_cost_bps", 1.0),
            ("stressed_after_cost_expectancy_bps", 1.0),
            ("after_cost_expectancy_bps", 1.0),
            ("stressed_after_cost_expectancy_per_trade", 10_000.0),
            ("after_cost_expectancy_per_trade", 10_000.0),
        ],
    )

    if "cost_survival_ratio" not in out.columns:
        out["cost_survival_ratio"] = pd.NA
    cost_missing = pd.to_numeric(out["cost_survival_ratio"], errors="coerce").isna()
    if bool(cost_missing.any()):
        derived = _derive_cost_survival_ratio_from_bridge_flags(out)
        fill_mask = cost_missing & derived.notna()
        if bool(fill_mask.any()):
            out.loc[fill_mask, "cost_survival_ratio"] = derived.loc[fill_mask]
    return out


def _diagnose_missing_fields(df: pd.DataFrame) -> list[str]:
    if df.empty:
        return []
    missing = []
    for field in REQUIRED_PROMOTION_FIELDS:
        if field not in df.columns:
            missing.append(field)
        elif df[field].isna().all():
            missing.append(f"{field} (all null)")
    return missing
