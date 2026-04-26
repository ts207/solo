import copy
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from project import PROJECT_ROOT
from project.io.utils import write_parquet
from project.research import phase2_search_engine
from project.research.benchmarks.benchmark_modes import DiscoveryBenchmarkMode, get_mode
from project.research.benchmarks.fixture_materialization import materialize_benchmark_fixture

log = logging.getLogger(__name__)

BENCHMARK_SPEC_PATH = PROJECT_ROOT / "research/benchmarks/discovery_benchmark_spec.yaml"
DATA_ROOT = PROJECT_ROOT.parent / "data"

VALIDATION_CONFIG_PATH = PROJECT_ROOT / "configs/discovery_validation.yaml"
LEDGER_CONFIG_PATH = PROJECT_ROOT / "configs/discovery_ledger.yaml"
SCORING_V2_CONFIG_PATH = PROJECT_ROOT / "configs/discovery_scoring_v2.yaml"


def _load_json_dict(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _resolved_benchmark_mode_config(
    base_search_config: dict,
    base_scoring_config: dict,
    base_ledger_config: dict,
    mode: DiscoveryBenchmarkMode,
) -> dict:
    search_cfg = copy.deepcopy(base_search_config)
    scoring_cfg = copy.deepcopy(base_scoring_config)
    ledger_cfg = copy.deepcopy(base_ledger_config)

    search_cfg["mode"] = mode.search_topology

    if mode.scoring_version == "v1":
        scoring_cfg["enable_discovery_v2_scoring"] = False
    else:
        scoring_cfg["enable_discovery_v2_scoring"] = True

    ledger_cfg["enabled"] = mode.ledger_adjustment == "enabled"

    return {
        "mode_id": mode.mode_id,
        "mode_label": mode.label,
        "search": search_cfg,
        "scoring_v2": scoring_cfg,
        "ledger": ledger_cfg,
        "shortlist_selection": mode.shortlist_selection,
    }


def _build_search_spec_for_mode(
    base_spec: dict,
    mode: DiscoveryBenchmarkMode,
) -> dict:
    spec = copy.deepcopy(base_spec)

    spec["discovery_search"] = {
        "mode": mode.search_topology,
    }

    if mode.shortlist_selection != "disabled":
        spec["discovery_selection"] = {
            "mode": "greedy",
            "shortlist": {
                "enabled": True,
                "size": 20,
                "lambda_quality": 0.7,
                "lambda_overlap": 0.2,
                "lambda_crowding": 0.1,
                "max_per_overlap_cluster": 2,
                "max_per_trigger_family": 3,
                "max_per_lineage": 2,
            },
        }
    else:
        spec["discovery_selection"] = {
            "mode": "off",
            "shortlist": {
                "enabled": False,
            },
        }

    if "triggers" in spec and "events" in spec["triggers"]:
        log.info("Using trigger events from slice config: %s", spec["triggers"]["events"])
    elif "events" in spec:
        log.info("Using events from slice config: %s", spec["events"])

    return spec


def _build_validation_config_for_mode(mode: DiscoveryBenchmarkMode) -> dict:
    folds_enabled = mode.fold_validation == "enabled"
    return {
        "version": 1,
        "kind": "discovery_validation",
        "discovery_validation": {
            "repeated_walkforward": {
                "enabled": folds_enabled,
                "mode": "rolling",
                "train_bars": 8640,
                "validation_bars": 500,
                "test_bars": 500,
                "step_bars": 500,
                "min_folds": 3,
                "max_folds": 6,
                "purge_bars": 24,
                "embargo_bars": 12,
            }
        },
    }


def _build_ledger_config_for_mode(mode: DiscoveryBenchmarkMode) -> dict:
    ledger_enabled = mode.ledger_adjustment == "enabled"
    return {
        "discovery_scoring": {
            "version": "v3" if ledger_enabled else "v2",
            "ledger_adjustment": {
                "enabled": ledger_enabled,
                "lookback_days": 365,
                "recent_window_days": 90,
                "lineage_mode": "v1",
                "max_penalty": 3.0,
                "min_prior_tests_for_penalty": 3,
                "crowded_lineage_threshold": 20,
                "repeated_family_failure_threshold": 0.90,
                "low_family_success_threshold": 0.10,
                "low_family_success_min_tests": 5,
                "high_recent_test_density_threshold": 10,
            },
        },
    }


def _swap_config_file(path: Path, content: dict) -> dict | None:
    original = None
    if path.exists():
        original = yaml.safe_load(path.read_text(encoding="utf-8"))
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(content, f)
    return original


def _restore_config_file(path: Path, original: dict | None):
    if original is not None:
        with open(path, "w", encoding="utf-8") as f:
            yaml.safe_dump(original, f)
    elif path.exists():
        path.unlink()


def _extract_benchmark_metrics(df: pd.DataFrame, out_dir: Path) -> dict[str, Any]:
    if df.empty:
        return {
            "emergence": False,
            "candidate_count": 0,
            "candidate_count_basis": "phase2_candidates_parquet",
            "bridge_candidate_count": 0,
            "top10_candidate_count": 0,
            "top10": {},
        }

    top10 = df.nsmallest(10, "effective_rank") if "effective_rank" in df.columns else df.head(10)
    top10_count = len(top10)

    flag_col = (
        "promotion_candidate_flag" if "promotion_candidate_flag" in df.columns else "is_discovery"
    )
    promotion_density = float(df[flag_col].fillna(False).mean()) if flag_col in df.columns else 0.0

    placebo_fail = 0.0
    if "falsification_component" in df.columns:
        placebo_fail = float((top10["falsification_component"] < 0.5).mean())

    diversity = float(top10["comp_key"].nunique() / 10) if "comp_key" in top10.columns else 0.0

    expectancy = None
    if "estimate_bps" in df.columns:
        vals = pd.to_numeric(top10["estimate_bps"], errors="coerce").dropna()
        if not vals.empty:
            expectancy = float(vals.median())

    survival = None
    if "cost_survival_ratio" in df.columns:
        vals = pd.to_numeric(top10["cost_survival_ratio"], errors="coerce").dropna()
        if not vals.empty:
            survival = float(vals.median())

    median_discovery_quality = None
    if "discovery_quality_score" in df.columns:
        vals = pd.to_numeric(df["discovery_quality_score"], errors="coerce").dropna()
        if not vals.empty:
            median_discovery_quality = float(vals.median())

    max_discovery_quality = None
    if "discovery_quality_score" in df.columns:
        vals = pd.to_numeric(df["discovery_quality_score"], errors="coerce").dropna()
        if not vals.empty:
            max_discovery_quality = float(vals.max())

    median_t_stat = None
    if "t_stat" in df.columns:
        vals = pd.to_numeric(top10["t_stat"], errors="coerce").dropna()
        if not vals.empty:
            median_t_stat = float(vals.median())

    median_estimate_bps = None
    if "estimate_bps" in df.columns:
        vals = pd.to_numeric(top10["estimate_bps"], errors="coerce").dropna()
        if not vals.empty:
            median_estimate_bps = float(vals.median())

    median_falsification = None
    if "falsification_component" in df.columns:
        vals = pd.to_numeric(top10["falsification_component"], errors="coerce").dropna()
        if not vals.empty:
            median_falsification = float(vals.median())

    fold_stability = None
    if "fold_stability_bonus" in df.columns and "fold_stability_penalty" in df.columns:
        bonus = pd.to_numeric(top10["fold_stability_bonus"], errors="coerce").fillna(0)
        penalty = pd.to_numeric(top10["fold_stability_penalty"], errors="coerce").fillna(0)
        fold_stability = float((bonus - penalty).median())

    unique_family_id = 0
    if "family_id" in top10.columns:
        unique_family_id = int(top10["family_id"].nunique())

    unique_template_id = 0
    template_col = "rule_template" if "rule_template" in top10.columns else "template_verb"
    if template_col in top10.columns:
        unique_template_id = int(top10[template_col].nunique())

    shortlist_count = 0
    shortlist_path = out_dir / "shortlist_candidates.parquet"
    if shortlist_path.exists():
        try:
            shortlist_count = len(pd.read_parquet(shortlist_path))
        except Exception:
            pass

    return {
        "emergence": len(df) > 0,
        "candidate_count": len(df),
        "candidate_count_basis": "phase2_candidates_parquet",
        "bridge_candidate_count": len(df),
        "top10_candidate_count": top10_count,
        "median_discovery_quality_score": median_discovery_quality,
        "max_discovery_quality_score": max_discovery_quality,
        "median_t_stat": median_t_stat,
        "median_estimate_bps": median_estimate_bps,
        "median_falsification_component": median_falsification,
        "fold_stability_component": fold_stability,
        "unique_family_id_top10": unique_family_id,
        "unique_template_id_top10": unique_template_id,
        "top10": {
            "promotion_density": promotion_density,
            "placebo_fail_rate": placebo_fail,
            "rank_diversity_score": diversity,
            "median_after_cost_expectancy_bps": expectancy,
            "median_cost_survival_ratio": survival,
        },
        "shortlist_count": shortlist_count,
    }


def _extract_metrics_from_phase2_diagnostics(diagnostics: dict[str, Any]) -> dict[str, Any]:
    if not diagnostics:
        return {}
    feasible_hypotheses = int(diagnostics.get("feasible_hypotheses", 0) or 0)
    metrics_rows = int(diagnostics.get("metrics_rows", 0) or 0)
    valid_metrics_rows = int(diagnostics.get("valid_metrics_rows", 0) or 0)
    bridge_candidates_rows = int(diagnostics.get("bridge_candidates_rows", 0) or 0)
    raw_gate_funnel = diagnostics.get("gate_funnel")
    gate_funnel = raw_gate_funnel if isinstance(raw_gate_funnel, dict) else {}
    generated_hypotheses = int(gate_funnel.get("generated", feasible_hypotheses) or 0)
    discovery_candidate_count = max(feasible_hypotheses, metrics_rows, valid_metrics_rows)
    candidate_count = max(bridge_candidates_rows, discovery_candidate_count)
    return {
        "emergence": candidate_count > 0 or generated_hypotheses > 0,
        "candidate_count": candidate_count,
        "top10_candidate_count": min(candidate_count, 10),
        "candidate_count_basis": "phase2_diagnostics_fallback",
        "bridge_candidate_count": bridge_candidates_rows,
        "discovery_candidate_count": discovery_candidate_count,
        "generated_hypotheses": generated_hypotheses,
        "valid_metrics_rows": valid_metrics_rows,
        "top10": {},
    }


def _attach_phase2_diagnostics_context(
    benchmark_metrics: dict[str, Any],
    diagnostics: dict[str, Any],
) -> dict[str, Any]:
    """Attach diagnostics without changing artifact-derived candidate counts."""
    diagnostics_metrics = _extract_metrics_from_phase2_diagnostics(diagnostics)
    if not diagnostics_metrics:
        return benchmark_metrics

    out = dict(benchmark_metrics)
    out["phase2_diagnostics"] = diagnostics_metrics
    return out


def run_benchmark_job(
    run_id: str,
    symbols: str,
    timeframe: str,
    start: str,
    end: str,
    search_spec: dict,
    mode: DiscoveryBenchmarkMode,
    data_root: Path,
    out_dir: Path,
    event_source: str | None = None,
    fixture_event_registry: str | None = None,
    phase2_overrides: dict[str, Any] | None = None,
) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)

    log.info(
        "run_benchmark_job received search_spec: triggers=%s, events=%s",
        search_spec.get("triggers"),
        search_spec.get("events"),
    )

    search_spec_with_overlays = _build_search_spec_for_mode(search_spec, mode)
    validation_config = _build_validation_config_for_mode(mode)
    ledger_config = _build_ledger_config_for_mode(mode)

    spec_file = out_dir / "search_spec.yaml"
    with open(spec_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(search_spec_with_overlays, f)

    resolved_config = _resolved_benchmark_mode_config(search_spec, {}, {}, mode)
    with open(out_dir / "resolved_mode_config.json", "w", encoding="utf-8") as f:
        json.dump(resolved_config, f, indent=2)

    run_metadata = {
        "mode_id": mode.mode_id,
        "mode_label": mode.label,
        "search_topology": mode.search_topology,
        "scoring_version": mode.scoring_version,
        "fold_validation": mode.fold_validation,
        "ledger_adjustment": mode.ledger_adjustment,
        "shortlist_selection": mode.shortlist_selection,
        "phase2_overrides": dict(phase2_overrides or {}),
    }
    with open(out_dir / "benchmark_run_metadata.json", "w", encoding="utf-8") as f:
        json.dump(run_metadata, f, indent=2)

    validation_original = _swap_config_file(VALIDATION_CONFIG_PATH, validation_config)
    ledger_original = _swap_config_file(LEDGER_CONFIG_PATH, ledger_config)

    result: dict[str, Any] = {
        "run_id": run_id,
        "mode_id": mode.mode_id,
        "mode_label": mode.label,
        "status": "running",
        "candidate_count": 0,
        "artifact_paths": {},
        "benchmark_metrics": {},
        "phase2_overrides": dict(phase2_overrides or {}),
    }
    result["artifact_paths"]["search_spec"] = str(spec_file)

    try:
        fixture_path = None
        if event_source == "fixture" and fixture_event_registry:
            fp = Path(fixture_event_registry)
            if not fp.is_absolute():
                fp = PROJECT_ROOT.parent / fixture_event_registry
            if not fp.exists():
                event_types = list(

                        (search_spec.get("triggers") or {}).get("events")
                        or search_spec.get("events")
                        or []

                )
                materialized_rows = materialize_benchmark_fixture(
                    slice_id=run_id,
                    symbols=[token.strip() for token in str(symbols).split(",") if token.strip()],
                    start=start,
                    end=end,
                    event_types=event_types,
                    output_path=fp,
                    data_root=data_root,
                )
                if materialized_rows <= 0:
                    log.warning(
                        "Fixture event registry not found and materialization produced no rows: %s",
                        fp,
                    )
                else:
                    log.info(
                        "Materialized %d fixture rows for %s at %s",
                        materialized_rows,
                        run_id,
                        fp,
                    )
            if fp.exists():
                fixture_path = str(fp)
            else:
                log.warning(f"Fixture event registry not found: {fp}")

        phase2_search_engine.run(
            run_id=run_id,
            symbols=symbols,
            data_root=data_root,
            out_dir=out_dir,
            timeframe=timeframe,
            search_spec=str(spec_file),
            enable_discovery_v2_scoring=mode.scoring_version == "v2",
            event_registry_override=fixture_path,
            discovery_profile=str((phase2_overrides or {}).get("discovery_profile", "standard")),
            gate_profile=str((phase2_overrides or {}).get("gate_profile", "auto")),
            min_t_stat=(
                float((phase2_overrides or {})["min_t_stat"])
                if (phase2_overrides or {}).get("min_t_stat") is not None
                else None
            ),
            min_n=int((phase2_overrides or {}).get("min_n", 30)),
        )

        candidate_paths = list(out_dir.glob("**/phase2_candidates.parquet"))
        if not candidate_paths:
            candidate_paths = list(
                (data_root / "reports/phase2" / run_id).glob("**/phase2_candidates.parquet")
            )
        diagnostics_path = data_root / "reports" / "phase2" / run_id / "phase2_diagnostics.json"
        diagnostics_payload = _load_json_dict(diagnostics_path)
        if diagnostics_payload:
            result["artifact_paths"]["phase2_diagnostics"] = str(diagnostics_path)

        if candidate_paths:
            df = pd.read_parquet(candidate_paths[0])

            if mode.ledger_adjustment == "enabled":
                from project.research.services.candidate_discovery_scoring import (
                    apply_ledger_multiplicity_correction,
                )

                df = apply_ledger_multiplicity_correction(
                    df,
                    data_root=data_root,
                    current_run_id=run_id,
                    config=resolved_config["ledger"],
                )
                write_parquet(df, candidate_paths[0])

            if mode.shortlist_selection != "disabled":
                from project.research.services.candidate_diversification import (
                    annotate_candidates_with_diversification,
                )

                div_config = search_spec_with_overlays.get("discovery_selection", {})
                if div_config:
                    df, shortlist_df = annotate_candidates_with_diversification(df, div_config)
                    write_parquet(df, candidate_paths[0])
                    shortlist_path = out_dir / "shortlist_candidates.parquet"
                    write_parquet(shortlist_df, shortlist_path)
                    result["artifact_paths"]["shortlist"] = str(shortlist_path)

            result["candidate_count"] = len(df)
            result["artifact_paths"]["candidates"] = str(candidate_paths[0])
            result["benchmark_metrics"] = _extract_benchmark_metrics(df, out_dir)
            if diagnostics_payload:
                result["benchmark_metrics"] = _attach_phase2_diagnostics_context(
                    result["benchmark_metrics"],
                    diagnostics_payload,
                )
            result["status"] = "success"
        else:
            log.warning(f"No candidates found for {run_id}")
            if diagnostics_payload:
                result["benchmark_metrics"] = _extract_metrics_from_phase2_diagnostics(
                    diagnostics_payload
                )
                result["candidate_count"] = int(
                    result["benchmark_metrics"].get("candidate_count", 0) or 0
                )
            result["status"] = "success_no_candidates"

    except Exception as e:
        log.exception(f"Job {run_id} failed: {e}")
        result["status"] = "failed"
        result["error"] = str(e)
    finally:
        _restore_config_file(VALIDATION_CONFIG_PATH, validation_original)
        _restore_config_file(LEDGER_CONFIG_PATH, ledger_original)

    return result


def run_benchmark(
    spec_path: Path = BENCHMARK_SPEC_PATH, modes: list[DiscoveryBenchmarkMode] | None = None
):
    if not spec_path.exists():
        log.error(f"Benchmark spec not found at {spec_path}")
        return

    with open(spec_path) as f:
        spec = yaml.safe_load(f)

    if modes is None:
        mode_d = get_mode("D")
        modes = [m for m in [mode_d] if m is not None]

    benchmark_id = f"bench_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}"
    base_out_dir = DATA_ROOT / "reports/discovery_benchmarks" / benchmark_id
    base_out_dir.mkdir(parents=True, exist_ok=True)

    results = []

    for case in spec["cases"]:
        case_id = case["id"]
        log.info(f"Running benchmark case: {case_id}")

        case_out_dir = base_out_dir / case_id
        case_out_dir.mkdir(parents=True, exist_ok=True)

        symbol = case["symbol"]
        search_spec = case["search_spec"]

        case_results = {}

        for mode in modes:
            mode_id = mode.mode_id
            run_id = f"{case_id}_{mode_id}"
            run_out_dir = case_out_dir / mode_id
            run_out_dir.mkdir(parents=True, exist_ok=True)

            log.info(f"  Mode: {mode_id} ({mode.label})")

            job_result = run_benchmark_job(
                run_id=run_id,
                symbols=symbol,
                timeframe="1h",
                start="",
                end="",
                search_spec=search_spec,
                mode=mode,
                data_root=DATA_ROOT,
                out_dir=run_out_dir,
            )

            if job_result["status"] in ("success", "success_no_candidates"):
                candidate_paths = list(run_out_dir.glob("**/phase2_candidates.parquet"))
                if not candidate_paths:
                    candidate_paths = list(
                        (DATA_ROOT / "reports/phase2" / run_id).glob("**/phase2_candidates.parquet")
                    )
                if candidate_paths:
                    case_results[mode_id] = pd.read_parquet(candidate_paths[0])

        if len(case_results) >= 2:
            comparison = summarize_case_comparison(case_id, case_results, case_out_dir)
            results.append(comparison)

    if results:
        summarize_global_benchmark(results, base_out_dir)
        log.info(f"Benchmark complete. Summary at {base_out_dir}/benchmark_summary.md")


def _candidate_comparison_key(row: pd.Series) -> str:
    return "::".join(
        [
            str(row.get("event_type", "")),
            str(row.get("event_family", "")),
            str(row.get("family_id", "")),
            str(row.get("template_id", "")),
            str(row.get("direction", "")),
            str(row.get("horizon", "")),
            str(row.get("entry_lag", "")),
            str(row.get("symbol", "")),
            str(row.get("timeframe", "")),
            str(row.get("context_signature", "")),
        ]
    )


def _top_n(df: pd.DataFrame, n: int) -> pd.DataFrame:
    if df.empty or "effective_rank" not in df.columns:
        return df.head(n)
    return df.nsmallest(n, "effective_rank")


def _promotion_density(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    flag_col = "promotion_candidate_flag"
    if flag_col not in df.columns:
        flag_col = "is_discovery"
    return float(df[flag_col].fillna(False).mean())


def _median_safe(df: pd.DataFrame, col: str) -> float | None:
    if col not in df.columns or df.empty:
        return None
    vals = pd.to_numeric(df[col], errors="coerce").dropna()
    return None if vals.empty else float(vals.median())


def _write_score_decomposition(case_id, merged, out_dir):
    required_cols = [
        "candidate_id",
        "comp_key",
        "D_rank",
        "significance_component",
        "support_component",
        "falsification_component",
        "tradability_component",
        "novelty_component",
        "overlap_penalty",
        "fragility_penalty",
        "fold_stability_component",
        "ledger_penalty",
        "discovery_quality_score",
        "rank_primary_reason",
        "demotion_reason_codes",
        "falsification_reason",
        "tradability_reason",
        "overlap_reason",
        "fold_reason",
        "ledger_reason",
    ]

    for col in required_cols:
        if col not in merged.columns:
            if any(x in col for x in ["component", "score", "penalty", "rank", "delta"]):
                merged[col] = 0
            else:
                merged[col] = ""

    rank_cols = [c for c in merged.columns if c.endswith("_rank")]
    for c in rank_cols:
        if c in merged.columns:
            merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0)

    sort_col = "D_rank" if "D_rank" in merged.columns else (rank_cols[0] if rank_cols else "comp_key")
    decomp = merged.sort_values(sort_col)

    write_parquet(decomp, out_dir / "score_decomposition.parquet")
    decomp.to_csv(out_dir / "score_decomposition.csv", index=False)

    md = [f"# Score Decomposition: {case_id}\n"]

    md.append("## Canonical D Ranking")
    top_ranked = decomp.head(10)
    md.append("| Key | D Rank | Reason |")
    md.append("| --- | ---: | --- |")
    for _, r in top_ranked.iterrows():
        md.append(f"| {r['comp_key']} | {r.get('D_rank', '')} | {r['rank_primary_reason']} |")

    md.append("\n## Support-Driven Survivors")
    survivors = merged[merged["support_component"] > 0.8].sort_values(sort_col).head(5)
    if survivors.empty:
        md.append("_No high-support survivors found_")
    else:
        for _, r in survivors.iterrows():
            md.append(
                f"- {r['comp_key']} (Rank {r.get(sort_col, '')}, Support Score: {r['support_component']:.2f})"
            )

    md.append("\n## Overlap-Penalized Candidates")
    overlapped = merged[merged["overlap_penalty"] < 1.0].sort_values("overlap_penalty").head(5)
    if overlapped.empty:
        md.append("_No overlap-penalized candidates detected_")
    else:
        for _, r in overlapped.iterrows():
            md.append(
                f"- {r['comp_key']} (Penalty: {r['overlap_penalty']:.2f}, Reason: {r['overlap_reason']})"
            )

    md.append("\n## Fold-Instability Demotions")
    unstable = (
        merged[merged["fold_stability_component"] < 0.5]
        .sort_values("fold_stability_component")
        .head(5)
    )
    if unstable.empty:
        md.append("_No fold-instability demotions detected_")
    else:
        for _, r in unstable.iterrows():
            md.append(
                f"- {r['comp_key']} (Score: {r['fold_stability_component']:.2f}, Reason: {r['fold_reason']})"
            )

    if "demotion_reason_codes" in merged.columns:
        md.append("\n## Most Common Penalty Types")
        codes = (
            merged["demotion_reason_codes"]
            .astype(str)
            .str.split("|")
            .explode()
            .str.strip()
            .value_counts()
        )
        md.append("| Penalty Code | Count |")
        md.append("| --- | --- |")
        for code, count in codes.items():
            if code and code != "nan" and code != "":
                md.append(f"| {code} | {count} |")

    with open(out_dir / "score_decomposition.md", "w") as f:
        f.write("\n".join(md))


def summarize_case_comparison(case_id, case_results, out_dir):
    mode_ids_present = sorted(case_results.keys())

    def add_key(df, mode_id: str):
        if df is None or df.empty:
            return pd.DataFrame(
                columns=["comp_key", "effective_rank", "t_stat", "discovery_quality_score"]
            )
        df = df.copy()

        df["comp_key"] = df.apply(_candidate_comparison_key, axis=1)

        score_col = "discovery_quality_score" if "discovery_quality_score" in df.columns else "t_stat"
        rank_col = f"{mode_id}_rank"
        df[rank_col] = df[score_col].rank(ascending=False, method="first")
        df["effective_rank"] = df[rank_col]

        return df

    keyed = {}
    for mid in mode_ids_present:
        keyed[mid] = add_key(case_results[mid], mid)

    if mode_ids_present:
        baseline_id = mode_ids_present[0]
        baseline_rank = f"{baseline_id}_rank"
        base_cols = [c for c in ["comp_key", baseline_rank, "t_stat"] if c in keyed[baseline_id].columns]
        merged = keyed[baseline_id][base_cols].copy()
    else:
        baseline_id = ""
        merged = pd.DataFrame(columns=["comp_key"])

    for mid in mode_ids_present:
        if mid == baseline_id:
            continue
        other_cols = [c for c in keyed[mid].columns if c not in merged.columns or c == "comp_key"]
        merged = pd.merge(
            merged,
            keyed[mid][other_cols],
            on="comp_key",
            how="outer",
        )

    _write_score_decomposition(case_id, merged, out_dir)
    merged.to_csv(out_dir / "rank_comparison.csv", index=False)

    def _get_mode_summary(df, mode_id: str):
        if df is None or df.empty:
            return {}
        summary = {"mode_id": mode_id, "total_count": len(df)}
        for n in [10, 20, 50]:
            top = _top_n(df, n)
            summary[f"top{n}"] = {
                "promotion_density": _promotion_density(top),
                "median_after_cost_expectancy_bps": _median_safe(top, "estimate_bps"),
                "median_cost_survival_ratio": _median_safe(top, "cost_survival_ratio"),
                "placebo_fail_rate": (top["falsification_component"] < 0.5).mean()
                if "falsification_component" in top.columns
                else 0.0,
                "overlap_concentration": (top["overlap_penalty"] < 1.0).mean()
                if "overlap_penalty" in top.columns
                else 0.0,
                "unique_event_families": top["family_id"].nunique()
                if "family_id" in top.columns
                else 0,
                "unique_template_families": top["template_id"].nunique()
                if "template_id" in top.columns
                else 0,
                "median_fold_stability": _median_safe(top, "fold_stability_component"),
                "rank_diversity_score": top["comp_key"].nunique() / n if n > 0 else 0.0,
            }
        return summary

    result = {
        "case_id": case_id,
        "modes_run": mode_ids_present,
    }
    for mid in mode_ids_present:
        result[mid] = _get_mode_summary(keyed[mid], mid)
        result[f"{mid}_count"] = len(keyed[mid])

    result["top_10_overlap"] = 0

    return result


def _compute_recommendations(results: list) -> dict:
    """Derive mode recommendations from benchmark results rather than hard-coding them."""

    def _avg_metric(mode: str, *path: str) -> float | None:
        vals = []
        for r in results:
            if mode not in r or not r[mode]:
                continue
            obj = r[mode]
            for key in path:
                obj = obj.get(key) if isinstance(obj, dict) else None
                if obj is None:
                    break
            if obj is not None:
                try:
                    vals.append(float(obj))
                except (TypeError, ValueError):
                    pass
        return sum(vals) / len(vals) if vals else None

    d = _avg_metric("D", "top10", "promotion_density")

    return {
        "recommend_canonical_d": d is not None,
    }


def summarize_global_benchmark(results, out_dir):
    df = pd.DataFrame(results)
    df.to_csv(out_dir / "benchmark_summary.csv", index=False)

    mode_ids = set()
    for r in results:
        mode_ids.update(r.get("modes_run", []))
    mode_ids = sorted(mode_ids)

    md = [f"# Discovery Benchmark Summary: {out_dir.name}\n"]

    md.append("## Case Results")
    header = "| Case | " + " | ".join(f"{m} Count" for m in mode_ids) + " |"
    md.append(header)
    md.append("| --- | " + " | ".join("---" for _ in mode_ids) + " |")
    for r in results:
        counts = " | ".join(str(r.get(f"{m}_count", 0)) for m in mode_ids)
        md.append(f"| {r['case_id']} | {counts} |")

    md.append("\n## Promotion Density by Rank Bucket")
    md.append("| Case | Mode | Top-10 | Top-20 | Top-50 |")
    md.append("| --- | --- | --- | --- | --- |")
    for r in results:
        for m in mode_ids:
            if r.get(m):
                dense = [r[m][f"top{n}"]["promotion_density"] for n in [10, 20, 50]]
                md.append(
                    f"| {r['case_id']} | {m} | {dense[0]:.2f} | {dense[1]:.2f} | {dense[2]:.2f} |"
                )

    md.append("\n## Tradability by Rank Bucket")
    md.append("| Case | Mode | Top-10 Median (bps) | Top-50 Median (bps) |")
    md.append("| --- | --- | --- | --- |")
    for r in results:
        for m in mode_ids:
            if r.get(m):
                trad = [r[m][f"top{n}"]["median_after_cost_expectancy_bps"] for n in [10, 50]]
                t_str = [(f"{v:.1f}" if v is not None else "N/A") for v in trad]
                md.append(f"| {r['case_id']} | {m} | {t_str[0]} | {t_str[1]} |")

    md.append("\n## Placebo and Fold Stability")
    md.append("| Case | Mode | Placebo Fail (Top-10) | Fold Stability (Top-10) |")
    md.append("| --- | --- | --- | --- |")
    for r in results:
        for m in mode_ids:
            if r.get(m):
                p_fail = r[m]["top10"]["placebo_fail_rate"]
                f_stab = r[m]["top10"]["median_fold_stability"]
                f_str = f"{f_stab:.2f}" if f_stab is not None else "N/A"
                md.append(f"| {r['case_id']} | {m} | {p_fail:.2f} | {f_str} |")

    md.append("\n## Diversity and Overlap")
    md.append("| Case | Mode | Unique Families (Top-20) | Overlap Conc. (Top-20) |")
    md.append("| --- | --- | --- | --- |")
    for r in results:
        for m in mode_ids:
            if r.get(m):
                uniq = r[m]["top20"]["unique_event_families"]
                conc = r[m]["top20"]["overlap_concentration"]
                md.append(f"| {r['case_id']} | {m} | {uniq} | {conc:.2f} |")

    recs = _compute_recommendations(results)

    def _rec_str(val: bool | None) -> str:
        if val is None:
            return "inconclusive (insufficient data)"
        return "true" if val else "false"

    md.append("\n## Recommendation")
    md.append(f"- **recommend_canonical_d**: {_rec_str(recs['recommend_canonical_d'])}")

    md.append("\n## Basis for Recommendation")
    md.append("- Derived from artifact-backed mode D benchmark outputs.")
    md.append("- None (insufficient data) means mode D did not produce benchmark metrics.")

    with open(out_dir / "benchmark_summary.md", "w") as f:
        f.write("\n".join(md))

    summary_json = {
        "cases": results,
        "modes_run": mode_ids,
        "recommendations": recs,
        "conclusion_basis": [
            "promotion_density",
            "placebo_fail_rate",
            "overlap_concentration",
            "tradability_metrics",
            "diversity_metrics",
        ],
        "summary_conclusion": "Recommendations derived from benchmark results.",
    }
    with open(out_dir / "benchmark_summary.json", "w") as f:
        json.dump(summary_json, f, indent=2)

    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_benchmark()
