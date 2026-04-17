#!/usr/bin/env python3
"""
Edge Research Platform — Backend
Run: python dashboard/server.py [port]
Default port: 7477
"""

from __future__ import annotations

import json
import pathlib
import sys
import subprocess
import threading
import uuid
import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

try:
    import yaml

    HAS_YAML = True
except ImportError:
    HAS_YAML = False

ROOT = pathlib.Path(__file__).parent.parent
REPORTS = ROOT / "data" / "reports"
LAKE_DIR = ROOT / "data" / "lake"
SPEC_EVENTS = ROOT / "spec" / "events"
SPEC_PROPOSALS = ROOT / "spec" / "proposals"
SPEC_TEMPLATES = ROOT / "spec" / "templates"
SPEC_DOMAIN = ROOT / "spec" / "domain" / "domain_graph.yaml"
LIVE_THESES = ROOT / "data" / "live" / "theses"
LIVE_PERSIST = ROOT / "live" / "persist"
FEATURES_YAML = ROOT / "project" / "configs" / "registries" / "features.yaml"
ARTIFACTS_DIR = ROOT / "data" / "artifacts" / "experiments"
STATIC = pathlib.Path(__file__).parent
JOBS_DIR = STATIC / ".jobs"

# ─── Job runner ──────────────────────────────────────────────────────────────

JOBS: dict[str, dict] = {}  # job_id → job record


def start_job(cmd: list[str], label: str) -> dict:
    """Launch a subprocess, capture stdout+stderr to a log file, return job record."""
    job_id = uuid.uuid4().hex[:8]
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = JOBS_DIR / f"{job_id}.log"
    started_at = datetime.datetime.now().isoformat()
    job: dict = {
        "id": job_id,
        "label": label,
        "cmd": " ".join(cmd),
        "status": "running",
        "started_at": started_at,
        "finished_at": None,
        "exit_code": None,
        "log_path": str(log_path),
    }
    JOBS[job_id] = job

    def _run():
        with log_path.open("w") as fh:
            fh.write(
                f"# Edge Job: {label}\n# Command: {' '.join(cmd)}\n"
                f"# Started: {started_at}\n# CWD: {ROOT}\n\n"
            )
            fh.flush()
            try:
                proc = subprocess.Popen(
                    cmd,
                    stdout=fh,
                    stderr=subprocess.STDOUT,
                    cwd=str(ROOT),
                    text=True,
                )
                job["pid"] = proc.pid
                exit_code = proc.wait()
                job["exit_code"] = exit_code
                job["status"] = "done" if exit_code == 0 else "failed"
            except Exception as exc:
                fh.write(f"\n# ERROR: {exc}\n")
                job["status"] = "failed"
            finally:
                job["finished_at"] = datetime.datetime.now().isoformat()

    threading.Thread(target=_run, daemon=True).start()
    return job


# ─── JSON/YAML helpers ────────────────────────────────────────────────────────


def _load_json(p: pathlib.Path) -> dict | list:
    try:
        return json.loads(p.read_text())
    except Exception:
        return {}


def _load_yaml(p: pathlib.Path) -> dict:
    if not HAS_YAML:
        return {}
    try:
        return yaml.safe_load(p.read_text()) or {}
    except Exception:
        return {}


# ─── Loaders ─────────────────────────────────────────────────────────────────


def load_signals() -> list[dict]:
    signals = []
    for edge_file in REPORTS.rglob("*_edge_summary.json"):
        parts = edge_file.parts
        if len(parts) < 3:
            continue
        try:
            category = parts[-3]
            run = parts[-2]
            event = edge_file.stem.replace("_edge_summary", "")

            edge = _load_json(edge_file)
            stab = _load_json(edge_file.parent / edge_file.name.replace("edge", "stability"))
            morph = _load_json(edge_file.parent / edge_file.name.replace("edge", "morphology"))
            integ = _load_json(edge_file.parent / edge_file.name.replace("edge", "integrity"))

            by_sym = edge.get("by_symbol", {})
            best_bps = (
                max((v.get("best_net_mean_bps", 0) for v in by_sym.values()), default=0)
                if by_sym
                else 0
            )
            best_sym = (
                max(by_sym, key=lambda k: by_sym[k].get("best_net_mean_bps", 0)) if by_sym else None
            )

            stab_by_sym = stab.get("by_symbol", {})
            morph_by_sym = morph.get("by_symbol", {})
            integ_by_sym = integ.get("by_symbol", {})

            avg_sign = (
                sum(v.get("sign_consistency", 0) for v in stab_by_sym.values()) / len(stab_by_sym)
                if stab_by_sym
                else 0
            )
            avg_post = (
                sum(v.get("post_event_return_bps", 0) for v in morph_by_sym.values())
                / len(morph_by_sym)
                if morph_by_sym
                else 0
            )
            avg_pre = (
                sum(v.get("pre_event_drift_bps", 0) for v in morph_by_sym.values())
                / len(morph_by_sym)
                if morph_by_sym
                else 0
            )
            avg_move = (
                sum(v.get("event_bar_move_bps", 0) for v in morph_by_sym.values())
                / len(morph_by_sym)
                if morph_by_sym
                else 0
            )

            all_months: dict[str, int] = {}
            for sv in integ_by_sym.values():
                for month, cnt in sv.get("events_per_month", {}).items():
                    all_months[month] = all_months.get(month, 0) + cnt

            sym_detail: dict[str, dict] = {}
            for sym, sv in by_sym.items():
                sym_detail[sym] = {
                    "n_events": sv.get("n_events", 0),
                    "best_bps": round(sv.get("best_net_mean_bps", 0), 2),
                    "best_horizon_bars": sv.get("best_horizon_bars"),
                    "sign_consistency": round(
                        stab_by_sym.get(sym, {}).get("sign_consistency", 0), 3
                    ),
                    "overall_mean_bps": round(
                        stab_by_sym.get(sym, {}).get("overall_mean_bps", 0), 2
                    ),
                    "post_return_bps": round(
                        morph_by_sym.get(sym, {}).get("post_event_return_bps", 0), 2
                    ),
                    "pre_drift_bps": round(
                        morph_by_sym.get(sym, {}).get("pre_event_drift_bps", 0), 2
                    ),
                    "event_bar_move_bps": round(
                        morph_by_sym.get(sym, {}).get("event_bar_move_bps", 0), 2
                    ),
                    "cluster_rate": round(integ_by_sym.get(sym, {}).get("cluster_rate", 0), 4),
                    "intensity_mean": round(morph_by_sym.get(sym, {}).get("intensity_mean", 0), 3),
                    "intensity_p90": round(morph_by_sym.get(sym, {}).get("intensity_p90", 0), 3),
                }

            signals.append(
                {
                    "id": f"{category}/{run}/{event}",
                    "category": category,
                    "run": run,
                    "event": event,
                    "n_events": edge.get("n_events", 0),
                    "n_symbols": edge.get("n_symbols", 0),
                    "best_bps": round(best_bps, 2),
                    "best_sym": best_sym,
                    "sign_consistency": round(avg_sign, 3),
                    "post_return_bps": round(avg_post, 2),
                    "pre_drift_bps": round(avg_pre, 2),
                    "event_bar_move_bps": round(avg_move, 2),
                    "by_symbol": sym_detail,
                    "events_per_month": all_months,
                }
            )
        except Exception:
            continue

    signals.sort(key=lambda x: x["best_bps"], reverse=True)
    return signals


def load_runs() -> list[dict]:
    runs = []
    phase2_dir = REPORTS / "phase2"
    if not phase2_dir.exists():
        return runs
    for diag_file in sorted(phase2_dir.glob("*/phase2_diagnostics.json")):
        try:
            run_id = diag_file.parent.name
            diag = _load_json(diag_file)
            burden = _load_json(diag_file.parent / "search_burden_summary.json")
            quality = _load_json(diag_file.parent / "discovery_quality_summary.json")
            funnel = diag.get("gate_funnel", {})
            runs.append(
                {
                    "run_id": run_id,
                    "symbols": diag.get("symbols_requested", []),
                    "timeframe": diag.get("timeframe", "5m"),
                    "hypotheses_generated": diag.get("hypotheses_generated", 0),
                    "feasible_hypotheses": diag.get("feasible_hypotheses", 0),
                    "metrics_emitted": funnel.get("metrics_emitted", 0),
                    "pass_min_sample": funnel.get("pass_min_sample_size", 0),
                    "phase2_candidates_written": funnel.get("phase2_candidates_written", 0),
                    "phase2_final": funnel.get("phase2_final", 0),
                    "multiplicity_discoveries": diag.get("multiplicity_discoveries", 0),
                    "rejected_by_min_t": diag.get("rejected_by_min_t_stat", 0),
                    "rejected_invalid": diag.get("rejected_invalid_metrics", 0),
                    "rejection_reasons": diag.get("rejection_reason_counts", {}),
                    "min_t_stat": diag.get("min_t_stat", 2.0),
                    "event_families": quality.get("event_families", []),
                    "gate_pass_rate": round(quality.get("gate_pass_rate", 0), 4),
                    "search_parameterizations": burden.get("search_parameterizations_attempted", 0),
                    "search_eligible": burden.get("search_candidates_eligible", 0),
                }
            )
        except Exception:
            continue
    return runs


def load_event_families(signals: list[dict]) -> list[dict]:
    best: dict[str, dict] = {}
    for s in signals:
        key = f"{s['category']}::{s['event']}"
        if key not in best or s["best_bps"] > best[key]["best_bps"]:
            best[key] = {**s, "key": key, "run_count": 0}
        best[key]["run_count"] = best[key].get("run_count", 0) + 1
    return sorted(best.values(), key=lambda x: x["best_bps"], reverse=True)


def load_event_specs() -> list[dict]:
    specs = []
    if not SPEC_EVENTS.exists():
        return specs
    for f in sorted(SPEC_EVENTS.glob("*.yaml")):
        d = _load_yaml(f)
        if not d or "event_type" not in d:
            continue
        g = d.get("governance", {})
        r = d.get("runtime", {})
        i = d.get("identity", {})
        specs.append(
            {
                "name": f.stem,
                "event_type": d.get("event_type", f.stem),
                "phase": i.get("phase", ""),
                "canonical_regime": i.get("canonical_regime", ""),
                "tier": g.get("tier", ""),
                "maturity": g.get("maturity", ""),
                "executable": g.get("default_executable", True),
                "research_only": g.get("research_only", False),
                "enabled": r.get("enabled", True),
                "detector": r.get("detector", ""),
                "operational_role": g.get("operational_role", ""),
                "runtime_category": g.get("runtime_category", ""),
                "deployment_disposition": g.get("deployment_disposition", ""),
                "instrument_classes": r.get("instrument_classes", []),
                "runtime_tags": r.get("runtime_tags", []),
            }
        )
    return specs


def load_proposals() -> list[dict]:
    proposals = []
    if not SPEC_PROPOSALS.exists():
        return proposals
    for f in sorted(SPEC_PROPOSALS.glob("*.yaml")):
        d = _load_yaml(f)
        proposals.append(
            {
                "filename": f.name,
                "path": str(f.relative_to(ROOT)),
                "content": d,
                "raw": f.read_text(),
            }
        )
    return proposals


def load_filter_templates() -> list[dict]:
    templates = []
    if not SPEC_TEMPLATES.exists():
        return templates
    for f in SPEC_TEMPLATES.glob("*.yaml"):
        d = _load_yaml(f)
        for k, v in d.items():
            if isinstance(v, dict):
                templates.append({"name": k, "source": f.name, **v})
    return templates


def load_theses() -> list[dict]:
    theses = []
    if not LIVE_THESES.exists():
        return theses
    for f in LIVE_THESES.rglob("promoted_theses.json"):
        try:
            data = _load_json(f)
            batch = data if isinstance(data, list) else data.get("theses", [])
            run_id = f.parent.name
            for t in batch:
                t["_run_id"] = run_id
                theses.append(t)
        except Exception:
            continue
    return theses


def load_lake() -> dict:
    if not LAKE_DIR.exists():
        return {"run_caches": [], "cleaned_symbols": [], "feature_symbols": []}

    run_caches = []
    runs_dir = LAKE_DIR / "runs"
    if runs_dir.exists():
        for d in sorted(runs_dir.iterdir()):
            if d.is_dir():
                n_files = sum(1 for _ in d.rglob("*.parquet"))
                size_mb = sum(f.stat().st_size for f in d.rglob("*") if f.is_file()) / 1e6
                run_caches.append(
                    {
                        "name": d.name,
                        "parquet_files": n_files,
                        "size_mb": round(size_mb, 1),
                    }
                )

    cleaned_syms = []
    cleaned_dir = LAKE_DIR / "cleaned" / "perp"
    if cleaned_dir.exists():
        for sym_dir in sorted(cleaned_dir.iterdir()):
            if sym_dir.is_dir():
                bars = list(sym_dir.rglob("*.parquet"))
                years = sorted(
                    {p.parent.parent.name.replace("year=", "") for p in bars if "year=" in str(p)}
                )
                cleaned_syms.append(
                    {"symbol": sym_dir.name, "bars_files": len(bars), "years": years}
                )

    feature_syms = []
    feat_dir = LAKE_DIR / "features" / "perp"
    if feat_dir.exists():
        for sym_dir in sorted(feat_dir.iterdir()):
            if sym_dir.is_dir():
                feature_syms.append(
                    {
                        "symbol": sym_dir.name,
                        "feature_types": [d.name for d in sym_dir.iterdir() if d.is_dir()],
                    }
                )

    return {
        "run_caches": run_caches,
        "cleaned_symbols": cleaned_syms,
        "feature_symbols": feature_syms,
    }


def load_templates() -> dict:
    """Load spec/templates/registry.yaml — families, expression/filter templates, param grids."""
    d = _load_yaml(SPEC_TEMPLATES / "registry.yaml")
    if not d:
        return {"families": [], "defaults": {}}

    defaults = d.get("defaults", {})
    families: list[dict] = []

    for key, val in d.items():
        if key in ("version", "kind", "metadata", "defaults") or not isinstance(val, dict):
            continue
        templates_list: list[dict] = []
        for tname, tval in val.items():
            if isinstance(tval, dict):
                templates_list.append({"name": tname, **tval})
        families.append(
            {
                "name": key,
                "templates": templates_list,
            }
        )

    return {"families": families, "defaults": defaults}


def load_features() -> list[dict]:
    """Load project/configs/registries/features.yaml."""
    d = _load_yaml(FEATURES_YAML)
    features = []
    for name, props in d.get("features", {}).items():
        if isinstance(props, dict):
            features.append({"name": name, **props})
    return features


def load_live_state() -> dict:
    """Load live/persist/ files."""
    recon = _load_json(LIVE_PERSIST / "thesis_reconciliation.json") if LIVE_PERSIST.exists() else {}
    batch = _load_json(LIVE_PERSIST / "thesis_batch_metadata.json") if LIVE_PERSIST.exists() else {}
    memories: list[dict] = []
    if ARTIFACTS_DIR.exists():
        for campaign_dir in sorted(ARTIFACTS_DIR.iterdir()):
            if campaign_dir.is_dir():
                mem_dir = campaign_dir / "memory"
                belief = _load_json(mem_dir / "belief_state.json") if mem_dir.exists() else {}
                actions = _load_json(mem_dir / "next_actions.json") if mem_dir.exists() else {}
                memories.append(
                    {
                        "campaign": campaign_dir.name,
                        "belief": belief,
                        "next_actions": actions,
                    }
                )
    return {
        "reconciliation": recon,
        "batch_metadata": batch,
        "campaign_memories": memories,
    }


def load_domain_graph_summary() -> dict:
    """Load spec/domain/domain_graph.yaml and return a lightweight summary."""
    d = _load_yaml(SPEC_DOMAIN)
    if not d:
        return {"events": [], "families": [], "regimes": []}

    events: list[dict] = []
    raw_events = d.get("events", d.get("event_nodes", {}))
    if isinstance(raw_events, dict):
        for ename, edata in raw_events.items():
            if isinstance(edata, dict):
                events.append(
                    {
                        "name": ename,
                        "research_family": edata.get("research_family", ""),
                        "canonical_regime": edata.get("canonical_regime", ""),
                        "detector_name": edata.get("detector_name", ""),
                        "tier": edata.get("tier", ""),
                        "templates": edata.get("templates", []),
                        "horizons": edata.get("horizons", []),
                        "phase": edata.get("phase", ""),
                    }
                )
    elif isinstance(raw_events, list):
        for edata in raw_events:
            if isinstance(edata, dict):
                name = edata.get("event_type", edata.get("name", ""))
                events.append(
                    {
                        "name": name,
                        "research_family": edata.get("research_family", ""),
                        "canonical_regime": edata.get("canonical_regime", ""),
                        "detector_name": edata.get("detector_name", ""),
                        "tier": edata.get("tier", ""),
                        "templates": edata.get("templates", []),
                        "horizons": edata.get("horizons", []),
                        "phase": edata.get("phase", ""),
                    }
                )

    # Collect unique families and regimes
    families = sorted({e["research_family"] for e in events if e["research_family"]})
    regimes = sorted({e["canonical_regime"] for e in events if e["canonical_regime"]})

    return {"events": events, "families": families, "regimes": regimes}


def _rel(path: pathlib.Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except Exception:
        return str(path)


def _file_list(paths) -> list[pathlib.Path]:
    return [p for p in paths if p.is_file() and not p.name.endswith(":Zone.Identifier")]


def _recent_relative(paths, limit: int = 5) -> list[str]:
    items = []
    for p in paths:
        try:
            items.append((p.stat().st_mtime, _rel(p)))
        except FileNotFoundError:
            continue
    items.sort(reverse=True)
    return [rel for _, rel in items[:limit]]


def _summarize_section(
    *,
    section_id: str,
    label: str,
    base_path: pathlib.Path,
    run_dirs: list[pathlib.Path],
    files: list[pathlib.Path],
    notes: str = "",
    extra: dict | None = None,
) -> dict:
    section = {
        "id": section_id,
        "label": label,
        "path": _rel(base_path),
        "run_count": len(run_dirs),
        "file_count": len(files),
        "sample_runs": [d.name for d in run_dirs[:6]],
        "sample_files": _recent_relative(files),
        "notes": notes,
    }
    if extra:
        section.update(extra)
    return section


def _artifact_section_catalog() -> list[dict]:
    sections: list[dict] = []

    data_quality = REPORTS / "data_quality"
    dq_runs = (
        sorted(d for d in data_quality.iterdir() if d.is_dir() and (d / "validation").exists())
        if data_quality.exists()
        else []
    )
    dq_files = _file_list(data_quality.rglob("validation/*.json")) if data_quality.exists() else []
    sections.append(
        _summarize_section(
            section_id="data_quality",
            label="Data quality validation",
            base_path=data_quality,
            run_dirs=dq_runs,
            files=dq_files,
            notes="Coverage and integrity validators emitted under per-run validation folders.",
        )
    )

    feature_quality = REPORTS / "feature_quality"
    fq_runs = (
        sorted(d for d in feature_quality.iterdir() if d.is_dir() and (d / "validation").exists())
        if feature_quality.exists()
        else []
    )
    fq_files = _file_list(feature_quality.rglob("validation/*")) if feature_quality.exists() else []
    sections.append(
        _summarize_section(
            section_id="feature_quality",
            label="Feature quality validation",
            base_path=feature_quality,
            run_dirs=fq_runs,
            files=fq_files,
            notes="Feature-level validation and per-symbol quality artifacts.",
        )
    )

    context_quality = REPORTS / "context_quality"
    cq_runs = (
        sorted(d for d in context_quality.iterdir() if d.is_dir())
        if context_quality.exists()
        else []
    )
    cq_files = _file_list(context_quality.rglob("*")) if context_quality.exists() else []
    sections.append(
        _summarize_section(
            section_id="context_quality",
            label="Context quality reports",
            base_path=context_quality,
            run_dirs=cq_runs,
            files=cq_files,
            notes="Context scoring and regime-conditioning support outputs.",
        )
    )

    phase2 = REPORTS / "phase2"
    p2_runs = (
        sorted(
            d for d in phase2.iterdir() if d.is_dir() and (d / "phase2_candidates.parquet").exists()
        )
        if phase2.exists()
        else []
    )
    p2_files = _file_list(p for d in p2_runs for p in d.glob("phase2_*"))
    sections.append(
        _summarize_section(
            section_id="phase2",
            label="Phase-2 candidates",
            base_path=phase2,
            run_dirs=p2_runs,
            files=p2_files,
            notes="Candidate tables, overlap metrics, fold metrics, and regime-conditionals.",
        )
    )

    edge_candidates = REPORTS / "edge_candidates"
    ec_runs = (
        sorted(
            d
            for d in edge_candidates.iterdir()
            if d.is_dir() and (d / "edge_candidates_normalized.json").exists()
        )
        if edge_candidates.exists()
        else []
    )
    ec_files = (
        _file_list(edge_candidates.rglob("edge_candidates_normalized.*"))
        if edge_candidates.exists()
        else []
    )
    sections.append(
        _summarize_section(
            section_id="edge_candidates",
            label="Normalized edge candidates",
            base_path=edge_candidates,
            run_dirs=ec_runs,
            files=ec_files,
            notes="Pre-phase-2 normalized candidate exports in JSON and parquet.",
        )
    )

    strategy_builder = REPORTS / "strategy_builder"
    sb_runs = (
        sorted(
            d
            for d in strategy_builder.iterdir()
            if d.is_dir() and (d / "strategy_candidates.json").exists()
        )
        if strategy_builder.exists()
        else []
    )
    sb_files = (
        _file_list(strategy_builder.rglob("strategy_candidates.*"))
        if strategy_builder.exists()
        else []
    )
    nonempty_strategy_json = 0
    for run_dir in sb_runs:
        data = _load_json(run_dir / "strategy_candidates.json")
        if isinstance(data, list) and data:
            nonempty_strategy_json += 1
    sections.append(
        _summarize_section(
            section_id="strategy_builder",
            label="Strategy builder outputs",
            base_path=strategy_builder,
            run_dirs=sb_runs,
            files=sb_files,
            notes="JSON companions are mostly empty; parquet and CSV outputs exist for each run.",
            extra={"nonempty_json_runs": nonempty_strategy_json},
        )
    )

    live_theses_runs = (
        sorted(
            d for d in LIVE_THESES.iterdir() if d.is_dir() and (d / "promoted_theses.json").exists()
        )
        if LIVE_THESES.exists()
        else []
    )
    live_theses_files = (
        _file_list(LIVE_THESES.rglob("promoted_theses.json")) if LIVE_THESES.exists() else []
    )
    sections.append(
        _summarize_section(
            section_id="live_theses",
            label="Exported theses",
            base_path=LIVE_THESES,
            run_dirs=live_theses_runs,
            files=live_theses_files,
            notes="Promotion export handoff into runtime thesis inventory.",
        )
    )

    runtime_files = _file_list(LIVE_PERSIST.rglob("*")) if LIVE_PERSIST.exists() else []
    sections.append(
        _summarize_section(
            section_id="runtime_persist",
            label="Runtime persist state",
            base_path=LIVE_PERSIST,
            run_dirs=[],
            files=runtime_files,
            notes="Thin runtime reconciliation and thesis-batch metadata.",
        )
    )

    trigger_dir = ROOT / "data" / "trigger_proposals"
    trigger_files = _file_list(trigger_dir.rglob("*")) if trigger_dir.exists() else []
    sections.append(
        _summarize_section(
            section_id="trigger_proposals",
            label="Trigger proposals",
            base_path=trigger_dir,
            run_dirs=[],
            files=trigger_files,
            notes="Advanced trigger-discovery proposal outputs. Empty when the lane has not been exercised.",
        )
    )

    memory_runs = (
        sorted(d for d in ARTIFACTS_DIR.iterdir() if d.is_dir() and (d / "memory").exists())
        if ARTIFACTS_DIR.exists()
        else []
    )
    memory_files = _file_list(ARTIFACTS_DIR.rglob("memory/*")) if ARTIFACTS_DIR.exists() else []
    sections.append(
        _summarize_section(
            section_id="campaign_memory",
            label="Campaign memory",
            base_path=ARTIFACTS_DIR,
            run_dirs=memory_runs,
            files=memory_files,
            notes="Belief state, next actions, reflections, evidence ledgers, and context statistics.",
        )
    )

    return sections


def load_artifact_inventory() -> dict:
    sections = _artifact_section_catalog()

    populated = [section for section in sections if section["file_count"] > 0]
    return {
        "sections": sections,
        "total_sections": len(sections),
        "populated_sections": len(populated),
        "total_files": sum(section["file_count"] for section in sections),
    }


def _safe_json_rows(rows: list[dict], limit: int = 3) -> list[dict]:
    clean = []
    for row in rows[:limit]:
        clean.append({k: str(v)[:160] for k, v in row.items()})
    return clean


def _preview_file(path_str: str) -> dict:
    path = ROOT / path_str
    preview = {"path": path_str, "kind": path.suffix.lstrip("."), "exists": path.exists()}
    if not path.exists() or path.name.endswith(":Zone.Identifier"):
        preview["error"] = "missing"
        return preview

    try:
        if path.suffix == ".json":
            data = json.loads(path.read_text())
            if isinstance(data, dict):
                preview["shape"] = "dict"
                preview["keys"] = list(data)[:20]
                preview["sample"] = {k: str(data[k])[:160] for k in list(data)[:8]}
            elif isinstance(data, list):
                preview["shape"] = "list"
                preview["rows"] = len(data)
                if data and isinstance(data[0], dict):
                    preview["columns"] = list(data[0])[:20]
                    preview["sample_rows"] = _safe_json_rows(data)
            else:
                preview["shape"] = type(data).__name__
        elif path.suffix in {".yaml", ".yml"} and HAS_YAML:
            data = yaml.safe_load(path.read_text()) or {}
            preview["shape"] = "dict" if isinstance(data, dict) else type(data).__name__
            if isinstance(data, dict):
                preview["keys"] = list(data)[:20]
        elif path.suffix == ".csv":
            import pandas as pd

            df = pd.read_csv(path, nrows=3)
            preview["shape"] = "table"
            preview["columns"] = list(df.columns)[:20]
            preview["sample_rows"] = _safe_json_rows(df.fillna("").to_dict(orient="records"))
        elif path.suffix == ".parquet":
            import pandas as pd

            df = pd.read_parquet(path)
            preview["shape"] = "table"
            preview["rows"] = len(df)
            preview["columns"] = list(df.columns)[:20]
            preview["sample_rows"] = _safe_json_rows(
                df.head(3).fillna("").to_dict(orient="records")
            )
        else:
            preview["shape"] = "file"
            preview["size_bytes"] = path.stat().st_size
    except Exception as exc:
        preview["error"] = str(exc)
    return preview


def load_artifact_detail(section_id: str) -> dict:
    sections = {section["id"]: section for section in _artifact_section_catalog()}
    section = sections.get(section_id)
    if not section:
        return {"error": f"unknown section: {section_id}"}

    sample_files = section.get("sample_files", [])
    previews = [_preview_file(path_str) for path_str in sample_files[:3]]
    return {
        "section": section,
        "previews": previews,
    }


def load_git_activity(limit: int = 30) -> list[dict]:
    try:
        raw = subprocess.check_output(
            [
                "git",
                "log",
                f"-{limit}",
                "--date=short",
                "--pretty=format:%h%x09%ad%x09%s",
            ],
            cwd=str(ROOT),
            text=True,
        )
    except Exception:
        return []

    commits = []
    for line in raw.splitlines():
        parts = line.split("\t", 2)
        if len(parts) != 3:
            continue
        commit_hash, date, subject = parts
        commits.append({"hash": commit_hash, "date": date, "subject": subject})
    return commits


def load_campaigns() -> list[dict]:
    if not ARTIFACTS_DIR.exists():
        return []
    campaigns = []
    for prog_dir in sorted(ARTIFACTS_DIR.iterdir()):
        if not prog_dir.is_dir():
            continue
        summary_path = prog_dir / "campaign_summary.json"
        rollup_path = prog_dir / "campaign_memory_rollup.json"
        belief_path = prog_dir / "memory" / "belief_state.json"
        if not summary_path.exists() and not rollup_path.exists():
            continue
        summary = {}
        if summary_path.exists():
            try:
                summary = json.loads(summary_path.read_text())
            except (json.JSONDecodeError, OSError):
                pass
        rollup = {}
        if rollup_path.exists():
            try:
                rollup = json.loads(rollup_path.read_text())
            except (json.JSONDecodeError, OSError):
                pass
        belief = {}
        if belief_path.exists():
            try:
                belief = json.loads(belief_path.read_text())
            except (json.JSONDecodeError, OSError):
                pass
        metrics = summary.get("metrics", {})
        totals = rollup.get("totals", {})
        top_events = rollup.get("top_events", [])
        promo_rate = metrics.get("promotion_rate", 0)
        evaluated = metrics.get("total_runs", totals.get("tested_region_rows", 0))
        focus = belief.get("current_focus", "unknown")
        promising = belief.get("promising_regions", [])
        decision = (
            "ONGOING"
            if promo_rate == 0 and evaluated > 0
            else ("PROMOTED" if promo_rate > 0 else "STOPPED")
        )
        top_ev = top_events[0] if top_events else {}
        stats = []
        if top_ev:
            if top_ev.get("avg_q_value") is not None:
                stats.append({"label": "Best q-value", "value": f"{top_ev['avg_q_value']:.4f}"})
            if top_ev.get("avg_after_cost_expectancy") is not None:
                stats.append(
                    {
                        "label": "Best expectancy",
                        "value": f"{top_ev['avg_after_cost_expectancy']:.6f}",
                    }
                )
            if top_ev.get("dominant_fail_gate"):
                stats.append({"label": "Dominant fail gate", "value": top_ev["dominant_fail_gate"]})
        stats.append({"label": "Evaluated runs", "value": str(evaluated)})
        stats.append({"label": "Promotion rate", "value": f"{promo_rate:.0%}"})
        stats.append({"label": "Focus", "value": focus})
        promising_desc = (
            ", ".join(
                p.get("event_type", "?")
                + "/"
                + p.get("direction", "?")
                + "/"
                + p.get("horizon", "?")
                for p in promising[:3]
            )
            if promising
            else "none"
        )
        stats.append({"label": "Promising regions", "value": promising_desc})
        campaigns.append(
            {
                "id": prog_dir.name,
                "name": prog_dir.name.upper()
                .replace("-", "_")
                .replace("_", " ", 1)
                .replace("_", "-", 1),
                "status": "active"
                if decision == "ONGOING"
                else ("promoted" if decision == "PROMOTED" else "stopped"),
                "decision": decision,
                "reason": f"promotion_rate={promo_rate:.0%}, focus={focus}, promising_regions={len(promising)}",
                "program_id": summary.get("program_id", rollup.get("program_id", prog_dir.name)),
                "latest_run_id": rollup.get("latest_run_id", ""),
                "stats": stats,
                "watchlist": belief.get("open_repairs", []) or "none",
            }
        )
    return campaigns


def load_overview(signals, runs, events) -> dict:
    total_hyp = sum(r.get("hypotheses_generated", 0) for r in runs)
    total_cands = sum(r.get("phase2_candidates_written", 0) for r in runs)
    total_met = sum(r.get("metrics_emitted", 0) for r in runs)
    best = signals[0] if signals else {}
    cat_counts: dict[str, int] = {}
    for s in signals:
        cat_counts[s["category"]] = cat_counts.get(s["category"], 0) + 1
    return {
        "total_runs": len(runs),
        "total_signals": len(signals),
        "total_hypotheses": total_hyp,
        "total_events_tested": total_met,
        "total_candidates_written": total_cands,
        "total_event_families": len(events),
        "best_signal_bps": best.get("best_bps", 0),
        "best_signal_event": best.get("event", ""),
        "best_signal_run": best.get("run", ""),
        "campaigns_stopped": 2,
        "campaigns_active": 1,
        "funnel": {
            "hypotheses": total_hyp,
            "feasible": sum(r.get("feasible_hypotheses", 0) for r in runs),
            "metrics_emitted": total_met,
            "min_sample_pass": sum(r.get("pass_min_sample", 0) for r in runs),
            "candidates_written": total_cands,
        },
        "category_counts": cat_counts,
        "top_signals": signals[:10],
    }


def save_proposal(filename: str, content: str) -> dict:
    SPEC_PROPOSALS.mkdir(parents=True, exist_ok=True)
    if not filename.endswith(".yaml"):
        filename += ".yaml"
    safe = "".join(c if c.isalnum() or c in "_-." else "_" for c in filename)
    target = SPEC_PROPOSALS / safe
    target.write_text(content)
    return {"ok": True, "path": str(target.relative_to(ROOT)), "filename": safe}


# ─── HTTP Handler ─────────────────────────────────────────────────────────────


class Handler(BaseHTTPRequestHandler):
    _data: dict = {}

    def log_message(self, fmt, *args):
        print(f"  {self.address_string()} {fmt % args}")

    def send_json(self, data, status: int = 200):
        body = json.dumps(data, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def send_file(self, path: pathlib.Path, ctype: str):
        try:
            body = path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except FileNotFoundError:
            self.send_response(404)
            self.end_headers()

    def read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length) if length else b""

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)
        d = self._data

        if path in ("/", "/index.html"):
            self.send_file(STATIC / "index.html", "text/html; charset=utf-8")
            return

        # Job log streaming
        if path.startswith("/api/jobs/") and path.endswith("/log"):
            job_id = path.split("/")[3]
            job = JOBS.get(job_id)
            if not job:
                self.send_json({"error": "not found"}, 404)
                return
            log_path = pathlib.Path(job["log_path"])
            offset = int(qs.get("offset", ["0"])[0])
            try:
                text = log_path.read_text(errors="replace")
                self.send_json({"text": text[offset:], "size": len(text), "status": job["status"]})
            except FileNotFoundError:
                self.send_json({"text": "", "size": 0, "status": job["status"]})
            return

        routes = {
            "/api/overview": lambda: d["overview"],
            "/api/signals": lambda: self._filter_signals(d["signals"], qs),
            "/api/runs": lambda: self._get_runs(d["runs"], qs),
            "/api/events": lambda: d["events"],
            "/api/event-specs": lambda: d["event_specs"],
            "/api/campaigns": lambda: load_campaigns(),
            "/api/lake": lambda: d["lake"],
            "/api/proposals": lambda: load_proposals(),
            "/api/theses": lambda: load_theses(),
            "/api/signal": lambda: self._get_signal(d["signals"], qs),
            "/api/jobs": lambda: list(JOBS.values()),
            "/api/templates": lambda: load_templates(),
            "/api/features": lambda: load_features(),
            "/api/live-state": lambda: load_live_state(),
            "/api/domain-graph": lambda: load_domain_graph_summary(),
            "/api/artifacts": lambda: load_artifact_inventory(),
            "/api/artifacts/detail": lambda: load_artifact_detail(qs.get("section", [""])[0]),
            "/api/git-activity": lambda: load_git_activity(),
        }

        handler = routes.get(path)
        if handler:
            try:
                self.send_json(handler())
            except Exception as e:
                self.send_json({"error": str(e)}, 500)
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/api/proposals/save":
            try:
                body = json.loads(self.read_body())
                result = save_proposal(body.get("filename", "proposal"), body.get("content", ""))
                self.send_json(result)
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 400)
            return

        if path == "/api/run":
            try:
                body = json.loads(self.read_body())
                stage = body.get("stage", "discover")
                subcmd = body.get("subcmd", "run")
                args = body.get("args", {})

                if stage == "discover" and subcmd == "run":
                    proposal = args.get("proposal", "")
                    run_id = args.get("run_id", "")
                    if not proposal:
                        self.send_json({"ok": False, "error": "proposal required"}, 400)
                        return
                    cmd = [
                        "python3",
                        "-m",
                        "project.cli",
                        "discover",
                        "run",
                        "--proposal",
                        proposal,
                    ]
                    if run_id:
                        cmd += ["--run_id", run_id]
                    label = f"discover·{run_id or proposal}"

                elif stage == "discover" and subcmd == "plan":
                    proposal = args.get("proposal", "")
                    run_id = args.get("run_id", "")
                    if not proposal:
                        self.send_json({"ok": False, "error": "proposal required"}, 400)
                        return
                    cmd = [
                        "python3",
                        "-m",
                        "project.cli",
                        "discover",
                        "plan",
                        "--proposal",
                        proposal,
                    ]
                    if run_id:
                        cmd += ["--run_id", run_id]
                    label = f"plan·{proposal}"

                elif stage == "validate":
                    run_id = args.get("run_id", "")
                    if not run_id:
                        self.send_json({"ok": False, "error": "run_id required"}, 400)
                        return
                    cmd = ["python3", "-m", "project.cli", "validate", "run", "--run_id", run_id]
                    label = f"validate·{run_id}"

                elif stage == "promote":
                    run_id = args.get("run_id", "")
                    symbols = args.get("symbols", "BTCUSDT,ETHUSDT")
                    if not run_id:
                        self.send_json({"ok": False, "error": "run_id required"}, 400)
                        return
                    cmd = [
                        "python3",
                        "-m",
                        "project.cli",
                        "promote",
                        "run",
                        "--run_id",
                        run_id,
                        "--symbols",
                        symbols,
                    ]
                    label = f"promote·{run_id}"

                elif stage == "export":
                    run_id = args.get("run_id", "")
                    if not run_id:
                        self.send_json({"ok": False, "error": "run_id required"}, 400)
                        return
                    cmd = ["python3", "-m", "project.cli", "promote", "export", "--run_id", run_id]
                    label = f"export·{run_id}"

                elif stage == "deploy" and subcmd == "paper":
                    run_id = args.get("run_id", "")
                    if not run_id:
                        self.send_json({"ok": False, "error": "run_id required"}, 400)
                        return
                    cmd = ["python3", "-m", "project.cli", "deploy", "paper", "--run_id", run_id]
                    label = f"deploy-paper·{run_id}"

                elif stage == "ingest":
                    run_id = args.get("run_id", "")
                    symbols = args.get("symbols", "BTCUSDT,ETHUSDT")
                    start = args.get("start", "2021-01-01")
                    end = args.get("end", "2024-12-31")
                    timeframe = args.get("timeframe", "5m")
                    exchange = args.get("exchange", "bybit")
                    data_type = args.get("data_type", "ohlcv")
                    cmd = [
                        "python3",
                        "-m",
                        "project.cli",
                        "ingest",
                        "--run_id",
                        run_id,
                        "--symbols",
                        symbols,
                        "--start",
                        start,
                        "--end",
                        end,
                        "--timeframe",
                        timeframe,
                        "--exchange",
                        exchange,
                        "--data_type",
                        data_type,
                    ]
                    label = f"ingest·{run_id}"

                elif stage == "build-graph":
                    cmd = ["python3", "project/scripts/build_domain_graph.py"]
                    label = "build-graph"

                else:
                    self.send_json({"ok": False, "error": f"unknown stage: {stage}"}, 400)
                    return

                job = start_job(cmd, label)
                self.send_json({"ok": True, "job": job})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 400)
            return

        if path == "/api/reload":
            try:
                self._data.update(self._reload_data())
                self.send_json({"ok": True, "reloaded_at": datetime.datetime.now().isoformat()})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 500)
            return

        self.send_response(404)
        self.end_headers()

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _filter_signals(self, sigs, qs):
        sym = qs.get("symbol", [None])[0]
        cat = qs.get("category", [None])[0]
        min_b = float(qs.get("min_bps", ["-999"])[0])
        q = (qs.get("q", [""])[0] or "").lower()
        if sym:
            sigs = [s for s in sigs if sym in s.get("by_symbol", {})]
        if cat:
            sigs = [s for s in sigs if s["category"] == cat]
        if min_b != -999:
            sigs = [s for s in sigs if s["best_bps"] >= min_b]
        if q:
            sigs = [s for s in sigs if q in s["event"].lower() or q in s["category"].lower()]
        return sigs

    def _get_runs(self, runs, qs):
        run_id = qs.get("id", [None])[0]
        if run_id:
            return next((r for r in runs if r["run_id"] == run_id), {})
        return runs

    def _get_signal(self, sigs, qs):
        sig_id = qs.get("id", [None])[0]
        return next((s for s in sigs if s["id"] == sig_id), {}) if sig_id else {}

    @staticmethod
    def _reload_data() -> dict:
        signals = load_signals()
        runs = load_runs()
        events = load_event_families(signals)
        return {
            "signals": signals,
            "runs": runs,
            "events": events,
            "lake": load_lake(),
            "event_specs": load_event_specs(),
            "overview": load_overview(signals, runs, events),
        }


# ─── Main ────────────────────────────────────────────────────────────────────


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 7477

    print("Edge Research Platform")
    print("=" * 44)
    print("Loading data...")

    data = Handler._reload_data()
    Handler._data = data

    print(f"  Signals:      {len(data['signals'])}")
    print(f"  Runs:         {len(data['runs'])}")
    print(f"  Events:       {len(data['events'])}")
    print(f"  Event specs:  {len(data['event_specs'])}")
    print(f"  Lake caches:  {len(data['lake']['run_caches'])}")
    print(
        f"  Best signal:  {data['overview']['best_signal_event']} "
        f"@ {data['overview']['best_signal_bps']} bps"
    )

    httpd = HTTPServer(("", port), Handler)
    print()
    print(f"  Platform → http://localhost:{port}")
    print("  Ctrl+C to stop")
    print()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n  Stopped.")


if __name__ == "__main__":
    main()
