#!/usr/bin/env python3
"""
Regenerate docs/all_results_2026-04-17.md from all project artifacts.
Run after any discover/promote/pipeline command to keep the index current.
"""
import glob
import os
import warnings
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore")

ROOT = Path(__file__).resolve().parents[2]
OUT_PATH = ROOT / "docs" / "all_results_2026-04-17.md"

KEY_COLS = [
    "event_type", "direction", "horizon", "template_id",
    "t_stat", "robustness_score", "n_events", "n",
    "q_value", "after_cost_expectancy_per_trade",
    "mean_return_bps", "is_discovery",
]

TEMPLATE_HINTS = {
    "exhaustion_reversal": ["liq", "std_gate", "climax", "forced_flow", "oi_flush",
                            "deleveraging", "oi_spike", "broad_oi", "broad_post",
                            "broad_forced", "broad_climax", "broad_liquidation"],
    "mean_reversion": ["mr_", "liqdirect", "direct_highvol", "golden_path", "mean_rev"],
    "continuation": ["cont_", "broad_vol_spike_short"],
    "reversal_or_squeeze": ["reversal_or", "targeted"],
}

PROMOTED_PROGS = {
    "broad_vol_spike_long_mr_24b",
    "campaign_pe_oi-spike-negative",
    "campaign_pe_oi_spike_neg_48b",
    "liquidation_std_gate_2yr",
}

SOURCE_PRIORITY = {"eval_results": 0, "manual": 1, "phase2_hyp": 2, "edge_cand": 3, "event_stats": 4}


def infer_template(prog: str, existing) -> str:
    if pd.notna(existing) and str(existing) not in ("nan", "", "unknown"):
        return str(existing)
    prog = str(prog).lower()
    for tmpl, keys in TEMPLATE_HINTS.items():
        if any(k in prog for k in keys):
            return tmpl
    return "unknown"


def extract_rows(df: pd.DataFrame, source: str, run_id: str, program_id: str) -> list:
    rows = []
    for _, r in df.iterrows():
        row = {"source_file": source, "run_id": run_id, "program_id": program_id}
        for c in KEY_COLS:
            row[c] = r.get(c)
        if row["n_events"] is None and row.get("n") is not None:
            row["n_events"] = row["n"]
        rows.append(row)
    return rows


def collect_all() -> pd.DataFrame:
    rows = []

    # evaluation_results from experiment artifacts
    for f in sorted(glob.glob(str(ROOT / "data/artifacts/experiments/*/*/evaluation_results.parquet"))):
        parts = f.split("/")
        program_id, run_id = parts[-3], parts[-2]
        try:
            df = pd.read_parquet(f)
            rows.extend(extract_rows(df, "eval_results", run_id, program_id))
        except Exception:
            pass

    # phase2 evaluated_hypotheses
    for f in sorted(glob.glob(str(ROOT / "data/reports/phase2/*/hypotheses/*/evaluated_hypotheses.parquet"))):
        run_id = f.split("/")[-4]
        try:
            df = pd.read_parquet(f)
            if len(df):
                rows.extend(extract_rows(df, "phase2_hyp", run_id, run_id))
        except Exception:
            pass

    # edge_candidates_normalized (older runs)
    for f in sorted(glob.glob(str(ROOT / "data/reports/edge_candidates/*/edge_candidates_normalized.parquet"))):
        run_id = f.split("/")[-2]
        try:
            df = pd.read_parquet(f)
            if len(df):
                rows.extend(extract_rows(df, "edge_cand", run_id, run_id))
        except Exception:
            pass

    # campaign_summary for runs without eval parquets
    for f in sorted(glob.glob(str(ROOT / "data/artifacts/experiments/*/campaign_summary.json"))):
        import json
        prog = f.split("/")[-2]
        try:
            d = json.load(open(f))
            stats = d.get("win_rates", {})
            by_event = stats.get("by_event_type", {})
            by_horizon = stats.get("by_horizon", {})
            for event, ev_stats in by_event.items():
                exp = ev_stats.get("avg_after_cost_expectancy")
                horizon = list(by_horizon.keys())[0] if by_horizon else ""
                rows.append({
                    "source_file": "event_stats", "run_id": prog, "program_id": prog,
                    "event_type": event, "direction": "", "horizon": horizon,
                    "template_id": "", "t_stat": None, "robustness_score": None,
                    "n_events": None, "n": None, "q_value": None,
                    "after_cost_expectancy_per_trade": exp,
                    "mean_return_bps": None, "is_discovery": False,
                })
        except Exception:
            pass

    return pd.DataFrame(rows)


def build() -> None:
    df = collect_all()

    df["t"] = pd.to_numeric(df["t_stat"], errors="coerce").round(4)
    df["rob"] = pd.to_numeric(df["robustness_score"], errors="coerce").round(4)
    df["n"] = pd.to_numeric(df["n_events"], errors="coerce").fillna(0).astype(int)
    df["q"] = pd.to_numeric(df["q_value"], errors="coerce").round(4)
    df["exp_bps"] = (pd.to_numeric(df["after_cost_expectancy_per_trade"], errors="coerce") * 10000).round(1)

    df = df[df["event_type"].notna() & (df["event_type"] != "")]
    df["template_id"] = df.apply(lambda r: infer_template(r["program_id"], r["template_id"]), axis=1)

    df["prio"] = df["source_file"].map(SOURCE_PRIORITY).fillna(99)
    df["t_r"] = df["t"].round(3)
    df = df.sort_values("prio").drop_duplicates(
        subset=["event_type", "direction", "horizon", "template_id", "t_r"], keep="first"
    )
    df = df.drop(columns=["prio", "t_r"])

    df["promoted"] = df["program_id"].isin(PROMOTED_PROGS)

    def status(r):
        if r["promoted"]:
            return "**PROMOTED**"
        t, rob, q = r["t"], r["rob"], r["q"]
        if pd.isna(t):
            return "no events"
        t, rob = float(t), float(rob) if pd.notna(rob) else 0.0
        q = float(q) if pd.notna(q) else 1.0
        if t >= 2.0 and rob >= 0.70:
            return "bridge gate"
        if t >= 2.0 and rob >= 0.60:
            return "phase2 gate"
        if t >= 2.0:
            return "t passes"
        if q < 0.05:
            return "discovery"
        if t < 0:
            return "negative"
        return "below gate"

    df["status"] = df.apply(status, axis=1)
    df = df.sort_values(
        ["event_type", "direction", "horizon", "t"],
        ascending=[True, True, True, False],
        na_position="last",
    )

    def fmt(v, fs):
        if pd.isna(v):
            return "—"
        try:
            return fs.format(float(v))
        except Exception:
            return str(v)

    n_events = df["event_type"].nunique()
    n_results = len(df)

    lines = [
        "# All Results — Edge Discovery Project",
        "",
        "*Auto-generated. Do not edit manually — rerun `project/scripts/update_results_index.py`.*",
        f"*{n_results} unique results across {n_events} events.*",
        "*Gates: bridge = t ≥ 2.0 AND rob ≥ 0.70; phase2 = rob ≥ 0.60. exp = after-cost per trade (bps).*",
        "",
        "---",
        "",
        "## Summary — Best Result Per Event",
        "",
        "| Event | Dir | Horizon | Template | t | rob | q | exp (bps) | Status |",
        "|-------|-----|---------|----------|---|-----|---|-----------|--------|",
    ]

    # For summary: prefer promoted row, else highest t
    best_rows = []
    for event in sorted(df["event_type"].dropna().unique()):
        edf = df[df["event_type"] == event]
        promoted = edf[edf["promoted"]]
        row = promoted.iloc[0] if len(promoted) else edf.sort_values("t", ascending=False, na_position="last").iloc[0]
        best_rows.append(row)

    for r in best_rows:
        lines.append(
            f"| {r['event_type']} | {r.get('direction', '')} | {r.get('horizon', '')} | {r.get('template_id', '')} "
            f"| {fmt(r.get('t'), '{:.2f}')} | {fmt(r.get('rob'), '{:.3f}')} "
            f"| {fmt(r.get('q'), '{:.4f}')} | {fmt(r.get('exp_bps'), '{:.1f}')} | {r['status']} |"
        )

    lines += ["", "---", ""]

    for event in sorted(df["event_type"].dropna().unique()):
        edf = df[df["event_type"] == event].sort_values("t", ascending=False, na_position="last")
        lines += [
            f"## {event}",
            "",
            "| dir | horizon | template | t | rob | n | q | exp (bps) | status | program_id |",
            "|-----|---------|----------|---|-----|---|---|-----------|--------|------------|",
        ]
        for _, r in edf.iterrows():
            short = str(r.get("program_id", ""))
            for rm in ["broad_", "campaign_pe_", "liquidation_", "liq_", "_20260413",
                       "_2021_2024", "_20260416T", "_20260417T", "direct_edge_", "direct_highvol_"]:
                short = short.replace(rm, "")
            if len(short) > 38:
                short = short[:38] + "…"
            lines.append(
                f"| {r.get('direction', '')} | {r.get('horizon', '')} | {r.get('template_id', '')} "
                f"| {fmt(r.get('t'), '{:.2f}')} | {fmt(r.get('rob'), '{:.3f}')} "
                f"| {fmt(r.get('n'), '{:.0f}')} | {fmt(r.get('q'), '{:.4f}')} "
                f"| {fmt(r.get('exp_bps'), '{:.1f}')} | {r['status']} | `{short}` |"
            )
        lines.append("")

    OUT_PATH.write_text("\n".join(lines))
    print(f"Updated {OUT_PATH} ({n_results} results, {n_events} events)")


if __name__ == "__main__":
    build()
