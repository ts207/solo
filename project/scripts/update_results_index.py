#!/usr/bin/env python3
"""
Regenerate docs/all_results_2026-04-17.md from all project artifacts.
Run after any discover/promote/pipeline command to keep the index current.
"""
import glob
import warnings
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore")

ROOT = Path(__file__).resolve().parents[2]
OUT_PATH = ROOT / "docs" / "research" / "results.md"

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

SOURCE_PRIORITY = {
    "eval_results": 0,
    "manual": 1,
    "phase2_hyp": 2,
    "edge_cand": 3,
}

STATUS_OVERRIDES = [
    {
        "match": {
            "event_type": "CLIMAX_VOLUME_BAR",
            "direction": "long",
            "horizon": "24b",
            "template_id": "exhaustion_reversal",
        },
        "status": "parked: forward failed",
    },
    {
        "match": {
            "event_type": "PRICE_DOWN_OI_DOWN",
            "direction": "long",
            "horizon": "24b",
        },
        "status": "control: year-split pending",
    },
    {
        "match": {
            "event_type": "OVERSHOOT_AFTER_SHOCK",
            "direction": "long",
            "horizon": "48b",
        },
        "status": "monitor-only: robustness failed",
    },
]

PARKED_FOLLOWUP_LANES = [
    {
        "event": "BAND_BREAK",
        "lane": "ETHUSDT / vol_regime=low / long / 24b / mean_reversion",
        "run_id": "single_event_band_break__20260429T051949Z_d7bff7f5e9",
        "reason": "governed reproduction failed: t_net=0.9394, robustness=0.6691, no bridge candidates",
    },
    {
        "event": "FALSE_BREAKOUT",
        "lane": "BTCUSDT / ms_trend_state=bullish / long / 48b / exhaustion_reversal",
        "run_id": "single_event_false_break_20260429T052713Z_47ac6a4a04",
        "reason": "governed reproduction failed: t_net=0.8627, robustness=0.4052, no bridge candidates",
    },
]


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
    eval_glob = ROOT / "data/artifacts/experiments/*/*/evaluation_results.parquet"
    for f in sorted(glob.glob(str(eval_glob))):
        parts = f.split("/")
        program_id, run_id = parts[-3], parts[-2]
        try:
            df = pd.read_parquet(f)
            rows.extend(extract_rows(df, "eval_results", run_id, program_id))
        except Exception:
            pass

    # phase2 evaluated_hypotheses
    phase2_glob = ROOT / "data/reports/phase2/*/hypotheses/*/evaluated_hypotheses.parquet"
    for f in sorted(glob.glob(str(phase2_glob))):
        run_id = f.split("/")[-4]
        try:
            df = pd.read_parquet(f)
            if len(df):
                rows.extend(extract_rows(df, "phase2_hyp", run_id, run_id))
        except Exception:
            pass

    # edge_candidates_normalized (older runs)
    edge_candidates_glob = (
        ROOT / "data/reports/edge_candidates/*/edge_candidates_normalized.parquet"
    )
    for f in sorted(glob.glob(str(edge_candidates_glob))):
        run_id = f.split("/")[-2]
        try:
            df = pd.read_parquet(f)
            if len(df):
                rows.extend(extract_rows(df, "edge_cand", run_id, run_id))
        except Exception:
            pass

    return pd.DataFrame(rows)


def _norm_value(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def status_override_for_row(r) -> str | None:
    for override in STATUS_OVERRIDES:
        if all(
            _norm_value(r.get(key)) == _norm_value(value)
            for key, value in override["match"].items()
        ):
            return override["status"]
    return None


def status_for_row(r) -> str:
    override = status_override_for_row(r)
    if override is not None:
        return override

    if bool(r.get("promoted")) and bool(r.get("_has_evaluable_metrics")):
        return "**PROMOTED**"

    t, rob, q = r["t"], r["rob"], r["q"]
    if not bool(r.get("_has_evaluable_metrics")):
        if pd.isna(t) and bool(r.get("_has_sample_count")) and int(r.get("n", 0)) == 0:
            return "no events"
        return "not evaluated"

    t = float(t)
    rob = float(rob) if pd.notna(rob) else 0.0
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


def prepare_results(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["t"] = pd.to_numeric(df["t_stat"], errors="coerce").round(4)
    df["rob"] = pd.to_numeric(df["robustness_score"], errors="coerce").round(4)
    df["_n_observed"] = pd.to_numeric(df["n_events"], errors="coerce")
    df["n"] = df["_n_observed"].fillna(0).astype(int)
    df["_has_sample_count"] = df["_n_observed"].notna()
    df["q"] = pd.to_numeric(df["q_value"], errors="coerce").round(4)
    expectancy = pd.to_numeric(df["after_cost_expectancy_per_trade"], errors="coerce")
    df["exp_bps"] = (expectancy * 10000).round(1)

    # Effect/expectancy is only interpretable when the row has evaluated metrics.
    # Campaign summaries can carry aggregate expectancy without t/q/sample evidence;
    # displaying those as "no events" made invalid rows look like measured effects.
    df["_has_evaluable_metrics"] = df["t"].notna() & df["_n_observed"].fillna(0).gt(0)
    df.loc[~df["_has_evaluable_metrics"], "exp_bps"] = pd.NA

    df = df[df["event_type"].notna() & (df["event_type"] != "")]
    df["template_id"] = df.apply(
        lambda r: infer_template(r["program_id"], r["template_id"]),
        axis=1,
    )

    df["prio"] = df["source_file"].map(SOURCE_PRIORITY).fillna(99)
    df["t_r"] = df["t"].round(3)
    df = df.sort_values("prio").drop_duplicates(
        subset=["event_type", "direction", "horizon", "template_id", "t_r"], keep="first"
    )
    df = df.drop(columns=["prio", "t_r"])

    df["promoted"] = df["program_id"].isin(PROMOTED_PROGS)
    df["status"] = df.apply(status_for_row, axis=1)
    return df.sort_values(
        ["event_type", "direction", "horizon", "t"],
        ascending=[True, True, True, False],
        na_position="last",
    )


def select_best_row(edf: pd.DataFrame) -> pd.Series:
    promoted_evaluable = edf[edf["promoted"] & edf["_has_evaluable_metrics"]]
    if len(promoted_evaluable):
        return promoted_evaluable.sort_values("t", ascending=False).iloc[0]

    evaluable = edf[edf["_has_evaluable_metrics"]]
    if len(evaluable):
        return evaluable.sort_values("t", ascending=False).iloc[0]

    promoted = edf[edf["promoted"]]
    if len(promoted):
        return promoted.iloc[0]

    return edf.sort_values(
        ["_has_sample_count", "t"],
        ascending=[False, False],
        na_position="last",
    ).iloc[0]


def build() -> None:
    df = prepare_results(collect_all())

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
        "*Gates: bridge = t ≥ 2.0 AND rob ≥ 0.70; "
        "phase2 = rob ≥ 0.60. exp = after-cost per trade (bps).*",
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
        best_rows.append(select_best_row(edf))

    for r in best_rows:
        lines.append(
            f"| {r['event_type']} | {r.get('direction', '')} | "
            f"{r.get('horizon', '')} | {r.get('template_id', '')} "
            f"| {fmt(r.get('t'), '{:.2f}')} | {fmt(r.get('rob'), '{:.3f}')} "
            f"| {fmt(r.get('q'), '{:.4f}')} | {fmt(r.get('exp_bps'), '{:.1f}')} | {r['status']} |"
        )

    lines += ["", "---", ""]

    if PARKED_FOLLOWUP_LANES:
        lines += [
            "## Parked Follow-Up Lanes",
            "",
            "| Event | Lane | Reproduction run | Current reason |",
            "|-------|------|------------------|----------------|",
        ]
        for row in PARKED_FOLLOWUP_LANES:
            lines.append(
                f"| {row['event']} | {row['lane']} | `{row['run_id']}` | {row['reason']} |"
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
