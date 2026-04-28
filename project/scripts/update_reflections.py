#!/usr/bin/env python3
"""
Maintain docs/reflections.md:
  - Preserves the human-written observation log above the AUTO marker
  - Regenerates the auto-detected patterns section below it after every run

Detects:
  - Current signal ranking
  - Ceiling patterns (event tested >=3 times, t not improving)
  - Template incompatibility incidents (estimated_hypothesis_count=0)
  - Regime sensitivity (more data -> lower t)
  - Zero-event events
"""

import glob
import json
import warnings
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

warnings.filterwarnings("ignore")

ROOT = Path(__file__).resolve().parents[2]
REFLECTIONS_PATH = ROOT / "docs" / "research" / "reflections.md"
AUTO_MARKER = "<!-- AUTO-GENERATED: do not edit below this line -->"

PROMOTED_PROGS = {
    "broad_vol_spike_long_mr_24b",
    "campaign_pe_oi-spike-negative",
    "campaign_pe_oi_spike_neg_48b",
    "liquidation_std_gate_2yr",
}

TEMPLATE_HINTS = {
    "exhaustion_reversal": [
        "liq",
        "std_gate",
        "climax",
        "forced_flow",
        "oi_flush",
        "deleveraging",
        "oi_spike",
        "broad_oi",
        "broad_post",
        "broad_forced",
        "broad_climax",
        "broad_liquidation",
    ],
    "mean_reversion": ["mr_", "liqdirect", "direct_highvol", "golden_path", "mean_rev"],
    "continuation": ["cont_", "broad_vol_spike_short"],
    "reversal_or_squeeze": ["reversal_or", "targeted"],
}


def infer_template(prog, existing):
    if pd.notna(existing) and str(existing) not in ("nan", "", "unknown"):
        return str(existing)
    prog = str(prog).lower()
    for tmpl, keys in TEMPLATE_HINTS.items():
        if any(k in prog for k in keys):
            return tmpl
    return "unknown"


def _is_missing(value: Any) -> bool:
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return value is None


def _first_present(row: pd.Series, *columns: str) -> Any:
    for column in columns:
        if column not in row:
            continue
        value = row.get(column)
        if _is_missing(value):
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _float_present(row: pd.Series, *columns: str, default: float = 0.0) -> float:
    value = _first_present(row, *columns)
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            payload = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, dict) else {}
    return {}


def _event_from_row(row: pd.Series) -> str | None:
    explicit = _first_present(row, "event_type", "event_id")
    if explicit is not None:
        return str(explicit)

    payload = _parse_payload(row.get("trigger_payload"))
    payload_event = payload.get("event_id") or payload.get("event_type")
    if payload_event:
        return str(payload_event)

    trigger_key = _first_present(row, "trigger_key")
    if isinstance(trigger_key, str) and trigger_key.startswith("event:"):
        event_id = trigger_key.split(":", 1)[1].strip()
        return event_id or None
    return None


def _q_value_from_row(row: pd.Series) -> float:
    return _float_present(row, "q_value", "p_value_for_fdr", "p_value", default=1.0)


def _n_events_from_row(row: pd.Series) -> int:
    value = _first_present(row, "n_events", "n", "sample_size", "validation_samples")
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _expectancy_bps_from_row(row: pd.Series) -> float:
    bps = _first_present(
        row,
        "after_cost_expectancy_bps",
        "mean_return_net_bps",
        "cost_adjusted_return_bps",
        "net_expectancy_bps",
    )
    if bps is not None:
        try:
            return float(bps)
        except (TypeError, ValueError):
            pass

    decimal = _first_present(row, "after_cost_expectancy_per_trade", "after_cost_expectancy")
    if decimal is None:
        return 0.0
    try:
        return float(decimal) * 10000
    except (TypeError, ValueError):
        return 0.0


def load_results() -> pd.DataFrame:
    rows = []

    for f in sorted(
        glob.glob(str(ROOT / "data/artifacts/experiments/*/*/evaluation_results.parquet"))
    ):
        parts = f.split("/")
        program_id, run_id = parts[-3], parts[-2]
        try:
            df = pd.read_parquet(f)
            for _, r in df.iterrows():
                rows.append(
                    {
                        "program_id": program_id,
                        "run_id": run_id,
                        "event_type": _event_from_row(r),
                        "direction": r.get("direction"),
                        "horizon": r.get("horizon"),
                        "template_id": r.get("template_id"),
                        "t": _float_present(r, "t_stat_net", "t_stat", default=0.0),
                        "rob": _float_present(r, "robustness_score", default=0.0),
                        "n": _n_events_from_row(r),
                        "q": _q_value_from_row(r),
                        "exp_bps": _expectancy_bps_from_row(r),
                        "is_discovery": bool(r.get("is_discovery", False)),
                        "source": "eval_results",
                    }
                )
        except Exception as exc:
            warnings.warn(f"Could not read evaluation results from {f}: {exc}", stacklevel=2)

    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    if df.empty:
        return df

    df["template_id"] = df.apply(
        lambda r: infer_template(r["program_id"], r["template_id"]), axis=1
    )
    df["promoted"] = df["program_id"].isin(PROMOTED_PROGS)
    df["t_r"] = df["t"].round(3)
    df = df.sort_values("promoted", ascending=False).drop_duplicates(
        subset=["event_type", "direction", "horizon", "template_id", "t_r"], keep="first"
    )
    return df.drop(columns=["t_r"])


def load_validated_plans() -> list[dict]:
    """Return all validated_plan.json files with estimated_hypothesis_count=0."""
    zero_plans = []
    for f in glob.glob(str(ROOT / "data/artifacts/experiments/*/*/validated_plan.json")):
        try:
            with open(f, encoding="utf-8") as handle:
                d = json.load(handle)
            if d.get("estimated_hypothesis_count", 1) == 0:
                parts = f.split("/")
                zero_plans.append(
                    {
                        "program_id": parts[-3],
                        "run_id": parts[-2],
                        "required_detectors": d.get("required_detectors", []),
                    }
                )
        except Exception as exc:
            warnings.warn(f"Could not read validated plan from {f}: {exc}", stacklevel=2)
    return zero_plans


def detect_ceilings(df: pd.DataFrame) -> list[dict]:
    """Events tested >=3 times at same direction/template, t not improving across horizons."""
    ceilings = []
    for (event, direction, template), grp in df.groupby(["event_type", "direction", "template_id"]):
        if len(grp) < 3 or event is None:
            continue
        grp = grp.sort_values("t", ascending=False)
        best_t = grp["t"].max()
        best_rob = grp["rob"].max()
        n_tests = len(grp)
        if best_t < 2.0 or best_rob < 0.70:
            ceilings.append(
                {
                    "event": event,
                    "direction": direction,
                    "template": template,
                    "best_t": round(best_t, 3),
                    "best_rob": round(best_rob, 3),
                    "n_tests": n_tests,
                    "horizons": sorted(grp["horizon"].dropna().unique().tolist()),
                    "gap_to_t_gate": round(2.0 - best_t, 3) if best_t < 2.0 else 0,
                    "gap_to_rob_gate": round(0.70 - best_rob, 3) if best_rob < 0.70 else 0,
                }
            )
    return sorted(ceilings, key=lambda x: (-x["best_t"], -x["best_rob"]))


def detect_regime_breaks(df: pd.DataFrame) -> list[dict]:
    """Events where adding more years of data reduced t_stat."""
    breaks = []
    # Look for pairs: program_id contains "24b_2021" or "3yr" vs base run
    regime_runs = df[df["program_id"].str.contains("2021|3yr|full", na=False, case=False)]
    base_runs = df[~df["program_id"].str.contains("2021|3yr|full", na=False, case=False)]

    for _, rr in regime_runs.iterrows():
        matches = base_runs[
            (base_runs["event_type"] == rr["event_type"])
            & (base_runs["direction"] == rr["direction"])
            & (base_runs["horizon"] == rr["horizon"])
            & (base_runs["template_id"] == rr["template_id"])
        ]
        if len(matches) == 0:
            continue
        base_t = matches["t"].max()
        if base_t > rr["t"] + 0.1:
            breaks.append(
                {
                    "event": rr["event_type"],
                    "horizon": rr["horizon"],
                    "template": rr["template_id"],
                    "base_t": round(base_t, 3),
                    "extended_t": round(rr["t"], 3),
                    "drop": round(base_t - rr["t"], 3),
                }
            )
    return sorted(breaks, key=lambda x: -x["drop"])


def generate_auto_section(df: pd.DataFrame) -> str:
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        AUTO_MARKER,
        "",
        f"*Last updated: {now}*",
        "",
        "---",
        "",
        "## Auto-detected patterns",
        "",
    ]

    # 1. Signal rankings
    lines += [
        "### Current signal rankings",
        "",
        "Sorted by t-stat. Gate: bridge = t >= 2.0 AND rob >= 0.70.",
        "",
        "| Event | Dir | Horizon | Template | t | rob | q/p | exp (bps) | status |",
        "|-------|-----|---------|----------|---|-----|---|-----------|--------|",
    ]

    def status(r):
        if r["promoted"]:
            return "**PROMOTED**"
        t, rob, q = r["t"], r["rob"], r["q"]
        if t >= 2.0 and rob >= 0.70:
            return "bridge gate (research)"
        if t >= 2.0 and rob >= 0.60:
            return "phase2 gate"
        if t >= 2.0:
            return "t-only pass (rob below gate)"
        if q < 0.05:
            return "discovery p<0.05"
        if t < 0:
            return "negative"
        return "below gate"

    if not df.empty:
        # Rankings: best result per (event, direction) for a cleaner signal view.
        top = (
            df[df["t"] >= 1.0]
            .sort_values("t", ascending=False)
            .drop_duplicates(subset=["event_type", "direction"], keep="first")
            .head(20)
        )
        for _, r in top.iterrows():
            lines.append(
                f"| {r['event_type']} | {r['direction']} | {r['horizon']} | {r['template_id']} "
                f"| {r['t']:.2f} | {r['rob']:.3f} | {r['q']:.4f} | {r['exp_bps']:.1f} | {status(r)} |"
            )
    lines.append("")

    # 2. Ceiling patterns
    ceilings = detect_ceilings(df) if not df.empty else []
    lines += [
        "### Ceiling patterns",
        "",
    ]
    if ceilings:
        lines += [
            "Events tested >=3 times that are still below bridge gate (t >= 2.0 AND rob >= 0.70):",
            "",
            "| Event | Dir | Template | Best t | Best rob | Tests | Horizons | Gap-to-t | Gap-to-rob |",
            "|-------|-----|----------|--------|----------|-------|----------|----------|------------|",
        ]
        for c in ceilings:
            lines.append(
                f"| {c['event']} | {c['direction']} | {c['template']} "
                f"| {c['best_t']:.2f} | {c['best_rob']:.3f} | {c['n_tests']} "
                f"| {', '.join(c['horizons'])} | {c['gap_to_t_gate']:.2f} | {c['gap_to_rob_gate']:.2f} |"
            )
    else:
        lines.append("*None detected.*")
    lines.append("")

    # 3. Template incompatibility incidents
    zero_plans = load_validated_plans()
    lines += ["### Template incompatibility warnings (estimated_hypothesis_count = 0)", ""]
    if zero_plans:
        lines += [
            "These runs produced 0 hypotheses - likely wrong template for the event family:",
            "",
            "| program_id | required_detectors |",
            "|------------|-------------------|",
        ]
        for z in sorted(zero_plans, key=lambda x: x["program_id"]):
            short = z["program_id"].replace("broad_", "").replace("campaign_pe_", "")
            det = ", ".join(z["required_detectors"]) or "none"
            lines.append(f"| `{short}` | {det} |")
    else:
        lines.append("*None detected.*")
    lines.append("")

    # 4. Regime sensitivity
    breaks = detect_regime_breaks(df) if not df.empty else []
    lines += ["### Regime sensitivity (more data -> lower t)", ""]
    if breaks:
        lines += [
            "| Event | Horizon | Template | Base t | Extended t | Drop |",
            "|-------|---------|----------|--------|------------|------|",
        ]
        for b in breaks:
            lines.append(
                f"| {b['event']} | {b['horizon']} | {b['template']} "
                f"| {b['base_t']:.2f} | {b['extended_t']:.2f} | -{b['drop']:.2f} |"
            )
    else:
        lines.append("*None detected.*")
    lines.append("")

    # 5. Events with zero qualifying events
    if not df.empty:
        zero_events = df[df["n"] == 0]["event_type"].dropna().unique()
        if len(zero_events):
            lines += [
                "### Events with zero qualifying events",
                "",
                ", ".join(sorted(zero_events)),
                "",
            ]

    return "\n".join(lines)


def ensure_base_document():
    """Create the reflections.md with the human-written section if it doesn't exist."""
    if REFLECTIONS_PATH.exists():
        return
    content = """\
# Reflections

This document has two parts:
1. **Observations** (below) - human-written entries. Add new ones at the top of this section. Never edit the auto-generated section.
2. **Auto-detected patterns** - regenerated automatically after every pipeline run.

To add an observation, insert a new `### [YYYY-MM-DD] Title` block before the AUTO marker.

---

## Observations

### [2026-04-17] Template-family incompatibility: the silent failure

`check_hypothesis_feasibility` drops incompatible hypotheses at plan time with no visible error.
VOL_SPIKE + `exhaustion_reversal` produced 0 hypotheses for the entire broad sweep and all batch4_vol runs.
The t=3.59 result was sitting one template swap away.

**Rule:** Verify `estimated_hypothesis_count > 0` in `validated_plan.json` before concluding an event has no edge.

VOLATILITY_EXPANSION/TRANSITION events require `mean_reversion`, `continuation`, or `impulse_continuation` - never `exhaustion_reversal`.

---

### [2026-04-17] 2022 is a regime break, not noise

Every extension to include 2022 data weakened signals. CVB 24b: t=1.95 (2023-2024) -> t=1.17 (2022-2024).
The bear market actively opposes the effect direction. This is structural, not sample-size noise.

All promoted signals are bull-market conditional. The robustness metric does not capture regime stability across cycles.

---

### [2026-04-17] run_id reuse overwrites phase2 results

When multiple proposals share the same `--run_id`, each sequential run overwrites
`data/reports/phase2/<run_id>/hypotheses/`. Results from earlier proposals in the sequence survive
only in `data/artifacts/experiments/<program_id>/`. Use `campaign_summary.json` or `event_statistics.parquet`
per experiment, not the shared phase2 dir.

---

### [2026-04-17] Mechanistic clarity predicts signal quality

All three promoted signals (VOL_SPIKE, OI_SPIKE_NEGATIVE, LIQUIDATION_CASCADE) have clear
forced-flow mechanisms. Events that fire at the wrong cycle point (VOL_SHOCK = relaxation phase)
or have no consistent directional consequence (FAILED_CONTINUATION) showed no edge.
Mechanistic plausibility is a better prior than statistical fishing.

---

### [2026-04-17] Below-gate cluster may unlock with multi-feature conditioning

CVB, PDR, OI_SPIKE_POS, FFE all show t=1.4-1.95 with rob=0.60-0.79. No single feature
(rv, trend, funding) concentrates the effect to bridge gate. These events tend to
co-occur in time - a learned regime label combining multiple features may unlock them.

"""
    REFLECTIONS_PATH.write_text(content)


def update():
    ensure_base_document()
    df = load_results()

    current = REFLECTIONS_PATH.read_text()

    # Split at AUTO marker (keep everything before it, replace everything after)
    if AUTO_MARKER in current:
        human_section = current[: current.index(AUTO_MARKER)]
    else:
        human_section = current.rstrip() + "\n\n"

    auto_section = generate_auto_section(df)
    REFLECTIONS_PATH.write_text(human_section + auto_section + "\n")

    n = len(df) if not df.empty else 0
    print(f"Updated {REFLECTIONS_PATH} ({n} results analyzed)")


if __name__ == "__main__":
    update()
