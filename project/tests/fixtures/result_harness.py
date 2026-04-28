"""
Result-delta harness for Sprint 1 forensics.

Captures a deterministic snapshot of every result-affecting metric produced
by the research pipeline so that any patch can produce a measurable
before / after diff.

Usage (CLI):
    python -m project.tests.fixtures.result_harness --save path/to/snapshot.json
    python -m project.tests.fixtures.result_harness \
        --diff path/to/before.json path/to/after.json \
        --save path/to/delta.json

Usage (library):
    from project.tests.fixtures.result_harness import (
        make_synthetic_features,
        make_synthetic_hypotheses,
        snapshot_metrics,
        delta,
    )
    before = snapshot_metrics(hypotheses, features)
    # ... apply patch ...
    after  = snapshot_metrics(hypotheses, features)
    print(delta(before, after))
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Synthetic fixture builder
# ---------------------------------------------------------------------------

def make_synthetic_features(
    *,
    n_rows: int = 40,
    n_events: int = 8,
    split_fracs: tuple[float, float, float] = (0.6, 0.2, 0.2),
    entry_lag: int = 1,
    horizon: int = 3,
    seed: int = 42,
    bar_minutes: int = 5,
    include_split_labels: bool = True,
) -> pd.DataFrame:
    """Build a minimal feature table suitable for hypothesis evaluation.

    Returns a DataFrame with columns:
        timestamp, close, volume, split_label, <vol_spike_signal_col> (synthetic event flag)

    The event rows are placed to exercise split-boundary interactions:
    one event is always placed at the last row of the training split so that
    double-counted entry_lag will misclassify it as OOS.
    """
    from project.events.event_specs import EVENT_REGISTRY_SPECS

    rng = np.random.default_rng(seed)
    base_ts = pd.Timestamp("2024-01-01", tz="UTC")
    timestamps = [base_ts + pd.Timedelta(minutes=bar_minutes * i) for i in range(n_rows)]
    close = 100.0 * np.cumprod(1 + rng.normal(0, 0.001, n_rows))

    df = pd.DataFrame(
        {
            "timestamp": timestamps,
            "close": close,
            "volume": rng.integers(100, 10_000, n_rows).astype(float),
        }
    )

    # Place events
    event_positions = sorted(rng.choice(n_rows - horizon - entry_lag - 1, size=n_events, replace=False))
    # Force one event near the train/validation boundary to test split compat
    train_boundary = int(n_rows * split_fracs[0]) - 1
    if train_boundary not in event_positions and train_boundary > 0:
        event_positions[0] = train_boundary

    event_col = np.zeros(n_rows, dtype=bool)
    for pos in event_positions:
        event_col[pos] = True

    # Use the canonical signal column name so trigger_mask() can find it
    vol_spike_spec = EVENT_REGISTRY_SPECS.get("VOL_SPIKE")
    signal_col = vol_spike_spec.signal_column if vol_spike_spec else "vol_spike_event"
    df[signal_col] = event_col

    # Split labels
    if include_split_labels:
        train_end = int(n_rows * split_fracs[0])
        val_end = train_end + int(n_rows * split_fracs[1])
        labels = pd.Series("", index=df.index, dtype=object)
        labels.iloc[:train_end] = "train"
        labels.iloc[train_end:val_end] = "validation"
        labels.iloc[val_end:] = "test"
        df["split_label"] = labels

    return df.reset_index(drop=True)


def make_synthetic_hypotheses(
    *,
    entry_lag: int = 1,
    horizon: str = "12b",
    direction: str = "long",
    context_timing: str = "entry",
) -> list:
    """Return a list of HypothesisSpec for the synthetic VOL_SPIKE event."""
    from project.domain.hypotheses import HypothesisSpec, TriggerSpec

    trigger = TriggerSpec.event("VOL_SPIKE")
    kwargs: dict[str, Any] = dict(
        trigger=trigger,
        direction=direction,
        horizon=horizon,
        template_id="continuation",
        entry_lag=entry_lag,
        cost_profile="standard",
        objective_profile="mean_return",
    )
    # context_timing is a new field added in Sprint 1 — pass it only if the
    # dataclass already supports it (graceful degradation for pre-patch tests).
    try:
        spec = HypothesisSpec(**kwargs, context_timing=context_timing)
    except TypeError:
        spec = HypothesisSpec(**kwargs)
    return [spec]


# ---------------------------------------------------------------------------
# Snapshot
# ---------------------------------------------------------------------------

def snapshot_metrics(
    hypotheses: list,
    features: pd.DataFrame,
    *,
    cost_bps: float = 2.0,
) -> dict[str, Any]:
    """Run the batch evaluator and return a flat snapshot dict.

    All numeric values are Python scalars (JSON-serialisable).
    """
    from project.research.search.evaluator import evaluate_hypothesis_batch

    # Count raw trigger hits before any lag
    trigger_hit_count = 0
    try:
        from project.research.search.evaluator_utils import trigger_mask as _tm
        for spec in hypotheses:
            raw = _tm(spec, features)
            trigger_hit_count += int(raw.sum())
    except Exception:
        trigger_hit_count = -1  # unavailable

    metrics = evaluate_hypothesis_batch(hypotheses, features, cost_bps=cost_bps)

    def _safe_sum(col: str) -> int:
        if col in metrics.columns:
            return int(pd.to_numeric(metrics[col], errors="coerce").fillna(0).sum())
        return 0

    def _safe_mean(col: str) -> float:
        if col in metrics.columns:
            v = pd.to_numeric(metrics[col], errors="coerce").dropna()
            return round(float(v.mean()), 6) if len(v) else float("nan")
        return float("nan")

    valid = metrics[metrics["valid"].fillna(False).astype(bool)] if not metrics.empty else metrics

    return {
        "hypothesis_count": len(hypotheses),
        "metrics_rows": len(metrics),
        "valid_metrics_rows": len(valid),
        "trigger_hit_count": trigger_hit_count,
        "post_entry_lag_event_count": _safe_sum("n"),
        "train_n_obs": _safe_sum("train_n_obs"),
        "validation_n_obs": _safe_sum("validation_n_obs"),
        "test_n_obs": _safe_sum("test_n_obs"),
        "validation_samples": _safe_sum("validation_samples"),
        "test_samples": _safe_sum("test_samples"),
        "mean_return_gross_bps": _safe_mean("mean_return_gross_bps"),
        "mean_return_net_bps": _safe_mean("mean_return_net_bps"),
        "t_stat_gross": _safe_mean("t_stat_gross"),
        "t_stat_net": _safe_mean("t_stat_net"),
        "p_value": _safe_mean("p_value"),
        "p_value_for_fdr": _safe_mean("p_value_for_fdr"),
        "invalid_reasons": (
            metrics["invalid_reason"].fillna("").astype(str).value_counts().to_dict()
            if "invalid_reason" in metrics.columns and not metrics.empty
            else {}
        ),
    }


# ---------------------------------------------------------------------------
# Delta
# ---------------------------------------------------------------------------

def delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    """Return a diff dict showing before/after values and whether each changed."""
    result: dict[str, Any] = {}
    all_keys = sorted(set(before) | set(after))
    changed_fields: list[str] = []
    for key in all_keys:
        b = before.get(key)
        a = after.get(key)
        # For dicts (invalid_reasons) do a deep comparison
        changed = b != a
        result[key] = {"before": b, "after": a, "changed": changed}
        if changed:
            changed_fields.append(key)
    result["_summary"] = {
        "total_fields": len(all_keys),
        "changed_fields": changed_fields,
        "primary_reason_for_change": (
            "see changed_fields list — attach a PR description explaining each delta"
            if changed_fields
            else "no result drift detected"
        ),
    }
    return result


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def _run_snapshot(args: argparse.Namespace) -> None:
    features = make_synthetic_features()
    hypotheses = make_synthetic_hypotheses()
    snap = snapshot_metrics(hypotheses, features)
    out_path = Path(args.save)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(snap, indent=2, default=str), encoding="utf-8")
    print(f"Snapshot written to {out_path}")


def _run_diff(args: argparse.Namespace) -> None:
    before_path, after_path = Path(args.diff[0]), Path(args.diff[1])
    before = json.loads(before_path.read_text(encoding="utf-8"))
    after = json.loads(after_path.read_text(encoding="utf-8"))
    d = delta(before, after)
    if args.save:
        out_path = Path(args.save)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(d, indent=2, default=str), encoding="utf-8")
        print(f"Delta written to {out_path}")
    else:
        print(json.dumps(d, indent=2, default=str))


def main() -> None:
    parser = argparse.ArgumentParser(description="Result-delta harness")
    parser.add_argument("--save", default="result_delta/snapshot.json", help="Output path")
    parser.add_argument("--diff", nargs=2, metavar=("BEFORE", "AFTER"), help="Diff two snapshots")
    args = parser.parse_args()
    if args.diff:
        _run_diff(args)
    else:
        _run_snapshot(args)


if __name__ == "__main__":
    main()
