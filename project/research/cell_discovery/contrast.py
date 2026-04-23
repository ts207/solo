from __future__ import annotations

import pandas as pd

from project.research.cell_discovery.models import ContrastRule

CONTRAST_COLUMNS = [
    "cell_id",
    "source_cell_id",
    "contrast_rule_id",
    "contrast_rule_type",
    "complement_net_mean_bps",
    "contrast_lift_bps",
    "contrast_pass",
    "contrast_blocked_reason",
]


def _empty_contrast_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=CONTRAST_COLUMNS)


def _primary_rule(rules: tuple[ContrastRule, ...] | None) -> ContrastRule:
    configured = tuple(rules or ())
    if not configured:
        return ContrastRule(
            rule_id="context_vs_unconditional",
            rule_type="in_bucket_vs_unconditional",
            required=True,
        )
    for rule in configured:
        if rule.rule_type == "in_bucket_vs_unconditional":
            return rule
    raise ValueError("edge-cell contrast requires an in_bucket_vs_unconditional rule")


def build_contrast_frame(
    cells: pd.DataFrame,
    *,
    rules: tuple[ContrastRule, ...] | None = None,
    min_lift_bps: float = 0.0,
) -> pd.DataFrame:
    if cells is None or cells.empty:
        return _empty_contrast_frame()
    rule = _primary_rule(rules)
    lift_threshold = (
        float(rule.min_lift_bps) if rule.min_lift_bps is not None else float(min_lift_bps)
    )
    out = cells.copy()
    unconditional = out[out["context_cell"].astype(str) == "unconditional"].copy()
    baseline: dict[tuple[str, str, str, str], float] = {}
    for _, row in unconditional.iterrows():
        key = (
            str(row.get("event_atom", "")),
            str(row.get("symbol", "")),
            str(row.get("direction", "")),
            str(row.get("horizon", "")),
            str(row.get("template", "")),
        )
        baseline[key] = float(row.get("net_mean_bps", row.get("mean_return_bps", 0.0)) or 0.0)

    rows = []
    for _, row in out.iterrows():
        key = (
            str(row.get("event_atom", "")),
            str(row.get("symbol", "")),
            str(row.get("direction", "")),
            str(row.get("horizon", "")),
            str(row.get("template", "")),
        )
        has_complement = key in baseline
        complement = baseline.get(key, 0.0)
        net = float(row.get("net_mean_bps", row.get("mean_return_bps", 0.0)) or 0.0)
        lift = net - complement
        is_unconditional = str(row.get("context_cell", "")) == "unconditional"
        blocked_reason = ""
        if is_unconditional:
            blocked_reason = "baseline_not_rankable"
        elif not has_complement:
            blocked_reason = "missing_complement"
        elif lift <= lift_threshold:
            blocked_reason = "insufficient_contrast_lift"
        rows.append(
            {
                "cell_id": str(row.get("cell_id", row.get("source_cell_id", ""))),
                "source_cell_id": str(row.get("source_cell_id", "")),
                "contrast_rule_id": rule.rule_id,
                "contrast_rule_type": rule.rule_type,
                "complement_net_mean_bps": complement,
                "contrast_lift_bps": 0.0 if is_unconditional else lift,
                "contrast_pass": bool(not blocked_reason),
                "contrast_blocked_reason": blocked_reason,
            }
        )
    return pd.DataFrame(rows, columns=CONTRAST_COLUMNS)
