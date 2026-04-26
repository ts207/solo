from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd


def rank_key(
    row: dict[str, object],
    *,
    safe_float_fn: Callable[[object, float], float],
    as_bool_fn: Callable[[object], bool],
) -> tuple[float, float, float, float, str]:
    after_cost = safe_float_fn(
        row.get(
            "bridge_expectancy_conservative",
            row.get(
                "after_cost_expectancy_per_trade",
                safe_float_fn(row.get("expectancy_per_trade"), 0.0),
            ),
        ),
        0.0,
    )
    stressed_after_cost = safe_float_fn(
        row.get("stressed_after_cost_expectancy_per_trade"), after_cost
    )
    robustness = safe_float_fn(row.get("robustness_score"), 0.0)
    oos_gate = as_bool_fn(
        row.get("gate_oos_validation", row.get("gate_oos_validation_test", False))
    )
    cost_ratio = safe_float_fn(row.get("cost_ratio"), 1.0)

    _ = stressed_after_cost
    return (
        -after_cost,
        cost_ratio,
        -robustness,
        -float(oos_gate),
        str(row.get("candidate_id", "")),
    )


def passes_fallback_gate(
    row: dict[str, object],
    gates: dict[str, Any],
    *,
    safe_float_fn: Callable[[object, float], float],
    safe_int_fn: Callable[[object, int], int],
) -> bool:
    if not gates:
        return False
    min_t = safe_float_fn(gates.get("min_t_stat"), 2.5)
    min_expectancy = safe_float_fn(gates.get("min_after_cost_expectancy_bps"), 1.0)
    min_samples = safe_int_fn(gates.get("min_sample_size"), 100)

    t_stat = safe_float_fn(
        row.get("t_stat"),
        safe_float_fn(row.get("expectancy", 0), 0.0)
        / (safe_float_fn(row.get("p_value", 1), 1.0) + 1e-9),
    )
    after_cost = safe_float_fn(row.get("after_cost_expectancy_per_trade"), 0.0) * 10000.0
    samples = safe_int_fn(row.get("n_events", row.get("sample_size", 0)), 0)

    if t_stat < min_t:
        return False
    if after_cost < min_expectancy:
        return False
    if samples < min_samples:
        return False
    return True


def choose_event_rows(
    *,
    run_id: str,
    event_type: str,
    edge_rows: list[dict[str, object]],
    phase2_df: pd.DataFrame,
    max_per_event: int,
    allow_fallback_blueprints: bool,
    strict_cost_fields: bool,
    min_events: int,
    min_robustness: float,
    require_positive_expectancy: bool,
    expected_cost_digest: str | None,
    naive_validation: dict[tuple[str, str], bool] | None,
    allow_naive_entry_fail: bool,
    mode: str,
    min_tob_coverage: float,
    min_net_expectancy_bps: float,
    max_fee_plus_slippage_bps: float | None,
    max_daily_turnover_multiple: float | None,
    data_root: Path,
    candidate_id_fn: Callable[[dict[str, object], int], str],
    load_gates_spec_fn: Callable[[], dict[str, Any]],
    passes_quality_floor_fn: Callable[..., bool],
    rank_key_fn: Callable[[dict[str, object]], tuple[float, float, float, float, str]],
    passes_fallback_gate_fn: Callable[[dict[str, object], dict[str, Any]], bool],
    as_bool_fn: Callable[[object], bool],
    safe_float_fn: Callable[[object, float], float],
) -> tuple[list[dict[str, object]], dict[str, object], pd.DataFrame]:
    phase2_lookup: dict[str, dict[str, object]] = {}
    if not phase2_df.empty:
        for idx, row in enumerate(phase2_df.to_dict(orient="records")):
            cid = candidate_id_fn(row, idx)
            phase2_lookup[cid] = dict(row)

    full_gates = load_gates_spec_fn()
    gates = full_gates.get("gate_v1_fallback", {})

    def _enrich(row: dict[str, object], idx: int, status_default: str) -> dict[str, object]:
        cid = str(row.get("candidate_id", "")).strip() or candidate_id_fn(row, idx)
        merged = dict(phase2_lookup.get(cid, {}))
        merged.update(dict(row))
        merged["candidate_id"] = cid
        merged["event"] = event_type
        merged["status"] = str(merged.get("status", status_default))
        if not str(merged.get("run_id", "")).strip():
            merged["run_id"] = run_id
        if "candidate_symbol" not in merged:
            merged["candidate_symbol"] = (
                str(merged.get("symbol", "ALL")).upper() if "symbol" in merged else "ALL"
            )
        if "source_path" not in merged or not str(merged.get("source_path", "")).strip():
            merged["source_path"] = str(
                data_root / "reports" / "phase2" / run_id / event_type / "phase2_candidates.csv"
            )
        return merged

    enriched_edge_rows = [
        _enrich(row, idx, str(row.get("status", "DRAFT"))) for idx, row in enumerate(edge_rows)
    ]

    if not enriched_edge_rows and not phase2_df.empty:
        enriched_edge_rows = [
            _enrich(row, idx, "DRAFT")
            for idx, row in enumerate(phase2_df.to_dict(orient="records"))
        ]

    eligible_rows = []
    ineligible_rows = []

    for row in enriched_edge_rows:
        is_eligible = True
        reason = ""

        is_disc = as_bool_fn(row.get("is_discovery", True))
        is_fall = passes_fallback_gate_fn(row, gates)

        if mode == "discovery" and not is_disc:
            is_eligible = False
            reason = "not_discovery"
        elif mode == "fallback" and not is_fall:
            is_eligible = False
            reason = "fallback_gate_fail"
        elif mode == "both" and not (is_disc or is_fall):
            is_eligible = False
            reason = "neither_disc_nor_fallback"

        if is_eligible and not allow_naive_entry_fail and naive_validation is not None:
            cid = str(row.get("candidate_id", "")).strip()
            if not naive_validation.get((event_type, cid), False):
                is_eligible = False
                reason = "naive_entry_fail"

        if is_eligible:
            eligible_rows.append(row)
        else:
            row["_ineligible_reason"] = reason
            ineligible_rows.append(row)

    selection_data = []

    def _add_selection_record(c: dict[str, object], reason: str, selected: bool, rank: int) -> None:
        after_cost = safe_float_fn(
            c.get(
                "bridge_expectancy_conservative",
                c.get("after_cost_expectancy_per_trade", 0.0),
            ),
            0.0,
        )
        cost_ratio = safe_float_fn(c.get("cost_ratio"), 1.0)
        robustness = safe_float_fn(c.get("robustness_score"), 0.0)
        digest = str(c.get("cost_config_digest", "")).strip()

        selection_data.append(
            {
                "candidate_id": c.get("candidate_id"),
                "event_type": event_type,
                "rank": rank,
                "selected": selected,
                "reason": reason,
                "status": c.get("status"),
                "robustness_score": robustness,
                "n_events": c.get("n_events"),
                "after_cost_expectancy": after_cost,
                "cost_ratio": cost_ratio,
                "cost_config_digest": digest,
                "rank_score_components": (
                    f"expectancy={after_cost:.6f},cost_ratio={cost_ratio:.4f},"
                    f"robustness={robustness:.4f}"
                ),
            }
        )

    for c in ineligible_rows:
        _add_selection_record(c, f"ineligible_{c.get('_ineligible_reason')}", False, 0)

    diagnostics: dict[str, object] = {
        "event_type": event_type,
        "selected_count": 0,
        "rejected_quality_floor_count": 0,
        "reason": "no_candidates",
        "used_fallback": False,
    }

    rejected_quality_floor_count = 0

    promoted = [row for row in eligible_rows if str(row.get("status", "")).upper() == "PROMOTED"]
    if promoted:
        promoted_sorted = sorted(promoted, key=rank_key_fn)
        promoted_quality = [
            row
            for row in promoted_sorted
            if passes_quality_floor_fn(
                row,
                strict_cost_fields=strict_cost_fields,
                min_events=min_events,
                min_robustness=min_robustness,
                require_positive_expectancy=require_positive_expectancy,
                expected_cost_digest=expected_cost_digest,
                min_tob_coverage=min_tob_coverage,
                min_net_expectancy_bps=min_net_expectancy_bps,
                max_fee_plus_slippage_bps=max_fee_plus_slippage_bps,
                max_daily_turnover_multiple=max_daily_turnover_multiple,
            )
        ]
        rejected_quality_floor_count += max(0, len(promoted_sorted) - len(promoted_quality))

        for c in promoted_sorted:
            if c not in promoted_quality:
                _add_selection_record(c, "quality_floor_fail_promoted", False, 0)

        if promoted_quality:
            selected = promoted_quality[:max_per_event]
            for i, c in enumerate(selected):
                _add_selection_record(c, "promoted_quality", True, i + 1)
            for i, c in enumerate(promoted_quality[max_per_event:]):
                _add_selection_record(c, "excluded_by_cap", False, max_per_event + i + 1)

            diagnostics.update(
                {
                    "selected_count": len(selected),
                    "rejected_quality_floor_count": int(rejected_quality_floor_count),
                    "reason": "promoted_quality",
                    "used_fallback": False,
                }
            )
            return selected, diagnostics, pd.DataFrame(selection_data)

    if not allow_fallback_blueprints:
        diagnostics.update(
            {
                "selected_count": 0,
                "rejected_quality_floor_count": int(rejected_quality_floor_count),
                "reason": "rejected_all_promoted_quality_floor",
                "used_fallback": False,
            }
        )
        return [], diagnostics, pd.DataFrame(selection_data)

    non_promoted_rows = [
        row for row in eligible_rows if str(row.get("status", "")).upper() != "PROMOTED"
    ]
    if non_promoted_rows:
        non_promoted_sorted = sorted(non_promoted_rows, key=rank_key_fn)
        non_promoted_quality = [
            row
            for row in non_promoted_sorted
            if passes_quality_floor_fn(
                row,
                strict_cost_fields=strict_cost_fields,
                min_events=min_events,
                min_robustness=min_robustness,
                require_positive_expectancy=False,
                expected_cost_digest=expected_cost_digest,
                min_tob_coverage=min_tob_coverage,
                min_net_expectancy_bps=min_net_expectancy_bps,
                max_fee_plus_slippage_bps=max_fee_plus_slippage_bps,
                max_daily_turnover_multiple=max_daily_turnover_multiple,
            )
        ]
        rejected_quality_floor_count += max(0, len(non_promoted_sorted) - len(non_promoted_quality))

        for c in non_promoted_sorted:
            if c not in non_promoted_quality:
                _add_selection_record(c, "quality_floor_fail_fallback_non_promoted", False, 0)

        if non_promoted_quality:
            selected = non_promoted_quality[:max_per_event]
            for i, c in enumerate(selected):
                _add_selection_record(c, "fallback_non_promoted_quality", True, i + 1)
            for i, c in enumerate(non_promoted_quality[max_per_event:]):
                _add_selection_record(c, "excluded_by_cap", False, max_per_event + i + 1)

            diagnostics.update(
                {
                    "selected_count": len(selected),
                    "rejected_quality_floor_count": int(rejected_quality_floor_count),
                    "reason": "fallback_non_promoted_quality",
                    "used_fallback": True,
                }
            )
            return selected, diagnostics, pd.DataFrame(selection_data)

        diagnostics.update(
            {
                "selected_count": 0,
                "rejected_quality_floor_count": int(rejected_quality_floor_count),
                "reason": "fallback_enabled_but_no_quality_rows",
                "used_fallback": True,
            }
        )
        return [], diagnostics, pd.DataFrame(selection_data)

    if not allow_fallback_blueprints:
        diagnostics.update(
            {
                "selected_count": 0,
                "rejected_quality_floor_count": int(rejected_quality_floor_count),
                "reason": "no_promoted_and_fallback_disabled",
                "used_fallback": False,
            }
        )
        return [], diagnostics, pd.DataFrame(selection_data)

    if eligible_rows:
        edge_sorted = sorted(eligible_rows, key=rank_key_fn)
        edge_quality = [
            row
            for row in edge_sorted
            if passes_quality_floor_fn(
                row,
                strict_cost_fields=strict_cost_fields,
                min_events=min_events,
                min_robustness=min_robustness,
                require_positive_expectancy=False,
                expected_cost_digest=expected_cost_digest,
                min_tob_coverage=min_tob_coverage,
                min_net_expectancy_bps=min_net_expectancy_bps,
                max_fee_plus_slippage_bps=max_fee_plus_slippage_bps,
                max_daily_turnover_multiple=max_daily_turnover_multiple,
            )
        ]
        rejected_quality_floor_count += max(0, len(edge_sorted) - len(edge_quality))

        for c in edge_sorted:
            if c not in edge_quality:
                _add_selection_record(c, "quality_floor_fail_fallback_edge", False, 0)

        if edge_quality:
            selected = edge_quality[:max_per_event]
            for i, c in enumerate(selected):
                _add_selection_record(c, "fallback_edge_quality", True, i + 1)
            for i, c in enumerate(edge_quality[max_per_event:]):
                _add_selection_record(c, "excluded_by_cap", False, max_per_event + i + 1)

            diagnostics.update(
                {
                    "selected_count": len(selected),
                    "rejected_quality_floor_count": int(rejected_quality_floor_count),
                    "reason": "fallback_edge_quality",
                    "used_fallback": True,
                }
            )
            return selected, diagnostics, pd.DataFrame(selection_data)

    if not phase2_df.empty:
        fallback_df = phase2_df.copy()
        for col in ("robustness_score", "profit_density_score"):
            if col not in fallback_df.columns:
                fallback_df[col] = 0.0
        if "candidate_id" not in fallback_df.columns:
            fallback_df["candidate_id"] = [
                candidate_id_fn(row, idx)
                for idx, row in enumerate(fallback_df.to_dict(orient="records"))
            ]
        ordered_rows = fallback_df.sort_values(
            by=["robustness_score", "profit_density_score", "candidate_id"],
            ascending=[False, False, True],
        ).to_dict(orient="records")
        parsed_rows: list[dict[str, object]] = []
        for idx, row in enumerate(ordered_rows):
            parsed_rows.append(_enrich(row, idx, "DRAFT"))

        eligible_parsed_rows = []
        for row in parsed_rows:
            is_eligible = True
            is_disc = as_bool_fn(row.get("rejected", False)) or as_bool_fn(
                row.get("is_discovery", False)
            )
            is_fall = passes_fallback_gate_fn(row, gates)

            if (mode == "discovery" and not is_disc) or (mode == "fallback" and not is_fall) or (mode == "both" and not (is_disc or is_fall)):
                is_eligible = False

            if is_eligible and not allow_naive_entry_fail and naive_validation is not None:
                cid = str(row.get("candidate_id", "")).strip()
                if not naive_validation.get((event_type, cid), False):
                    is_eligible = False
                    _add_selection_record(
                        row, "ineligible_naive_entry_fail_phase2_fallback", False, 0
                    )

            if is_eligible:
                eligible_parsed_rows.append(row)

        phase2_quality = [
            row
            for row in eligible_parsed_rows
            if passes_quality_floor_fn(
                row,
                strict_cost_fields=strict_cost_fields,
                min_events=min_events,
                expected_cost_digest=expected_cost_digest,
                min_tob_coverage=min_tob_coverage,
                min_net_expectancy_bps=min_net_expectancy_bps,
                max_fee_plus_slippage_bps=max_fee_plus_slippage_bps,
                max_daily_turnover_multiple=max_daily_turnover_multiple,
            )
        ]
        rejected_quality_floor_count += max(0, len(eligible_parsed_rows) - len(phase2_quality))

        for c in eligible_parsed_rows:
            if c not in phase2_quality:
                _add_selection_record(c, "quality_floor_fail_phase2", False, 0)

        if phase2_quality:
            selected = phase2_quality[:max_per_event]
            for i, c in enumerate(selected):
                _add_selection_record(c, "fallback_phase2_quality", True, i + 1)
            for i, c in enumerate(phase2_quality[max_per_event:]):
                _add_selection_record(c, "excluded_by_cap", False, max_per_event + i + 1)

            diagnostics.update(
                {
                    "selected_count": len(selected),
                    "rejected_quality_floor_count": int(rejected_quality_floor_count),
                    "reason": "fallback_phase2_quality",
                    "used_fallback": True,
                }
            )
            return selected, diagnostics, pd.DataFrame(selection_data)

    diagnostics.update(
        {
            "selected_count": 0,
            "rejected_quality_floor_count": int(rejected_quality_floor_count),
            "reason": "fallback_enabled_but_no_quality_rows",
            "used_fallback": True,
        }
    )
    return [], diagnostics, pd.DataFrame(selection_data)
