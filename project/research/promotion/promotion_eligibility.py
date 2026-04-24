from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping

import numpy as np
import pandas as pd

from project.core.coercion import safe_float
from project.research.utils.decision_safety import coerce_numeric_nan

_OVERLAP_TOKEN_SPLIT_RE = re.compile(r"[^a-z0-9]+")
_DEPLOY_RUN_MODES = {"production", "certification", "promotion", "deploy"}


def _quiet_float(value: Any, default: float) -> float:
    if value is None or (isinstance(value, float) and not np.isfinite(value)):
        return float(default)
    coerced = safe_float(value, default)
    return float(default if coerced is None else coerced)


@dataclass
class _ReasonRecorder:
    reject_reasons: List[str]
    promo_fail_reasons: List[str]
    deploy_only_reject_reasons: List[str]
    categorized_reject_reasons: Dict[str, List[str]]
    categorized_promo_fail_reasons: Dict[str, List[str]]

    @classmethod
    def create(cls) -> "_ReasonRecorder":
        return cls(
            reject_reasons=[],
            promo_fail_reasons=[],
            deploy_only_reject_reasons=[],
            categorized_reject_reasons={},
            categorized_promo_fail_reasons={},
        )

    @staticmethod
    def _normalize_token(value: str) -> str:
        return " ".join(str(value).strip().split())

    def add_reject(
        self, reason: str, *, category: str = "uncategorized", deploy_only: bool = False
    ) -> None:
        token = self._normalize_token(reason)
        if not token:
            return
        self.reject_reasons.append(token)
        self.categorized_reject_reasons.setdefault(category, []).append(token)
        if deploy_only:
            self.deploy_only_reject_reasons.append(token)

    def add_promo_fail(self, reason: str, *, category: str = "uncategorized") -> None:
        token = self._normalize_token(reason)
        if not token:
            return
        self.promo_fail_reasons.append(token)
        self.categorized_promo_fail_reasons.setdefault(category, []).append(token)

    def add_pair(
        self,
        *,
        reject_reason: str | None = None,
        promo_fail_reason: str | None = None,
        category: str = "uncategorized",
        deploy_only: bool = False,
    ) -> None:
        if reject_reason:
            self.add_reject(reject_reason, category=category, deploy_only=deploy_only)
        if promo_fail_reason:
            self.add_promo_fail(promo_fail_reason, category=category)

    def unique_reject_reason_str(self) -> str:
        return "|".join(sorted(set(self.reject_reasons)))

    def unique_deploy_only_reject_reason_str(self) -> str:
        return "|".join(sorted(set(self.deploy_only_reject_reasons)))

    def primary_promo_fail(self) -> str:
        return self.promo_fail_reasons[0] if self.promo_fail_reasons else ""

    def categorized_reject_json(self) -> str:
        payload = {k: sorted(set(v)) for k, v in sorted(self.categorized_reject_reasons.items())}
        return json.dumps(payload, sort_keys=True)

    def categorized_promo_fail_json(self) -> str:
        payload = {
            k: sorted(set(v)) for k, v in sorted(self.categorized_promo_fail_reasons.items())
        }
        return json.dumps(payload, sort_keys=True)


def _normalized_run_mode(row: Mapping[str, Any]) -> str:
    return str(row.get("run_mode", "")).strip().lower()


def _is_deploy_mode(row: Mapping[str, Any]) -> bool:
    return _normalized_run_mode(row) in _DEPLOY_RUN_MODES


def _has_finite_numeric(row: Mapping[str, Any], key: str) -> bool:
    if key not in row:
        return False
    value = row.get(key)
    if value is None:
        return False
    if isinstance(value, bool):
        return True
    if isinstance(value, (int, float, np.integer, np.floating)):
        return bool(np.isfinite(value))
    coerced = safe_float(value, np.nan)
    return bool(np.isfinite(coerced))


def _has_explicit_oos_samples(row: Mapping[str, Any]) -> bool:
    if _has_finite_numeric(row, "validation_samples") or _has_finite_numeric(row, "test_samples"):
        return True
    if _has_finite_numeric(row, "bridge_validation_trades"):
        return True
    return ("mean_validation_return" in row) or ("mean_test_return" in row)


def sign_consistency(row: Dict[str, Any]) -> float:
    # Respect a pre-computed sign_consistency column (e.g. from phase2 bridge evaluation)
    pre_computed = row.get("sign_consistency")
    if pre_computed is not None:
        val = _quiet_float(pre_computed, np.nan)
        if np.isfinite(val):
            return float(val)

    base_effect = coerce_numeric_nan(
        row.get("effect_shrunk_state", row.get("expectancy", row.get("effect_raw")))
    )
    if np.isnan(base_effect):
        return 0.0
    base_sign = 1.0 if base_effect >= 0.0 else -1.0

    t_stats = []
    for key in ("val_t_stat", "oos1_t_stat", "test_t_stat"):
        if key in row:
            value = _quiet_float(row.get(key), np.nan)
            if np.isfinite(value):
                t_stats.append(value)
    if not t_stats:
        return 0.0

    matches = [
        1.0 if (t >= 0.0 and base_sign > 0.0) or (t < 0.0 and base_sign < 0.0) else 0.0
        for t in t_stats
    ]
    return float(sum(matches) / len(matches))


def cost_survival_ratio(row: Dict[str, Any]) -> float:
    scenario_keys = [
        "gate_after_cost_positive",
        "gate_after_cost_stressed_positive",
        "gate_bridge_after_cost_positive_validation",
        "gate_bridge_after_cost_stressed_positive_validation",
    ]
    present = [key for key in scenario_keys if key in row and pd.notna(row.get(key))]
    if not present:
        return np.nan
    passed = sum(1 for key in present if row.get(key) == "pass" or row.get(key) is True)
    return float(passed / len(present))


def control_rate_details_for_event(
    *,
    row: Dict[str, Any],
    event_type: str,
    summary: Dict[str, Any],
) -> Dict[str, Any]:
    if "control_pass_rate" in row:
        val = _quiet_float(row.get("control_pass_rate"), np.nan)
        if np.isfinite(val):
            return {"rate": float(val), "source": "candidate_row"}
    by_event = summary.get("by_event", {})
    if isinstance(by_event, dict):
        item = by_event.get(event_type)
        if isinstance(item, dict):
            for key in ("pass_rate_after_bh", "control_pass_rate", "pass_rate"):
                if key in item:
                    val = _quiet_float(item.get(key), np.nan)
                    if np.isfinite(val):
                        return {
                            "rate": float(val),
                            "source": f"summary.by_event.{event_type}.{key}",
                        }
        elif item is not None:
            val = _quiet_float(item, np.nan)
            if np.isfinite(val):
                return {"rate": float(val), "source": f"summary.by_event.{event_type}"}
    for key in ("pass_rate_after_bh", "control_pass_rate", "global_pass_rate_after_bh"):
        if key in summary:
            val = _quiet_float(summary.get(key), np.nan)
            if np.isfinite(val):
                return {"rate": float(val), "source": f"summary.{key}"}
    global_node = summary.get("global", {})
    if isinstance(global_node, dict):
        for key in ("pass_rate_after_bh", "control_pass_rate", "pass_rate"):
            if key in global_node:
                val = _quiet_float(global_node.get(key), np.nan)
                if np.isfinite(val):
                    return {"rate": float(val), "source": f"summary.global.{key}"}
    return {"rate": None, "source": "missing"}
