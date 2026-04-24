from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from project.domain.hypotheses import HypothesisSpec, TriggerSpec
from project.io.utils import write_parquet
from project.spec_registry import load_unified_event_registry


@dataclass(frozen=True)
class Hypothesis:
    """Explicit tested hypothesis metadata."""

    event_family: str
    event_type: str
    symbol_scope: str
    side: str
    horizon: str
    condition_template: str
    state_filter: str
    parameterization_id: str
    family_id: str
    cluster_id: str

    @property
    def primary_event_id(self) -> str:
        return str(self.event_type).strip().upper()

    @property
    def compat_event_family(self) -> str:
        return str(self.event_family).strip().upper()

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["primary_event_id"] = self.primary_event_id
        payload["compat_event_family"] = self.compat_event_family
        return payload

    def to_spec(self) -> HypothesisSpec:
        """Return the canonical HypothesisSpec representation."""
        ctx = (
            {"state_filter": self.state_filter}
            if self.state_filter and self.state_filter != "all"
            else None
        )
        return HypothesisSpec(
            trigger=TriggerSpec.event(self.event_type),
            direction=self.side,
            horizon=self.horizon,
            template_id=self.condition_template,
            context=ctx,
        )

    def hypothesis_id(self) -> str:
        """Deterministic ID delegating to HypothesisSpec for cross-path comparability."""
        return self.to_spec().hypothesis_id()


@lru_cache(maxsize=1)
def _event_registry_payload() -> Dict[str, Any]:
    payload = load_unified_event_registry()
    return payload if isinstance(payload, dict) else {}


def _event_spec(event_type: str) -> Dict[str, Any]:
    payload = _event_registry_payload()
    events = payload.get("events", {}) if isinstance(payload, dict) else {}
    if not isinstance(events, dict):
        return {}
    return events.get(str(event_type).strip().upper(), {}) or {}


def _canonical_family(event_type: str) -> str:
    spec = _event_spec(event_type)
    family = None
    if isinstance(spec, dict):
        family = spec.get("research_family") or spec.get("canonical_family") or spec.get("canonical_regime")
    if family:
        return str(family)
    # Harden fallback: If not in registry, use 'UNKNOWN_FAMILY' or raise to avoid 'VOL' nonsense
    return "UNKNOWN_FAMILY"


def _spec_templates(event_type: str) -> List[str]:
    spec = _event_spec(event_type)
    templates = spec.get("templates", []) if isinstance(spec, dict) else []
    if isinstance(templates, (list, tuple)):
        cleaned = [str(t).strip() for t in templates if str(t).strip()]
        if cleaned:
            return cleaned
    return ["unconditional"]


def _spec_horizons(event_type: str, requested: List[str]) -> List[str]:
    spec = _event_spec(event_type)
    horizons = spec.get("horizons", []) if isinstance(spec, dict) else []
    normalized_requested = [str(h).strip() for h in requested if str(h).strip()]
    normalized_spec = (
        [str(h).strip() for h in horizons if str(h).strip()]
        if isinstance(horizons, (list, tuple))
        else []
    )
    if normalized_spec and normalized_requested:
        overlap = [h for h in normalized_requested if h in normalized_spec]
        if overlap:
            return overlap
    if normalized_spec:
        return normalized_spec
    return normalized_requested or ["15m"]


def _template_side(template: str) -> str:
    token = str(template).strip().lower()
    continuation_like = {
        "continuation",
        "trend_following",
        "momentum",
        "breakout_follow",
        "carry_continuation",
    }
    contrarian_like = {
        "mean_reversion",
        "momentum_fade",
        "overshoot_repair",
        "stop_run_repair",
        "fade",
        "reversal",
    }
    if token in continuation_like:
        return "long"
    if token in contrarian_like:
        return "short"
    return "both"


class HypothesisRegistry:
    """Registry for managing the searched hypothesis set."""

    def __init__(self):
        self.hypotheses: Dict[str, Hypothesis] = {}

    def register(self, hyp: Hypothesis) -> str:
        hyp_id = hyp.hypothesis_id()
        self.hypotheses[hyp_id] = hyp
        return hyp_id

    def to_dataframe(self) -> pd.DataFrame:
        rows = []
        for hid, hyp in self.hypotheses.items():
            row = hyp.to_dict()
            row["hypothesis_id"] = hid
            rows.append(row)
        if not rows:
            return pd.DataFrame(
                columns=[
                    "primary_event_id",
                    "compat_event_family",
                    "event_family",
                    "event_type",
                    "symbol_scope",
                    "side",
                    "horizon",
                    "condition_template",
                    "state_filter",
                    "parameterization_id",
                    "family_id",
                    "cluster_id",
                    "hypothesis_id",
                ]
            )
        return pd.DataFrame(rows)

    def write_artifacts(self, out_dir: Path) -> str:
        """Write registry and return its content hash."""
        out_dir.mkdir(parents=True, exist_ok=True)
        df = self.to_dataframe()
        write_parquet(df, out_dir / "hypothesis_registry.parquet")

        # Compute registry hash
        payload = df.sort_values("hypothesis_id").to_json(orient="records").encode("utf-8")
        reg_hash = hashlib.sha256(payload).hexdigest()
        (out_dir / "hypothesis_registry_hash.txt").write_text(reg_hash)

        return reg_hash


def generate_discovery_registry(
    event_types: List[str],
    symbols: List[str],
    horizons: List[str],
) -> HypothesisRegistry:
    """Pre-populate a discovery registry from declared event-spec templates.

    This avoids emitting template placeholders like side="both" and keeps the
    registry aligned with the actual event ontology used by discovery.
    """
    registry = HypothesisRegistry()
    for et in event_types:
        event_family = _canonical_family(et)
        event_horizons = _spec_horizons(et, horizons)
        templates = _spec_templates(et)
        for sym in symbols:
            for h in event_horizons:
                for template in templates:
                    side = _template_side(template)
                    # Mapping 'conditional' to 'both' to satisfy VALID_DIRECTIONS
                    if side == "conditional":
                        side = "both"
                    cluster_suffix = template if side != "conditional" else "mixed"
                    hyp = Hypothesis(
                        event_family=event_family,
                        event_type=str(et).strip().upper(),
                        symbol_scope=str(sym).strip().upper(),
                        side=side,
                        horizon=h,
                        condition_template=template,
                        state_filter="all",
                        parameterization_id="registry_v1",
                        family_id=f"fam_{event_family}_{et}_{template}_{h}",
                        cluster_id=f"cluster_{event_family}_{cluster_suffix}",
                    )
                    registry.register(hyp)
    return registry
