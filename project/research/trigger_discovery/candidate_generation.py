from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple

import pandas as pd

from project.domain.hypotheses import HypothesisSpec, TriggerSpec
from project.research.trigger_discovery.candidate_clustering import (
    cluster_excursions,
    extract_excursions,
)

log = logging.getLogger(__name__)


@dataclass
class TriggerFeatureColumns:
    """Named container for synthetic boolean trigger columns produced by trigger discovery.

    Returned alongside proposals from ``generate_parameter_sweep`` and
    ``generate_feature_clusters``.  Callers that need an augmented feature
    DataFrame should reconstruct it explicitly using
    ``apply_to_features(features)``, rather than receiving a mutated copy.

    This design prevents the "mutated frame anti-pattern" where a caller
    accidentally passes the augmented frame back into the main discovery
    pipeline, which would inject synthetic trigger columns that have no
    counterpart in the live feature pipeline.
    """

    columns: Dict[str, pd.Series] = field(default_factory=dict)

    def apply_to_features(self, features: pd.DataFrame) -> pd.DataFrame:
        """Return a *copy* of *features* with the trigger columns appended.

        The original *features* frame is never modified.  The returned frame
        should only be used within the trigger-discovery evaluation path.
        """
        if not self.columns:
            return features.copy()
        out = features.copy()
        for col_name, series in self.columns.items():
            out[col_name] = series.reindex(out.index).fillna(False).astype(bool)
        return out

    def column_names(self) -> List[str]:
        """Return the list of injected column names."""
        return list(self.columns.keys())

    def __getitem__(self, key: str) -> pd.Series:
        return self.columns[key]

    def __contains__(self, key: str) -> bool:
        return key in self.columns


class TriggerProposal:
    """Wrapper that tracks a mined candidate trigger along with its metadata."""
    def __init__(
        self,
        candidate_trigger_id: str,
        source_lane: str,
        detector_family: str,
        parameterization: Dict[str, Any],
        dominant_features: List[str] = None,
        suggested_trigger_name: str = "",
        spec: HypothesisSpec = None,
    ):
        self.candidate_trigger_id = candidate_trigger_id
        self.source_lane = source_lane
        self.detector_family = detector_family
        self.parameterization = parameterization
        self.dominant_features = dominant_features or []
        self.suggested_trigger_name = suggested_trigger_name
        self.spec = spec


def generate_parameter_sweep(
    features: pd.DataFrame,
    family_grid: Dict[str, Dict[str, List[float]]],
    base_template_id: str = "continuation",
    base_direction: str = "long",
    base_horizon: str = "12b"
) -> Tuple[List[TriggerProposal], TriggerFeatureColumns]:
    """Lane A: Generate parameterized threshold masks for known detector proxies.

    Returns proposals and a :class:`TriggerFeatureColumns` container holding
    the synthetic boolean columns produced for each proposal.

    Callers that need an augmented DataFrame for evaluation should call
    ``trigger_feature_cols.apply_to_features(features)`` explicitly.  This
    prevents the caller from accidentally passing augmented features containing
    synthetic columns back into the main discovery pipeline.

    Parameters
    ----------
    features:
        The canonical feature table for the symbol being researched.
    family_grid:
        Dict mapping detector family names to parameter grids.
    base_template_id, base_direction, base_horizon:
        Hypothesis defaults applied to every generated proposal.

    Returns
    -------
    proposals:
        List of :class:`TriggerProposal` objects, one per parameterization.
    trigger_cols:
        :class:`TriggerFeatureColumns` container holding the injected boolean
        Series for each proposal.  Use ``.apply_to_features(features)`` to
        materialise an augmented DataFrame for evaluation.
    """
    if features.empty:
        return [], TriggerFeatureColumns()

    proposals = []
    injected: Dict[str, pd.Series] = {}

    # We construct pseudo-detectors directly against raw numeric columns
    # Example family: "vol_shock" mapping to testing thresholds over "realized_vol"

    for family, grid in family_grid.items():
        if family == "vol_shock":
            base_col = next((c for c in features.columns if "rv" in c.lower() or "vol" in c.lower() and "shock" not in c.lower()), None)
            if not base_col:
                log.warning("No realized volatility proxy feature found for vol_shock sweep")
                continue

            thresholds = grid.get("z_threshold", [2.0])
            for count, z in enumerate(thresholds):
                pseudo_event_id = f"PROPOSED_VOL_SHOCK_Z{str(z).replace('.', 'p')}"
                mask_col = f"{pseudo_event_id.upper()}_EVENT"

                # Approximate dynamic threshold logic
                series = pd.to_numeric(features[base_col], errors="coerce").fillna(0.0)
                rm = series.rolling(288, min_periods=20).mean()
                rs = series.rolling(288, min_periods=20).std().replace(0, 1e-9)
                z_series = (series - rm) / rs

                # Onset crossing
                onset = (z_series >= z) & (z_series.shift(1, fill_value=0.0) < z)
                injected[mask_col] = onset.fillna(False).astype(bool)

                spec = HypothesisSpec(
                    trigger=TriggerSpec.feature_predicate(feature=mask_col, operator="==", threshold=1.0),
                    direction=base_direction,
                    horizon=base_horizon,
                    template_id=base_template_id
                )

                prop = TriggerProposal(
                    candidate_trigger_id=f"cand_{pseudo_event_id.lower()}",
                    source_lane="parameter_sweep",
                    detector_family=family,
                    parameterization={"z_threshold": z},
                    suggested_trigger_name=pseudo_event_id,
                    spec=spec
                )
                proposals.append(prop)

    return proposals, TriggerFeatureColumns(columns=injected)


def generate_feature_clusters(
    features: pd.DataFrame,
    target_columns: List[str],
    min_support: int = 5,
    base_template_id: str = "continuation",
    base_direction: str = "long",
    base_horizon: str = "12b"
) -> Tuple[List[TriggerProposal], TriggerFeatureColumns]:
    """Lane B: Mine excursions from arbitrary target feature columns and cluster them.

    Returns proposals and a :class:`TriggerFeatureColumns` container.  Callers
    must use ``.apply_to_features(features)`` to obtain an augmented DataFrame
    for evaluation — the original features are never modified here.
    """
    if features.empty or not target_columns:
        return [], TriggerFeatureColumns()

    injected: Dict[str, pd.Series] = {}

    excursions_df = extract_excursions(features, target_columns, threshold_z=2.5, min_persistence=1)
    if excursions_df.empty:
        return [], TriggerFeatureColumns()

    clusters = cluster_excursions(excursions_df, target_columns, min_support=min_support)

    proposals = []

    for clst in clusters:
        candidate_trigger_id = clst["candidate_cluster_id"]
        dom = clst["dominant_features"]
        suggested_name = clst["suggested_trigger_family"]

        # Build the intersection mask for this cluster
        mask_col = f"PROPOSED_{candidate_trigger_id.upper()}_EVENT"
        mask = excursions_df[dom].all(axis=1)
        injected[mask_col] = mask.astype(bool)

        spec = HypothesisSpec(
            trigger=TriggerSpec.feature_predicate(feature=mask_col, operator="==", threshold=1.0),
            direction=base_direction,
            horizon=base_horizon,
            template_id=base_template_id
        )

        prop = TriggerProposal(
            candidate_trigger_id=candidate_trigger_id,
            source_lane="feature_cluster",
            detector_family="excursion_cluster",
            parameterization={"source_columns": dom, "threshold_z": 2.5},
            dominant_features=dom,
            suggested_trigger_name=suggested_name,
            spec=spec
        )
        proposals.append(prop)

    return proposals, TriggerFeatureColumns(columns=injected)
