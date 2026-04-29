from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class FeatureDefinition:
    name: str
    definition: str
    dependencies: tuple[str, ...]
    units: str
    source_stage: str
    causal: bool = True
    owner: str = "project.pipelines.features.build_features"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["dependencies"] = list(self.dependencies)
        return payload


_FEATURE_DEFINITIONS: dict[str, FeatureDefinition] = {}


def register_feature_definition(definition: FeatureDefinition) -> None:
    _FEATURE_DEFINITIONS[definition.name] = definition


def get_feature_definition(name: str) -> FeatureDefinition | None:
    return _FEATURE_DEFINITIONS.get(name)


def list_feature_definitions() -> list[FeatureDefinition]:
    return [_FEATURE_DEFINITIONS[key] for key in sorted(_FEATURE_DEFINITIONS)]


def has_feature_definition(name: str) -> bool:
    return name in _FEATURE_DEFINITIONS


def ensure_core_feature_definitions_registered() -> None:
    for definition in (
        FeatureDefinition(
            name="basis_bps",
            definition="Perp versus spot basis in basis points.",
            dependencies=("close", "spot_close"),
            units="bps",
            source_stage="build_features",
        ),
        FeatureDefinition(
            name="basis_zscore",
            definition="Rolling z-score of perp versus spot basis.",
            dependencies=("basis_bps",),
            units="zscore",
            source_stage="build_features",
        ),
        FeatureDefinition(
            name="spread_bps",
            definition="Estimated spread in basis points from microstructure inputs.",
            dependencies=("close", "high", "low", "volume"),
            units="bps",
            source_stage="build_features",
        ),
        FeatureDefinition(
            name="spread_zscore",
            definition="Rolling z-score of spread_bps.",
            dependencies=("spread_bps",),
            units="zscore",
            source_stage="build_features",
        ),
        FeatureDefinition(
            name="rv_96",
            definition="Lagged rolling realized volatility over the canonical 96-bar window.",
            dependencies=("close",),
            units="volatility",
            source_stage="build_features",
        ),
        FeatureDefinition(
            name="rv_pct_17280",
            definition="Lagged rolling percentile rank of rv_96 over the long lookback window.",
            dependencies=("rv_96",),
            units="percentile",
            source_stage="build_features",
        ),
        FeatureDefinition(
            name="funding_rate_scaled",
            definition="Canonical funding rate aligned to bar timestamps.",
            dependencies=("funding_rate", "timestamp"),
            units="decimal_rate",
            source_stage="build_features",
        ),
        FeatureDefinition(
            name="funding_abs_pct",
            definition="Lagged rolling percentile rank of absolute funding magnitude.",
            dependencies=("funding_rate_scaled",),
            units="percentile",
            source_stage="build_features",
        ),
        FeatureDefinition(
            name="imbalance",
            definition="Buy versus sell pressure imbalance from synthetic or microstructure inputs.",
            dependencies=("taker_base_volume", "volume"),
            units="ratio",
            source_stage="build_features",
        ),
        FeatureDefinition(
            name="micro_depth_depletion",
            definition="Depth-depletion proxy for stressed microstructure conditions.",
            dependencies=("volume", "spread_bps"),
            units="ratio",
            source_stage="build_features",
        ),
    ):
        register_feature_definition(definition)


def ensure_market_context_feature_definitions_registered() -> None:
    for definition in (
        FeatureDefinition(
            name="ms_vol_state",
            definition="Canonical volatility state code derived from realized-vol percentile bands.",
            dependencies=("rv_pct_17280",),
            units="state_code",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="ms_liq_state",
            definition="Canonical liquidity state code derived from rolling quote-volume percentile bands.",
            dependencies=("quote_volume",),
            units="state_code",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="ms_oi_state",
            definition="Canonical open-interest state code derived from trailing oi_delta_1h behavior.",
            dependencies=("oi_delta_1h",),
            units="state_code",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="ms_funding_state",
            definition="Canonical funding state code derived from signed funding persistence and extremes.",
            dependencies=("funding_rate_bps",),
            units="state_code",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="ms_trend_state",
            definition="Canonical trend state code derived from rolling returns normalized by realized volatility.",
            dependencies=("logret_1",),
            units="state_code",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="ms_spread_state",
            definition="Canonical spread state code derived from spread_zscore.",
            dependencies=("spread_zscore",),
            units="state_code",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="macro_regime",
            definition="Multi-month trend cycle label: 0=flat, 1=bull (close >5% above 90d SMA), 2=bear (close >5% below 90d SMA). Identifies macro bear/bull cycles that ms_trend_state (30-day window) misses.",
            dependencies=("close",),
            units="state_code",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="ms_context_state_code",
            definition="Encoded composite market-state code across vol, liquidity, OI, funding, trend, and spread dimensions.",
            dependencies=(
                "ms_vol_state",
                "ms_liq_state",
                "ms_oi_state",
                "ms_funding_state",
                "ms_trend_state",
                "ms_spread_state",
            ),
            units="state_code",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="prob_vol_low",
            definition="Probability that the volatility regime is LOW.",
            dependencies=("rv_pct_17280",),
            units="probability",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="prob_vol_mid",
            definition="Probability that the volatility regime is MID.",
            dependencies=("rv_pct_17280",),
            units="probability",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="prob_vol_high",
            definition="Probability that the volatility regime is HIGH.",
            dependencies=("rv_pct_17280",),
            units="probability",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="prob_vol_shock",
            definition="Probability that the volatility regime is SHOCK.",
            dependencies=("rv_pct_17280",),
            units="probability",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="ms_vol_confidence",
            definition="Confidence of the canonical volatility regime classification.",
            dependencies=("prob_vol_low", "prob_vol_mid", "prob_vol_high", "prob_vol_shock"),
            units="probability",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="ms_vol_entropy",
            definition="Normalized entropy of the volatility regime probability distribution.",
            dependencies=("prob_vol_low", "prob_vol_mid", "prob_vol_high", "prob_vol_shock"),
            units="entropy",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="prob_liq_thin",
            definition="Probability that the liquidity regime is THIN.",
            dependencies=("quote_volume",),
            units="probability",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="prob_liq_normal",
            definition="Probability that the liquidity regime is NORMAL.",
            dependencies=("quote_volume",),
            units="probability",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="prob_liq_flush",
            definition="Probability that the liquidity regime is FLUSH.",
            dependencies=("quote_volume",),
            units="probability",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="ms_liq_confidence",
            definition="Confidence of the canonical liquidity regime classification.",
            dependencies=("prob_liq_thin", "prob_liq_normal", "prob_liq_flush"),
            units="probability",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="ms_liq_entropy",
            definition="Normalized entropy of the liquidity regime probability distribution.",
            dependencies=("prob_liq_thin", "prob_liq_normal", "prob_liq_flush"),
            units="entropy",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="prob_oi_decel",
            definition="Probability that the OI regime is DECEL.",
            dependencies=("oi_delta_1h",),
            units="probability",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="prob_oi_stable",
            definition="Probability that the OI regime is STABLE.",
            dependencies=("oi_delta_1h",),
            units="probability",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="prob_oi_accel",
            definition="Probability that the OI regime is ACCEL.",
            dependencies=("oi_delta_1h",),
            units="probability",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="ms_oi_confidence",
            definition="Confidence of the canonical OI regime classification.",
            dependencies=("prob_oi_decel", "prob_oi_stable", "prob_oi_accel"),
            units="probability",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="ms_oi_entropy",
            definition="Normalized entropy of the OI regime probability distribution.",
            dependencies=("prob_oi_decel", "prob_oi_stable", "prob_oi_accel"),
            units="entropy",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="prob_funding_neutral",
            definition="Probability that the funding regime is NEUTRAL.",
            dependencies=("funding_rate_bps",),
            units="probability",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="prob_funding_persistent",
            definition="Probability that the funding regime is PERSISTENT.",
            dependencies=("funding_rate_bps",),
            units="probability",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="prob_funding_extreme",
            definition="Probability that the funding regime is EXTREME.",
            dependencies=("funding_rate_bps",),
            units="probability",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="ms_funding_confidence",
            definition="Confidence of the canonical funding regime classification.",
            dependencies=(
                "prob_funding_neutral",
                "prob_funding_persistent",
                "prob_funding_extreme",
            ),
            units="probability",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="ms_funding_entropy",
            definition="Normalized entropy of the funding regime probability distribution.",
            dependencies=(
                "prob_funding_neutral",
                "prob_funding_persistent",
                "prob_funding_extreme",
            ),
            units="entropy",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="prob_trend_chop",
            definition="Probability that the trend regime is CHOP.",
            dependencies=("logret_1",),
            units="probability",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="prob_trend_bull",
            definition="Probability that the trend regime is BULL.",
            dependencies=("logret_1",),
            units="probability",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="prob_trend_bear",
            definition="Probability that the trend regime is BEAR.",
            dependencies=("logret_1",),
            units="probability",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="ms_trend_confidence",
            definition="Confidence of the canonical trend regime classification.",
            dependencies=("prob_trend_chop", "prob_trend_bull", "prob_trend_bear"),
            units="probability",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="ms_trend_entropy",
            definition="Normalized entropy of the trend regime probability distribution.",
            dependencies=("prob_trend_chop", "prob_trend_bull", "prob_trend_bear"),
            units="entropy",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="prob_spread_tight",
            definition="Probability that the spread regime is TIGHT.",
            dependencies=("spread_zscore",),
            units="probability",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="prob_spread_wide",
            definition="Probability that the spread regime is WIDE.",
            dependencies=("spread_zscore",),
            units="probability",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="ms_spread_confidence",
            definition="Confidence of the canonical spread regime classification.",
            dependencies=("prob_spread_tight", "prob_spread_wide"),
            units="probability",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="ms_spread_entropy",
            definition="Normalized entropy of the spread regime probability distribution.",
            dependencies=("prob_spread_tight", "prob_spread_wide"),
            units="entropy",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="fp_active",
            definition="Canonical funding-persistence active flag from the shared funding persistence feature surface.",
            dependencies=("funding_rate_scaled",),
            units="flag",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="fp_age_bars",
            definition="Age in bars since the funding persistence state became active.",
            dependencies=("fp_active",),
            units="bars",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="fp_severity",
            definition="Canonical funding persistence severity from the shared funding persistence feature surface.",
            dependencies=("funding_rate_scaled",),
            units="score",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="funding_rate_bps",
            definition="Funding rate in basis points derived from funding_rate_scaled.",
            dependencies=("funding_rate_scaled",),
            units="bps",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="carry_state_code",
            definition="Signed carry regime marker: positive funding is +1, negative funding is -1.",
            dependencies=("funding_rate_scaled",),
            units="state_code",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="funding_persistence_state",
            definition="Indicator that signed funding has persisted for the configured run-length window.",
            dependencies=("funding_rate_scaled",),
            units="flag",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="high_vol_regime",
            definition="Indicator that realized volatility is in the configured high-vol percentile band.",
            dependencies=("rv_pct_17280",),
            units="flag",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="low_vol_regime",
            definition="Indicator that realized volatility is in the configured low-vol percentile band.",
            dependencies=("rv_pct_17280",),
            units="flag",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="spread_elevated_state",
            definition="Indicator that spread_zscore exceeds the elevated-spread threshold.",
            dependencies=("spread_zscore",),
            units="flag",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="refill_lag_state",
            definition="Indicator that open interest is falling and liquidity refill is lagging.",
            dependencies=("oi_delta_1h",),
            units="flag",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="deleveraging_state",
            definition="Indicator that open-interest decline is large enough to imply active deleveraging.",
            dependencies=("oi_delta_1h", "oi_notional"),
            units="flag",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="aftershock_state",
            definition="Indicator that high-vol and elevated-spread states are both active.",
            dependencies=("high_vol_regime", "spread_elevated_state"),
            units="flag",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="compression_state_flag",
            definition="Indicator that volatility is low and spread is not elevated.",
            dependencies=("low_vol_regime", "spread_elevated_state"),
            units="flag",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="crowding_state",
            definition="Indicator that open interest is elevated while funding remains positive.",
            dependencies=("oi_notional", "funding_rate_scaled"),
            units="flag",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="bull_trend_regime",
            definition="Indicator that rolling returns exceed rolling volatility on the upside.",
            dependencies=("logret_1",),
            units="flag",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="bear_trend_regime",
            definition="Indicator that rolling returns exceed rolling volatility on the downside.",
            dependencies=("logret_1",),
            units="flag",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="chop_regime",
            definition="Indicator that rolling returns stay within the rolling volatility band.",
            dependencies=("logret_1",),
            units="flag",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
        FeatureDefinition(
            name="ms_liquidation_state",
            definition="Indicator that rolling liquidation pressure is elevated versus recent history.",
            dependencies=("liquidation_notional",),
            units="flag",
            source_stage="build_market_context",
            owner="project.pipelines.features.build_market_context",
        ),
    ):
        register_feature_definition(definition)
