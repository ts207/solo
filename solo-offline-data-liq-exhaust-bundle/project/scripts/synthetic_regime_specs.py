from typing import Any, Dict, List, Tuple

REGIME_SEQUENCE = (
    "basis_desync",
    "funding_dislocation",
    "trend_acceleration_exhaustion",
    "breakout_failure",
    "liquidity_stress",
    "deleveraging_burst",
    "post_deleveraging_rebound",
)

REGIME_EXPECTATIONS: Dict[str, Dict[str, Any]] = {
    "basis_desync": {
        "intended_effect_direction": "desync_signaled",
        "expected_event_types": [
            "CROSS_VENUE_DESYNC",
            "BASIS_DISLOC",
            "SPOT_PERP_BASIS_SHOCK",
        ],
        "expected_detector_families": [
            "cross_venue_desync",
            "basis_dislocation",
            "information_desync",
        ],
    },
    "funding_dislocation": {
        "intended_effect_direction": "funding_extreme_signaled",
        "expected_event_types": [
            "CROSS_VENUE_DESYNC",
            "FND_DISLOC",
            "FUNDING_NORMALIZATION_TRIGGER",
        ],
        "supporting_event_types": [
            "FUNDING_FLIP",
        ],
        "expected_detector_families": [
            "funding_dislocation",
            "positioning_extremes",
        ],
    },
    "trend_acceleration_exhaustion": {
        "intended_effect_direction": "trend_then_reversal",
        "expected_event_types": [
            "TREND_ACCELERATION",
        ],
        "supporting_event_types": [
            "TREND_EXHAUSTION_TRIGGER",
            "MOMENTUM_DIVERGENCE_TRIGGER",
        ],
        "expected_detector_families": [
            "trend_structure",
            "forced_flow_and_exhaustion",
        ],
    },
    "breakout_failure": {
        "intended_effect_direction": "failed_breakout_reversal",
        "expected_event_types": [
            "FALSE_BREAKOUT",
            "BREAKOUT_TRIGGER",
            "FAILED_CONTINUATION",
        ],
        "expected_detector_families": [
            "trend_structure",
            "volatility_transition",
            "forced_flow_and_exhaustion",
        ],
    },
    "liquidity_stress": {
        "intended_effect_direction": "liquidity_deterioration",
        "expected_event_types": [
            "LIQUIDITY_STRESS_DIRECT",
            "LIQUIDITY_STRESS_PROXY",
        ],
        "supporting_event_types": [
            "ABSORPTION_PROXY",
            "DEPTH_STRESS_PROXY",
            "PRICE_VOL_IMBALANCE_PROXY",
            "SPREAD_REGIME_WIDENING_EVENT",
        ],
        "expected_detector_families": [
            "liquidity_shock",
            "liquidity_dislocation",
            "temporal",
        ],
    },
    "deleveraging_burst": {
        "intended_effect_direction": "forced_deleveraging",
        "expected_event_types": [
            "DELEVERAGING_WAVE",
            "OI_FLUSH",
            "FORCED_FLOW_EXHAUSTION",
        ],
        "supporting_event_types": [
            "CLIMAX_VOLUME_BAR",
        ],
        "expected_detector_families": [
            "positioning_extremes",
            "forced_flow_and_exhaustion",
        ],
    },
    "post_deleveraging_rebound": {
        "intended_effect_direction": "rebound_after_deleveraging",
        "expected_event_types": [
            "POST_DELEVERAGING_REBOUND",
            "LIQUIDATION_EXHAUSTION_REVERSAL",
        ],
        "expected_detector_families": [
            "forced_flow_and_exhaustion",
        ],
    },
}

PROFILE_SETTINGS: Dict[str, Dict[str, Any]] = {
    "default": {
        "noise_mult": 1.0,
        "drift_mult": 1.0,
        "basis_wave_mult": 1.0,
        "spread_mult": 1.0,
        "volume_mult": 1.0,
        "oi_mult": 1.0,
        "regime_amplitude_mult": 1.0,
        "schedule_cycle_days": 60,
        "price_anchor": {"BTCUSDT": 95_000.0, "ETHUSDT": 3_200.0, "SOLUSDT": 145.0},
    },
    "2021_bull": {
        "noise_mult": 1.85,
        "drift_mult": 1.45,
        "basis_wave_mult": 1.2,
        "spread_mult": 0.92,
        "volume_mult": 1.35,
        "oi_mult": 1.20,
        "regime_amplitude_mult": 1.15,
        "schedule_cycle_days": 54,
        "price_anchor": {"BTCUSDT": 35_000.0, "ETHUSDT": 2_200.0, "SOLUSDT": 45.0},
    },
    "range_chop": {
        "noise_mult": 1.15,
        "drift_mult": 0.35,
        "basis_wave_mult": 0.75,
        "spread_mult": 1.1,
        "volume_mult": 0.85,
        "oi_mult": 0.92,
        "regime_amplitude_mult": 0.80,
        "schedule_cycle_days": 42,
        "price_anchor": {"BTCUSDT": 68_000.0, "ETHUSDT": 2_950.0, "SOLUSDT": 135.0},
    },
    "stress_crash": {
        "noise_mult": 2.35,
        "drift_mult": 0.55,
        "basis_wave_mult": 1.45,
        "spread_mult": 1.65,
        "volume_mult": 1.40,
        "oi_mult": 0.82,
        "regime_amplitude_mult": 1.45,
        "schedule_cycle_days": 36,
        "price_anchor": {"BTCUSDT": 58_000.0, "ETHUSDT": 2_850.0, "SOLUSDT": 118.0},
    },
    "alt_rotation": {
        "noise_mult": 1.45,
        "drift_mult": 1.05,
        "basis_wave_mult": 1.25,
        "spread_mult": 1.18,
        "volume_mult": 1.55,
        "oi_mult": 1.10,
        "regime_amplitude_mult": 1.20,
        "schedule_cycle_days": 48,
        "price_anchor": {"BTCUSDT": 82_000.0, "ETHUSDT": 4_100.0, "SOLUSDT": 185.0},
    },
}


def resolve_regime_offsets(symbol: str) -> Dict[str, List[Tuple[int, int, int, float]]]:
    if symbol.upper() == "BTCUSDT":
        return {
            "basis_desync": [
                (4, 12, 1, 42.0),
                (18, 10, -1, 38.0),
                (34, 14, 1, 48.0),
                (49, 10, -1, 44.0),
            ],
            "funding_dislocation": [
                (8, 16, 1, 1.8),
                (24, 16, -1, 2.0),
                (40, 16, 1, 1.9),
                (54, 12, -1, 1.7),
            ],
            "trend_acceleration_exhaustion": [
                (12, 18, 1, 1.0),
                (28, 18, -1, 1.1),
                (44, 18, 1, 0.9),
            ],
            "breakout_failure": [(15, 10, 1, 1.3), (31, 10, -1, 1.2), (47, 10, 1, 1.4)],
            "liquidity_stress": [(20, 8, 1, 1.0), (36, 8, -1, 1.0), (52, 8, 1, 1.1)],
            "deleveraging_burst": [
                (10, 8, -1, 1.3),
                (26, 8, -1, 1.4),
                (42, 8, -1, 1.2),
                (56, 6, -1, 1.1),
            ],
        }
    return {
        "basis_desync": [
            (6, 12, -1, 45.0),
            (21, 10, 1, 50.0),
            (37, 14, -1, 55.0),
            (51, 10, 1, 49.0),
        ],
        "funding_dislocation": [
            (9, 16, -1, 1.7),
            (25, 16, 1, 1.8),
            (41, 16, -1, 1.9),
            (55, 12, 1, 1.6),
        ],
        "trend_acceleration_exhaustion": [(13, 18, -1, 1.2), (29, 18, 1, 1.3), (45, 18, -1, 1.15)],
        "breakout_failure": [(17, 10, -1, 1.4), (33, 10, 1, 1.45), (48, 10, -1, 1.55)],
        "liquidity_stress": [(22, 8, 1, 1.0), (38, 8, -1, 1.0), (53, 8, 1, 1.0)],
        "deleveraging_burst": [
            (11, 8, -1, 1.0),
            (27, 8, -1, 1.1),
            (43, 8, -1, 1.0),
            (57, 6, -1, 0.9),
        ],
    }
