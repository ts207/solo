from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from project.research.context_labels import expand_dimension_values

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_ROOT = REPO_ROOT / "data"
DEFAULT_OUTPUT_ROOT = DEFAULT_DATA_ROOT / "reports" / "regime_baselines"
SCHEMA_VERSION = "regime_baseline_v1"
SEARCH_BURDEN_SCHEMA_VERSION = "regime_search_burden_v1"
DEFAULT_OVERLAP_POLICY = "suppress"
DEFAULT_TIMEFRAME = "5m"
LOGGER = logging.getLogger(__name__)

CORE_V1_REGIMES: tuple[dict[str, str], ...] = (
    {"vol_regime": "high"},
    {"vol_regime": "low"},
    {"carry_state": "funding_neg"},
    {"carry_state": "funding_pos"},
    {"vol_regime": "high", "carry_state": "funding_neg"},
    {"vol_regime": "high", "carry_state": "funding_pos"},
    {"ms_trend_state": "bullish"},
    {"ms_trend_state": "bearish"},
    {"ms_trend_state": "chop"},
)

FUNDING_SQUEEZE_POSITIONING_V1_REGIMES: tuple[dict[str, str], ...] = (
    {
        "carry_state": "funding_neg",
        "vol_regime": "high",
        "oi_phase": "expansion",
        "price_oi_quadrant": "price_down_oi_up",
    },
    {"funding_phase": "negative_persistent", "oi_phase": "expansion"},
    {"funding_phase": "negative_onset", "oi_phase": "expansion"},
    {"funding_regime": "crowded", "oi_phase": "expansion"},
    {"carry_state": "funding_neg", "oi_phase": "expansion", "ms_trend_state": "bearish"},
)

FUNDING_SQUEEZE_POSITIONING_V1_PROPOSAL_ELIGIBLE: tuple[bool, ...] = (
    True,
    False,
    False,
    False,
    False,
)

FORCED_FLOW_CRISIS_V1_REGIMES: tuple[dict[str, str], ...] = (
    {"vol_regime": "high", "carry_state": "funding_neg", "ms_trend_state": "bearish"},
    {"vol_regime": "high", "ms_trend_state": "bearish"},
    {"carry_state": "funding_neg", "ms_trend_state": "bearish"},
    {"vol_regime": "high", "carry_state": "funding_neg"},
)

FORCED_FLOW_CRISIS_V1_PROPOSAL_ELIGIBLE: tuple[bool, ...] = (
    True,
    False,
    False,
    False,
)

VOLATILITY_COMPRESSION_RELEASE_V1_REGIMES: tuple[dict[str, str], ...] = (
    {
        "vol_phase": "compressed",
        "execution_friction": "normal",
        "liquidity_phase": "normal",
        "high_vol_regime": "0.0",
    },
)

VOLATILITY_COMPRESSION_RELEASE_V1_PROPOSAL_ELIGIBLE: tuple[bool, ...] = (
    True,
)

REGIME_MATRIX_DEFINITIONS: dict[str, tuple[dict[str, str], ...]] = {
    "core_v1": CORE_V1_REGIMES,
    "funding_squeeze_positioning_v1": FUNDING_SQUEEZE_POSITIONING_V1_REGIMES,
    "forced_flow_crisis_v1": FORCED_FLOW_CRISIS_V1_REGIMES,
    "volatility_compression_release_v1": VOLATILITY_COMPRESSION_RELEASE_V1_REGIMES,
}

REGIME_MATRIX_PROPOSAL_ELIGIBILITY: dict[str, tuple[bool, ...]] = {
    "funding_squeeze_positioning_v1": FUNDING_SQUEEZE_POSITIONING_V1_PROPOSAL_ELIGIBLE,
    "forced_flow_crisis_v1": FORCED_FLOW_CRISIS_V1_PROPOSAL_ELIGIBLE,
    "volatility_compression_release_v1": VOLATILITY_COMPRESSION_RELEASE_V1_PROPOSAL_ELIGIBLE,
}

SUPPORTED_REGIME_MATRICES = set(REGIME_MATRIX_DEFINITIONS)

FORBIDDEN_LOOKAHEAD_TOKENS = (
    "rebound_confirmed",
    "recovered",
    "normalized",
    "post_",
    "cooldown",
    "refill",
)

ROW_COLUMNS = [
    "schema_version",
    "run_id",
    "matrix_id",
    "regime_id",
    "proposal_path_eligible",
    "regime_role",
    "filters",
    "symbol",
    "direction",
    "horizon_bars",
    "overlap_policy",
    "n",
    "effective_n",
    "mean_gross_bps",
    "mean_net_bps",
    "median_net_bps",
    "t_stat_net",
    "hit_rate",
    "p25_net_bps",
    "p75_net_bps",
    "max_drawdown_bps",
    "cost_bps",
    "mean_net_bps_2x_cost",
    "year_stats",
    "max_year_pnl_share",
    "positive_year_count",
    "classification",
    "decision",
    "reason",
]


@dataclass(frozen=True)
class RegimeBaselineRequest:
    run_id: str
    matrix_id: str
    symbols: tuple[str, ...]
    horizons: tuple[int, ...]
    data_root: Path = DEFAULT_DATA_ROOT
    directions: tuple[str, ...] = ("long", "short")
    source_run_id: str | None = None
    timeframe: str = DEFAULT_TIMEFRAME
    overlap_policy: str = DEFAULT_OVERLAP_POLICY


def regime_id(filters: dict[str, str]) -> str:
    return "+".join(f"{key}={value}" for key, value in filters.items())


def core_v1_matrix() -> list[dict[str, str]]:
    return [dict(item) for item in CORE_V1_REGIMES]


def funding_squeeze_positioning_v1_matrix() -> list[dict[str, str]]:
    return [dict(item) for item in FUNDING_SQUEEZE_POSITIONING_V1_REGIMES]


def forced_flow_crisis_v1_matrix() -> list[dict[str, str]]:
    return [dict(item) for item in FORCED_FLOW_CRISIS_V1_REGIMES]


def volatility_compression_release_v1_matrix() -> list[dict[str, str]]:
    return [dict(item) for item in VOLATILITY_COMPRESSION_RELEASE_V1_REGIMES]


def regime_matrix(matrix_id: str) -> list[dict[str, str]]:
    try:
        return [dict(item) for item in REGIME_MATRIX_DEFINITIONS[matrix_id]]
    except KeyError as exc:
        raise ValueError(f"Unsupported matrix_id: {matrix_id}") from exc


def proposal_path_eligible_for_matrix(matrix_id: str, index: int) -> bool:
    eligibility = REGIME_MATRIX_PROPOSAL_ELIGIBILITY.get(matrix_id)
    if eligibility is not None:
        return bool(eligibility[index])
    return True


def regime_role_for_matrix(matrix_id: str, index: int) -> str:
    if matrix_id in REGIME_MATRIX_PROPOSAL_ELIGIBILITY:
        return "primary" if proposal_path_eligible_for_matrix(matrix_id, index) else "diagnostic"
    return "primary"


def validate_regime_filters(filters: dict[str, str]) -> None:
    for dimension, value in filters.items():
        if dimension != dimension.lower() or value != value.lower():
            raise ValueError(f"Regime filter must be registry-native lowercase: {dimension}={value}")
        label = f"{dimension}={value}"
        if any(token in label for token in FORBIDDEN_LOOKAHEAD_TOKENS):
            raise ValueError(f"Regime filter uses lookahead-prone label: {label}")


def _run_market_context_paths(
    data_root: Path,
    *,
    run_id: str,
    symbol: str,
    timeframe: str,
) -> list[Path]:
    base = data_root / "lake" / "runs" / run_id / "features" / "perp" / symbol / timeframe / "market_context"
    if not base.exists():
        return []
    return sorted(base.glob("year=*/month=*/*.parquet"))


def discover_market_context_run(
    data_root: Path,
    *,
    symbols: tuple[str, ...],
    timeframe: str = DEFAULT_TIMEFRAME,
) -> str | None:
    runs_dir = data_root / "lake" / "runs"
    if not runs_dir.exists():
        return None
    scored: list[tuple[int, float, str]] = []
    for run_dir in runs_dir.iterdir():
        if not run_dir.is_dir():
            continue
        count = sum(
            len(
                _run_market_context_paths(
                    data_root,
                    run_id=run_dir.name,
                    symbol=symbol,
                    timeframe=timeframe,
                )
            )
            for symbol in symbols
        )
        if count <= 0:
            continue
        scored.append((count, run_dir.stat().st_mtime, run_dir.name))
    if not scored:
        return None
    return max(scored)[2]


def load_market_context(
    data_root: Path,
    *,
    source_run_id: str,
    symbol: str,
    timeframe: str = DEFAULT_TIMEFRAME,
) -> pd.DataFrame:
    paths = _run_market_context_paths(
        data_root,
        run_id=source_run_id,
        symbol=symbol,
        timeframe=timeframe,
    )
    if not paths:
        return pd.DataFrame()
    frames: list[pd.DataFrame] = []
    for path in paths:
        try:
            frames.append(pd.read_parquet(path))
        except Exception as exc:
            LOGGER.warning("Could not read market context parquet %s: %s", path, exc)
            continue
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
    return df.reset_index(drop=True)


def _cost_series(df: pd.DataFrame) -> tuple[pd.Series | None, str]:
    for column in ("cost_bps", "effective_cost_bps", "estimated_cost_bps", "avg_dynamic_cost_bps"):
        if column in df.columns:
            return pd.to_numeric(df[column], errors="coerce").clip(lower=0.0), column
    if "spread_bps" in df.columns:
        return pd.to_numeric(df["spread_bps"], errors="coerce").clip(lower=0.0), "spread_bps"
    return None, ""


def _context_mask(df: pd.DataFrame, filters: dict[str, str]) -> tuple[pd.Series | None, str]:
    mask = pd.Series(True, index=df.index)
    for dimension, value in filters.items():
        if dimension not in df.columns:
            return None, f"missing context columns: {dimension}"
        candidates = expand_dimension_values(dimension, [value])
        series = df[dimension]
        candidate_strings = {str(item).strip().lower() for item in candidates}
        numeric_candidates = pd.to_numeric(pd.Series(candidates), errors="coerce").dropna()
        current = series.astype(str).str.strip().str.lower().isin(candidate_strings)
        if not numeric_candidates.empty:
            numeric_series = pd.to_numeric(series, errors="coerce")
            current = current | numeric_series.isin(numeric_candidates.tolist())
        mask = mask & current.fillna(False)
    return mask, ""


def _suppress_overlap(df: pd.DataFrame, horizon_bars: int) -> pd.DataFrame:
    if df.empty:
        return df
    selected_positions: list[int] = []
    last_pos = -10**12
    for pos in df["_pos"].tolist():
        pos_int = int(pos)
        if pos_int - last_pos >= int(horizon_bars):
            selected_positions.append(pos_int)
            last_pos = pos_int
    return df[df["_pos"].isin(selected_positions)].copy()


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    return numeric


def _t_stat(values: pd.Series) -> float | None:
    vals = pd.to_numeric(values, errors="coerce").dropna()
    if len(vals) < 2:
        return None
    std = float(vals.std(ddof=1))
    if std <= 0:
        return None
    return float(vals.mean()) / (std / math.sqrt(len(vals)))


def _max_drawdown_bps(values: pd.Series) -> float | None:
    vals = pd.to_numeric(values, errors="coerce").dropna()
    if vals.empty:
        return None
    curve = vals.cumsum()
    drawdown = curve - curve.cummax()
    return float(drawdown.min())


def _year_stats(df: pd.DataFrame) -> tuple[dict[str, dict[str, float | int | None]], float | None, int]:
    if df.empty or "timestamp" not in df.columns:
        return {}, None, 0
    working = df.copy()
    working["year"] = pd.to_datetime(working["timestamp"], utc=True, errors="coerce").dt.year
    stats: dict[str, dict[str, float | int | None]] = {}
    yearly_pnl: dict[str, float] = {}
    positive_year_count = 0
    for year, group in working.dropna(subset=["year"]).groupby("year"):
        key = str(int(year))
        net = pd.to_numeric(group["net_bps"], errors="coerce").dropna()
        pnl = float(net.sum()) if not net.empty else 0.0
        yearly_pnl[key] = pnl
        mean_net = _safe_float(net.mean()) if not net.empty else None
        if mean_net is not None and mean_net > 0:
            positive_year_count += 1
        stats[key] = {
            "n": len(net),
            "mean_net_bps": mean_net,
            "pnl_share": None,
        }
    positive_total = sum(value for value in yearly_pnl.values() if value > 0)
    max_share: float | None = None
    if positive_total > 0:
        shares = {
            year: (pnl / positive_total if pnl > 0 else 0.0)
            for year, pnl in yearly_pnl.items()
        }
        max_share = max(shares.values()) if shares else None
        for year, share in shares.items():
            stats[year]["pnl_share"] = float(share)
    return stats, max_share, positive_year_count


def classify_baseline(
    *,
    n: int,
    effective_n: int,
    mean_net_bps: float | None,
    t_stat_net: float | None,
    max_year_pnl_share: float | None,
    positive_year_count: int,
    mean_net_bps_2x_cost: float | None,
    support_reason: str,
) -> tuple[str, str, str]:
    if support_reason:
        return "insufficient_support", "data_repair", support_reason
    if n < 100:
        return "insufficient_support", "data_repair", "n < 100"
    if effective_n < 50:
        return "insufficient_support", "data_repair", "effective_n < 50"
    if mean_net_bps is None:
        return "insufficient_support", "data_repair", "missing net return estimate"
    if mean_net_bps < 0:
        return "negative", "reject", "mean_net_bps < 0"
    if t_stat_net is not None and t_stat_net <= -1.5:
        return "negative", "reject", "materially negative t-stat"
    if (
        mean_net_bps > 0
        and t_stat_net is not None
        and t_stat_net >= 1.5
        and (max_year_pnl_share is not None and max_year_pnl_share <= 0.50)
        and positive_year_count >= 2
        and (mean_net_bps_2x_cost is not None and mean_net_bps_2x_cost >= 0)
    ):
        return "stable_positive", "advance_to_event_lift", "passes initial stable-positive thresholds"
    if mean_net_bps > 0 and (
        (max_year_pnl_share is not None and max_year_pnl_share > 0.50)
        or positive_year_count < 2
    ):
        return "year_conditional", "park", "positive aggregate but year concentration is too high"
    if mean_net_bps > 0:
        return "unstable", "monitor", "positive aggregate but weak stability evidence"
    return "unstable", "monitor", "no positive aggregate edge"


def _empty_row(
    request: RegimeBaselineRequest,
    *,
    filters: dict[str, str],
    symbol: str,
    direction: str,
    horizon_bars: int,
    reason: str,
    proposal_path_eligible: bool = True,
    regime_role: str = "primary",
) -> dict[str, Any]:
    classification, decision, final_reason = classify_baseline(
        n=0,
        effective_n=0,
        mean_net_bps=None,
        t_stat_net=None,
        max_year_pnl_share=None,
        positive_year_count=0,
        mean_net_bps_2x_cost=None,
        support_reason=reason,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": request.run_id,
        "matrix_id": request.matrix_id,
        "regime_id": regime_id(filters),
        "proposal_path_eligible": bool(proposal_path_eligible),
        "regime_role": str(regime_role),
        "filters": dict(filters),
        "symbol": symbol,
        "direction": direction,
        "horizon_bars": int(horizon_bars),
        "overlap_policy": request.overlap_policy,
        "n": 0,
        "effective_n": 0,
        "mean_gross_bps": None,
        "mean_net_bps": None,
        "median_net_bps": None,
        "t_stat_net": None,
        "hit_rate": None,
        "p25_net_bps": None,
        "p75_net_bps": None,
        "max_drawdown_bps": None,
        "cost_bps": None,
        "mean_net_bps_2x_cost": None,
        "year_stats": {},
        "max_year_pnl_share": None,
        "positive_year_count": 0,
        "classification": classification,
        "decision": decision,
        "reason": final_reason,
    }


def evaluate_regime_baseline(
    request: RegimeBaselineRequest,
    *,
    features: pd.DataFrame,
    filters: dict[str, str],
    symbol: str,
    direction: str,
    horizon_bars: int,
    proposal_path_eligible: bool = True,
    regime_role: str = "primary",
) -> dict[str, Any]:
    validate_regime_filters(filters)
    if features.empty:
        return _empty_row(
            request,
            filters=filters,
            symbol=symbol,
            direction=direction,
            horizon_bars=horizon_bars,
            reason="missing prices",
            proposal_path_eligible=proposal_path_eligible,
            regime_role=regime_role,
        )
    if "close" not in features.columns:
        return _empty_row(
            request,
            filters=filters,
            symbol=symbol,
            direction=direction,
            horizon_bars=horizon_bars,
            reason="missing prices",
            proposal_path_eligible=proposal_path_eligible,
            regime_role=regime_role,
        )
    mask, context_reason = _context_mask(features, filters)
    if mask is None:
        return _empty_row(
            request,
            filters=filters,
            symbol=symbol,
            direction=direction,
            horizon_bars=horizon_bars,
            reason=context_reason,
            proposal_path_eligible=proposal_path_eligible,
            regime_role=regime_role,
        )
    costs, cost_source = _cost_series(features)
    if costs is None:
        return _empty_row(
            request,
            filters=filters,
            symbol=symbol,
            direction=direction,
            horizon_bars=horizon_bars,
            reason="missing cost fields",
            proposal_path_eligible=proposal_path_eligible,
            regime_role=regime_role,
        )

    working = features.copy()
    close = pd.to_numeric(working["close"], errors="coerce")
    future_close = close.shift(-int(horizon_bars))
    direction_sign = 1.0 if direction == "long" else -1.0
    gross_bps = direction_sign * ((future_close / close) - 1.0) * 10_000.0
    working["_pos"] = range(len(working))
    working["gross_bps"] = gross_bps
    working["cost_bps"] = costs
    working["net_bps"] = working["gross_bps"] - working["cost_bps"]
    working["net_bps_2x_cost"] = working["gross_bps"] - (2.0 * working["cost_bps"])
    eligible = working[mask & working["gross_bps"].notna() & working["cost_bps"].notna()].copy()
    n = len(eligible)
    sampled = _suppress_overlap(eligible, horizon_bars)
    effective_n = len(sampled)

    support_reason = ""
    if n == 0:
        support_reason = "no rows match regime filters"
    elif effective_n == 0:
        support_reason = "no rows remain after overlap suppression"

    year_stats, max_year_pnl_share, positive_year_count = _year_stats(sampled)
    mean_net_bps = _safe_float(sampled["net_bps"].mean()) if not sampled.empty else None
    t_stat_net = _t_stat(sampled["net_bps"]) if not sampled.empty else None
    mean_net_bps_2x_cost = (
        _safe_float(sampled["net_bps_2x_cost"].mean()) if not sampled.empty else None
    )
    classification, decision, reason = classify_baseline(
        n=n,
        effective_n=effective_n,
        mean_net_bps=mean_net_bps,
        t_stat_net=t_stat_net,
        max_year_pnl_share=max_year_pnl_share,
        positive_year_count=positive_year_count,
        mean_net_bps_2x_cost=mean_net_bps_2x_cost,
        support_reason=support_reason,
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": request.run_id,
        "matrix_id": request.matrix_id,
        "regime_id": regime_id(filters),
        "proposal_path_eligible": bool(proposal_path_eligible),
        "regime_role": str(regime_role),
        "filters": dict(filters),
        "symbol": symbol,
        "direction": direction,
        "horizon_bars": int(horizon_bars),
        "overlap_policy": request.overlap_policy,
        "n": n,
        "effective_n": effective_n,
        "mean_gross_bps": _safe_float(sampled["gross_bps"].mean()) if not sampled.empty else None,
        "mean_net_bps": mean_net_bps,
        "median_net_bps": _safe_float(sampled["net_bps"].median()) if not sampled.empty else None,
        "t_stat_net": t_stat_net,
        "hit_rate": _safe_float((sampled["net_bps"] > 0).mean()) if not sampled.empty else None,
        "p25_net_bps": _safe_float(sampled["net_bps"].quantile(0.25)) if not sampled.empty else None,
        "p75_net_bps": _safe_float(sampled["net_bps"].quantile(0.75)) if not sampled.empty else None,
        "max_drawdown_bps": _max_drawdown_bps(sampled["net_bps"]),
        "cost_bps": _safe_float(sampled["cost_bps"].mean()) if not sampled.empty else None,
        "mean_net_bps_2x_cost": mean_net_bps_2x_cost,
        "year_stats": year_stats,
        "max_year_pnl_share": max_year_pnl_share,
        "positive_year_count": int(positive_year_count),
        "classification": classification,
        "decision": decision,
        "reason": f"{reason}; cost_source={cost_source}" if cost_source and reason else reason,
    }


def build_search_burden(request: RegimeBaselineRequest, num_regimes: int) -> dict[str, Any]:
    proposal_path_eligible_regimes = sum(
        1
        for index in range(num_regimes)
        if proposal_path_eligible_for_matrix(request.matrix_id, index)
    )
    return {
        "schema_version": SEARCH_BURDEN_SCHEMA_VERSION,
        "run_id": request.run_id,
        "matrix_id": request.matrix_id,
        "predeclared": True,
        "num_regimes": int(num_regimes),
        "proposal_path_eligible_regimes": int(proposal_path_eligible_regimes),
        "num_symbols": len(request.symbols),
        "num_directions": len(request.directions),
        "num_horizons": len(request.horizons),
        "num_tests": int(num_regimes * len(request.symbols) * len(request.directions) * len(request.horizons)),
    }


def run_regime_baselines(request: RegimeBaselineRequest) -> tuple[pd.DataFrame, dict[str, Any], str | None]:
    matrix = regime_matrix(request.matrix_id)
    source_run_id = request.source_run_id or discover_market_context_run(
        request.data_root,
        symbols=request.symbols,
        timeframe=request.timeframe,
    )
    features_by_symbol = {
        symbol: load_market_context(
            request.data_root,
            source_run_id=source_run_id,
            symbol=symbol,
            timeframe=request.timeframe,
        )
        if source_run_id
        else pd.DataFrame()
        for symbol in request.symbols
    }

    rows: list[dict[str, Any]] = []
    for index, filters in enumerate(matrix):
        validate_regime_filters(filters)
        proposal_path_eligible = proposal_path_eligible_for_matrix(request.matrix_id, index)
        regime_role = regime_role_for_matrix(request.matrix_id, index)
        for symbol in request.symbols:
            features = features_by_symbol[symbol]
            missing_reason = "missing prices" if features.empty else ""
            for direction in request.directions:
                for horizon in request.horizons:
                    if missing_reason:
                        rows.append(
                            _empty_row(
                                request,
                                filters=filters,
                                symbol=symbol,
                                direction=direction,
                                horizon_bars=horizon,
                                reason=missing_reason,
                                proposal_path_eligible=proposal_path_eligible,
                                regime_role=regime_role,
                            )
                        )
                    else:
                        rows.append(
                            evaluate_regime_baseline(
                                request,
                                features=features,
                                filters=filters,
                                symbol=symbol,
                                direction=direction,
                                horizon_bars=horizon,
                                proposal_path_eligible=proposal_path_eligible,
                                regime_role=regime_role,
                            )
                        )
    df = pd.DataFrame(rows, columns=ROW_COLUMNS)
    return df, build_search_burden(request, len(matrix)), source_run_id


def _records_for_json(df: pd.DataFrame) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for row in df.to_dict(orient="records"):
        item: dict[str, Any] = {}
        for column in ROW_COLUMNS:
            value = row.get(column)
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                value = None
            item[column] = value
        records.append(item)
    return records


def write_regime_baseline_outputs(
    df: pd.DataFrame,
    burden: dict[str, Any],
    *,
    output_dir: Path,
    source_run_id: str | None = None,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "source_run_id": source_run_id,
        "row_count": len(df),
        "rows": _records_for_json(df),
    }
    (output_dir / "regime_baselines.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    df.to_parquet(output_dir / "regime_baselines.parquet", index=False)
    (output_dir / "regime_search_burden.json").write_text(
        json.dumps(burden, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_regime_baseline_markdown(df, burden, output_dir=output_dir, source_run_id=source_run_id)


def write_regime_baseline_markdown(
    df: pd.DataFrame,
    burden: dict[str, Any],
    *,
    output_dir: Path,
    source_run_id: str | None,
) -> None:
    counts = df["classification"].value_counts().to_dict() if not df.empty else {}
    lines = [
        "# Regime Baselines",
        "",
        f"- run_id: `{burden['run_id']}`",
        f"- matrix_id: `{burden['matrix_id']}`",
        f"- source_run_id: `{source_run_id or ''}`",
        f"- num_tests: `{burden['num_tests']}`",
        "",
        "## Classification Counts",
        "",
    ]
    lines.extend(f"- {key}: {int(counts[key])}" for key in sorted(counts))
    lines.extend(["", "## Top Rows", ""])
    top = df.sort_values("mean_net_bps", ascending=False, na_position="last").head(10)
    for _, row in top.iterrows():
        lines.append(
            "- "
            f"{row['regime_id']} {row['symbol']} {row['direction']} h{row['horizon_bars']}: "
            f"{row['classification']} mean_net_bps={row['mean_net_bps']}"
        )
    (output_dir / "regime_baselines.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
