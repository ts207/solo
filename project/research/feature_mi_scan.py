"""
Phase 4.1 — Feature Mutual Information Scan.

Closes the discovery-generation loop: instead of proposing FEATURE_PREDICATE
hypotheses only from the 8 manually-specified predicates in search_space.yaml,
this pipeline stage computes mutual information between every numeric feature
column and forward log-returns at candidate horizons, stratified by available
regime labels.  High-MI features generate threshold-derived predicate candidates
that the campaign controller uses to augment the static predicate set.

Output artefacts
----------------
data/reports/feature_mi/<run_id>/
    feature_horizon_mi.parquet   — ranked table with columns:
                                   feature, horizon_bars, regime_label,
                                   mi_score, n_samples, percentile_25,
                                   percentile_75, percentile_90
    candidate_predicates.json    — list of predicate dicts ready for
                                   trigger_space.feature_predicates.include

Usage (CLI)
-----------
    python -m project.research.feature_mi_scan \\
        --run_id <run_id> \\
        --symbols BTCUSDT \\
        --timeframe 5m

Usage (library)
---------------
    from project.research.feature_mi_scan import run_feature_mi_scan
    result = run_feature_mi_scan(features_df, out_dir=Path("..."))
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.feature_selection import mutual_info_regression

from project.core.config import get_data_root
from project.research.phase2 import load_features
from project.io.utils import write_parquet

LOG = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Candidate forward-return horizons (bars)
DEFAULT_HORIZONS: list[int] = [6, 12, 24, 48]

# Only report / generate predicates for features with MI above this floor.
# Typical uninformative feature MI ≈ 0.001; floor at 0.005 retains signal.
DEFAULT_MI_THRESHOLD: float = 0.005

# Percentiles used to derive threshold candidates for each high-MI feature.
# Upper tail (75th, 90th) → operator ">="
# Lower tail (25th)       → operator "<="
THRESHOLD_PERCENTILES: list[int] = [25, 75, 90]

# Regime columns produced by build_market_context that define stratification
# strata.  Each column's unique non-NaN values become individual regime labels.
REGIME_COLUMNS: list[str] = ["ms_vol_state", "ms_trend_state", "ms_spread_state"]

# Columns that are regime labels, event flags, or metadata — never MI features.
_EXCLUDE_PREFIXES = (
    "event_",
    "ms_",
    "high_vol",
    "low_vol",
    "bull_trend",
    "bear_trend",
    "chop_regime",
    "spread_elevated",
    "split_label",
)
_EXCLUDE_EXACT = frozenset(
    {
        "timestamp",
        "symbol",
        "time_open",
        "time_close",
        "logret_1",   # forward-return target — not a predictor
        "close",      # raw price level — autocorrelated, not an informative predictor
                      # (use logret_1, rv_96, or spread_bps as volatility proxies instead)
    }
)

# sklearn MI kwargs
_MI_KWARGS: dict[str, Any] = {"n_neighbors": 5, "random_state": 42}

# MI table output schema — used for empty-frame construction and validation
_MI_TABLE_COLUMNS = [
    "feature",
    "horizon_bars",
    "regime_label",
    "mi_score",
    "n_samples",
    "percentile_25",
    "percentile_75",
    "percentile_90",
    "above_threshold",
]


# ---------------------------------------------------------------------------
# Feature column selection
# ---------------------------------------------------------------------------


def _select_feature_columns(df: pd.DataFrame) -> list[str]:
    """Return numeric, non-excluded columns suitable for MI computation."""
    cols = []
    for col in df.columns:
        if col in _EXCLUDE_EXACT:
            continue
        if any(col.startswith(pfx) for pfx in _EXCLUDE_PREFIXES):
            continue
        if not pd.api.types.is_numeric_dtype(df[col]):
            continue
        cols.append(col)
    return cols


# ---------------------------------------------------------------------------
# Forward return computation
# ---------------------------------------------------------------------------


def _forward_log_returns(df: pd.DataFrame, horizon_bars: int) -> pd.Series:
    """Compute forward log return at *horizon_bars* bars ahead.

    Uses ``logret_1`` if present (already a log return); otherwise computes
    log(close / close.shift(1)) directly and accumulates over the horizon.
    The series is aligned to the *current* bar (shifted back by horizon),
    so it is point-in-time safe for the MI computation.
    """
    if "logret_1" in df.columns:
        lr = pd.to_numeric(df["logret_1"], errors="coerce")
    elif "close" in df.columns:
        close = pd.to_numeric(df["close"], errors="coerce")
        lr = np.log(close / close.shift(1))
    else:
        return pd.Series(dtype=float, index=df.index)

    # Cumulative forward return over the horizon window, aligned to current bar
    fwd = lr.shift(-1).rolling(horizon_bars, min_periods=horizon_bars).sum().shift(-(horizon_bars - 1))
    return fwd


# ---------------------------------------------------------------------------
# Threshold-derived predicate candidates
# ---------------------------------------------------------------------------


def _derive_predicates(
    df: pd.DataFrame,
    feature: str,
    mi_score: float,
    regime_label: str,
) -> list[dict[str, Any]]:
    """Derive predicate dicts from the empirical distribution of *feature*.

    Returns one predicate per percentile threshold:
      - 90th percentile → operator ">=" (top-decile extreme)
      - 75th percentile → operator ">=" (top-quartile)
      - 25th percentile → operator "<=" (bottom-quartile)
    """
    if feature not in df.columns:
        return []
    vals = pd.to_numeric(df[feature], errors="coerce").dropna()
    if len(vals) < 30:
        return []

    predicates = []
    for pct in THRESHOLD_PERCENTILES:
        threshold = float(np.percentile(vals, pct))
        if np.isnan(threshold) or not np.isfinite(threshold):
            continue
        operator = ">=" if pct >= 50 else "<="
        predicates.append(
            {
                "feature": feature,
                "operator": operator,
                "threshold": round(threshold, 6),
                "source": "mi_scan",
                "mi_score": round(mi_score, 6),
                "regime_label": regime_label,
                "percentile": pct,
            }
        )
    return predicates


# ---------------------------------------------------------------------------
# Core MI computation (per regime stratum)
# ---------------------------------------------------------------------------


def _compute_mi_for_stratum(
    df: pd.DataFrame,
    feature_cols: list[str],
    horizon_bars: int,
    regime_label: str,
    mi_threshold: float,
) -> list[dict[str, Any]]:
    """Compute MI between all feature columns and forward returns for one stratum.

    Returns a list of row-dicts for feature_horizon_mi.parquet.
    """
    fwd = _forward_log_returns(df, horizon_bars)
    valid_mask = fwd.notna()

    rows = []
    for feat in feature_cols:
        if feat not in df.columns:
            continue
        x_raw = pd.to_numeric(df[feat], errors="coerce")
        combined_mask = valid_mask & x_raw.notna()
        n = int(combined_mask.sum())
        if n < 30:
            continue

        X = x_raw[combined_mask].values.reshape(-1, 1)
        y = fwd[combined_mask].values

        try:
            mi = float(mutual_info_regression(X, y, **_MI_KWARGS)[0])
        except Exception as exc:
            LOG.debug("MI failed for feature=%s horizon=%d: %s", feat, horizon_bars, exc)
            continue

        feat_vals = x_raw[combined_mask]
        rows.append(
            {
                "feature": feat,
                "horizon_bars": horizon_bars,
                "regime_label": regime_label,
                "mi_score": round(mi, 8),
                "n_samples": n,
                "percentile_25": round(float(np.percentile(feat_vals, 25)), 6),
                "percentile_75": round(float(np.percentile(feat_vals, 75)), 6),
                "percentile_90": round(float(np.percentile(feat_vals, 90)), 6),
                "above_threshold": mi >= mi_threshold,
            }
        )

    return rows


# ---------------------------------------------------------------------------
# Public library entry point
# ---------------------------------------------------------------------------


def run_feature_mi_scan(
    features: pd.DataFrame,
    *,
    out_dir: Path,
    horizons: list[int] | None = None,
    mi_threshold: float = DEFAULT_MI_THRESHOLD,
) -> dict[str, Any]:
    """Run the feature MI scan and write artefacts to *out_dir*.

    Parameters
    ----------
    features:
        Wide feature DataFrame — typically the output of ``load_features()``.
        Must contain numeric feature columns and a ``logret_1`` or ``close``
        column for forward-return derivation.
    out_dir:
        Directory where ``feature_horizon_mi.parquet`` and
        ``candidate_predicates.json`` are written.  Created if absent.
    horizons:
        Forward-return horizons in bars.  Defaults to ``[6, 12, 24, 48]``.
    mi_threshold:
        Minimum MI score for a feature to generate predicate candidates.
        Features below this floor are retained in the parquet but do not
        contribute to ``candidate_predicates.json``.

    Returns
    -------
    dict with keys:
        ``mi_rows`` (int), ``candidate_predicates`` (int),
        ``features_scanned`` (int), ``horizons`` (list[int]),
        ``out_dir`` (str)
    """
    if horizons is None:
        horizons = DEFAULT_HORIZONS

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if features.empty:
        LOG.warning("feature_mi_scan: empty features DataFrame — writing empty artefacts.")
        write_parquet(pd.DataFrame(columns=_MI_TABLE_COLUMNS), out_dir / "feature_horizon_mi.parquet")
        (out_dir / "candidate_predicates.json").write_text("[]", encoding="utf-8")
        return {
            "mi_rows": 0,
            "candidate_predicates": 0,
            "features_scanned": 0,
            "horizons": horizons,
            "out_dir": str(out_dir),
        }

    feature_cols = _select_feature_columns(features)
    LOG.info(
        "feature_mi_scan: %d feature columns × %d horizons across %d rows",
        len(feature_cols), len(horizons), len(features),
    )

    all_rows: list[dict[str, Any]] = []

    # ── 1. Unconditional pass ────────────────────────────────────────────
    for h in horizons:
        rows = _compute_mi_for_stratum(
            features, feature_cols, h, "unconditional", mi_threshold
        )
        all_rows.extend(rows)

    # ── 2. Regime-stratified passes ──────────────────────────────────────
    for regime_col in REGIME_COLUMNS:
        if regime_col not in features.columns:
            continue
        labels = (
            pd.to_numeric(features[regime_col], errors="coerce").dropna().unique()
        )
        for label_val in sorted(labels):
            mask = pd.to_numeric(features[regime_col], errors="coerce") == label_val
            stratum = features[mask]
            if len(stratum) < 50:
                continue
            regime_label = f"{regime_col}={label_val:.1f}"
            for h in horizons:
                rows = _compute_mi_for_stratum(
                    stratum, feature_cols, h, regime_label, mi_threshold
                )
                all_rows.extend(rows)

    # ── 3. Build parquet ─────────────────────────────────────────────────
    mi_df = (
        pd.DataFrame(all_rows, columns=_MI_TABLE_COLUMNS)
        if all_rows
        else pd.DataFrame(columns=_MI_TABLE_COLUMNS)
    )
    if not mi_df.empty:
        mi_df = mi_df.sort_values(
            ["regime_label", "horizon_bars", "mi_score"],
            ascending=[True, True, False],
        ).reset_index(drop=True)

    write_parquet(mi_df, out_dir / "feature_horizon_mi.parquet")
    LOG.info("feature_mi_scan: wrote %d MI rows to %s", len(mi_df), out_dir)

    # ── 4. Derive predicate candidates ───────────────────────────────────
    candidate_predicates: list[dict[str, Any]] = []
    seen_keys: set[str] = set()

    high_mi = mi_df[mi_df["above_threshold"] == True] if not mi_df.empty else pd.DataFrame()  # noqa: E712

    for _, row in high_mi.iterrows():
        feat = str(row["feature"])
        regime = str(row["regime_label"])
        preds = _derive_predicates(features, feat, float(row["mi_score"]), regime)
        for pred in preds:
            key = f"{pred['feature']}|{pred['operator']}|{pred['threshold']}|{pred['regime_label']}"
            if key not in seen_keys:
                seen_keys.add(key)
                candidate_predicates.append(pred)

    # Sort by MI score descending so highest-signal predicates come first
    candidate_predicates.sort(key=lambda p: p.get("mi_score", 0.0), reverse=True)

    (out_dir / "candidate_predicates.json").write_text(
        json.dumps(candidate_predicates, indent=2), encoding="utf-8"
    )
    LOG.info(
        "feature_mi_scan: %d candidate predicates written to %s",
        len(candidate_predicates), out_dir,
    )

    return {
        "mi_rows": len(mi_df),
        "candidate_predicates": len(candidate_predicates),
        "features_scanned": len(feature_cols),
        "horizons": horizons,
        "out_dir": str(out_dir),
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _make_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Phase 4.1 — Run feature mutual information scan."
    )
    p.add_argument("--run_id", required=True, help="Run ID (used to locate features and name output dir)")
    p.add_argument("--symbols", required=True, help="Comma-separated symbol list, e.g. BTCUSDT")
    p.add_argument("--timeframe", default="5m", help="Feature table timeframe (default: 5m)")
    p.add_argument(
        "--horizons",
        default="6,12,24,48",
        help="Comma-separated forward-return horizons in bars (default: 6,12,24,48)",
    )
    p.add_argument(
        "--mi_threshold",
        type=float,
        default=DEFAULT_MI_THRESHOLD,
        help=f"Minimum MI score for predicate generation (default: {DEFAULT_MI_THRESHOLD})",
    )
    p.add_argument(
        "--out_dir",
        default=None,
        help="Override output directory (default: data/reports/feature_mi/<run_id>)",
    )
    p.add_argument("--data_root", default=None, help="Override data root")
    return p


def main() -> int:
    parser = _make_parser()
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")

    data_root = Path(args.data_root) if args.data_root else get_data_root()
    out_dir = (
        Path(args.out_dir)
        if args.out_dir
        else data_root / "reports" / "feature_mi" / args.run_id
    )
    horizons = [int(h.strip()) for h in str(args.horizons).split(",") if h.strip()]
    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]

    # Load and concatenate features across all symbols
    parts: list[pd.DataFrame] = []
    for sym in symbols:
        df = load_features(data_root, args.run_id, sym, timeframe=args.timeframe)
        if not df.empty:
            df = df.copy()
            df["symbol"] = sym
            parts.append(df)

    if not parts:
        LOG.warning(
            "feature_mi_scan: no features loaded for run_id=%s symbols=%s — writing empty artefacts.",
            args.run_id, symbols,
        )
        features = pd.DataFrame()
    else:
        features = pd.concat(parts, ignore_index=True)

    result = run_feature_mi_scan(
        features,
        out_dir=out_dir,
        horizons=horizons,
        mi_threshold=float(args.mi_threshold),
    )

    LOG.info(
        "feature_mi_scan complete: %d MI rows, %d candidate predicates → %s",
        result["mi_rows"], result["candidate_predicates"], result["out_dir"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
