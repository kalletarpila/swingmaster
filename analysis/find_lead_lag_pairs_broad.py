"""
V5b: Broad lead-lag candidate search.

Unlike V5 (find_lead_lag_pairs.py) which tests V4 residual-similar candidates
for lead-lag and found all pairs same-day dominant, V5b broadens the candidate
universe (raw + residual similarity outputs) and explicitly reports the best
non-zero lag for every pair even when same-day correlation remains stronger.

Allowed imports: argparse, sqlite3, pathlib, pandas, numpy only.
"""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

RAW_CANDIDATE_FILES = [
    "top_pairs_overall.csv",
    "top_pairs_same_industry.csv",
    "top_pairs_same_sector_cross_industry.csv",
    "top_pairs_cross_sector.csv",
    "top_pairs_unusual_sync.csv",
]

RESIDUAL_CANDIDATE_FILES = [
    "residual_top_pairs_overall.csv",
    "residual_top_pairs_same_industry.csv",
    "residual_top_pairs_same_sector_cross_industry.csv",
    "residual_top_pairs_cross_sector.csv",
    "residual_top_pairs_unusual_sync.csv",
]

# Explicit sort order for broad_lead_lag_signal_class (ascending = best first)
BROAD_SIGNAL_CLASS_ORDER: dict[str, int] = {
    "POSSIBLE_NONZERO_LAG": 0,
    "POSSIBLE_NONZERO_LAG_BUT_OPPOSITE_NOT_CLEAR": 1,
    "NONZERO_LAG_LOW_CORRELATION": 2,
    "SAME_DAY_STRONGER_NO_EDGE": 3,
    "INSUFFICIENT_DATA": 4,
}

CANDIDATE_OUT_COLS = [
    "canonical_ticker_1",
    "canonical_ticker_2",
    "source_family",
    "source_report",
    "source_score",
    "source_raw_correlation",
    "source_full_period_correlation",
    "source_sector_residual_correlation",
    "source_rolling_corr_mean",
    "source_rolling_residual_corr_mean",
    "sector_1",
    "industry_1",
    "sector_2",
    "industry_2",
    "same_sector",
    "same_industry",
]

ALL_LAG_COLS = [
    "canonical_ticker_1",
    "canonical_ticker_2",
    "lag",
    "lag_direction_description",
    "lag_correlation",
    "overlap_obs",
    "source_family",
    "source_report",
    "source_score",
    "sector_1",
    "industry_1",
    "sector_2",
    "industry_2",
    "same_sector",
    "same_industry",
]

SUMMARY_COLS = [
    "canonical_ticker_1",
    "canonical_ticker_2",
    "same_day_correlation",
    "best_lag_including_zero",
    "best_lag_correlation_including_zero",
    "best_nonzero_lag",
    "best_nonzero_lag_abs_days",
    "best_nonzero_lag_correlation",
    "nonzero_edge_vs_same_day",
    "opposite_lag",
    "opposite_lag_correlation",
    "nonzero_edge_vs_opposite_lag",
    "nonzero_lag_direction",
    "broad_lead_lag_signal_class",
    "rolling_best_nonzero_lag_corr_mean",
    "rolling_best_nonzero_lag_corr_median",
    "rolling_best_nonzero_lag_corr_min",
    "rolling_best_nonzero_lag_corr_max",
    "rolling_best_nonzero_lag_corr_latest",
    "rolling_best_nonzero_lag_valid_obs",
    "share_rolling_best_nonzero_lag_corr_gt_015",
    "share_rolling_best_nonzero_lag_corr_gt_020",
    "share_rolling_best_nonzero_lag_corr_gt_030",
    "share_rolling_best_nonzero_lag_corr_gt_050",
    "broad_stability_signal",
    "source_family",
    "source_report",
    "source_score",
    "source_raw_correlation",
    "source_full_period_correlation",
    "source_sector_residual_correlation",
    "source_rolling_corr_mean",
    "source_rolling_residual_corr_mean",
    "sector_1",
    "industry_1",
    "sector_2",
    "industry_2",
    "same_sector",
    "same_industry",
]

BY_SOURCE_COLS = [
    "source_report",
    "candidate_pair_count",
    "possible_nonzero_lag_count",
    "stable_possible_nonzero_lag_count",
    "same_day_stronger_no_edge_count",
    "insufficient_data_count",
]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="V5b: Broad lead-lag candidate search across raw + residual similarity outputs."
    )
    p.add_argument("--db", required=True)
    p.add_argument("--raw-candidate-dir", required=True)
    p.add_argument("--residual-candidate-dir", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--start-date", default="2024-01-01")
    p.add_argument("--end-date", default="2026-12-31")
    p.add_argument("--min-obs", type=int, default=400)
    p.add_argument("--min-sector-peers", type=int, default=5)
    p.add_argument("--max-lag", type=int, default=5)
    p.add_argument("--min-overlap", type=int, default=250)
    p.add_argument("--rolling-window", type=int, default=60)
    p.add_argument("--rolling-min-periods", type=int, default=40)
    p.add_argument("--max-candidates", type=int, default=2000)
    p.add_argument("--min-source-correlation", type=float, default=0.25)
    p.add_argument("--min-nonzero-lag-correlation", type=float, default=0.15)
    p.add_argument("--min-nonzero-edge-vs-same-day", type=float, default=0.03)
    p.add_argument("--min-nonzero-edge-vs-opposite-lag", type=float, default=0.03)
    p.add_argument("--min-stability-share-gt-015", type=float, default=0.40)
    return p.parse_args()


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_database(db_path: Path) -> None:
    if not db_path.exists():
        raise SystemExit(f"ERROR: database not found: {db_path}")
    conn = sqlite3.connect(str(db_path))
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table';"
            ).fetchall()
        }
        for t in ("instruments", "price_change_daily"):
            if t not in tables:
                raise SystemExit(f"ERROR: required table '{t}' not found in database")
        required_cols: dict[str, set[str]] = {
            "instruments": {"ticker", "sector", "industry"},
            "price_change_daily": {"ticker", "trade_date", "close_change"},
        }
        for table, cols in required_cols.items():
            existing = {
                row[1] for row in conn.execute(f"PRAGMA table_info({table});").fetchall()
            }
            missing = cols - existing
            if missing:
                raise SystemExit(
                    f"ERROR: table '{table}' missing columns: {', '.join(sorted(missing))}"
                )
    finally:
        conn.close()


def validate_candidate_dirs(
    raw_dir: Path, residual_dir: Path
) -> tuple[list[Path], list[Path]]:
    if not raw_dir.exists():
        raise SystemExit(f"ERROR: raw candidate directory not found: {raw_dir}")
    if not residual_dir.exists():
        raise SystemExit(
            f"ERROR: residual candidate directory not found: {residual_dir}"
        )
    raw_found = [raw_dir / f for f in RAW_CANDIDATE_FILES if (raw_dir / f).exists()]
    residual_found = [
        residual_dir / f
        for f in RESIDUAL_CANDIDATE_FILES
        if (residual_dir / f).exists()
    ]
    if not raw_found and not residual_found:
        raise SystemExit(
            "ERROR: no expected candidate files found in either candidate directory"
        )
    return raw_found, residual_found


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_str(v: object) -> str:
    """Convert value to stripped string; return empty string for None/NaN."""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return ""
    return str(v).strip()


def _to_float(v: object) -> float:
    """Convert value to float; return NaN on failure."""
    if v is None:
        return np.nan
    try:
        return float(v)
    except (ValueError, TypeError):
        return np.nan


def _sorted_csv(*values: str) -> str:
    """
    Build a sorted, deduplicated comma-separated string from non-empty string values.
    Used for combining source_report and source_family from multiple rows.
    """
    parts = sorted({v.strip() for v in values if v and v.strip()})
    return ", ".join(parts)


# ---------------------------------------------------------------------------
# Load candidate pairs
# ---------------------------------------------------------------------------

def load_candidate_pairs(
    raw_found: list[Path],
    residual_found: list[Path],
    min_source_correlation: float,
    max_candidates: int,
) -> tuple[pd.DataFrame, int, int, int, int]:
    """
    Read candidate pairs from raw and residual source files.

    Returns (df, raw_row_count, unique_before_score_filter, after_score_filter, after_max).
    """
    all_rows: list[dict] = []
    raw_count = 0

    def _read_files(paths: list[Path], family: str) -> None:
        nonlocal raw_count
        for path in paths:
            source_name = path.stem
            try:
                df = pd.read_csv(path)
            except Exception:
                continue
            if "ticker_1" not in df.columns or "ticker_2" not in df.columns:
                continue
            raw_count += len(df)
            for _, row in df.iterrows():
                t1 = _to_str(row.get("ticker_1", ""))
                t2 = _to_str(row.get("ticker_2", ""))
                if not t1 or not t2:
                    continue

                # Canonical alphabetical order
                ct1, ct2 = min(t1, t2), max(t1, t2)
                flipped = ct1 != t1

                if flipped:
                    s1 = _to_str(row.get("sector_2", ""))
                    s2 = _to_str(row.get("sector_1", ""))
                    i1 = _to_str(row.get("industry_2", ""))
                    i2 = _to_str(row.get("industry_1", ""))
                else:
                    s1 = _to_str(row.get("sector_1", ""))
                    s2 = _to_str(row.get("sector_2", ""))
                    i1 = _to_str(row.get("industry_1", ""))
                    i2 = _to_str(row.get("industry_2", ""))

                same_sector = _to_str(row.get("same_sector", "")).lower() in (
                    "true", "1"
                )
                same_industry = _to_str(row.get("same_industry", "")).lower() in (
                    "true", "1"
                )

                all_rows.append(
                    {
                        "canonical_ticker_1": ct1,
                        "canonical_ticker_2": ct2,
                        "_source": source_name,
                        "_family": family,
                        "source_raw_correlation": _to_float(
                            row.get("raw_correlation", np.nan)
                        ),
                        "source_full_period_correlation": _to_float(
                            row.get("full_period_correlation", np.nan)
                        ),
                        "source_sector_residual_correlation": _to_float(
                            row.get("sector_residual_correlation", np.nan)
                        ),
                        "source_rolling_corr_mean": _to_float(
                            row.get("rolling_corr_mean", np.nan)
                        ),
                        "source_rolling_residual_corr_mean": _to_float(
                            row.get("rolling_residual_corr_mean", np.nan)
                        ),
                        "sector_1": s1,
                        "industry_1": i1,
                        "sector_2": s2,
                        "industry_2": i2,
                        "same_sector": same_sector,
                        "same_industry": same_industry,
                    }
                )

    _read_files(raw_found, "RAW")
    _read_files(residual_found, "RESIDUAL")

    if not all_rows:
        return pd.DataFrame(), raw_count, 0, 0, 0

    combined = pd.DataFrame(all_rows)

    # Deduplicate: group by canonical pair, combine source metadata
    pair_key = ["canonical_ticker_1", "canonical_ticker_2"]
    deduped_rows: list[dict] = []
    for (ct1, ct2), group in combined.groupby(pair_key, sort=True):
        sources = sorted({s for s in group["_source"].tolist() if s})
        families = sorted({f for f in group["_family"].tolist() if f})
        first = group.iloc[0]

        # Compute source_score: prefer sector_residual_correlation, then full_period, then raw
        src_residual = _to_float(first.get("source_sector_residual_correlation", np.nan))
        src_full = _to_float(first.get("source_full_period_correlation", np.nan))
        src_raw = _to_float(first.get("source_raw_correlation", np.nan))

        if not np.isnan(src_residual):
            source_score = src_residual
        elif not np.isnan(src_full):
            source_score = src_full
        elif not np.isnan(src_raw):
            source_score = src_raw
        else:
            source_score = np.nan

        deduped_rows.append(
            {
                "canonical_ticker_1": ct1,
                "canonical_ticker_2": ct2,
                "source_family": ", ".join(families),
                "source_report": ", ".join(sources),
                "source_score": source_score,
                "source_raw_correlation": first["source_raw_correlation"],
                "source_full_period_correlation": first["source_full_period_correlation"],
                "source_sector_residual_correlation": first[
                    "source_sector_residual_correlation"
                ],
                "source_rolling_corr_mean": first["source_rolling_corr_mean"],
                "source_rolling_residual_corr_mean": first[
                    "source_rolling_residual_corr_mean"
                ],
                "sector_1": first["sector_1"],
                "industry_1": first["industry_1"],
                "sector_2": first["sector_2"],
                "industry_2": first["industry_2"],
                "same_sector": first["same_sector"],
                "same_industry": first["same_industry"],
            }
        )

    deduped = pd.DataFrame(deduped_rows)
    unique_before_score = len(deduped)

    # Filter by min_source_correlation: keep NaN scores or scores >= threshold
    score_mask = deduped["source_score"].isna() | (
        deduped["source_score"] >= min_source_correlation
    )
    deduped = deduped[score_mask].copy()
    after_score = len(deduped)

    if deduped.empty:
        return deduped, raw_count, unique_before_score, after_score, 0

    # Sort for deterministic ranking before max cutoff:
    # 1. source_score descending (NaN last), 2. source_family, 3. ct1, 4. ct2
    deduped["_score_sort"] = deduped["source_score"].fillna(-np.inf)
    deduped = deduped.sort_values(
        ["_score_sort", "source_family", "canonical_ticker_1", "canonical_ticker_2"],
        ascending=[False, True, True, True],
    ).drop(columns=["_score_sort"]).reset_index(drop=True)

    # Apply max candidates cap
    deduped = deduped.head(max_candidates).copy()
    after_max = len(deduped)

    return deduped, raw_count, unique_before_score, after_score, after_max


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_data(db_path: Path, start_date: str, end_date: str) -> pd.DataFrame:
    conn = sqlite3.connect(str(db_path))
    try:
        df = pd.read_sql_query(
            """
            SELECT
                p.trade_date,
                p.ticker,
                p.close_change,
                i.sector,
                i.industry
            FROM price_change_daily p
            JOIN instruments i ON i.ticker = p.ticker
            WHERE p.trade_date BETWEEN ? AND ?
            ORDER BY p.trade_date, p.ticker
            """,
            conn,
            params=(start_date, end_date),
        )
    finally:
        conn.close()

    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["sector"] = df["sector"].fillna("").astype(str).str.strip()
    df["industry"] = df["industry"].fillna("").astype(str).str.strip()
    return df


# ---------------------------------------------------------------------------
# Residual return computation (same logic as V4)
# ---------------------------------------------------------------------------

def compute_residual_returns(df: pd.DataFrame, min_sector_peers: int) -> pd.DataFrame:
    """
    Compute sector-neutral residual returns via same-day cross-sectional
    leave-one-out neutralization, identical to V4 logic.

    Steps:
    1. market_return_ex_ticker: LOO mean of all other tickers same day
    2. market_residual: raw_return - market_return_ex_ticker
    3. sector_market_residual_ex_ticker: LOO mean market_residual of same-sector
       peers (only when sector_count-1 >= min_sector_peers)
    4. sector_neutral_residual: market_residual - sector_market_residual_ex_ticker
       Falls back to market_residual when sector adjustment unavailable.
    """
    enriched = df.copy()

    # Daily universe statistics
    daily = (
        enriched.groupby("trade_date")
        .agg(
            daily_sum_return=("close_change", "sum"),
            daily_count=("close_change", "count"),
        )
        .reset_index()
    )
    enriched = enriched.merge(daily, on="trade_date", how="left")

    # Leave-one-out market return
    enriched["market_return_ex_ticker"] = np.where(
        enriched["daily_count"] > 1,
        (enriched["daily_sum_return"] - enriched["close_change"])
        / (enriched["daily_count"] - 1),
        np.nan,
    )
    enriched["market_residual"] = (
        enriched["close_change"] - enriched["market_return_ex_ticker"]
    )

    # Sector-level statistics
    sector_stats = (
        enriched.groupby(["trade_date", "sector"])
        .agg(
            daily_sector_sum_market_residual=("market_residual", "sum"),
            daily_sector_count=("market_residual", "count"),
        )
        .reset_index()
    )
    enriched = enriched.merge(sector_stats, on=["trade_date", "sector"], how="left")

    has_sector_adjustment = (
        enriched["sector"].ne("")
        & enriched["market_residual"].notna()
        & ((enriched["daily_sector_count"] - 1) >= min_sector_peers)
    )

    # Leave-one-out sector mean of market residuals
    enriched["sector_market_residual_ex_ticker"] = np.where(
        has_sector_adjustment,
        (enriched["daily_sector_sum_market_residual"] - enriched["market_residual"])
        / (enriched["daily_sector_count"] - 1),
        np.nan,
    )

    # sector_neutral_residual: use sector adjustment when available, else market_residual
    enriched["sector_neutral_residual"] = np.where(
        enriched["sector_market_residual_ex_ticker"].notna(),
        enriched["market_residual"] - enriched["sector_market_residual_ex_ticker"],
        enriched["market_residual"],
    )

    # Track which basis was used per row (informational, not output)
    enriched["residual_basis"] = np.where(
        enriched["sector_market_residual_ex_ticker"].notna(),
        "MARKET_PLUS_SECTOR",
        "MARKET_ONLY",
    )

    return enriched


def build_residual_wide_matrix(
    df: pd.DataFrame,
    enriched: pd.DataFrame,
    min_obs: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build wide residual matrix. Tickers with fewer than min_obs raw observations
    are excluded.

    Returns (residual_wide, meta).
    residual_wide: index=trade_date, columns=ticker, values=sector_neutral_residual
    """
    meta = (
        df.groupby("ticker", sort=True)
        .agg(obs_count_raw=("close_change", "count"))
        .reset_index()
    )
    valid_tickers = sorted(
        meta.loc[meta["obs_count_raw"] >= min_obs, "ticker"].tolist()
    )

    residual_wide = enriched.pivot(
        index="trade_date", columns="ticker", values="sector_neutral_residual"
    )
    residual_wide.columns.name = None

    valid_in_residual = [t for t in valid_tickers if t in residual_wide.columns]
    residual_wide = residual_wide.reindex(columns=valid_in_residual)

    return residual_wide, meta[meta["ticker"].isin(valid_in_residual)].copy()


# ---------------------------------------------------------------------------
# Lag correlation
# ---------------------------------------------------------------------------

def compute_lag_correlation(
    x: pd.Series,
    y: pd.Series,
    lag: int,
    min_overlap: int,
) -> tuple[float, int]:
    """
    Compute Pearson correlation at a given lag. Both series are indexed by trade_date.

    lag = 0 : corr(x_t, y_t)
    lag > 0 : corr(x_t, y_{t+lag})   — canonical_ticker_1 leads canonical_ticker_2
              Implementation: aligned_x=x, aligned_y=y.shift(-lag)
    lag < 0 : corr(y_t, x_{t+abs(lag)})  — canonical_ticker_2 leads canonical_ticker_1
              Implementation: aligned_x=x.shift(-abs(lag)), aligned_y=y
              Negative lag means canonical_ticker_2 leads canonical_ticker_1.

    Returns (pearson_r, overlap_count). Returns (NaN, overlap) if overlap < min_overlap.
    """
    if lag == 0:
        s1, s2 = x, y
    elif lag > 0:
        # canonical_ticker_1 leads canonical_ticker_2 by lag days:
        # align x_t with y_{t+lag} via shifting y backward by lag positions
        s1 = x
        s2 = y.shift(-lag)
    else:
        # lag < 0: canonical_ticker_2 leads canonical_ticker_1 by abs(lag) days
        # align x_{t+abs(lag)} with y_t via shifting x backward by abs(lag) positions
        lag_abs = abs(lag)
        s1 = x.shift(-lag_abs)
        s2 = y

    aligned = pd.concat([s1, s2], axis=1).dropna()
    overlap = len(aligned)
    if overlap < min_overlap:
        return np.nan, overlap

    corr = aligned.iloc[:, 0].corr(aligned.iloc[:, 1])
    return float(corr), overlap


# ---------------------------------------------------------------------------
# Per-pair broad lead-lag computation
# ---------------------------------------------------------------------------

def compute_pair_broad_lead_lag(
    x: pd.Series,
    y: pd.Series,
    ct1: str,
    ct2: str,
    max_lag: int,
    min_overlap: int,
) -> tuple[list[dict], dict]:
    """
    Compute lag correlations from -max_lag to +max_lag for a candidate pair.

    Returns:
        lag_rows  : per-lag result dicts
        metrics   : dict with all best-lag metrics for the pair summary
    """
    lag_rows: list[dict] = []
    same_day_corr = np.nan
    lag_corr_map: dict[int, float] = {}

    for lag in range(-max_lag, max_lag + 1):
        corr, overlap = compute_lag_correlation(x, y, lag, min_overlap)

        if lag == 0:
            direction = "SAME_DAY"
            if not np.isnan(corr):
                same_day_corr = float(corr)
        elif lag > 0:
            direction = (
                f"{ct1} leads {ct2} by {lag} trading days"
            )
        else:
            # Negative lag: canonical_ticker_2 leads canonical_ticker_1
            direction = (
                f"{ct2} leads {ct1} by {abs(lag)} trading days"
            )

        lag_rows.append(
            {
                "lag": lag,
                "lag_direction_description": direction,
                "lag_correlation": corr,
                "overlap_obs": overlap,
            }
        )
        if not np.isnan(corr):
            lag_corr_map[lag] = float(corr)

    # --- best lag including zero ---
    if lag_corr_map:
        best_lag_incl = max(lag_corr_map, key=lambda k: lag_corr_map[k])
        best_lag_corr_incl = lag_corr_map[best_lag_incl]
    else:
        best_lag_incl = None
        best_lag_corr_incl = np.nan

    # --- best non-zero lag ---
    nonzero_map = {k: v for k, v in lag_corr_map.items() if k != 0}
    if nonzero_map:
        best_nz_lag = max(nonzero_map, key=lambda k: nonzero_map[k])
        best_nz_corr = nonzero_map[best_nz_lag]
        opposite_lag = -best_nz_lag
        opposite_corr = lag_corr_map.get(opposite_lag, np.nan)
    else:
        best_nz_lag = None
        best_nz_corr = np.nan
        opposite_lag = None
        opposite_corr = np.nan

    # --- edges ---
    nonzero_edge_vs_same = (
        float(best_nz_corr) - float(same_day_corr)
        if (best_nz_lag is not None and not np.isnan(same_day_corr))
        else np.nan
    )
    nonzero_edge_vs_opposite = (
        float(best_nz_corr) - float(opposite_corr)
        if (best_nz_lag is not None and not np.isnan(opposite_corr))
        else np.nan
    )

    metrics = {
        "same_day_correlation": same_day_corr,
        "best_lag_including_zero": float(best_lag_incl) if best_lag_incl is not None else np.nan,
        "best_lag_correlation_including_zero": best_lag_corr_incl,
        "best_nonzero_lag": float(best_nz_lag) if best_nz_lag is not None else np.nan,
        "best_nonzero_lag_abs_days": int(abs(best_nz_lag)) if best_nz_lag is not None else np.nan,
        "best_nonzero_lag_correlation": best_nz_corr,
        "nonzero_edge_vs_same_day": nonzero_edge_vs_same,
        "opposite_lag": float(opposite_lag) if opposite_lag is not None else np.nan,
        "opposite_lag_correlation": opposite_corr,
        "nonzero_edge_vs_opposite_lag": nonzero_edge_vs_opposite,
        "_best_nz_lag_int": best_nz_lag,  # internal, for rolling stability
    }

    return lag_rows, metrics


# ---------------------------------------------------------------------------
# Rolling stability at best non-zero lag
# ---------------------------------------------------------------------------

def compute_rolling_best_nonzero_lag_stability(
    x: pd.Series,
    y: pd.Series,
    best_nz_lag_int: int,
    rolling_window: int,
    rolling_min_periods: int,
) -> dict:
    """
    Compute rolling correlation at the selected best non-zero lag.

    best_nz_lag_int > 0: rolling corr(x_t, y_{t+best_nz_lag_int})
                         implementation: rolling corr(x, y.shift(-best_nz_lag_int))
    best_nz_lag_int < 0: rolling corr(y_t, x_{t+abs(best_nz_lag_int)})
                         implementation: rolling corr(x.shift(-abs(best_nz_lag_int)), y)
                         Negative lag means canonical_ticker_2 leads canonical_ticker_1.
    """
    if best_nz_lag_int > 0:
        s1 = x
        s2 = y.shift(-best_nz_lag_int)
    else:
        # best_nz_lag_int < 0
        lag_abs = abs(best_nz_lag_int)
        s1 = x.shift(-lag_abs)
        s2 = y

    rolling = s1.rolling(window=rolling_window, min_periods=rolling_min_periods).corr(s2)
    valid = rolling.dropna()

    if valid.empty:
        return {
            "rolling_best_nonzero_lag_corr_mean": np.nan,
            "rolling_best_nonzero_lag_corr_median": np.nan,
            "rolling_best_nonzero_lag_corr_min": np.nan,
            "rolling_best_nonzero_lag_corr_max": np.nan,
            "rolling_best_nonzero_lag_corr_latest": np.nan,
            "rolling_best_nonzero_lag_valid_obs": 0,
            "share_rolling_best_nonzero_lag_corr_gt_015": np.nan,
            "share_rolling_best_nonzero_lag_corr_gt_020": np.nan,
            "share_rolling_best_nonzero_lag_corr_gt_030": np.nan,
            "share_rolling_best_nonzero_lag_corr_gt_050": np.nan,
        }

    n = len(valid)
    return {
        "rolling_best_nonzero_lag_corr_mean": float(valid.mean()),
        "rolling_best_nonzero_lag_corr_median": float(valid.median()),
        "rolling_best_nonzero_lag_corr_min": float(valid.min()),
        "rolling_best_nonzero_lag_corr_max": float(valid.max()),
        "rolling_best_nonzero_lag_corr_latest": float(valid.iloc[-1]),
        "rolling_best_nonzero_lag_valid_obs": n,
        "share_rolling_best_nonzero_lag_corr_gt_015": float((valid > 0.15).sum() / n),
        "share_rolling_best_nonzero_lag_corr_gt_020": float((valid > 0.20).sum() / n),
        "share_rolling_best_nonzero_lag_corr_gt_030": float((valid > 0.30).sum() / n),
        "share_rolling_best_nonzero_lag_corr_gt_050": float((valid > 0.50).sum() / n),
    }


def _empty_rolling_stats() -> dict:
    return {
        "rolling_best_nonzero_lag_corr_mean": np.nan,
        "rolling_best_nonzero_lag_corr_median": np.nan,
        "rolling_best_nonzero_lag_corr_min": np.nan,
        "rolling_best_nonzero_lag_corr_max": np.nan,
        "rolling_best_nonzero_lag_corr_latest": np.nan,
        "rolling_best_nonzero_lag_valid_obs": 0,
        "share_rolling_best_nonzero_lag_corr_gt_015": np.nan,
        "share_rolling_best_nonzero_lag_corr_gt_020": np.nan,
        "share_rolling_best_nonzero_lag_corr_gt_030": np.nan,
        "share_rolling_best_nonzero_lag_corr_gt_050": np.nan,
    }


# ---------------------------------------------------------------------------
# Classification helpers
# ---------------------------------------------------------------------------

def _nonzero_lag_direction(best_nz_lag: float, ct1: str, ct2: str) -> str:
    if pd.isna(best_nz_lag):
        return "INSUFFICIENT_DATA"
    if best_nz_lag > 0:
        return f"{ct1}_LEADS_{ct2}"
    return f"{ct2}_LEADS_{ct1}"


def _broad_signal_class(
    best_nz_lag: float,
    best_nz_corr: float,
    nonzero_edge_vs_same: float,
    nonzero_edge_vs_opposite: float,
    min_nz_corr: float,
    min_edge_same: float,
    min_edge_opposite: float,
) -> str:
    if pd.isna(best_nz_lag) or pd.isna(best_nz_corr):
        return "INSUFFICIENT_DATA"

    edge_same_ok = (not pd.isna(nonzero_edge_vs_same)) and (
        nonzero_edge_vs_same >= min_edge_same
    )

    if not edge_same_ok:
        return "SAME_DAY_STRONGER_NO_EDGE"

    edge_opposite_ok = (not pd.isna(nonzero_edge_vs_opposite)) and (
        nonzero_edge_vs_opposite >= min_edge_opposite
    )

    if best_nz_corr >= min_nz_corr and edge_same_ok and not edge_opposite_ok:
        return "POSSIBLE_NONZERO_LAG_BUT_OPPOSITE_NOT_CLEAR"

    if best_nz_corr >= min_nz_corr and edge_same_ok and edge_opposite_ok:
        return "POSSIBLE_NONZERO_LAG"

    if edge_same_ok and best_nz_corr < min_nz_corr:
        return "NONZERO_LAG_LOW_CORRELATION"

    return "SAME_DAY_STRONGER_NO_EDGE"


def _broad_stability_signal(
    rolling_valid_obs: int,
    share_gt_015: float,
    min_stability_share: float,
) -> str:
    if rolling_valid_obs == 0 or pd.isna(share_gt_015):
        return "INSUFFICIENT_DATA"
    if share_gt_015 >= min_stability_share:
        return "STABLE"
    return "UNSTABLE"


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

def _safe_cols(df: pd.DataFrame, cols: list[str]) -> list[str]:
    """Return only columns that exist in df."""
    return [c for c in cols if c in df.columns]


def write_broad_lead_lag_reports(
    candidates: pd.DataFrame,
    all_lag_rows: list[dict],
    summary_rows: list[dict],
    out_dir: Path,
    min_nz_corr: float,
    min_edge_same: float,
    min_edge_opposite: float,
    min_stability_share: float,
) -> dict[str, int]:
    counts: dict[str, int] = {}

    # 1. broad_lead_lag_candidate_pairs.csv
    cand_out = candidates[_safe_cols(candidates, CANDIDATE_OUT_COLS)].copy()
    cand_out["_score_sort"] = cand_out["source_score"].fillna(-np.inf)
    cand_out = cand_out.sort_values(
        ["_score_sort", "canonical_ticker_1", "canonical_ticker_2"],
        ascending=[False, True, True],
    ).drop(columns=["_score_sort"]).reset_index(drop=True)
    cand_out.to_csv(out_dir / "broad_lead_lag_candidate_pairs.csv", index=False)
    counts["candidate_pairs"] = len(cand_out)

    # 2. broad_lead_lag_all_lags.csv
    lag_df = pd.DataFrame(all_lag_rows)
    if not lag_df.empty:
        lag_df = lag_df.sort_values(
            ["canonical_ticker_1", "canonical_ticker_2", "lag"],
            ascending=[True, True, True],
        ).reset_index(drop=True)
    lag_df[_safe_cols(lag_df, ALL_LAG_COLS)].to_csv(
        out_dir / "broad_lead_lag_all_lags.csv", index=False
    )
    counts["all_lag_rows"] = len(lag_df)

    # 3. broad_lead_lag_pair_summary.csv
    summary_df = pd.DataFrame(summary_rows)
    if not summary_df.empty:
        summary_df["_class_order"] = summary_df["broad_lead_lag_signal_class"].map(
            lambda c: BROAD_SIGNAL_CLASS_ORDER.get(c, 99)
        )
        summary_df["_nze_sort"] = summary_df["nonzero_edge_vs_same_day"].fillna(-np.inf)
        summary_df["_nzc_sort"] = summary_df["best_nonzero_lag_correlation"].fillna(
            -np.inf
        )
        summary_df = summary_df.sort_values(
            [
                "_class_order",
                "_nze_sort",
                "_nzc_sort",
                "canonical_ticker_1",
                "canonical_ticker_2",
            ],
            ascending=[True, False, False, True, True],
        ).drop(columns=["_class_order", "_nze_sort", "_nzc_sort"]).reset_index(drop=True)

    out_cols = _safe_cols(summary_df, SUMMARY_COLS) if not summary_df.empty else SUMMARY_COLS
    (summary_df[out_cols] if not summary_df.empty else pd.DataFrame(columns=SUMMARY_COLS)).to_csv(
        out_dir / "broad_lead_lag_pair_summary.csv", index=False
    )
    counts["summary_rows"] = len(summary_df)

    # 4. broad_lead_lag_filtered_nonzero_signals.csv
    if not summary_df.empty:
        mask = (
            (summary_df["broad_lead_lag_signal_class"] == "POSSIBLE_NONZERO_LAG")
            & (summary_df["broad_stability_signal"] == "STABLE")
            & summary_df["best_nonzero_lag"].notna()
            & (summary_df["best_nonzero_lag"] != 0)
            & (summary_df["best_nonzero_lag_correlation"] >= min_nz_corr)
            & (summary_df["nonzero_edge_vs_same_day"] >= min_edge_same)
            & (summary_df["nonzero_edge_vs_opposite_lag"] >= min_edge_opposite)
            & (summary_df["share_rolling_best_nonzero_lag_corr_gt_015"] >= min_stability_share)
        )
        filtered = summary_df[mask].copy()
        filtered["_nze"] = filtered["nonzero_edge_vs_same_day"].fillna(-np.inf)
        filtered["_nzc"] = filtered["best_nonzero_lag_correlation"].fillna(-np.inf)
        filtered["_shr"] = filtered["share_rolling_best_nonzero_lag_corr_gt_015"].fillna(
            -np.inf
        )
        filtered = filtered.sort_values(
            ["_nze", "_nzc", "_shr", "canonical_ticker_1", "canonical_ticker_2"],
            ascending=[False, False, False, True, True],
        ).drop(columns=["_nze", "_nzc", "_shr"]).reset_index(drop=True)
        filtered[out_cols].to_csv(
            out_dir / "broad_lead_lag_filtered_nonzero_signals.csv", index=False
        )
        counts["filtered_signals"] = len(filtered)
    else:
        pd.DataFrame(columns=SUMMARY_COLS).to_csv(
            out_dir / "broad_lead_lag_filtered_nonzero_signals.csv", index=False
        )
        counts["filtered_signals"] = 0

    # 5. broad_lead_lag_interesting_nonzero_candidates.csv
    if not summary_df.empty:
        imask = (
            summary_df["best_nonzero_lag"].notna()
            & (summary_df["best_nonzero_lag"] != 0)
            & (summary_df["best_nonzero_lag_correlation"] >= min_nz_corr)
            & (summary_df["nonzero_edge_vs_same_day"] >= min_edge_same)
        )
        interesting = summary_df[imask].copy()
        interesting["_nze"] = interesting["nonzero_edge_vs_same_day"].fillna(-np.inf)
        interesting["_nzc"] = interesting["best_nonzero_lag_correlation"].fillna(-np.inf)
        interesting["_opp"] = interesting["nonzero_edge_vs_opposite_lag"].fillna(-np.inf)
        interesting = interesting.sort_values(
            ["_nze", "_nzc", "_opp", "canonical_ticker_1", "canonical_ticker_2"],
            ascending=[False, False, False, True, True],
        ).drop(columns=["_nze", "_nzc", "_opp"]).reset_index(drop=True)
        interesting[out_cols].to_csv(
            out_dir / "broad_lead_lag_interesting_nonzero_candidates.csv", index=False
        )
        counts["interesting_nonzero"] = len(interesting)
    else:
        pd.DataFrame(columns=SUMMARY_COLS).to_csv(
            out_dir / "broad_lead_lag_interesting_nonzero_candidates.csv", index=False
        )
        counts["interesting_nonzero"] = 0

    # 6. broad_lead_lag_by_source_report.csv
    if not summary_df.empty:
        src_rows: list[dict] = []
        for _, row in summary_df.iterrows():
            parts = [
                s.strip()
                for s in str(row.get("source_report", "")).split(",")
                if s.strip()
            ]
            for src in parts:
                src_rows.append(
                    {
                        "source_report": src,
                        "signal_class": row["broad_lead_lag_signal_class"],
                        "stability": row.get("broad_stability_signal", ""),
                    }
                )

        src_df = pd.DataFrame(src_rows)
        by_source_rows: list[dict] = []
        for src in sorted(src_df["source_report"].unique()):
            g = src_df[src_df["source_report"] == src]
            by_source_rows.append(
                {
                    "source_report": src,
                    "candidate_pair_count": len(g),
                    "possible_nonzero_lag_count": int(
                        (g["signal_class"] == "POSSIBLE_NONZERO_LAG").sum()
                    ),
                    "stable_possible_nonzero_lag_count": int(
                        (
                            (g["signal_class"] == "POSSIBLE_NONZERO_LAG")
                            & (g["stability"] == "STABLE")
                        ).sum()
                    ),
                    "same_day_stronger_no_edge_count": int(
                        (g["signal_class"] == "SAME_DAY_STRONGER_NO_EDGE").sum()
                    ),
                    "insufficient_data_count": int(
                        (g["signal_class"] == "INSUFFICIENT_DATA").sum()
                    ),
                }
            )
        by_source = pd.DataFrame(by_source_rows)
    else:
        by_source = pd.DataFrame(columns=BY_SOURCE_COLS)

    by_source.to_csv(out_dir / "broad_lead_lag_by_source_report.csv", index=False)
    counts["by_source_rows"] = len(by_source)

    return counts


def write_readme(
    out_dir: Path,
    db_path: Path,
    raw_candidate_dir: Path,
    residual_candidate_dir: Path,
    start_date: str,
    end_date: str,
    max_lag: int,
    min_overlap: int,
    rolling_window: int,
    rolling_min_periods: int,
    min_nz_corr: float,
    min_edge_same: float,
    min_edge_opposite: float,
    min_stability_share: float,
) -> None:
    lines = [
        "# V5b: Broad Lead-Lag Analysis",
        "",
        f"Input database: `{db_path}`",
        f"Raw candidate directory: `{raw_candidate_dir}`",
        f"Residual candidate directory: `{residual_candidate_dir}`",
        f"Output directory: `{out_dir}`",
        f"Date range: `{start_date}` to `{end_date}`",
        f"Max lag: `{max_lag}` trading days",
        f"Min overlap: `{min_overlap}` aligned observations",
        f"Rolling window: `{rolling_window}` days, min_periods `{rolling_min_periods}`",
        f"Min non-zero lag correlation: `{min_nz_corr}`",
        f"Min non-zero edge vs same-day: `{min_edge_same}`",
        f"Min non-zero edge vs opposite lag: `{min_edge_opposite}`",
        f"Min stability share (rolling > 0.15): `{min_stability_share}`",
        "",
        "## Difference Between V5 and V5b",
        "",
        "V5 (`find_lead_lag_pairs.py`) tests V4 residual-similar candidate pairs for",
        "true lead-lag behavior. V5 found all pairs to be same-day dominant, which is",
        "a valid result: V4 candidates were selected for strong same-day residual correlation.",
        "",
        "V5b (`find_lead_lag_pairs_broad.py`) broadens the candidate universe by reading",
        "both raw similarity outputs and residual similarity outputs across all report",
        "files. V5b explicitly reports the best non-zero lag for every pair even when",
        "same-day correlation remains stronger, allowing research into episodic or",
        "weak lead-lag patterns not captured by V5.",
        "",
        "## Residual Return Construction",
        "",
        "Residual returns are recomputed from the SQLite database using same-day",
        "cross-sectional leave-one-out neutralization identical to V4.",
        "Raw return is the daily percentage close change from `price_change_daily`.",
        "Market residual removes the equal-weight market return of all other tickers",
        "on the same day (leave-one-out). Sector-neutral residual further removes the",
        "same-day mean market residual of same-sector peers (leave-one-out).",
        "",
        "## Lag Interpretation",
        "",
        f"Lags are tested from `-{max_lag}` to `+{max_lag}` trading days.",
        "",
        "- Positive lag k: `canonical_ticker_1` leads `canonical_ticker_2` by k days.",
        "  Correlation is computed between residual(ticker_1, t) and residual(ticker_2, t+k).",
        "- Negative lag k: `canonical_ticker_2` leads `canonical_ticker_1` by abs(k) days.",
        "  Correlation is computed between residual(ticker_1, t+abs(k)) and residual(ticker_2, t).",
        "- Lag 0: same-day correlation baseline.",
        "",
        "## Key Metrics",
        "",
        "- `same_day_correlation`: baseline lag=0 Pearson correlation.",
        "- `best_nonzero_lag_correlation`: highest correlation among non-zero lags.",
        f"- `nonzero_edge_vs_same_day`: best non-zero lag corr minus same-day corr.",
        "  Positive value means the lagged relationship is stronger than same-day.",
        f"- `nonzero_edge_vs_opposite_lag`: best non-zero lag corr minus its mirror lag.",
        "  Positive value indicates directional asymmetry (one direction leads the other).",
        "",
        "## Why Broad Lead-Lag Candidates Are Not Trading Rules",
        "",
        "Lead-lag correlation is a statistical measure only. It is not evidence of causality.",
        "A detected lead-lag pattern may reflect shared information sources, sector dynamics",
        "not fully removed by neutralization, common macro sensitivity, or statistical noise.",
        "Results may be episodic: a pattern detected in one period may not persist.",
        "Filtered and interesting candidate files are starting points for further research,",
        "not ready-to-use trading rules. All results require independent validation.",
    ]
    (out_dir / "broad_lead_lag_readme.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    db_path = Path(args.db)
    raw_dir = Path(args.raw_candidate_dir)
    residual_dir = Path(args.residual_candidate_dir)
    out_dir = Path(args.output_dir)

    validate_database(db_path)
    raw_found, residual_found = validate_candidate_dirs(raw_dir, residual_dir)

    (
        candidates,
        raw_count,
        unique_before_score,
        after_score,
        after_max,
    ) = load_candidate_pairs(
        raw_found, residual_found, args.min_source_correlation, args.max_candidates
    )

    def _print_no_candidates(reason: str) -> None:
        print(f"SUMMARY db={db_path}")
        print(f"SUMMARY raw_candidate_dir={raw_dir}")
        print(f"SUMMARY residual_candidate_dir={residual_dir}")
        print(f"SUMMARY output_dir={out_dir}")
        print(f"SUMMARY start_date={args.start_date}")
        print(f"SUMMARY end_date={args.end_date}")
        print(f"SUMMARY raw_candidate_files_found={len(raw_found)}")
        print(f"SUMMARY residual_candidate_files_found={len(residual_found)}")
        print(f"SUMMARY candidate_rows_raw={raw_count}")
        print(f"SUMMARY candidate_pairs_unique_before_score_filter={unique_before_score}")
        print(f"SUMMARY candidate_pairs_after_score_filter={after_score}")
        print(f"SUMMARY candidate_pairs_after_max_candidates={after_max}")
        print("SUMMARY status=NO_USABLE_CANDIDATES")
        print(f"SUMMARY reason={reason}")

    if candidates.empty:
        _print_no_candidates("no_valid_pairs_in_candidate_files")
        return

    df = load_data(db_path, args.start_date, args.end_date)
    raw_rows = int(len(df))
    raw_tickers = int(df["ticker"].nunique()) if not df.empty else 0

    if df.empty:
        _print_no_candidates("no_price_data_in_date_range")
        return

    enriched = compute_residual_returns(df, args.min_sector_peers)
    residual_wide, meta = build_residual_wide_matrix(df, enriched, args.min_obs)
    filtered_tickers = int(len(residual_wide.columns))
    date_rows = int(len(residual_wide.index))

    valid_ticker_set = set(residual_wide.columns.tolist())
    pair_mask = candidates["canonical_ticker_1"].isin(valid_ticker_set) & candidates[
        "canonical_ticker_2"
    ].isin(valid_ticker_set)
    analyzed_candidates = candidates[pair_mask].copy().reset_index(drop=True)

    if analyzed_candidates.empty:
        out_dir.mkdir(parents=True, exist_ok=True)
        _print_no_candidates("no_candidate_pairs_survived_ticker_filter")
        return

    out_dir.mkdir(parents=True, exist_ok=True)

    all_lag_rows: list[dict] = []
    summary_rows: list[dict] = []

    for _, cand_row in analyzed_candidates.iterrows():
        ct1 = cand_row["canonical_ticker_1"]
        ct2 = cand_row["canonical_ticker_2"]
        x = residual_wide[ct1]
        y = residual_wide[ct2]

        lag_rows, metrics = compute_pair_broad_lead_lag(
            x, y, ct1, ct2, args.max_lag, args.min_overlap
        )

        meta_cols = {
            "source_family": cand_row.get("source_family", ""),
            "source_report": cand_row.get("source_report", ""),
            "source_score": cand_row.get("source_score", np.nan),
            "sector_1": cand_row.get("sector_1", ""),
            "industry_1": cand_row.get("industry_1", ""),
            "sector_2": cand_row.get("sector_2", ""),
            "industry_2": cand_row.get("industry_2", ""),
            "same_sector": cand_row.get("same_sector", False),
            "same_industry": cand_row.get("same_industry", False),
        }

        for lr in lag_rows:
            all_lag_rows.append(
                {"canonical_ticker_1": ct1, "canonical_ticker_2": ct2, **lr, **meta_cols}
            )

        best_nz_lag = metrics["best_nonzero_lag"]
        best_nz_corr = metrics["best_nonzero_lag_correlation"]
        nonzero_edge_vs_same = metrics["nonzero_edge_vs_same_day"]
        nonzero_edge_vs_opposite = metrics["nonzero_edge_vs_opposite_lag"]
        best_nz_lag_int = metrics["_best_nz_lag_int"]

        signal_class = _broad_signal_class(
            best_nz_lag,
            best_nz_corr,
            nonzero_edge_vs_same,
            nonzero_edge_vs_opposite,
            args.min_nonzero_lag_correlation,
            args.min_nonzero_edge_vs_same_day,
            args.min_nonzero_edge_vs_opposite_lag,
        )
        direction = _nonzero_lag_direction(best_nz_lag, ct1, ct2)

        if best_nz_lag_int is not None:
            rolling_stats = compute_rolling_best_nonzero_lag_stability(
                x, y, best_nz_lag_int, args.rolling_window, args.rolling_min_periods
            )
        else:
            rolling_stats = _empty_rolling_stats()

        stability = _broad_stability_signal(
            rolling_stats["rolling_best_nonzero_lag_valid_obs"],
            rolling_stats.get("share_rolling_best_nonzero_lag_corr_gt_015", np.nan),
            args.min_stability_share_gt_015,
        )

        summary_rows.append(
            {
                "canonical_ticker_1": ct1,
                "canonical_ticker_2": ct2,
                "same_day_correlation": metrics["same_day_correlation"],
                "best_lag_including_zero": metrics["best_lag_including_zero"],
                "best_lag_correlation_including_zero": metrics["best_lag_correlation_including_zero"],
                "best_nonzero_lag": best_nz_lag,
                "best_nonzero_lag_abs_days": metrics["best_nonzero_lag_abs_days"],
                "best_nonzero_lag_correlation": best_nz_corr,
                "nonzero_edge_vs_same_day": nonzero_edge_vs_same,
                "opposite_lag": metrics["opposite_lag"],
                "opposite_lag_correlation": metrics["opposite_lag_correlation"],
                "nonzero_edge_vs_opposite_lag": nonzero_edge_vs_opposite,
                "nonzero_lag_direction": direction,
                "broad_lead_lag_signal_class": signal_class,
                **rolling_stats,
                "broad_stability_signal": stability,
                "source_family": cand_row.get("source_family", ""),
                "source_report": cand_row.get("source_report", ""),
                "source_score": cand_row.get("source_score", np.nan),
                "source_raw_correlation": cand_row.get("source_raw_correlation", np.nan),
                "source_full_period_correlation": cand_row.get(
                    "source_full_period_correlation", np.nan
                ),
                "source_sector_residual_correlation": cand_row.get(
                    "source_sector_residual_correlation", np.nan
                ),
                "source_rolling_corr_mean": cand_row.get("source_rolling_corr_mean", np.nan),
                "source_rolling_residual_corr_mean": cand_row.get(
                    "source_rolling_residual_corr_mean", np.nan
                ),
                "sector_1": cand_row.get("sector_1", ""),
                "industry_1": cand_row.get("industry_1", ""),
                "sector_2": cand_row.get("sector_2", ""),
                "industry_2": cand_row.get("industry_2", ""),
                "same_sector": cand_row.get("same_sector", False),
                "same_industry": cand_row.get("same_industry", False),
            }
        )

    report_counts = write_broad_lead_lag_reports(
        analyzed_candidates,
        all_lag_rows,
        summary_rows,
        out_dir,
        args.min_nonzero_lag_correlation,
        args.min_nonzero_edge_vs_same_day,
        args.min_nonzero_edge_vs_opposite_lag,
        args.min_stability_share_gt_015,
    )

    write_readme(
        out_dir,
        db_path,
        raw_dir,
        residual_dir,
        args.start_date,
        args.end_date,
        args.max_lag,
        args.min_overlap,
        args.rolling_window,
        args.rolling_min_periods,
        args.min_nonzero_lag_correlation,
        args.min_nonzero_edge_vs_same_day,
        args.min_nonzero_edge_vs_opposite_lag,
        args.min_stability_share_gt_015,
    )

    summary_df = pd.DataFrame(summary_rows)

    def _count(col: str, val: str) -> int:
        return int((summary_df[col] == val).sum()) if not summary_df.empty else 0

    possible_nz = _count("broad_lead_lag_signal_class", "POSSIBLE_NONZERO_LAG")
    stable_possible_nz = (
        int(
            (
                (summary_df["broad_lead_lag_signal_class"] == "POSSIBLE_NONZERO_LAG")
                & (summary_df["broad_stability_signal"] == "STABLE")
            ).sum()
        )
        if not summary_df.empty
        else 0
    )
    interesting_nz = report_counts.get("interesting_nonzero", 0)
    same_day_stronger = _count("broad_lead_lag_signal_class", "SAME_DAY_STRONGER_NO_EDGE")
    insufficient = _count("broad_lead_lag_signal_class", "INSUFFICIENT_DATA")

    print(f"SUMMARY db={db_path}")
    print(f"SUMMARY raw_candidate_dir={raw_dir}")
    print(f"SUMMARY residual_candidate_dir={residual_dir}")
    print(f"SUMMARY output_dir={out_dir}")
    print(f"SUMMARY start_date={args.start_date}")
    print(f"SUMMARY end_date={args.end_date}")
    print(f"SUMMARY raw_rows={raw_rows}")
    print(f"SUMMARY raw_tickers={raw_tickers}")
    print(f"SUMMARY filtered_tickers={filtered_tickers}")
    print(f"SUMMARY date_rows={date_rows}")
    print(f"SUMMARY min_obs={args.min_obs}")
    print(f"SUMMARY min_sector_peers={args.min_sector_peers}")
    print(f"SUMMARY max_lag={args.max_lag}")
    print(f"SUMMARY min_overlap={args.min_overlap}")
    print(f"SUMMARY raw_candidate_files_found={len(raw_found)}")
    print(f"SUMMARY residual_candidate_files_found={len(residual_found)}")
    print(f"SUMMARY candidate_rows_raw={raw_count}")
    print(f"SUMMARY candidate_pairs_unique_before_score_filter={unique_before_score}")
    print(f"SUMMARY candidate_pairs_after_score_filter={after_score}")
    print(f"SUMMARY candidate_pairs_after_max_candidates={after_max}")
    print(f"SUMMARY candidate_pairs_analyzed={len(analyzed_candidates)}")
    print(f"SUMMARY all_lag_rows={len(all_lag_rows)}")
    print(f"SUMMARY possible_nonzero_lag_pairs={possible_nz}")
    print(f"SUMMARY stable_possible_nonzero_lag_pairs={stable_possible_nz}")
    print(f"SUMMARY interesting_nonzero_lag_candidates={interesting_nz}")
    print(f"SUMMARY same_day_stronger_no_edge_pairs={same_day_stronger}")
    print(f"SUMMARY insufficient_data_pairs={insufficient}")
    print("SUMMARY status=OK")


if __name__ == "__main__":
    main()
