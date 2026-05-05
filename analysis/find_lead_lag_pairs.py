from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CANDIDATE_FILES = [
    "residual_top_pairs_same_industry.csv",
    "residual_top_pairs_same_sector_cross_industry.csv",
    "residual_top_pairs_cross_sector.csv",
    "residual_top_pairs_unusual_sync.csv",
]

# Explicit sort order for signal class (ascending = best first)
SIGNAL_CLASS_ORDER = {
    "POSSIBLE_LEAD_LAG": 0,
    "POSSIBLE_BUT_LOW_CORRELATION": 1,
    "WEAK_EDGE": 2,
    "SAME_DAY_DOMINANT": 3,
    "INSUFFICIENT_DATA": 4,
}

SUMMARY_COLS = [
    "canonical_ticker_1",
    "canonical_ticker_2",
    "best_lag",
    "best_lag_abs_days",
    "best_lag_correlation",
    "same_day_correlation",
    "lead_lag_edge",
    "lead_lag_direction",
    "lead_lag_signal_class",
    "rolling_best_lag_corr_mean",
    "rolling_best_lag_corr_median",
    "rolling_best_lag_corr_min",
    "rolling_best_lag_corr_max",
    "rolling_best_lag_corr_latest",
    "rolling_best_lag_valid_obs",
    "share_rolling_best_lag_corr_gt_020",
    "share_rolling_best_lag_corr_gt_030",
    "share_rolling_best_lag_corr_gt_050",
    "stability_signal",
    "source_report",
    "source_raw_correlation",
    "source_sector_residual_correlation",
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
    "source_report",
    "sector_1",
    "industry_1",
    "sector_2",
    "industry_2",
    "same_sector",
    "same_industry",
]

CANDIDATE_COLS = [
    "canonical_ticker_1",
    "canonical_ticker_2",
    "source_report",
    "source_raw_correlation",
    "source_sector_residual_correlation",
    "source_rolling_residual_corr_mean",
    "sector_1",
    "industry_1",
    "sector_2",
    "industry_2",
    "same_sector",
    "same_industry",
]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Lead-lag analysis on residual-similar candidate pairs."
    )
    parser.add_argument("--db", required=True, help="Path to usa_close_change.db")
    parser.add_argument(
        "--candidate-dir", required=True, help="V4 residual similarity output directory"
    )
    parser.add_argument("--output-dir", required=True, help="Output directory")
    parser.add_argument("--start-date", default="2024-01-01")
    parser.add_argument("--end-date", default="2026-12-31")
    parser.add_argument("--min-obs", type=int, default=400)
    parser.add_argument("--min-sector-peers", type=int, default=5)
    parser.add_argument(
        "--max-lag",
        type=int,
        default=5,
        help="Maximum lag in trading days; tests -max_lag to +max_lag",
    )
    parser.add_argument(
        "--min-overlap",
        type=int,
        default=250,
        help="Min aligned observations required for a lag correlation",
    )
    parser.add_argument("--rolling-window", type=int, default=60)
    parser.add_argument("--rolling-min-periods", type=int, default=40)
    parser.add_argument("--min-best-lag-correlation", type=float, default=0.20)
    parser.add_argument("--min-lead-lag-edge", type=float, default=0.05)
    parser.add_argument("--min-stability-share-gt-020", type=float, default=0.50)
    return parser.parse_args()


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
        for required in ("instruments", "price_change_daily"):
            if required not in tables:
                raise SystemExit(f"ERROR: required table '{required}' not found in database")
        required_cols = {
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


def validate_candidate_files(candidate_dir: Path) -> list[Path]:
    if not candidate_dir.exists():
        raise SystemExit(f"ERROR: candidate directory not found: {candidate_dir}")
    found = [candidate_dir / f for f in CANDIDATE_FILES if (candidate_dir / f).exists()]
    if not found:
        raise SystemExit(
            f"ERROR: none of the expected candidate files found in {candidate_dir}"
        )
    return found


# ---------------------------------------------------------------------------
# Load candidate pairs
# ---------------------------------------------------------------------------

def _to_str(v: object) -> str:
    """Convert value to stripped string; return empty string for None/NaN."""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return ""
    return str(v).strip()


def load_candidate_pairs(found_files: list[Path]) -> tuple[pd.DataFrame, int, int]:
    """
    Read candidate pairs from found files, canonicalize pair order, deduplicate.

    Canonical pair: alphabetically sorted so canonical_ticker_1 <= canonical_ticker_2.
    When pair appears in multiple source files, source_report values are combined.
    sector_1/sector_2 and industry_1/industry_2 are swapped when canonical order
    differs from the source order to ensure they correspond to the canonical tickers.

    Returns (deduped_df, raw_row_count, unique_pair_count).
    """
    all_rows: list[dict] = []
    raw_count = 0

    for path in found_files:
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

            # Canonical order: alphabetically sort the two tickers
            ct1 = min(t1, t2)
            ct2 = max(t1, t2)
            # If canonical order differs from source, swap sector/industry metadata
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

            same_sector = _to_str(row.get("same_sector", "")).lower() in ("true", "1")
            same_industry = _to_str(row.get("same_industry", "")).lower() in ("true", "1")

            all_rows.append(
                {
                    "canonical_ticker_1": ct1,
                    "canonical_ticker_2": ct2,
                    "_source": source_name,
                    "source_raw_correlation": row.get("raw_correlation", np.nan),
                    "source_sector_residual_correlation": row.get(
                        "sector_residual_correlation", np.nan
                    ),
                    "source_rolling_residual_corr_mean": row.get(
                        "rolling_residual_corr_mean", np.nan
                    ),
                    "sector_1": s1,
                    "industry_1": i1,
                    "sector_2": s2,
                    "industry_2": i2,
                    "same_sector": same_sector,
                    "same_industry": same_industry,
                }
            )

    if not all_rows:
        return pd.DataFrame(), raw_count, 0

    combined = pd.DataFrame(all_rows)

    # Deduplicate: group by canonical pair, combine source_report, keep first row metadata
    pair_key = ["canonical_ticker_1", "canonical_ticker_2"]
    deduped_rows: list[dict] = []
    for (ct1, ct2), group in combined.groupby(pair_key, sort=True):
        sources = sorted({s for s in group["_source"].tolist() if s})
        source_report = ", ".join(sources)
        first = group.iloc[0]
        deduped_rows.append(
            {
                "canonical_ticker_1": ct1,
                "canonical_ticker_2": ct2,
                "source_report": source_report,
                "source_raw_correlation": first["source_raw_correlation"],
                "source_sector_residual_correlation": first[
                    "source_sector_residual_correlation"
                ],
                "source_rolling_residual_corr_mean": first["source_rolling_residual_corr_mean"],
                "sector_1": first["sector_1"],
                "industry_1": first["industry_1"],
                "sector_2": first["sector_2"],
                "industry_2": first["industry_2"],
                "same_sector": first["same_sector"],
                "same_industry": first["same_industry"],
            }
        )

    deduped = pd.DataFrame(deduped_rows)
    return deduped, raw_count, len(deduped)


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
            JOIN instruments i
                ON i.ticker = p.ticker
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
    Compute sector-neutral residual returns using same cross-sectional same-day
    leave-one-out logic as find_residual_similar_stocks.py (V4).

    Steps:
    1. market_return_ex_ticker: equal-weight mean of all OTHER tickers same day
    2. market_residual: raw_return - market_return_ex_ticker
    3. sector_market_residual_ex_ticker: equal-weight mean market_residual of
       same-sector peers excluding self, only when >= min_sector_peers peers available
    4. sector_neutral_residual: market_residual - sector_market_residual_ex_ticker
       Falls back to market_residual when sector adjustment is unavailable.
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
    enriched["market_residual"] = enriched["close_change"] - enriched["market_return_ex_ticker"]

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

    # Leave-one-out sector residual
    enriched["sector_market_residual_ex_ticker"] = np.where(
        has_sector_adjustment,
        (enriched["daily_sector_sum_market_residual"] - enriched["market_residual"])
        / (enriched["daily_sector_count"] - 1),
        np.nan,
    )

    # Sector-neutral residual: fallback to market_residual when sector unavailable
    enriched["sector_neutral_residual"] = np.where(
        enriched["sector_market_residual_ex_ticker"].notna(),
        enriched["market_residual"] - enriched["sector_market_residual_ex_ticker"],
        enriched["market_residual"],
    )

    return enriched


def build_residual_wide_matrix(
    df: pd.DataFrame,
    enriched: pd.DataFrame,
    min_obs: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build wide matrix of sector_neutral_residual.
    Tickers with fewer than min_obs raw observations are excluded.
    Returns (residual_wide, meta).
    """
    meta = (
        df.groupby("ticker", sort=True)
        .agg(obs_count_raw=("close_change", "count"))
        .reset_index()
    )
    valid_tickers = sorted(meta.loc[meta["obs_count_raw"] >= min_obs, "ticker"].tolist())

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

    lag = 0 : corr(x_t, y_t)                    — same-day baseline
    lag > 0 : corr(x_t, y_{t+lag})               — canonical_ticker_1 leads by lag days
              y.shift(-lag)[t] = y[t+lag], so: corr(x, y.shift(-lag))
    lag < 0 : corr(x_{t+abs(lag)}, y_t)          — canonical_ticker_2 leads by abs(lag) days
              x.shift(lag)[t] = x[t+abs(lag)] (lag is negative, so shift goes backward),
              so: corr(x.shift(lag), y)

    Returns (pearson_r, overlap_count). Returns (NaN, overlap) if overlap < min_overlap.
    """
    if lag == 0:
        s1, s2 = x, y
    elif lag > 0:
        # canonical_ticker_1 leads: align x_t with y_{t+lag}
        s1, s2 = x, y.shift(-lag)
    else:
        # canonical_ticker_2 leads: align x_{t+abs(lag)} with y_t
        # x.shift(lag) with lag < 0 shifts x backward by abs(lag) positions
        s1, s2 = x.shift(lag), y

    aligned = pd.concat([s1, s2], axis=1).dropna()
    overlap = len(aligned)
    if overlap < min_overlap:
        return np.nan, overlap

    corr = aligned.iloc[:, 0].corr(aligned.iloc[:, 1])
    return float(corr), overlap


# ---------------------------------------------------------------------------
# Per-pair lead-lag computation
# ---------------------------------------------------------------------------

def compute_pair_lead_lag(
    x: pd.Series,
    y: pd.Series,
    ct1: str,
    ct2: str,
    max_lag: int,
    min_overlap: int,
) -> tuple[list[dict], float, float, float, object, float]:
    """
    Compute lag correlations from -max_lag to +max_lag for a candidate pair.

    Returns:
        lag_rows          : list of per-lag dicts
        best_lag          : float (NaN if no valid lags)
        best_lag_corr     : float (NaN if no valid lags)
        same_day_corr     : float (NaN if lag=0 had insufficient overlap)
        best_lag_int      : int or None (None if no valid lags)
        lead_lag_edge     : float (NaN if undetermined)
    """
    lag_rows: list[dict] = []
    same_day_corr = np.nan

    for lag in range(-max_lag, max_lag + 1):
        corr, overlap = compute_lag_correlation(x, y, lag, min_overlap)

        if lag == 0:
            direction = "SAME_DAY"
            if not np.isnan(corr):
                same_day_corr = float(corr)
        elif lag > 0:
            direction = f"{ct1} leads {ct2} by {lag} trading days"
        else:
            direction = f"{ct2} leads {ct1} by {abs(lag)} trading days"

        lag_rows.append(
            {
                "lag": lag,
                "lag_direction_description": direction,
                "lag_correlation": corr,
                "overlap_obs": overlap,
            }
        )

    # Select best lag by maximum correlation (positive correlations preferred;
    # negative correlations are not lead-lag sync signals but are still tracked)
    valid_lags = [
        (r["lag"], r["lag_correlation"])
        for r in lag_rows
        if not np.isnan(r["lag_correlation"])
    ]
    if not valid_lags:
        return lag_rows, np.nan, np.nan, same_day_corr, None, np.nan

    best_lag_int, best_lag_corr = max(valid_lags, key=lambda t: t[1])
    best_lag = float(best_lag_int)

    if best_lag_int == 0:
        lead_lag_edge = 0.0
    elif np.isnan(same_day_corr):
        lead_lag_edge = np.nan
    else:
        lead_lag_edge = float(best_lag_corr) - float(same_day_corr)

    return lag_rows, best_lag, float(best_lag_corr), same_day_corr, best_lag_int, lead_lag_edge


# ---------------------------------------------------------------------------
# Rolling stability at best lag
# ---------------------------------------------------------------------------

def compute_rolling_best_lag_stability(
    x: pd.Series,
    y: pd.Series,
    best_lag_int: int,
    rolling_window: int,
    rolling_min_periods: int,
) -> dict:
    """
    Compute rolling correlation at the selected best lag.

    best_lag_int = 0 : rolling corr(x, y)
    best_lag_int > 0 : rolling corr(x, y.shift(-best_lag_int))   — x leads y
    best_lag_int < 0 : rolling corr(x.shift(best_lag_int), y)    — y leads x
    """
    if best_lag_int == 0:
        s1, s2 = x, y
    elif best_lag_int > 0:
        s1, s2 = x, y.shift(-best_lag_int)
    else:
        # best_lag_int < 0: x.shift(best_lag_int)[t] = x[t+abs(best_lag_int)]
        s1, s2 = x.shift(best_lag_int), y

    rolling = s1.rolling(window=rolling_window, min_periods=rolling_min_periods).corr(s2)
    valid = rolling.dropna()

    if valid.empty:
        return {
            "rolling_best_lag_corr_mean": np.nan,
            "rolling_best_lag_corr_median": np.nan,
            "rolling_best_lag_corr_min": np.nan,
            "rolling_best_lag_corr_max": np.nan,
            "rolling_best_lag_corr_latest": np.nan,
            "rolling_best_lag_valid_obs": 0,
            "share_rolling_best_lag_corr_gt_020": np.nan,
            "share_rolling_best_lag_corr_gt_030": np.nan,
            "share_rolling_best_lag_corr_gt_050": np.nan,
        }

    n = len(valid)
    return {
        "rolling_best_lag_corr_mean": float(valid.mean()),
        "rolling_best_lag_corr_median": float(valid.median()),
        "rolling_best_lag_corr_min": float(valid.min()),
        "rolling_best_lag_corr_max": float(valid.max()),
        "rolling_best_lag_corr_latest": float(valid.iloc[-1]),
        "rolling_best_lag_valid_obs": n,
        "share_rolling_best_lag_corr_gt_020": float((valid > 0.20).sum() / n),
        "share_rolling_best_lag_corr_gt_030": float((valid > 0.30).sum() / n),
        "share_rolling_best_lag_corr_gt_050": float((valid > 0.50).sum() / n),
    }


# ---------------------------------------------------------------------------
# Classification helpers
# ---------------------------------------------------------------------------

def _classify_signal(
    best_lag: float,
    best_lag_corr: float,
    lead_lag_edge: float,
    min_best_lag_corr: float,
    min_lead_lag_edge: float,
) -> str:
    if pd.isna(best_lag) or pd.isna(best_lag_corr):
        return "INSUFFICIENT_DATA"
    if best_lag == 0:
        return "SAME_DAY_DOMINANT"
    # best_lag != 0
    edge_ok = (not pd.isna(lead_lag_edge)) and (lead_lag_edge >= min_lead_lag_edge)
    if edge_ok and best_lag_corr >= min_best_lag_corr:
        return "POSSIBLE_LEAD_LAG"
    if edge_ok and best_lag_corr < min_best_lag_corr:
        return "POSSIBLE_BUT_LOW_CORRELATION"
    return "WEAK_EDGE"


def _lead_lag_direction(best_lag: float, ct1: str, ct2: str) -> str:
    if pd.isna(best_lag):
        return "INSUFFICIENT_DATA"
    if best_lag == 0:
        return "NO_LEAD_LAG"
    if best_lag > 0:
        return f"{ct1}_LEADS_{ct2}"
    return f"{ct2}_LEADS_{ct1}"


def _stability_signal(
    rolling_valid_obs: int,
    share_gt_020: float,
    min_stability_share: float,
) -> str:
    if rolling_valid_obs == 0 or pd.isna(share_gt_020):
        return "INSUFFICIENT_DATA"
    if share_gt_020 >= min_stability_share:
        return "STABLE"
    return "UNSTABLE"


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

def write_lead_lag_reports(
    candidates: pd.DataFrame,
    all_lag_rows: list[dict],
    summary_rows: list[dict],
    out_dir: Path,
    min_best_lag_corr: float,
    min_lead_lag_edge: float,
    min_stability_share: float,
) -> dict[str, int]:
    counts: dict[str, int] = {}

    # 1. lead_lag_candidate_pairs.csv
    cand_out_cols = [c for c in CANDIDATE_COLS if c in candidates.columns]
    cand_out = candidates[cand_out_cols].copy()
    cand_out = cand_out.sort_values(
        ["canonical_ticker_1", "canonical_ticker_2"]
    ).reset_index(drop=True)
    cand_out.to_csv(out_dir / "lead_lag_candidate_pairs.csv", index=False)
    counts["candidate_pairs"] = len(cand_out)

    # 2. lead_lag_all_lags.csv
    lag_df = pd.DataFrame(all_lag_rows)
    if not lag_df.empty:
        lag_df = lag_df.sort_values(
            ["canonical_ticker_1", "canonical_ticker_2", "lag"],
            ascending=[True, True, True],
        ).reset_index(drop=True)
    out_lag_cols = [c for c in ALL_LAG_COLS if c in lag_df.columns]
    lag_df[out_lag_cols].to_csv(out_dir / "lead_lag_all_lags.csv", index=False)
    counts["all_lag_rows"] = len(lag_df)

    # 3. lead_lag_pair_summary.csv
    summary_df = pd.DataFrame(summary_rows)
    if not summary_df.empty:
        summary_df["_class_order"] = summary_df["lead_lag_signal_class"].map(
            lambda c: SIGNAL_CLASS_ORDER.get(c, 99)
        )
        summary_df = summary_df.sort_values(
            [
                "_class_order",
                "lead_lag_edge",
                "best_lag_correlation",
                "canonical_ticker_1",
                "canonical_ticker_2",
            ],
            ascending=[True, False, False, True, True],
        ).drop(columns=["_class_order"]).reset_index(drop=True)

    out_summary_cols = [c for c in SUMMARY_COLS if c in summary_df.columns] if not summary_df.empty else SUMMARY_COLS
    (summary_df[out_summary_cols] if not summary_df.empty else pd.DataFrame(columns=SUMMARY_COLS)).to_csv(
        out_dir / "lead_lag_pair_summary.csv", index=False
    )
    counts["summary_rows"] = len(summary_df)

    # 4. lead_lag_filtered_signals.csv
    if not summary_df.empty:
        mask = (
            (summary_df["lead_lag_signal_class"] == "POSSIBLE_LEAD_LAG")
            & (summary_df["stability_signal"] == "STABLE")
            & (summary_df["best_lag"] != 0)
            & (summary_df["best_lag_correlation"] >= min_best_lag_corr)
            & (summary_df["lead_lag_edge"] >= min_lead_lag_edge)
            & (summary_df["share_rolling_best_lag_corr_gt_020"] >= min_stability_share)
        )
        filtered = summary_df[mask].copy()
        filtered = filtered.sort_values(
            [
                "lead_lag_edge",
                "best_lag_correlation",
                "share_rolling_best_lag_corr_gt_020",
                "canonical_ticker_1",
                "canonical_ticker_2",
            ],
            ascending=[False, False, False, True, True],
        ).reset_index(drop=True)
        filtered[out_summary_cols].to_csv(out_dir / "lead_lag_filtered_signals.csv", index=False)
        counts["filtered_signals"] = len(filtered)
    else:
        pd.DataFrame(columns=SUMMARY_COLS).to_csv(
            out_dir / "lead_lag_filtered_signals.csv", index=False
        )
        counts["filtered_signals"] = 0

    # 5. lead_lag_by_source_report.csv
    # Each pair is counted once per source_report component (pairs in multiple files
    # are counted in each relevant source row).
    if not summary_df.empty:
        source_rows: list[dict] = []
        for _, row in summary_df.iterrows():
            # Split combined source_report back into individual file names
            parts = [
                s.strip()
                for s in str(row.get("source_report", "")).split(",")
                if s.strip()
            ]
            for src in parts:
                source_rows.append(
                    {
                        "source_report": src,
                        "signal_class": row["lead_lag_signal_class"],
                        "stability": row.get("stability_signal", ""),
                    }
                )

        src_df = pd.DataFrame(source_rows)
        by_source_rows: list[dict] = []
        for src in sorted(src_df["source_report"].unique()):
            g = src_df[src_df["source_report"] == src]
            by_source_rows.append(
                {
                    "source_report": src,
                    "candidate_pair_count": len(g),
                    "possible_lead_lag_count": int(
                        (g["signal_class"] == "POSSIBLE_LEAD_LAG").sum()
                    ),
                    "stable_possible_lead_lag_count": int(
                        (
                            (g["signal_class"] == "POSSIBLE_LEAD_LAG")
                            & (g["stability"] == "STABLE")
                        ).sum()
                    ),
                    "same_day_dominant_count": int(
                        (g["signal_class"] == "SAME_DAY_DOMINANT").sum()
                    ),
                    "weak_edge_count": int((g["signal_class"] == "WEAK_EDGE").sum()),
                    "insufficient_data_count": int(
                        (g["signal_class"] == "INSUFFICIENT_DATA").sum()
                    ),
                }
            )
        by_source = pd.DataFrame(by_source_rows)
    else:
        by_source = pd.DataFrame(
            columns=[
                "source_report",
                "candidate_pair_count",
                "possible_lead_lag_count",
                "stable_possible_lead_lag_count",
                "same_day_dominant_count",
                "weak_edge_count",
                "insufficient_data_count",
            ]
        )

    by_source.to_csv(out_dir / "lead_lag_by_source_report.csv", index=False)
    counts["by_source_rows"] = len(by_source)

    return counts


def write_readme(
    out_dir: Path,
    db_path: Path,
    candidate_dir: Path,
    start_date: str,
    end_date: str,
    max_lag: int,
    min_overlap: int,
    rolling_window: int,
    rolling_min_periods: int,
    min_best_lag_corr: float,
    min_lead_lag_edge: float,
    min_stability_share: float,
) -> None:
    lines = [
        "# Lead-Lag Pair Analysis",
        "",
        f"Input database: `{db_path}`",
        f"Candidate directory: `{candidate_dir}`",
        f"Output directory: `{out_dir}`",
        f"Date range: `{start_date}` to `{end_date}`",
        f"Max lag: `{max_lag}` trading days",
        f"Min overlap: `{min_overlap}` aligned observations",
        f"Rolling window: `{rolling_window}` days, min_periods `{rolling_min_periods}`",
        f"Min best-lag correlation: `{min_best_lag_corr}`",
        f"Min lead-lag edge: `{min_lead_lag_edge}`",
        f"Min stability share (rolling > 0.20): `{min_stability_share}`",
        "",
        "## Residual Return Construction",
        "",
        "Market- and sector-neutral residual returns are recomputed from the SQLite database",
        "using the same cross-sectional same-day leave-one-out neutralization as V4.",
        "Raw return is the daily percentage close change from `price_change_daily`.",
        "Market residual removes the equal-weight market return of all other tickers on the same day.",
        "Sector-neutral residual further removes the same-day mean market residual of same-sector peers.",
        "",
        "## Lag Interpretation",
        "",
        "Lags are tested from `-max_lag` to `+max_lag` trading days.",
        "",
        f"- Positive lag k: `canonical_ticker_1` leads `canonical_ticker_2` by k days.",
        "  Correlation is computed between residual(ticker_1, t) and residual(ticker_2, t+k).",
        f"- Negative lag k: `canonical_ticker_2` leads `canonical_ticker_1` by abs(k) days.",
        "  Correlation is computed between residual(ticker_1, t+abs(k)) and residual(ticker_2, t).",
        "- Lag 0: same-day correlation baseline.",
        "",
        "## Lead-Lag Edge",
        "",
        "lead_lag_edge = best_lag_correlation - same_day_correlation.",
        f"A meaningful edge requires lead_lag_edge >= {min_lead_lag_edge}.",
        "If best_lag == 0, edge is defined as 0.0.",
        "",
        "## Signal Classes",
        "",
        "- POSSIBLE_LEAD_LAG: best_lag != 0, best_lag_correlation >= threshold, edge >= threshold.",
        "- POSSIBLE_BUT_LOW_CORRELATION: best_lag != 0, edge >= threshold, correlation < threshold.",
        "- WEAK_EDGE: best_lag != 0, but edge below threshold.",
        "- SAME_DAY_DOMINANT: best_lag == 0.",
        "- INSUFFICIENT_DATA: no valid lag correlations available.",
        "",
        "## Important Caveats",
        "",
        "Lead-lag correlation is a statistical measure only. It is not evidence of causality.",
        "A detected lead-lag pattern may reflect shared information sources, sector dynamics",
        "not fully removed by neutralization, common macro sensitivity, or statistical noise.",
        "Filtered signals are candidates for further research, not ready-to-use trading rules.",
        "Past lead-lag patterns are not guaranteed to persist in the future.",
    ]
    (out_dir / "lead_lag_readme.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    db_path = Path(args.db)
    candidate_dir = Path(args.candidate_dir)
    out_dir = Path(args.output_dir)

    validate_database(db_path)
    found_files = validate_candidate_files(candidate_dir)

    candidates, raw_count, unique_count = load_candidate_pairs(found_files)

    if candidates.empty:
        print(f"SUMMARY db={db_path}")
        print(f"SUMMARY candidate_dir={candidate_dir}")
        print(f"SUMMARY output_dir={out_dir}")
        print(f"SUMMARY start_date={args.start_date}")
        print(f"SUMMARY end_date={args.end_date}")
        print(f"SUMMARY candidate_files_found={len(found_files)}")
        print(f"SUMMARY candidate_rows_raw={raw_count}")
        print(f"SUMMARY candidate_pairs_unique=0")
        print("SUMMARY status=NO_USABLE_CANDIDATES")
        print("SUMMARY reason=no_valid_pairs_found_in_candidate_files")
        return

    df = load_data(db_path, args.start_date, args.end_date)
    raw_rows = int(len(df))
    raw_tickers = int(df["ticker"].nunique()) if not df.empty else 0

    if df.empty:
        print(f"SUMMARY db={db_path}")
        print(f"SUMMARY candidate_dir={candidate_dir}")
        print(f"SUMMARY output_dir={out_dir}")
        print(f"SUMMARY start_date={args.start_date}")
        print(f"SUMMARY end_date={args.end_date}")
        print(f"SUMMARY raw_rows={raw_rows}")
        print(f"SUMMARY raw_tickers={raw_tickers}")
        print("SUMMARY status=NO_USABLE_CANDIDATES")
        print("SUMMARY reason=no_price_data_in_date_range")
        return

    enriched = compute_residual_returns(df, args.min_sector_peers)
    residual_wide, meta = build_residual_wide_matrix(df, enriched, args.min_obs)
    filtered_tickers = int(len(residual_wide.columns))
    date_rows = int(len(residual_wide.index))

    # Filter candidates to pairs where both canonical tickers passed min_obs
    valid_ticker_set = set(residual_wide.columns.tolist())
    pair_mask = candidates["canonical_ticker_1"].isin(valid_ticker_set) & candidates[
        "canonical_ticker_2"
    ].isin(valid_ticker_set)
    analyzed_candidates = candidates[pair_mask].copy().reset_index(drop=True)

    if analyzed_candidates.empty:
        out_dir.mkdir(parents=True, exist_ok=True)
        print(f"SUMMARY db={db_path}")
        print(f"SUMMARY candidate_dir={candidate_dir}")
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
        print(f"SUMMARY candidate_files_found={len(found_files)}")
        print(f"SUMMARY candidate_rows_raw={raw_count}")
        print(f"SUMMARY candidate_pairs_unique={unique_count}")
        print(f"SUMMARY candidate_pairs_analyzed=0")
        print("SUMMARY status=NO_USABLE_CANDIDATES")
        print("SUMMARY reason=no_candidate_pairs_survived_ticker_filter")
        return

    out_dir.mkdir(parents=True, exist_ok=True)

    all_lag_rows: list[dict] = []
    summary_rows: list[dict] = []

    for _, cand_row in analyzed_candidates.iterrows():
        ct1 = cand_row["canonical_ticker_1"]
        ct2 = cand_row["canonical_ticker_2"]
        x = residual_wide[ct1]
        y = residual_wide[ct2]

        (
            lag_rows,
            best_lag,
            best_lag_corr,
            same_day_corr,
            best_lag_int,
            edge,
        ) = compute_pair_lead_lag(x, y, ct1, ct2, args.max_lag, args.min_overlap)

        meta_cols = {
            "source_report": cand_row.get("source_report", ""),
            "sector_1": cand_row.get("sector_1", ""),
            "industry_1": cand_row.get("industry_1", ""),
            "sector_2": cand_row.get("sector_2", ""),
            "industry_2": cand_row.get("industry_2", ""),
            "same_sector": cand_row.get("same_sector", False),
            "same_industry": cand_row.get("same_industry", False),
        }

        for lag_row in lag_rows:
            all_lag_rows.append(
                {
                    "canonical_ticker_1": ct1,
                    "canonical_ticker_2": ct2,
                    **lag_row,
                    **meta_cols,
                }
            )

        signal_class = _classify_signal(
            best_lag, best_lag_corr, edge,
            args.min_best_lag_correlation, args.min_lead_lag_edge,
        )
        direction = _lead_lag_direction(best_lag, ct1, ct2)

        if best_lag_int is not None:
            rolling_stats = compute_rolling_best_lag_stability(
                x, y, best_lag_int, args.rolling_window, args.rolling_min_periods
            )
        else:
            rolling_stats = {
                "rolling_best_lag_corr_mean": np.nan,
                "rolling_best_lag_corr_median": np.nan,
                "rolling_best_lag_corr_min": np.nan,
                "rolling_best_lag_corr_max": np.nan,
                "rolling_best_lag_corr_latest": np.nan,
                "rolling_best_lag_valid_obs": 0,
                "share_rolling_best_lag_corr_gt_020": np.nan,
                "share_rolling_best_lag_corr_gt_030": np.nan,
                "share_rolling_best_lag_corr_gt_050": np.nan,
            }

        stability = _stability_signal(
            rolling_stats["rolling_best_lag_valid_obs"],
            rolling_stats.get("share_rolling_best_lag_corr_gt_020", np.nan),
            args.min_stability_share_gt_020,
        )

        best_lag_abs = (
            int(abs(best_lag_int)) if best_lag_int is not None else np.nan
        )

        summary_rows.append(
            {
                "canonical_ticker_1": ct1,
                "canonical_ticker_2": ct2,
                "best_lag": best_lag,
                "best_lag_abs_days": best_lag_abs,
                "best_lag_correlation": best_lag_corr,
                "same_day_correlation": same_day_corr,
                "lead_lag_edge": edge,
                "lead_lag_direction": direction,
                "lead_lag_signal_class": signal_class,
                **rolling_stats,
                "stability_signal": stability,
                "source_report": cand_row.get("source_report", ""),
                "source_raw_correlation": cand_row.get("source_raw_correlation", np.nan),
                "source_sector_residual_correlation": cand_row.get(
                    "source_sector_residual_correlation", np.nan
                ),
                "source_rolling_residual_corr_mean": cand_row.get(
                    "source_rolling_residual_corr_mean", np.nan
                ),
                **{
                    k: cand_row.get(k, "")
                    for k in ("sector_1", "industry_1", "sector_2", "industry_2")
                },
                "same_sector": cand_row.get("same_sector", False),
                "same_industry": cand_row.get("same_industry", False),
            }
        )

    report_counts = write_lead_lag_reports(
        analyzed_candidates,
        all_lag_rows,
        summary_rows,
        out_dir,
        args.min_best_lag_correlation,
        args.min_lead_lag_edge,
        args.min_stability_share_gt_020,
    )

    write_readme(
        out_dir,
        db_path,
        candidate_dir,
        args.start_date,
        args.end_date,
        args.max_lag,
        args.min_overlap,
        args.rolling_window,
        args.rolling_min_periods,
        args.min_best_lag_correlation,
        args.min_lead_lag_edge,
        args.min_stability_share_gt_020,
    )

    # Summarize signal class counts
    summary_df_counts = pd.DataFrame(summary_rows)
    def _count(col: str, val: str) -> int:
        return int((summary_df_counts[col] == val).sum()) if not summary_df_counts.empty else 0

    possible_ll = _count("lead_lag_signal_class", "POSSIBLE_LEAD_LAG")
    stable_possible_ll = (
        int(
            (
                (summary_df_counts["lead_lag_signal_class"] == "POSSIBLE_LEAD_LAG")
                & (summary_df_counts["stability_signal"] == "STABLE")
            ).sum()
        )
        if not summary_df_counts.empty
        else 0
    )
    same_day_dom = _count("lead_lag_signal_class", "SAME_DAY_DOMINANT")
    weak_edge = _count("lead_lag_signal_class", "WEAK_EDGE")
    insufficient = _count("lead_lag_signal_class", "INSUFFICIENT_DATA")

    print(f"SUMMARY db={db_path}")
    print(f"SUMMARY candidate_dir={candidate_dir}")
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
    print(f"SUMMARY candidate_files_found={len(found_files)}")
    print(f"SUMMARY candidate_rows_raw={raw_count}")
    print(f"SUMMARY candidate_pairs_unique={unique_count}")
    print(f"SUMMARY candidate_pairs_analyzed={len(analyzed_candidates)}")
    print(f"SUMMARY all_lag_rows={len(all_lag_rows)}")
    print(f"SUMMARY possible_lead_lag_pairs={possible_ll}")
    print(f"SUMMARY stable_possible_lead_lag_pairs={stable_possible_ll}")
    print(f"SUMMARY same_day_dominant_pairs={same_day_dom}")
    print(f"SUMMARY weak_edge_pairs={weak_edge}")
    print(f"SUMMARY insufficient_data_pairs={insufficient}")
    print("SUMMARY status=OK")


if __name__ == "__main__":
    main()
