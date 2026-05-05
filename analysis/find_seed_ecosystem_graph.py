"""
V6b: Seed-centric ecosystem graph analysis from full pair files.

Unlike V6, which uses top-pair report CSVs and finds generally strong
ecosystem graphs, V6b:
  - Reads full pair CSVs (millions of rows, chunked for memory efficiency)
  - Starts from provided seed tickers
  - Finds seed-adjacent (first-hop) and second-order (second-hop) ecosystem candidates
  - Is intended for thematic research such as datacenter, where relevant companies
    may be spread across sectors and may not appear in generic top lists

All graph algorithms implemented without external graph libraries.
No networkx, scipy, or sklearn.
"""
from __future__ import annotations

import argparse
import sqlite3
from collections import defaultdict, deque
from pathlib import Path
from typing import Iterator

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_SEED_TICKERS = (
    "NVDA,AMD,AVGO,TSM,ASML,AMAT,LRCX,KLAC,ANET,MRVL,MPWR,VRT,ETN,"
    "PWR,NVT,APH,GLW,CIEN,DELL,SMCI,HPE,NTAP,EQIX,DLR,CEG,VST,NEE,GEV,EMR,TT"
)

# Base weights for combined_score
BASE_WEIGHTS = {
    "raw_correlation": 0.25,
    "raw_rolling_corr_mean": 0.15,
    "residual_correlation": 0.40,
    "residual_rolling_corr_mean": 0.20,
}

EDGE_OUTPUT_COLS = [
    "ticker_1", "ticker_2", "combined_score",
    "raw_correlation", "raw_rolling_corr_mean", "raw_rolling_corr_median",
    "raw_rolling_corr_latest",
    "residual_correlation", "residual_rolling_corr_mean",
    "residual_rolling_corr_median", "residual_rolling_corr_latest",
    "residual_delta_vs_raw",
    "edge_category", "cross_sector_edge", "cross_industry_edge",
    "seed_edge", "both_seed_edge", "graph_distance_class",
    "source_family", "source_file",
    "sector_1", "industry_1", "sector_2", "industry_2",
    "same_sector", "same_industry",
    "ticker_1_is_seed", "ticker_2_is_seed",
]

NODE_OUTPUT_COLS = [
    "ticker", "sector", "industry", "is_seed", "seed_status",
    "seed_distance", "nearest_seed_tickers",
    "component_id", "component_size",
    "degree", "weighted_degree", "cross_sector_degree", "cross_industry_degree",
    "seed_neighbor_count", "seed_neighbors",
    "best_seed_edge_score", "best_seed_neighbor",
    "best_first_hop_edge_score", "best_first_hop_neighbor",
]

COMP_OUTPUT_COLS = [
    "component_id", "ticker_count", "seed_count",
    "first_hop_count", "second_hop_count",
    "sector_count", "industry_count",
    "edge_count", "average_edge_score", "max_edge_score",
    "cross_sector_edge_count", "cross_industry_edge_count",
    "sectors", "industries", "tickers", "seed_tickers",
    "first_hop_tickers", "second_hop_tickers",
    "top_weighted_degree_tickers",
]

CLIQUE_OUTPUT_COLS = [
    "clique_id", "clique_size", "tickers", "seed_count", "seed_tickers",
    "first_hop_count", "second_hop_count",
    "sector_count", "industry_count", "sectors", "industries",
    "edge_count", "average_combined_score", "min_combined_score",
    "max_combined_score",
    "cross_sector_edge_count", "cross_industry_edge_count", "edge_categories",
]

SEED_SUMMARY_COLS = [
    "seed_ticker", "found_in_graph", "sector", "industry",
    "degree", "weighted_degree",
    "direct_non_seed_neighbor_count", "direct_non_seed_neighbors",
    "first_hop_component_id", "first_hop_component_size",
    "cross_sector_direct_edge_count", "cross_industry_direct_edge_count",
    "best_direct_edge_score", "best_direct_neighbor",
]

FIRST_HOP_COLS = [
    "theme_name", "ticker", "sector", "industry",
    "seed_distance", "nearest_seed_tickers",
    "seed_neighbor_count", "seed_neighbors",
    "best_seed_edge_score", "best_seed_neighbor",
    "component_id", "component_size",
    "degree", "weighted_degree", "cross_sector_degree", "cross_industry_degree",
    "evidence_level",
]

SECOND_HOP_COLS = [
    "theme_name", "ticker", "sector", "industry",
    "seed_distance", "nearest_seed_tickers",
    "seed_neighbor_count", "seed_neighbors",
    "best_first_hop_edge_score", "best_first_hop_neighbor",
    "component_id", "component_size",
    "degree", "weighted_degree", "cross_sector_degree", "cross_industry_degree",
    "evidence_level",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_float(v: object) -> float:
    if v is None:
        return np.nan
    try:
        f = float(v)
        return np.nan if f != f else f
    except (TypeError, ValueError):
        return np.nan


def _to_str(v: object) -> str:
    if v is None or (isinstance(v, float) and v != v):
        return ""
    return str(v).strip()


def _safe_ge(v: object, threshold: float) -> bool:
    f = _to_float(v)
    return not (f != f) and f >= threshold


def _sorted_csv(*values: object) -> str:
    parts = sorted({_to_str(x) for x in values if _to_str(x)})
    return ", ".join(parts)


def _nan_to_none(v: float) -> float | None:
    return None if (v != v) else v


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="V6b: Seed-centric ecosystem graph from full pair files."
    )
    p.add_argument("--db", required=True)
    p.add_argument("--raw-full-dir", required=True)
    p.add_argument("--residual-full-dir", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--theme-name", default="datacenter")
    p.add_argument("--seed-tickers", default=DEFAULT_SEED_TICKERS)
    p.add_argument("--raw-min-correlation", type=float, default=0.35)
    p.add_argument("--residual-min-correlation", type=float, default=0.20)
    p.add_argument("--seed-raw-min-correlation", type=float, default=0.30)
    p.add_argument("--seed-residual-min-correlation", type=float, default=0.15)
    p.add_argument("--second-hop-min-combined-score", type=float, default=0.35)
    p.add_argument("--min-combined-score", type=float, default=0.25)
    p.add_argument("--max-first-hop-nodes", type=int, default=500)
    p.add_argument("--max-second-hop-nodes", type=int, default=1000)
    p.add_argument("--min-component-size", type=int, default=3)
    p.add_argument("--min-clique-size", type=int, default=3)
    p.add_argument("--max-clique-size", type=int, default=6)
    p.add_argument("--max-cliques", type=int, default=1000)
    p.add_argument("--chunksize", type=int, default=500_000)
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
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table';"
            ).fetchall()
        }
        if "instruments" not in tables:
            raise SystemExit("ERROR: table 'instruments' not found")
        cols = {
            r[1]
            for r in conn.execute("PRAGMA table_info(instruments);").fetchall()
        }
        for c in ("ticker", "sector", "industry"):
            if c not in cols:
                raise SystemExit(f"ERROR: instruments missing column '{c}'")
    finally:
        conn.close()


def validate_input_dirs(
    raw_dir: Path, residual_dir: Path
) -> tuple[Path | None, Path | None, Path | None, Path | None]:
    """
    Returns (raw_full, residual_full, raw_rolling, residual_rolling).
    At least one full-period file must exist.
    """
    raw_full = raw_dir / "similar_pairs_full_period.csv"
    res_full = residual_dir / "residual_pairs_full_period.csv"
    raw_roll = raw_dir / "similar_pairs_rolling.csv"
    res_roll = residual_dir / "residual_pairs_rolling.csv"

    rf = raw_full if raw_full.exists() else None
    resf = res_full if res_full.exists() else None
    rr = raw_roll if raw_roll.exists() else None
    resr = res_roll if res_roll.exists() else None

    if rf is None and resf is None:
        raise SystemExit(
            "ERROR: no full-period pair files found in either directory"
        )
    return rf, resf, rr, resr


# ---------------------------------------------------------------------------
# Load instruments
# ---------------------------------------------------------------------------

def load_instruments(db_path: Path) -> dict[str, dict]:
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            "SELECT ticker, sector, industry FROM instruments ORDER BY ticker;"
        ).fetchall()
    finally:
        conn.close()
    result: dict[str, dict] = {}
    for ticker, sector, industry in rows:
        t = _to_str(ticker)
        if t:
            result[t] = {"sector": _to_str(sector), "industry": _to_str(industry)}
    return result


# ---------------------------------------------------------------------------
# Pair normalization
# ---------------------------------------------------------------------------

def normalize_pair(t1: str, t2: str) -> tuple[str, str] | None:
    """Return canonical (smaller, larger) or None if invalid."""
    t1 = _to_str(t1)
    t2 = _to_str(t2)
    if not t1 or not t2 or t1 == t2:
        return None
    return (min(t1, t2), max(t1, t2))


def merge_edge_records(existing: dict, new: dict) -> dict:
    """
    Merge two edge record dicts: combine source lists, take max numeric metrics.
    """
    merged = dict(existing)
    # Combine source fields
    for fld in ("source_file", "source_family"):
        ex_val = _to_str(existing.get(fld, ""))
        nw_val = _to_str(new.get(fld, ""))
        all_parts = sorted(
            {p.strip() for p in (ex_val + "," + nw_val).split(",") if p.strip()}
        )
        merged[fld] = ", ".join(all_parts)
    # Take max of numeric metrics
    numeric_keys = [
        "raw_correlation", "raw_rolling_corr_mean", "raw_rolling_corr_median",
        "raw_rolling_corr_latest",
        "raw_share_rolling_corr_gt_030", "raw_share_rolling_corr_gt_050",
        "raw_share_rolling_corr_gt_070",
        "residual_correlation", "residual_rolling_corr_mean",
        "residual_rolling_corr_median", "residual_rolling_corr_latest",
        "residual_share_rolling_corr_gt_020", "residual_share_rolling_corr_gt_030",
        "residual_share_rolling_corr_gt_050", "residual_share_rolling_corr_gt_070",
        "residual_delta_vs_raw",
    ]
    for k in numeric_keys:
        ev = _to_float(existing.get(k))
        nv = _to_float(new.get(k))
        if ev != ev and nv != nv:
            merged[k] = np.nan
        elif ev != ev:
            merged[k] = nv
        elif nv != nv:
            merged[k] = ev
        else:
            merged[k] = max(ev, nv)
    return merged


# ---------------------------------------------------------------------------
# Combined score
# ---------------------------------------------------------------------------

def compute_combined_score(rec: dict) -> float:
    """
    Weighted average of available metrics. Missing weights are rescaled.
    """
    candidates = [
        ("raw_correlation", _to_float(rec.get("raw_correlation"))),
        ("raw_rolling_corr_mean", _to_float(rec.get("raw_rolling_corr_mean"))),
        ("residual_correlation", _to_float(rec.get("residual_correlation"))),
        ("residual_rolling_corr_mean", _to_float(rec.get("residual_rolling_corr_mean"))),
    ]
    available = [
        (BASE_WEIGHTS[name], v)
        for name, v in candidates
        if not (v != v)
    ]
    if not available:
        return np.nan
    total_w = sum(w for w, _ in available)
    return sum(w * v for w, v in available) / total_w


# ---------------------------------------------------------------------------
# Stream full pair edges (chunked)
# ---------------------------------------------------------------------------

def stream_full_pair_edges(
    path: Path,
    family: str,
    instruments: dict[str, dict],
    chunksize: int,
) -> Iterator[dict]:
    """
    Yield normalized edge dicts from a full-period pair CSV, one row at a time.
    Skips rows with missing tickers or tickers not in instruments.
    family: RAW_FULL or RESIDUAL_FULL
    """
    source_file = path.stem

    is_residual = family == "RESIDUAL_FULL"

    for chunk in pd.read_csv(path, chunksize=chunksize, low_memory=False):
        if "ticker_1" not in chunk.columns or "ticker_2" not in chunk.columns:
            continue
        for _, row in chunk.iterrows():
            pair = normalize_pair(
                str(row.get("ticker_1", "")), str(row.get("ticker_2", ""))
            )
            if pair is None:
                continue
            ct1, ct2 = pair
            if ct1 not in instruments or ct2 not in instruments:
                continue

            if is_residual:
                raw_corr = _to_float(row.get("raw_correlation"))
                res_corr = _to_float(row.get("sector_residual_correlation"))
                res_delta = _to_float(row.get("correlation_delta_vs_raw"))
                rec = {
                    "canonical_ticker_1": ct1,
                    "canonical_ticker_2": ct2,
                    "source_file": source_file,
                    "source_family": family,
                    "raw_correlation": raw_corr,
                    "residual_correlation": res_corr,
                    "residual_delta_vs_raw": res_delta,
                    # rolling fields absent in full-period file
                    "raw_rolling_corr_mean": np.nan,
                    "raw_rolling_corr_median": np.nan,
                    "raw_rolling_corr_latest": np.nan,
                    "raw_share_rolling_corr_gt_030": np.nan,
                    "raw_share_rolling_corr_gt_050": np.nan,
                    "raw_share_rolling_corr_gt_070": np.nan,
                    "residual_rolling_corr_mean": np.nan,
                    "residual_rolling_corr_median": np.nan,
                    "residual_rolling_corr_latest": np.nan,
                    "residual_share_rolling_corr_gt_020": np.nan,
                    "residual_share_rolling_corr_gt_030": np.nan,
                    "residual_share_rolling_corr_gt_050": np.nan,
                    "residual_share_rolling_corr_gt_070": np.nan,
                }
            else:
                # raw full-period: column is 'correlation'
                raw_corr = _to_float(row.get("correlation"))
                rec = {
                    "canonical_ticker_1": ct1,
                    "canonical_ticker_2": ct2,
                    "source_file": source_file,
                    "source_family": family,
                    "raw_correlation": raw_corr,
                    "residual_correlation": np.nan,
                    "residual_delta_vs_raw": np.nan,
                    "raw_rolling_corr_mean": np.nan,
                    "raw_rolling_corr_median": np.nan,
                    "raw_rolling_corr_latest": np.nan,
                    "raw_share_rolling_corr_gt_030": np.nan,
                    "raw_share_rolling_corr_gt_050": np.nan,
                    "raw_share_rolling_corr_gt_070": np.nan,
                    "residual_rolling_corr_mean": np.nan,
                    "residual_rolling_corr_median": np.nan,
                    "residual_rolling_corr_latest": np.nan,
                    "residual_share_rolling_corr_gt_020": np.nan,
                    "residual_share_rolling_corr_gt_030": np.nan,
                    "residual_share_rolling_corr_gt_050": np.nan,
                    "residual_share_rolling_corr_gt_070": np.nan,
                }
            yield rec


# ---------------------------------------------------------------------------
# Load rolling pair edges (small files, load all at once)
# ---------------------------------------------------------------------------

def load_rolling_pair_edges(
    path: Path,
    family: str,
    instruments: dict[str, dict],
) -> list[dict]:
    """
    Load rolling pair CSV into list of normalized edge dicts.
    family: RAW_ROLLING or RESIDUAL_ROLLING
    """
    source_file = path.stem
    is_residual = family == "RESIDUAL_ROLLING"

    try:
        df = pd.read_csv(path, low_memory=False)
    except Exception:
        return []

    if "ticker_1" not in df.columns or "ticker_2" not in df.columns:
        return []

    records: list[dict] = []
    for _, row in df.iterrows():
        pair = normalize_pair(
            str(row.get("ticker_1", "")), str(row.get("ticker_2", ""))
        )
        if pair is None:
            continue
        ct1, ct2 = pair
        if ct1 not in instruments or ct2 not in instruments:
            continue

        if is_residual:
            rec = {
                "canonical_ticker_1": ct1,
                "canonical_ticker_2": ct2,
                "source_file": source_file,
                "source_family": family,
                "raw_correlation": _to_float(row.get("raw_correlation")),
                "residual_correlation": _to_float(row.get("sector_residual_correlation")),
                "residual_delta_vs_raw": _to_float(row.get("correlation_delta_vs_raw")),
                "raw_rolling_corr_mean": np.nan,
                "raw_rolling_corr_median": np.nan,
                "raw_rolling_corr_latest": np.nan,
                "raw_share_rolling_corr_gt_030": np.nan,
                "raw_share_rolling_corr_gt_050": np.nan,
                "raw_share_rolling_corr_gt_070": np.nan,
                "residual_rolling_corr_mean": _to_float(row.get("rolling_residual_corr_mean")),
                "residual_rolling_corr_median": _to_float(row.get("rolling_residual_corr_median")),
                "residual_rolling_corr_latest": _to_float(row.get("rolling_residual_corr_latest")),
                "residual_share_rolling_corr_gt_020": _to_float(row.get("share_rolling_residual_corr_gt_020")),
                "residual_share_rolling_corr_gt_030": _to_float(row.get("share_rolling_residual_corr_gt_030")),
                "residual_share_rolling_corr_gt_050": _to_float(row.get("share_rolling_residual_corr_gt_050")),
                "residual_share_rolling_corr_gt_070": _to_float(row.get("share_rolling_residual_corr_gt_070")),
            }
        else:
            rec = {
                "canonical_ticker_1": ct1,
                "canonical_ticker_2": ct2,
                "source_file": source_file,
                "source_family": family,
                "raw_correlation": _to_float(row.get("full_period_correlation")),
                "residual_correlation": np.nan,
                "residual_delta_vs_raw": np.nan,
                "raw_rolling_corr_mean": _to_float(row.get("rolling_corr_mean")),
                "raw_rolling_corr_median": _to_float(row.get("rolling_corr_median")),
                "raw_rolling_corr_latest": _to_float(row.get("rolling_corr_latest")),
                "raw_share_rolling_corr_gt_030": _to_float(row.get("share_rolling_corr_gt_030")),
                "raw_share_rolling_corr_gt_050": _to_float(row.get("share_rolling_corr_gt_050")),
                "raw_share_rolling_corr_gt_070": _to_float(row.get("share_rolling_corr_gt_070")),
                "residual_rolling_corr_mean": np.nan,
                "residual_rolling_corr_median": np.nan,
                "residual_rolling_corr_latest": np.nan,
                "residual_share_rolling_corr_gt_020": np.nan,
                "residual_share_rolling_corr_gt_030": np.nan,
                "residual_share_rolling_corr_gt_050": np.nan,
                "residual_share_rolling_corr_gt_070": np.nan,
            }
        records.append(rec)
    return records


# ---------------------------------------------------------------------------
# PASS 1: collect direct seed edges
# ---------------------------------------------------------------------------

def pass1_collect_seed_edges(
    full_files: list[tuple[Path, str]],
    seed_set: set[str],
    instruments: dict[str, dict],
    chunksize: int,
    seed_raw_min: float,
    seed_res_min: float,
    min_combined: float,
) -> tuple[dict[tuple[str, str], dict], int, int]:
    """
    Stream all full pair files. Collect edges where at least one ticker is a seed
    and passes seed thresholds. Deduplicate by canonical pair (take max metrics).

    Returns:
        seed_edges: canonical pair -> edge record
        rows_scanned: total rows read
        skipped_metadata: rows skipped due to missing metadata
    """
    seed_edges: dict[tuple[str, str], dict] = {}
    rows_scanned = 0
    skipped_metadata = 0

    for path, family in full_files:
        source_file = path.stem
        is_residual = family == "RESIDUAL_FULL"

        for chunk in pd.read_csv(path, chunksize=chunksize, low_memory=False):
            rows_scanned += len(chunk)
            if "ticker_1" not in chunk.columns or "ticker_2" not in chunk.columns:
                continue

            for _, row in chunk.iterrows():
                t1_raw = str(row.get("ticker_1", ""))
                t2_raw = str(row.get("ticker_2", ""))
                pair = normalize_pair(t1_raw, t2_raw)
                if pair is None:
                    continue
                ct1, ct2 = pair

                if ct1 not in instruments or ct2 not in instruments:
                    skipped_metadata += 1
                    continue

                # Only care about seed edges in pass 1
                if ct1 not in seed_set and ct2 not in seed_set:
                    continue

                if is_residual:
                    raw_corr = _to_float(row.get("raw_correlation"))
                    res_corr = _to_float(row.get("sector_residual_correlation"))
                    res_delta = _to_float(row.get("correlation_delta_vs_raw"))
                    rec = {
                        "canonical_ticker_1": ct1,
                        "canonical_ticker_2": ct2,
                        "source_file": source_file,
                        "source_family": family,
                        "raw_correlation": raw_corr,
                        "residual_correlation": res_corr,
                        "residual_delta_vs_raw": res_delta,
                        "raw_rolling_corr_mean": np.nan,
                        "raw_rolling_corr_median": np.nan,
                        "raw_rolling_corr_latest": np.nan,
                        "raw_share_rolling_corr_gt_030": np.nan,
                        "raw_share_rolling_corr_gt_050": np.nan,
                        "raw_share_rolling_corr_gt_070": np.nan,
                        "residual_rolling_corr_mean": np.nan,
                        "residual_rolling_corr_median": np.nan,
                        "residual_rolling_corr_latest": np.nan,
                        "residual_share_rolling_corr_gt_020": np.nan,
                        "residual_share_rolling_corr_gt_030": np.nan,
                        "residual_share_rolling_corr_gt_050": np.nan,
                        "residual_share_rolling_corr_gt_070": np.nan,
                    }
                else:
                    raw_corr = _to_float(row.get("correlation"))
                    rec = {
                        "canonical_ticker_1": ct1,
                        "canonical_ticker_2": ct2,
                        "source_file": source_file,
                        "source_family": family,
                        "raw_correlation": raw_corr,
                        "residual_correlation": np.nan,
                        "residual_delta_vs_raw": np.nan,
                        "raw_rolling_corr_mean": np.nan,
                        "raw_rolling_corr_median": np.nan,
                        "raw_rolling_corr_latest": np.nan,
                        "raw_share_rolling_corr_gt_030": np.nan,
                        "raw_share_rolling_corr_gt_050": np.nan,
                        "raw_share_rolling_corr_gt_070": np.nan,
                        "residual_rolling_corr_mean": np.nan,
                        "residual_rolling_corr_median": np.nan,
                        "residual_rolling_corr_latest": np.nan,
                        "residual_share_rolling_corr_gt_020": np.nan,
                        "residual_share_rolling_corr_gt_030": np.nan,
                        "residual_share_rolling_corr_gt_050": np.nan,
                        "residual_share_rolling_corr_gt_070": np.nan,
                    }
                    raw_corr = rec["raw_correlation"]

                # Quick pre-filter before combined score
                raw_ok = _safe_ge(rec["raw_correlation"], seed_raw_min)
                res_ok = _safe_ge(rec["residual_correlation"], seed_res_min)
                if not raw_ok and not res_ok:
                    continue

                cs = compute_combined_score(rec)
                if not _safe_ge(cs, min_combined):
                    continue

                rec["combined_score"] = cs

                key = (ct1, ct2)
                if key in seed_edges:
                    seed_edges[key] = merge_edge_records(seed_edges[key], rec)
                    # recompute combined_score after merge
                    seed_edges[key]["combined_score"] = compute_combined_score(seed_edges[key])
                else:
                    seed_edges[key] = rec

    return seed_edges, rows_scanned, skipped_metadata


# ---------------------------------------------------------------------------
# Rank first-hop nodes
# ---------------------------------------------------------------------------

def rank_first_hop_nodes(
    seed_edges: dict[tuple[str, str], dict],
    seed_set: set[str],
    max_first_hop: int,
) -> set[str]:
    """
    From seed edges, extract non-seed tickers.
    Rank by their best direct-seed edge combined_score descending.
    Return top max_first_hop non-seed tickers plus all seeds that appear.
    """
    best_score: dict[str, float] = {}
    for (ct1, ct2), edge in seed_edges.items():
        cs = _to_float(edge.get("combined_score"))
        if cs != cs:
            continue
        for t in (ct1, ct2):
            if t not in seed_set:
                if t not in best_score or cs > best_score[t]:
                    best_score[t] = cs

    # Sort by score desc, then ticker asc for determinism
    ranked = sorted(best_score.items(), key=lambda x: (-x[1], x[0]))
    top_nodes = {t for t, _ in ranked[:max_first_hop]}
    return top_nodes


# ---------------------------------------------------------------------------
# PASS 2: collect second-hop edges
# ---------------------------------------------------------------------------

def pass2_collect_second_hop_edges(
    full_files: list[tuple[Path, str]],
    first_hop_set: set[str],  # non-seed first-hop nodes
    seed_set: set[str],
    all_retained_nodes: set[str],  # seed + first_hop
    instruments: dict[str, dict],
    chunksize: int,
    raw_min: float,
    res_min: float,
    second_hop_min_cs: float,
    min_combined: float,
) -> tuple[dict[tuple[str, str], dict], int]:
    """
    Stream full pair files. Collect edges where:
    - one endpoint is in first_hop_set (non-seed first-hop)
    - other endpoint is outside all_retained_nodes
    - passes general thresholds and second-hop min combined score

    Returns: second_hop_edges dict, rows_scanned_total
    """
    second_hop_edges: dict[tuple[str, str], dict] = {}
    rows_scanned = 0

    for path, family in full_files:
        source_file = path.stem
        is_residual = family == "RESIDUAL_FULL"

        for chunk in pd.read_csv(path, chunksize=chunksize, low_memory=False):
            rows_scanned += len(chunk)
            if "ticker_1" not in chunk.columns or "ticker_2" not in chunk.columns:
                continue

            for _, row in chunk.iterrows():
                pair = normalize_pair(
                    str(row.get("ticker_1", "")), str(row.get("ticker_2", ""))
                )
                if pair is None:
                    continue
                ct1, ct2 = pair

                if ct1 not in instruments or ct2 not in instruments:
                    continue

                # One must be in first_hop, the other must NOT be in all_retained_nodes
                ct1_fh = ct1 in first_hop_set
                ct2_fh = ct2 in first_hop_set
                ct1_ret = ct1 in all_retained_nodes
                ct2_ret = ct2 in all_retained_nodes

                # Valid second-hop edges:
                # first_hop_node -> new_node OR new_node -> first_hop_node
                if ct1_fh and not ct2_ret:
                    pass  # ct2 is the new second-hop candidate
                elif ct2_fh and not ct1_ret:
                    pass  # ct1 is the new second-hop candidate
                else:
                    continue

                if is_residual:
                    raw_corr = _to_float(row.get("raw_correlation"))
                    res_corr = _to_float(row.get("sector_residual_correlation"))
                    res_delta = _to_float(row.get("correlation_delta_vs_raw"))
                    rec = {
                        "canonical_ticker_1": ct1,
                        "canonical_ticker_2": ct2,
                        "source_file": source_file,
                        "source_family": family,
                        "raw_correlation": raw_corr,
                        "residual_correlation": res_corr,
                        "residual_delta_vs_raw": res_delta,
                        "raw_rolling_corr_mean": np.nan,
                        "raw_rolling_corr_median": np.nan,
                        "raw_rolling_corr_latest": np.nan,
                        "raw_share_rolling_corr_gt_030": np.nan,
                        "raw_share_rolling_corr_gt_050": np.nan,
                        "raw_share_rolling_corr_gt_070": np.nan,
                        "residual_rolling_corr_mean": np.nan,
                        "residual_rolling_corr_median": np.nan,
                        "residual_rolling_corr_latest": np.nan,
                        "residual_share_rolling_corr_gt_020": np.nan,
                        "residual_share_rolling_corr_gt_030": np.nan,
                        "residual_share_rolling_corr_gt_050": np.nan,
                        "residual_share_rolling_corr_gt_070": np.nan,
                    }
                else:
                    rec = {
                        "canonical_ticker_1": ct1,
                        "canonical_ticker_2": ct2,
                        "source_file": source_file,
                        "source_family": family,
                        "raw_correlation": _to_float(row.get("correlation")),
                        "residual_correlation": np.nan,
                        "residual_delta_vs_raw": np.nan,
                        "raw_rolling_corr_mean": np.nan,
                        "raw_rolling_corr_median": np.nan,
                        "raw_rolling_corr_latest": np.nan,
                        "raw_share_rolling_corr_gt_030": np.nan,
                        "raw_share_rolling_corr_gt_050": np.nan,
                        "raw_share_rolling_corr_gt_070": np.nan,
                        "residual_rolling_corr_mean": np.nan,
                        "residual_rolling_corr_median": np.nan,
                        "residual_rolling_corr_latest": np.nan,
                        "residual_share_rolling_corr_gt_020": np.nan,
                        "residual_share_rolling_corr_gt_030": np.nan,
                        "residual_share_rolling_corr_gt_050": np.nan,
                        "residual_share_rolling_corr_gt_070": np.nan,
                    }

                raw_ok = _safe_ge(rec["raw_correlation"], raw_min)
                res_ok = _safe_ge(rec["residual_correlation"], res_min)
                if not raw_ok and not res_ok:
                    continue

                cs = compute_combined_score(rec)
                if not _safe_ge(cs, second_hop_min_cs):
                    continue

                rec["combined_score"] = cs
                key = (ct1, ct2)
                if key in second_hop_edges:
                    second_hop_edges[key] = merge_edge_records(second_hop_edges[key], rec)
                    second_hop_edges[key]["combined_score"] = compute_combined_score(second_hop_edges[key])
                else:
                    second_hop_edges[key] = rec

    return second_hop_edges, rows_scanned


# ---------------------------------------------------------------------------
# Rank second-hop nodes
# ---------------------------------------------------------------------------

def rank_second_hop_nodes(
    second_hop_edges: dict[tuple[str, str], dict],
    first_hop_set: set[str],
    all_retained_nodes: set[str],
    max_second_hop: int,
) -> set[str]:
    """
    Rank new (non-retained) nodes by best edge score to first_hop nodes.
    Return top max_second_hop.
    """
    best_score: dict[str, float] = {}
    for (ct1, ct2), edge in second_hop_edges.items():
        cs = _to_float(edge.get("combined_score"))
        if cs != cs:
            continue
        for t, other in ((ct1, ct2), (ct2, ct1)):
            if t not in all_retained_nodes and other in first_hop_set:
                if t not in best_score or cs > best_score[t]:
                    best_score[t] = cs

    ranked = sorted(best_score.items(), key=lambda x: (-x[1], x[0]))
    return {t for t, _ in ranked[:max_second_hop]}


# ---------------------------------------------------------------------------
# Build final edge set (merge pass1 + pass2 + rolling)
# ---------------------------------------------------------------------------

def build_final_edge_set(
    seed_edges: dict[tuple[str, str], dict],
    second_hop_edges: dict[tuple[str, str], dict],
    rolling_records: list[dict],
    all_retained: set[str],
    min_combined: float,
    instruments: dict[str, dict],
    seed_set: set[str],
    first_hop_set: set[str],
    second_hop_set: set[str],
) -> tuple[dict[tuple[str, str], dict], int]:
    """
    Merge all edge sources. Keep only edges where both endpoints are in all_retained.
    Enrich with metadata. Return final edge_map and skipped_below_threshold count.
    """
    combined: dict[tuple[str, str], dict] = {}

    def _add(edges: dict[tuple[str, str], dict]) -> None:
        for key, rec in edges.items():
            if key in combined:
                combined[key] = merge_edge_records(combined[key], rec)
                combined[key]["combined_score"] = compute_combined_score(combined[key])
            else:
                combined[key] = dict(rec)
                if "combined_score" not in combined[key]:
                    combined[key]["combined_score"] = compute_combined_score(combined[key])

    _add(seed_edges)
    _add(second_hop_edges)

    # rolling records: add for any pair both in all_retained
    for rec in rolling_records:
        ct1 = rec["canonical_ticker_1"]
        ct2 = rec["canonical_ticker_2"]
        if ct1 not in all_retained or ct2 not in all_retained:
            continue
        key = (ct1, ct2)
        if key in combined:
            combined[key] = merge_edge_records(combined[key], rec)
            combined[key]["combined_score"] = compute_combined_score(combined[key])
        else:
            combined[key] = dict(rec)
            combined[key]["combined_score"] = compute_combined_score(combined[key])

    # Filter: both endpoints retained, score >= min_combined, enrich metadata
    final: dict[tuple[str, str], dict] = {}
    skipped = 0

    for (ct1, ct2), rec in sorted(combined.items()):
        if ct1 not in all_retained or ct2 not in all_retained:
            continue
        cs = _to_float(rec.get("combined_score"))
        if not _safe_ge(cs, min_combined):
            skipped += 1
            continue

        # Determine distance class
        t1_seed = ct1 in seed_set
        t2_seed = ct2 in seed_set
        t1_fh = ct1 in first_hop_set
        t2_fh = ct2 in first_hop_set
        t1_sh = ct1 in second_hop_set
        t2_sh = ct2 in second_hop_set

        if t1_seed and t2_seed:
            dist_class = "SEED_TO_SEED"
        elif (t1_seed and t2_fh) or (t2_seed and t1_fh):
            dist_class = "SEED_TO_FIRST_HOP"
        elif (t1_fh and t2_fh):
            dist_class = "FIRST_HOP_TO_FIRST_HOP"
        elif (t1_fh and t2_sh) or (t2_fh and t1_sh):
            dist_class = "FIRST_HOP_TO_SECOND_HOP"
        else:
            dist_class = "OTHER_RETAINED_EDGE"

        # Enrich metadata from instruments DB (override CSV metadata)
        meta1 = instruments.get(ct1, {"sector": "", "industry": ""})
        meta2 = instruments.get(ct2, {"sector": "", "industry": ""})
        s1, i1 = meta1["sector"], meta1["industry"]
        s2, i2 = meta2["sector"], meta2["industry"]

        same_sector = bool(s1 and s2 and s1 == s2)
        same_industry = bool(i1 and i2 and i1 == i2)

        if same_industry:
            edge_cat = "SAME_INDUSTRY"
        elif same_sector:
            edge_cat = "SAME_SECTOR_CROSS_INDUSTRY"
        elif s1 and s2:
            edge_cat = "CROSS_SECTOR"
        else:
            edge_cat = "UNKNOWN"

        rec = dict(rec)
        rec.update({
            "ticker_1": ct1,
            "ticker_2": ct2,
            "sector_1": s1, "industry_1": i1,
            "sector_2": s2, "industry_2": i2,
            "same_sector": same_sector,
            "same_industry": same_industry,
            "edge_category": edge_cat,
            "cross_sector_edge": not same_sector and bool(s1 and s2),
            "cross_industry_edge": not same_industry and bool(i1 and i2),
            "seed_edge": t1_seed or t2_seed,
            "both_seed_edge": t1_seed and t2_seed,
            "graph_distance_class": dist_class,
            "ticker_1_is_seed": t1_seed,
            "ticker_2_is_seed": t2_seed,
        })

        final[(ct1, ct2)] = rec

    return final, skipped


# ---------------------------------------------------------------------------
# Build graph structures
# ---------------------------------------------------------------------------

def build_graph(
    edge_map: dict[tuple[str, str], dict],
) -> dict[str, set[str]]:
    adj: dict[str, set[str]] = {}
    for (ct1, ct2) in edge_map:
        adj.setdefault(ct1, set()).add(ct2)
        adj.setdefault(ct2, set()).add(ct1)
    return adj


# ---------------------------------------------------------------------------
# Connected components (BFS, deterministic)
# ---------------------------------------------------------------------------

def compute_connected_components(
    adj: dict[str, set[str]],
) -> tuple[list[list[str]], dict[str, int], dict[int, int]]:
    visited: set[str] = set()
    comp_list: list[list[str]] = []

    for start in sorted(adj.keys()):
        if start in visited:
            continue
        comp: list[str] = []
        queue: deque[str] = deque([start])
        visited.add(start)
        while queue:
            node = queue.popleft()
            comp.append(node)
            for nb in sorted(adj[node]):
                if nb not in visited:
                    visited.add(nb)
                    queue.append(nb)
        comp_list.append(sorted(comp))

    comp_list.sort(key=lambda c: c[0])
    node_to_comp: dict[str, int] = {}
    comp_sizes: dict[int, int] = {}
    for idx, comp in enumerate(comp_list):
        comp_sizes[idx] = len(comp)
        for t in comp:
            node_to_comp[t] = idx
    return comp_list, node_to_comp, comp_sizes


# ---------------------------------------------------------------------------
# Seed distances (multi-source BFS)
# ---------------------------------------------------------------------------

def compute_seed_distances(
    adj: dict[str, set[str]], seed_set: set[str]
) -> tuple[dict[str, int], dict[str, set[str]]]:
    graph_seeds = sorted(seed_set & set(adj.keys()))
    distances: dict[str, int] = {}
    seed_reach: dict[str, set[str]] = {}

    for s in graph_seeds:
        distances[s] = 0
        seed_reach[s] = {s}

    current_level = list(graph_seeds)
    d = 0
    while current_level:
        next_map: dict[str, set[str]] = {}
        for node in current_level:
            for nb in adj[node]:
                if nb not in distances:
                    if nb not in next_map:
                        next_map[nb] = set()
                    next_map[nb].update(seed_reach[node])
        next_level = sorted(next_map.keys())
        d += 1
        for nb in next_level:
            distances[nb] = d
            seed_reach[nb] = next_map[nb]
        current_level = next_level

    return distances, seed_reach


# ---------------------------------------------------------------------------
# Node metrics
# ---------------------------------------------------------------------------

def compute_node_metrics(
    adj: dict[str, set[str]],
    edge_map: dict[tuple[str, str], dict],
    node_to_comp: dict[str, int],
    comp_sizes: dict[int, int],
    distances: dict[str, int],
    seed_reach: dict[str, set[str]],
    seed_set: set[str],
    first_hop_set: set[str],
    second_hop_set: set[str],
    instruments: dict[str, dict],
) -> pd.DataFrame:
    rows: list[dict] = []

    for ticker in sorted(adj.keys()):
        neighbors = sorted(adj[ticker])
        degree = len(neighbors)

        weighted_deg = 0.0
        cross_sector_deg = 0
        cross_industry_deg = 0
        seed_nb_count = 0
        seed_nbs: list[str] = []
        best_seed_score = np.nan
        best_seed_nb = ""
        best_fh_score = np.nan
        best_fh_nb = ""

        for nb in neighbors:
            key = (min(ticker, nb), max(ticker, nb))
            edge = edge_map.get(key, {})
            cs = _to_float(edge.get("combined_score"))
            if not (cs != cs):
                weighted_deg += cs
            if edge.get("cross_sector_edge", False):
                cross_sector_deg += 1
            if edge.get("cross_industry_edge", False):
                cross_industry_deg += 1
            if nb in seed_set:
                seed_nb_count += 1
                seed_nbs.append(nb)
                if not (cs != cs) and (best_seed_score != best_seed_score or cs > best_seed_score):
                    best_seed_score = cs
                    best_seed_nb = nb
            if nb in first_hop_set and ticker not in seed_set:
                if not (cs != cs) and (best_fh_score != best_fh_score or cs > best_fh_score):
                    best_fh_score = cs
                    best_fh_nb = nb

        comp_idx = node_to_comp.get(ticker, -1)
        sd = distances.get(ticker, None)
        nearest = sorted(seed_reach.get(ticker, set()) & seed_set)

        is_seed = ticker in seed_set
        if is_seed:
            status = "SEED"
        elif ticker in first_hop_set:
            status = "FIRST_HOP"
        elif ticker in second_hop_set:
            status = "SECOND_HOP"
        else:
            status = "UNREACHED_RETAINED"

        info = instruments.get(ticker, {"sector": "", "industry": ""})

        rows.append({
            "ticker": ticker,
            "sector": info["sector"],
            "industry": info["industry"],
            "is_seed": is_seed,
            "seed_status": status,
            "seed_distance": sd,
            "nearest_seed_tickers": ", ".join(nearest),
            "component_id": comp_idx + 1 if comp_idx >= 0 else None,
            "component_size": comp_sizes.get(comp_idx, 0) if comp_idx >= 0 else 0,
            "degree": degree,
            "weighted_degree": weighted_deg,
            "cross_sector_degree": cross_sector_deg,
            "cross_industry_degree": cross_industry_deg,
            "seed_neighbor_count": seed_nb_count,
            "seed_neighbors": ", ".join(sorted(seed_nbs)),
            "best_seed_edge_score": best_seed_score,
            "best_seed_neighbor": best_seed_nb,
            "best_first_hop_edge_score": best_fh_score,
            "best_first_hop_neighbor": best_fh_nb,
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Component reports
# ---------------------------------------------------------------------------

def compute_component_reports(
    components: list[list[str]],
    node_df: pd.DataFrame,
    edge_map: dict[tuple[str, str], dict],
    seed_set: set[str],
    first_hop_set: set[str],
    second_hop_set: set[str],
    instruments: dict[str, dict],
    min_size: int,
) -> pd.DataFrame:
    node_wd = (
        node_df.set_index("ticker")["weighted_degree"].to_dict()
        if not node_df.empty else {}
    )
    node_status = (
        node_df.set_index("ticker")["seed_status"].to_dict()
        if not node_df.empty else {}
    )

    rows: list[dict] = []
    for idx, comp in enumerate(components):
        if len(comp) < min_size:
            continue
        comp_set = set(comp)

        comp_edges = [
            ed for (ct1, ct2), ed in edge_map.items()
            if ct1 in comp_set and ct2 in comp_set
        ]
        edge_count = len(comp_edges)
        scores = [_to_float(e["combined_score"]) for e in comp_edges]
        valid = [s for s in scores if not (s != s)]
        avg_score = float(np.mean(valid)) if valid else np.nan
        max_score = float(max(valid)) if valid else np.nan

        cs_cnt = sum(1 for e in comp_edges if e.get("cross_sector_edge", False))
        ci_cnt = sum(1 for e in comp_edges if e.get("cross_industry_edge", False))

        sectors = sorted({instruments.get(t, {}).get("sector", "") for t in comp if instruments.get(t, {}).get("sector", "")})
        inds = sorted({instruments.get(t, {}).get("industry", "") for t in comp if instruments.get(t, {}).get("industry", "")})

        seed_in = sorted(comp_set & seed_set)
        fh_in = sorted(comp_set & first_hop_set)
        sh_in = sorted(comp_set & second_hop_set)

        by_wd = sorted(comp, key=lambda t: (-node_wd.get(t, 0.0), t))[:15]

        rows.append({
            "component_id": idx + 1,
            "ticker_count": len(comp),
            "seed_count": len(seed_in),
            "first_hop_count": len(fh_in),
            "second_hop_count": len(sh_in),
            "sector_count": len(sectors),
            "industry_count": len(inds),
            "edge_count": edge_count,
            "average_edge_score": avg_score,
            "max_edge_score": max_score,
            "cross_sector_edge_count": cs_cnt,
            "cross_industry_edge_count": ci_cnt,
            "sectors": ", ".join(sectors),
            "industries": ", ".join(inds),
            "tickers": ", ".join(sorted(comp)),
            "seed_tickers": ", ".join(seed_in),
            "first_hop_tickers": ", ".join(fh_in),
            "second_hop_tickers": ", ".join(sh_in),
            "top_weighted_degree_tickers": ", ".join(by_wd),
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Cliques (Bron-Kerbosch with pivoting, deterministic)
# ---------------------------------------------------------------------------

def find_cliques(
    adj: dict[str, set[str]],
    edge_map: dict[tuple[str, str], dict],
    min_size: int,
    max_size: int,
    max_total: int,
) -> list[list[str]]:
    nodes_sorted = sorted(adj.keys())
    adj_f: dict[str, frozenset] = {v: frozenset(adj[v]) for v in nodes_sorted}
    found: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()
    _safety = max(max_total * 20, 20_000)

    def bk(R: frozenset, P: frozenset, X: frozenset) -> None:
        if len(found) >= _safety:
            return
        if len(R) + len(P) < min_size:
            return
        if len(R) == max_size:
            key = tuple(sorted(R))
            if key not in seen:
                seen.add(key)
                found.append(list(key))
            return
        if not P and not X:
            if len(R) >= min_size:
                key = tuple(sorted(R))
                if key not in seen:
                    seen.add(key)
                    found.append(list(key))
            return
        PX = P | X
        pivot = max(sorted(PX), key=lambda u: len(adj_f.get(u, frozenset()) & P))
        for v in sorted(P - adj_f.get(pivot, frozenset())):
            bk(R | {v}, P & adj_f[v], X & adj_f[v])
            P = P - {v}
            X = X | {v}

    bk(frozenset(), frozenset(nodes_sorted), frozenset())
    return found


# ---------------------------------------------------------------------------
# Write reports
# ---------------------------------------------------------------------------

def write_reports(
    out_dir: Path,
    edge_map: dict[tuple[str, str], dict],
    node_df: pd.DataFrame,
    comp_df: pd.DataFrame,
    cliques_raw: list[list[str]],
    seed_set: set[str],
    first_hop_set: set[str],
    second_hop_set: set[str],
    instruments: dict[str, dict],
    theme_name: str,
    seed_tickers: list[str],
    min_clique_size: int,
    max_cliques: int,
    node_to_comp: dict[str, int],
    comp_sizes: dict[int, int],
) -> dict[str, int]:
    counts: dict[str, int] = {}

    # ------------------------------------------------------------------
    # 1. seed_ecosystem_edges.csv
    # ------------------------------------------------------------------
    edge_rows = [dict(ed) for ed in edge_map.values()]
    if edge_rows:
        edf = pd.DataFrame(edge_rows)
        edf["_se"] = edf["seed_edge"].astype(int)
        edf = edf.sort_values(
            ["_se", "combined_score", "ticker_1", "ticker_2"],
            ascending=[False, False, True, True],
        ).drop(columns=["_se"]).reset_index(drop=True)
        out_cols = [c for c in EDGE_OUTPUT_COLS if c in edf.columns]
        edf[out_cols].to_csv(out_dir / "seed_ecosystem_edges.csv", index=False)
        counts["edges_kept"] = len(edf)
    else:
        pd.DataFrame(columns=EDGE_OUTPUT_COLS).to_csv(
            out_dir / "seed_ecosystem_edges.csv", index=False
        )
        counts["edges_kept"] = 0

    # ------------------------------------------------------------------
    # 2. seed_ecosystem_nodes.csv
    # ------------------------------------------------------------------
    if not node_df.empty:
        ndf = node_df.copy()
        ndf["_sd"] = ndf["seed_distance"].fillna(9999)
        ndf["_bss"] = ndf["best_seed_edge_score"].fillna(-np.inf)
        ndf = ndf.sort_values(
            ["is_seed", "_sd", "_bss", "weighted_degree", "ticker"],
            ascending=[False, True, False, False, True],
        ).drop(columns=["_sd", "_bss"]).reset_index(drop=True)
        out_cols = [c for c in NODE_OUTPUT_COLS if c in ndf.columns]
        ndf[out_cols].to_csv(out_dir / "seed_ecosystem_nodes.csv", index=False)
        counts["nodes"] = len(ndf)
    else:
        pd.DataFrame(columns=NODE_OUTPUT_COLS).to_csv(
            out_dir / "seed_ecosystem_nodes.csv", index=False
        )
        counts["nodes"] = 0

    # ------------------------------------------------------------------
    # 3. seed_ecosystem_direct_seed_edges.csv
    # ------------------------------------------------------------------
    ds_rows = []
    for (ct1, ct2), ed in edge_map.items():
        if not ed.get("seed_edge", False):
            continue
        row = dict(ed)
        t1_seed = ed.get("ticker_1_is_seed", False)
        t2_seed = ed.get("ticker_2_is_seed", False)
        if t1_seed and t2_seed:
            row["seed_ticker"] = _sorted_csv(ct1, ct2)
            row["non_seed_ticker"] = ""
        elif t1_seed:
            row["seed_ticker"] = ct1
            row["non_seed_ticker"] = ct2
        else:
            row["seed_ticker"] = ct2
            row["non_seed_ticker"] = ct1
        ds_rows.append(row)

    ds_cols = EDGE_OUTPUT_COLS + ["seed_ticker", "non_seed_ticker"]
    if ds_rows:
        dsdf = pd.DataFrame(ds_rows)
        dsdf = dsdf.sort_values(
            ["combined_score", "seed_ticker", "non_seed_ticker"],
            ascending=[False, True, True],
        ).reset_index(drop=True)
        out_cols = [c for c in ds_cols if c in dsdf.columns]
        dsdf[out_cols].to_csv(
            out_dir / "seed_ecosystem_direct_seed_edges.csv", index=False
        )
        counts["direct_seed_edges"] = len(dsdf)
    else:
        pd.DataFrame(columns=ds_cols).to_csv(
            out_dir / "seed_ecosystem_direct_seed_edges.csv", index=False
        )
        counts["direct_seed_edges"] = 0

    # ------------------------------------------------------------------
    # 4. seed_ecosystem_first_hop_candidates.csv
    # ------------------------------------------------------------------
    if not node_df.empty:
        fh_rows = []
        for _, row in node_df.iterrows():
            t = row["ticker"]
            if t in seed_set:
                continue
            sd = row.get("seed_distance")
            if sd != 1:
                continue
            fh_rows.append({
                "theme_name": theme_name,
                "ticker": t,
                "sector": row.get("sector", ""),
                "industry": row.get("industry", ""),
                "seed_distance": sd,
                "nearest_seed_tickers": row.get("nearest_seed_tickers", ""),
                "seed_neighbor_count": row.get("seed_neighbor_count", 0),
                "seed_neighbors": row.get("seed_neighbors", ""),
                "best_seed_edge_score": row.get("best_seed_edge_score"),
                "best_seed_neighbor": row.get("best_seed_neighbor", ""),
                "component_id": row.get("component_id"),
                "component_size": row.get("component_size", 0),
                "degree": row.get("degree", 0),
                "weighted_degree": row.get("weighted_degree", 0.0),
                "cross_sector_degree": row.get("cross_sector_degree", 0),
                "cross_industry_degree": row.get("cross_industry_degree", 0),
                "evidence_level": "DIRECT_SEED_NEIGHBOR",
            })
        if fh_rows:
            fhdf = pd.DataFrame(fh_rows)
            fhdf["_bss"] = fhdf["best_seed_edge_score"].fillna(-np.inf)
            fhdf = fhdf.sort_values(
                ["_bss", "seed_neighbor_count", "weighted_degree", "ticker"],
                ascending=[False, False, False, True],
            ).drop(columns=["_bss"]).reset_index(drop=True)
            out_cols = [c for c in FIRST_HOP_COLS if c in fhdf.columns]
            fhdf[out_cols].to_csv(
                out_dir / "seed_ecosystem_first_hop_candidates.csv", index=False
            )
        else:
            pd.DataFrame(columns=FIRST_HOP_COLS).to_csv(
                out_dir / "seed_ecosystem_first_hop_candidates.csv", index=False
            )
        counts["first_hop_candidates"] = len(fh_rows)
    else:
        pd.DataFrame(columns=FIRST_HOP_COLS).to_csv(
            out_dir / "seed_ecosystem_first_hop_candidates.csv", index=False
        )
        counts["first_hop_candidates"] = 0

    # ------------------------------------------------------------------
    # 5. seed_ecosystem_second_hop_candidates.csv
    # ------------------------------------------------------------------
    if not node_df.empty:
        sh_rows = []
        for _, row in node_df.iterrows():
            t = row["ticker"]
            if t in seed_set:
                continue
            sd = row.get("seed_distance")
            if sd != 2:
                continue
            sh_rows.append({
                "theme_name": theme_name,
                "ticker": t,
                "sector": row.get("sector", ""),
                "industry": row.get("industry", ""),
                "seed_distance": sd,
                "nearest_seed_tickers": row.get("nearest_seed_tickers", ""),
                "seed_neighbor_count": row.get("seed_neighbor_count", 0),
                "seed_neighbors": row.get("seed_neighbors", ""),
                "best_first_hop_edge_score": row.get("best_first_hop_edge_score"),
                "best_first_hop_neighbor": row.get("best_first_hop_neighbor", ""),
                "component_id": row.get("component_id"),
                "component_size": row.get("component_size", 0),
                "degree": row.get("degree", 0),
                "weighted_degree": row.get("weighted_degree", 0.0),
                "cross_sector_degree": row.get("cross_sector_degree", 0),
                "cross_industry_degree": row.get("cross_industry_degree", 0),
                "evidence_level": "SECOND_ORDER_NEIGHBOR",
            })
        if sh_rows:
            shdf = pd.DataFrame(sh_rows)
            shdf["_bfh"] = shdf["best_first_hop_edge_score"].fillna(-np.inf)
            shdf = shdf.sort_values(
                ["_bfh", "weighted_degree", "ticker"],
                ascending=[False, False, True],
            ).drop(columns=["_bfh"]).reset_index(drop=True)
            out_cols = [c for c in SECOND_HOP_COLS if c in shdf.columns]
            shdf[out_cols].to_csv(
                out_dir / "seed_ecosystem_second_hop_candidates.csv", index=False
            )
        else:
            pd.DataFrame(columns=SECOND_HOP_COLS).to_csv(
                out_dir / "seed_ecosystem_second_hop_candidates.csv", index=False
            )
        counts["second_hop_candidates"] = len(sh_rows)
    else:
        pd.DataFrame(columns=SECOND_HOP_COLS).to_csv(
            out_dir / "seed_ecosystem_second_hop_candidates.csv", index=False
        )
        counts["second_hop_candidates"] = 0

    # ------------------------------------------------------------------
    # 6. seed_ecosystem_components.csv
    # ------------------------------------------------------------------
    if not comp_df.empty:
        cdf = comp_df.sort_values(
            ["seed_count", "ticker_count", "average_edge_score", "component_id"],
            ascending=[False, False, False, True],
        ).reset_index(drop=True)
        out_cols = [c for c in COMP_OUTPUT_COLS if c in cdf.columns]
        cdf[out_cols].to_csv(out_dir / "seed_ecosystem_components.csv", index=False)
        counts["components"] = len(cdf)
    else:
        pd.DataFrame(columns=COMP_OUTPUT_COLS).to_csv(
            out_dir / "seed_ecosystem_components.csv", index=False
        )
        counts["components"] = 0

    # ------------------------------------------------------------------
    # 7. seed_ecosystem_cross_sector_bridges.csv
    # ------------------------------------------------------------------
    cs_rows = [
        ed for ed in edge_map.values()
        if ed.get("cross_sector_edge", False)
    ]
    if cs_rows:
        csdf = pd.DataFrame(cs_rows)
        csdf["_se"] = csdf["seed_edge"].astype(int)
        csdf["_rc"] = csdf.get("raw_correlation", np.nan).fillna(-np.inf) if "raw_correlation" in csdf.columns else -np.inf
        csdf["_resc"] = csdf.get("residual_correlation", np.nan).fillna(-np.inf) if "residual_correlation" in csdf.columns else -np.inf
        csdf = csdf.sort_values(
            ["_se", "combined_score", "_resc", "_rc", "ticker_1", "ticker_2"],
            ascending=[False, False, False, False, True, True],
        ).drop(columns=["_se", "_rc", "_resc"]).reset_index(drop=True)
        out_cols = [c for c in EDGE_OUTPUT_COLS if c in csdf.columns]
        csdf[out_cols].to_csv(
            out_dir / "seed_ecosystem_cross_sector_bridges.csv", index=False
        )
        counts["cross_sector_edges"] = len(csdf)
    else:
        pd.DataFrame(columns=EDGE_OUTPUT_COLS).to_csv(
            out_dir / "seed_ecosystem_cross_sector_bridges.csv", index=False
        )
        counts["cross_sector_edges"] = 0

    # ------------------------------------------------------------------
    # 8. seed_ecosystem_cliques_3plus.csv
    # ------------------------------------------------------------------
    clique_rows: list[dict] = []
    node_status_map = (
        node_df.set_index("ticker")["seed_status"].to_dict()
        if not node_df.empty else {}
    )

    for tickers_list in cliques_raw:
        n = len(tickers_list)
        clique_set = set(tickers_list)
        pairs = [
            (tickers_list[i], tickers_list[j])
            for i in range(n) for j in range(i + 1, n)
        ]
        c_edges = []
        for a, b in pairs:
            key = (min(a, b), max(a, b))
            ed = edge_map.get(key)
            if ed:
                c_edges.append(ed)

        c_scores = [_to_float(e["combined_score"]) for e in c_edges]
        valid_c = [s for s in c_scores if not (s != s)]
        c_sectors = sorted({instruments.get(t, {}).get("sector", "") for t in tickers_list if instruments.get(t, {}).get("sector", "")})
        c_inds = sorted({instruments.get(t, {}).get("industry", "") for t in tickers_list if instruments.get(t, {}).get("industry", "")})
        c_seed = sorted(clique_set & seed_set)
        c_fh = [t for t in tickers_list if t in first_hop_set]
        c_sh = [t for t in tickers_list if t in second_hop_set]
        c_cats = sorted({e["edge_category"] for e in c_edges if e.get("edge_category")})

        clique_rows.append({
            "clique_size": n,
            "tickers": ", ".join(sorted(tickers_list)),
            "seed_count": len(c_seed),
            "seed_tickers": ", ".join(c_seed),
            "first_hop_count": len(c_fh),
            "second_hop_count": len(c_sh),
            "sector_count": len(c_sectors),
            "industry_count": len(c_inds),
            "sectors": ", ".join(c_sectors),
            "industries": ", ".join(c_inds),
            "edge_count": n * (n - 1) // 2,
            "average_combined_score": float(np.mean(valid_c)) if valid_c else np.nan,
            "min_combined_score": float(min(valid_c)) if valid_c else np.nan,
            "max_combined_score": float(max(valid_c)) if valid_c else np.nan,
            "cross_sector_edge_count": sum(1 for e in c_edges if e.get("cross_sector_edge", False)),
            "cross_industry_edge_count": sum(1 for e in c_edges if e.get("cross_industry_edge", False)),
            "edge_categories": ", ".join(c_cats),
        })

    counts["cliques_found"] = len(clique_rows)
    if clique_rows:
        cldf = pd.DataFrame(clique_rows)
        cldf["_avg"] = cldf["average_combined_score"].fillna(-np.inf)
        cldf = cldf.sort_values(
            ["seed_count", "clique_size", "_avg", "tickers"],
            ascending=[False, False, False, True],
        ).drop(columns=["_avg"]).reset_index(drop=True)
        cldf = cldf.head(max_cliques).reset_index(drop=True)
        cldf.insert(0, "clique_id", range(1, len(cldf) + 1))
        out_cols = [c for c in CLIQUE_OUTPUT_COLS if c in cldf.columns]
        cldf[out_cols].to_csv(
            out_dir / "seed_ecosystem_cliques_3plus.csv", index=False
        )
        counts["cliques_written"] = len(cldf)
    else:
        pd.DataFrame(columns=CLIQUE_OUTPUT_COLS).to_csv(
            out_dir / "seed_ecosystem_cliques_3plus.csv", index=False
        )
        counts["cliques_written"] = 0

    # ------------------------------------------------------------------
    # 9. seed_ecosystem_datacenter_summary.csv
    # ------------------------------------------------------------------
    node_idx = (
        node_df.set_index("ticker").to_dict(orient="index")
        if not node_df.empty else {}
    )
    seed_sum_rows: list[dict] = []
    for s in seed_tickers:
        info = instruments.get(s, {"sector": "", "industry": ""})
        nd = node_idx.get(s, {})
        found = s in node_idx

        # Direct non-seed neighbors
        dns = [
            nb for nb in (nd.get("seed_neighbors", "") or "").split(", ")
            if nb and nb not in seed_set
        ] if found else []
        # Actually seed_neighbors are seed tickers; we want non-seed neighbors
        # Get actual non-seed direct neighbors from adj
        if found:
            # best approach: use the direct_seed_edges data for this seed
            non_seed_nbs = sorted([
                nb for (ct1, ct2), ed in edge_map.items()
                if (ct1 == s or ct2 == s)
                for nb in ([ct2 if ct1 == s else ct1])
                if nb not in seed_set
            ])
        else:
            non_seed_nbs = []

        # cross-sector/industry direct edges
        cs_direct = 0
        ci_direct = 0
        best_score = np.nan
        best_nb = ""
        for (ct1, ct2), ed in edge_map.items():
            if ct1 != s and ct2 != s:
                continue
            if ed.get("cross_sector_edge", False):
                cs_direct += 1
            if ed.get("cross_industry_edge", False):
                ci_direct += 1
            sc = _to_float(ed.get("combined_score"))
            if not (sc != sc) and (best_score != best_score or sc > best_score):
                best_score = sc
                best_nb = ct2 if ct1 == s else ct1

        comp_id = nd.get("component_id") if found else None
        comp_size = nd.get("component_size", 0) if found else 0

        seed_sum_rows.append({
            "seed_ticker": s,
            "found_in_graph": found,
            "sector": info["sector"],
            "industry": info["industry"],
            "degree": nd.get("degree", None) if found else None,
            "weighted_degree": nd.get("weighted_degree", None) if found else None,
            "direct_non_seed_neighbor_count": len(non_seed_nbs),
            "direct_non_seed_neighbors": ", ".join(non_seed_nbs),
            "first_hop_component_id": comp_id,
            "first_hop_component_size": comp_size,
            "cross_sector_direct_edge_count": cs_direct,
            "cross_industry_direct_edge_count": ci_direct,
            "best_direct_edge_score": best_score,
            "best_direct_neighbor": best_nb,
        })

    ssdf = pd.DataFrame(seed_sum_rows)
    ssdf["_wd"] = ssdf["weighted_degree"].fillna(-np.inf)
    ssdf = ssdf.sort_values(
        ["found_in_graph", "_wd", "seed_ticker"],
        ascending=[False, False, True],
    ).drop(columns=["_wd"]).reset_index(drop=True)
    out_cols = [c for c in SEED_SUMMARY_COLS if c in ssdf.columns]
    ssdf[out_cols].to_csv(
        out_dir / "seed_ecosystem_datacenter_summary.csv", index=False
    )
    counts["seed_tickers_found"] = int(ssdf["found_in_graph"].sum())

    return counts


# ---------------------------------------------------------------------------
# Markdown reports
# ---------------------------------------------------------------------------

def write_markdown_reports(
    out_dir: Path,
    db_path: Path,
    raw_dir: Path,
    residual_dir: Path,
    theme_name: str,
    seed_tickers: list[str],
    args: argparse.Namespace,
    stats: dict,
) -> None:
    summary_lines = [
        f"# Seed Ecosystem Theme: {theme_name}",
        "",
        f"Input database: `{db_path}`",
        f"Raw full directory: `{raw_dir}`",
        f"Residual full directory: `{residual_dir}`",
        f"Output directory: `{out_dir}`",
        "",
        f"Theme name: `{theme_name}`",
        f"Seed tickers ({len(seed_tickers)}): {', '.join(seed_tickers)}",
        "",
        "## Why V6b is Seed-Centric",
        "",
        "The previous V6 graph engine used only top-pair report files, which contain the most",
        "generally correlated pairs across all tickers. This means thematic companies (e.g.",
        "datacenter ecosystem) can be underrepresented if their strongest relationships are",
        "with specific sector peers rather than the globally top-correlated pairs.",
        "",
        "V6b instead reads full pair files (millions of rows) and builds the graph starting",
        "from seed tickers. This allows discovering all statistically connected peers, even",
        "those that would never appear in a generic top-N list.",
        "",
        "## Thresholds",
        "",
        f"- Seed raw min correlation: `{args.seed_raw_min_correlation}`",
        f"- Seed residual min correlation: `{args.seed_residual_min_correlation}`",
        f"- General raw min correlation: `{args.raw_min_correlation}`",
        f"- General residual min correlation: `{args.residual_min_correlation}`",
        f"- Second-hop min combined score: `{args.second_hop_min_combined_score}`",
        f"- Overall min combined score: `{args.min_combined_score}`",
        f"- Max first-hop nodes: `{args.max_first_hop_nodes}`",
        f"- Max second-hop nodes: `{args.max_second_hop_nodes}`",
        f"- Min component size: `{args.min_component_size}`",
        f"- Min / max clique size: `{args.min_clique_size}` / `{args.max_clique_size}`",
        "",
        "## Graph Construction",
        "",
        "**Pass 1**: Stream all full pair files. Extract edges where at least one ticker",
        "is a seed. Apply seed-specific (lower) correlation thresholds. Rank non-seed",
        f"first-hop nodes by best seed edge score. Retain at most {args.max_first_hop_nodes} first-hop nodes.",
        "",
        "**Pass 2**: Stream all full pair files again. Extract edges from retained first-hop",
        "nodes to new (non-retained) second-hop candidates. Apply general thresholds and",
        f"second-hop min combined score. Retain at most {args.max_second_hop_nodes} second-hop nodes.",
        "",
        "**Final edge set**: Combine pass1 + pass2 + rolling edges. Keep only edges where",
        "both endpoints are retained. Apply overall min combined score.",
        "",
        "## Summary Statistics",
        "",
        f"- Seed tickers found in graph: `{stats.get('seed_tickers_found_in_graph', 0)}`",
        f"- Retained first-hop nodes: `{stats.get('retained_first_hop_nodes', 0)}`",
        f"- Retained second-hop nodes: `{stats.get('retained_second_hop_nodes', 0)}`",
        f"- Graph nodes: `{stats.get('nodes', 0)}`",
        f"- Graph edges: `{stats.get('edges_kept', 0)}`",
        f"- Components (total): `{stats.get('components_total', 0)}`",
        f"- First-hop candidates: `{stats.get('first_hop_candidates', 0)}`",
        f"- Second-hop candidates: `{stats.get('second_hop_candidates', 0)}`",
        f"- Cross-sector edges: `{stats.get('cross_sector_edges', 0)}`",
        f"- Cliques found: `{stats.get('cliques_found', 0)}`",
        f"- Cliques written: `{stats.get('cliques_written', 0)}`",
        "",
        "## Combined Score",
        "",
        "Each edge has a `combined_score` computed as a weighted average of available metrics:",
        "raw_correlation (0.25), raw_rolling_corr_mean (0.15), residual_correlation (0.40),",
        "residual_rolling_corr_mean (0.20). Missing metrics are dropped and remaining",
        "weights rescaled to sum to 1.",
        "",
        "## First-Hop and Second-Hop Nodes",
        "",
        "**First-hop**: Non-seed tickers directly connected to at least one seed ticker.",
        "These are the most direct statistical peers of the theme tickers.",
        "",
        "**Second-hop**: Tickers connected to first-hop nodes but not directly to seeds.",
        "These are statistical peers of peers — potentially theme-adjacent companies.",
        "",
        "## Important Caveats",
        "",
        "Graph connections are statistical similarity measures, not proof of business",
        "relationship or causality. Two stocks may have high correlation for reasons",
        "unrelated to the datacenter theme: shared macro sensitivity, sector rotation,",
        "or statistical coincidence.",
        "",
        "Datacenter ecosystem interpretation requires business validation. Not all tickers",
        "in the graph are directly linked to the datacenter theme.",
    ]
    (out_dir / "seed_ecosystem_theme_summary.md").write_text(
        "\n".join(summary_lines) + "\n", encoding="utf-8"
    )

    readme_lines = [
        "# Seed Ecosystem Graph Output Files",
        "",
        "Generated by `analysis/find_seed_ecosystem_graph.py`.",
        "",
        "## Files",
        "",
        "### seed_ecosystem_edges.csv",
        "All retained graph edges with combined_score, raw/residual correlation metrics,",
        "edge category, graph_distance_class, source family, and seed flags.",
        "",
        "### seed_ecosystem_nodes.csv",
        "All retained ticker nodes with sector/industry, seed_status, seed_distance,",
        "component, degree, weighted_degree, and best seed/first-hop edge scores.",
        "",
        "### seed_ecosystem_direct_seed_edges.csv",
        "Edges where at least one ticker is a seed. Sorted by combined_score descending.",
        "",
        "### seed_ecosystem_first_hop_candidates.csv",
        "Non-seed tickers with seed_distance == 1 (direct seed neighbors).",
        "",
        "### seed_ecosystem_second_hop_candidates.csv",
        "Non-seed tickers with seed_distance == 2 (peers of first-hop nodes).",
        "",
        f"### seed_ecosystem_components.csv",
        f"Connected components with >= {args.min_component_size} tickers.",
        "",
        "### seed_ecosystem_cross_sector_bridges.csv",
        "Cross-sector edges only.",
        "",
        "### seed_ecosystem_cliques_3plus.csv",
        f"Cliques of size {args.min_clique_size}–{args.max_clique_size}.",
        "",
        "### seed_ecosystem_datacenter_summary.csv",
        "One row per seed ticker with presence, degree, and best-neighbor information.",
        "",
        "### seed_ecosystem_theme_summary.md",
        "Methodology explanation and summary statistics.",
        "",
        "### seed_ecosystem_readme.md",
        "This file.",
    ]
    (out_dir / "seed_ecosystem_readme.md").write_text(
        "\n".join(readme_lines) + "\n", encoding="utf-8"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    db_path = Path(args.db)
    raw_dir = Path(args.raw_full_dir)
    residual_dir = Path(args.residual_full_dir)
    out_dir = Path(args.output_dir)

    seed_tickers: list[str] = sorted(
        {t.strip() for t in args.seed_tickers.split(",") if t.strip()}
    )
    seed_set: set[str] = set(seed_tickers)

    validate_database(db_path)
    rf, resf, rr, resr = validate_input_dirs(raw_dir, residual_dir)
    instruments = load_instruments(db_path)

    raw_full_found = 1 if rf else 0
    res_full_found = 1 if resf else 0
    raw_roll_found = 1 if rr else 0
    res_roll_found = 1 if resr else 0

    full_files: list[tuple[Path, str]] = []
    if rf:
        full_files.append((rf, "RAW_FULL"))
    if resf:
        full_files.append((resf, "RESIDUAL_FULL"))

    def _no_graph(reason: str, rows1: int = 0, rows2: int = 0,
                  de: int = 0, skipped: int = 0) -> None:
        print(f"SUMMARY db={db_path}")
        print(f"SUMMARY raw_full_dir={raw_dir}")
        print(f"SUMMARY residual_full_dir={residual_dir}")
        print(f"SUMMARY output_dir={out_dir}")
        print(f"SUMMARY theme_name={args.theme_name}")
        print(f"SUMMARY seed_tickers_requested={len(seed_tickers)}")
        print(f"SUMMARY seed_tickers_found_in_graph=0")
        print(f"SUMMARY raw_full_file_found={raw_full_found}")
        print(f"SUMMARY residual_full_file_found={res_full_found}")
        print(f"SUMMARY raw_rolling_file_found={raw_roll_found}")
        print(f"SUMMARY residual_rolling_file_found={res_roll_found}")
        print(f"SUMMARY pass1_rows_scanned={rows1}")
        print(f"SUMMARY pass1_direct_seed_edges_raw={de}")
        print(f"SUMMARY retained_first_hop_nodes=0")
        print(f"SUMMARY pass2_rows_scanned={rows2}")
        print(f"SUMMARY pass2_second_hop_edges_raw=0")
        print(f"SUMMARY retained_second_hop_nodes=0")
        print(f"SUMMARY edges_kept=0")
        print(f"SUMMARY nodes_total=0")
        print(f"SUMMARY components_total=0")
        print(f"SUMMARY components_min_size={args.min_component_size}")
        print(f"SUMMARY cross_sector_edges=0")
        print(f"SUMMARY cross_industry_edges=0")
        print(f"SUMMARY direct_seed_edges=0")
        print(f"SUMMARY first_hop_candidates=0")
        print(f"SUMMARY second_hop_candidates=0")
        print(f"SUMMARY cliques_found=0")
        print(f"SUMMARY cliques_written=0")
        print(f"SUMMARY skipped_pairs_missing_metadata={skipped}")
        print(f"SUMMARY skipped_pairs_below_threshold=0")
        print(f"SUMMARY status=NO_USABLE_GRAPH")
        print(f"SUMMARY reason={reason}")

    # ---- PASS 1 -----------------------------------------------------------
    seed_edges, p1_rows, skipped_meta = pass1_collect_seed_edges(
        full_files,
        seed_set,
        instruments,
        args.chunksize,
        args.seed_raw_min_correlation,
        args.seed_residual_min_correlation,
        args.min_combined_score,
    )

    if not seed_edges:
        _no_graph("no_direct_seed_edges_found", rows1=p1_rows, skipped=skipped_meta)
        return

    p1_raw = len(seed_edges)
    first_hop_set = rank_first_hop_nodes(seed_edges, seed_set, args.max_first_hop_nodes)
    all_retained_after_p1 = seed_set | first_hop_set

    # ---- Load rolling files (small) -------------------------------------
    rolling_records: list[dict] = []
    if rr:
        rolling_records.extend(load_rolling_pair_edges(rr, "RAW_ROLLING", instruments))
    if resr:
        rolling_records.extend(load_rolling_pair_edges(resr, "RESIDUAL_ROLLING", instruments))

    # ---- PASS 2 ----------------------------------------------------------
    second_hop_edges, p2_rows = pass2_collect_second_hop_edges(
        full_files,
        first_hop_set,
        seed_set,
        all_retained_after_p1,
        instruments,
        args.chunksize,
        args.raw_min_correlation,
        args.residual_min_correlation,
        args.second_hop_min_combined_score,
        args.min_combined_score,
    )

    p2_raw = len(second_hop_edges)
    second_hop_set = rank_second_hop_nodes(
        second_hop_edges, first_hop_set, all_retained_after_p1, args.max_second_hop_nodes
    )

    all_retained = seed_set | first_hop_set | second_hop_set

    # ---- Build final edge set -------------------------------------------
    final_edges, skipped_threshold = build_final_edge_set(
        seed_edges, second_hop_edges, rolling_records,
        all_retained, args.min_combined_score,
        instruments, seed_set, first_hop_set, second_hop_set,
    )

    if not final_edges:
        _no_graph("no_edges_in_final_set",
                  rows1=p1_rows, rows2=p2_rows, de=p1_raw,
                  skipped=skipped_meta)
        return

    out_dir.mkdir(parents=True, exist_ok=True)

    adj = build_graph(final_edges)
    components, node_to_comp, comp_sizes = compute_connected_components(adj)
    distances, seed_reach = compute_seed_distances(adj, seed_set)
    node_df = compute_node_metrics(
        adj, final_edges, node_to_comp, comp_sizes,
        distances, seed_reach, seed_set, first_hop_set, second_hop_set,
        instruments,
    )
    comp_df = compute_component_reports(
        components, node_df, final_edges,
        seed_set, first_hop_set, second_hop_set,
        instruments, args.min_component_size,
    )
    cliques_raw = find_cliques(
        adj, final_edges, args.min_clique_size, args.max_clique_size, args.max_cliques
    )

    counts = write_reports(
        out_dir, final_edges, node_df, comp_df, cliques_raw,
        seed_set, first_hop_set, second_hop_set,
        instruments, args.theme_name, seed_tickers,
        args.min_clique_size, args.max_cliques,
        node_to_comp, comp_sizes,
    )

    seed_in_graph = sorted(seed_set & set(adj.keys()))
    comp_count_all = len(components)
    cross_industry_count = sum(
        1 for ed in final_edges.values()
        if ed.get("cross_industry_edge", False)
    )

    write_markdown_reports(
        out_dir, db_path, raw_dir, residual_dir,
        args.theme_name, seed_tickers, args,
        {
            **counts,
            "components_total": comp_count_all,
            "seed_tickers_found_in_graph": len(seed_in_graph),
            "retained_first_hop_nodes": len(first_hop_set),
            "retained_second_hop_nodes": len(second_hop_set),
        },
    )

    print(f"SUMMARY db={db_path}")
    print(f"SUMMARY raw_full_dir={raw_dir}")
    print(f"SUMMARY residual_full_dir={residual_dir}")
    print(f"SUMMARY output_dir={out_dir}")
    print(f"SUMMARY theme_name={args.theme_name}")
    print(f"SUMMARY seed_tickers_requested={len(seed_tickers)}")
    print(f"SUMMARY seed_tickers_found_in_graph={len(seed_in_graph)}")
    print(f"SUMMARY raw_full_file_found={raw_full_found}")
    print(f"SUMMARY residual_full_file_found={res_full_found}")
    print(f"SUMMARY raw_rolling_file_found={raw_roll_found}")
    print(f"SUMMARY residual_rolling_file_found={res_roll_found}")
    print(f"SUMMARY pass1_rows_scanned={p1_rows}")
    print(f"SUMMARY pass1_direct_seed_edges_raw={p1_raw}")
    print(f"SUMMARY retained_first_hop_nodes={len(first_hop_set)}")
    print(f"SUMMARY pass2_rows_scanned={p2_rows}")
    print(f"SUMMARY pass2_second_hop_edges_raw={p2_raw}")
    print(f"SUMMARY retained_second_hop_nodes={len(second_hop_set)}")
    print(f"SUMMARY edges_kept={counts.get('edges_kept', 0)}")
    print(f"SUMMARY nodes_total={counts.get('nodes', 0)}")
    print(f"SUMMARY components_total={comp_count_all}")
    print(f"SUMMARY components_min_size={args.min_component_size}")
    print(f"SUMMARY cross_sector_edges={counts.get('cross_sector_edges', 0)}")
    print(f"SUMMARY cross_industry_edges={cross_industry_count}")
    print(f"SUMMARY direct_seed_edges={counts.get('direct_seed_edges', 0)}")
    print(f"SUMMARY first_hop_candidates={counts.get('first_hop_candidates', 0)}")
    print(f"SUMMARY second_hop_candidates={counts.get('second_hop_candidates', 0)}")
    print(f"SUMMARY cliques_found={counts.get('cliques_found', 0)}")
    print(f"SUMMARY cliques_written={counts.get('cliques_written', 0)}")
    print(f"SUMMARY skipped_pairs_missing_metadata={skipped_meta}")
    print(f"SUMMARY skipped_pairs_below_threshold={skipped_threshold}")
    print(f"SUMMARY status=OK")


if __name__ == "__main__":
    main()
