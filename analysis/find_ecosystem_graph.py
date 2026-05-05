"""
V6: Ecosystem graph / theme graph analysis.

Reads similarity pair reports from V3 (raw) and V4 (residual) analyses,
builds an undirected weighted graph, and outputs node, edge, component,
clique, and seed expansion reports for a named theme (e.g. datacenter).

All graph algorithms (BFS, DFS, Bron-Kerbosch) are implemented without
external graph libraries. No networkx, scipy, or sklearn.
"""
from __future__ import annotations

import argparse
import sqlite3
from collections import deque
from pathlib import Path

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

RAW_REPORT_FILES = [
    "top_pairs_overall.csv",
    "top_pairs_same_industry.csv",
    "top_pairs_same_sector_cross_industry.csv",
    "top_pairs_cross_sector.csv",
    "top_pairs_unusual_sync.csv",
]

RESIDUAL_REPORT_FILES = [
    "residual_top_pairs_overall.csv",
    "residual_top_pairs_same_industry.csv",
    "residual_top_pairs_same_sector_cross_industry.csv",
    "residual_top_pairs_cross_sector.csv",
    "residual_top_pairs_unusual_sync.csv",
]

# Base weights for combined_score computation
BASE_WEIGHTS = {
    "raw_correlation": 0.25,
    "raw_rolling_corr_mean": 0.20,
    "residual_correlation": 0.35,
    "residual_rolling_corr_mean": 0.20,
}

DEFAULT_SEED_TICKERS = (
    "NVDA,AMD,AVGO,TSM,ASML,AMAT,LRCX,KLAC,ANET,MRVL,MPWR,VRT,ETN,"
    "PWR,NVT,APH,GLW,CIEN,DELL,SMCI,HPE,NTAP,EQIX,DLR,CEG,VST,NEE,GEV,EMR,TT"
)

EDGE_COLS = [
    "ticker_1", "ticker_2",
    "combined_score",
    "raw_correlation", "raw_rolling_corr_mean", "raw_rolling_corr_latest",
    "residual_correlation", "residual_rolling_corr_mean", "residual_rolling_corr_latest",
    "residual_delta_vs_raw",
    "edge_category", "cross_sector_edge", "cross_industry_edge",
    "source_family", "source_report",
    "sector_1", "industry_1", "sector_2", "industry_2",
    "same_sector", "same_industry",
    "ticker_1_is_seed", "ticker_2_is_seed",
]

NODE_COLS = [
    "ticker", "sector", "industry", "is_seed",
    "component_id", "component_size",
    "degree", "weighted_degree",
    "raw_edge_degree", "residual_edge_degree",
    "cross_sector_degree", "cross_industry_degree",
    "seed_distance", "nearest_seed_tickers",
    "direct_seed_neighbor_count", "direct_seed_neighbors",
]

COMP_COLS = [
    "component_id", "ticker_count", "seed_count",
    "sector_count", "industry_count",
    "edge_count", "average_edge_score", "max_edge_score",
    "cross_sector_edge_count", "cross_industry_edge_count",
    "sectors", "industries", "tickers", "seed_tickers",
    "top_weighted_degree_tickers",
]

SEED_EXP_COLS = [
    "theme_name", "ticker", "sector", "industry",
    "seed_distance", "nearest_seed_tickers",
    "direct_seed_neighbor_count", "direct_seed_neighbors",
    "component_id", "component_size",
    "degree", "weighted_degree", "cross_sector_degree",
    "direct_edge_score_to_best_seed", "best_seed_neighbor",
    "evidence_level",
]

CLIQUE_COLS = [
    "clique_id", "clique_size", "tickers",
    "seed_count", "seed_tickers",
    "sector_count", "industry_count", "sectors", "industries",
    "edge_count", "average_combined_score", "min_combined_score", "max_combined_score",
    "cross_sector_edge_count", "cross_industry_edge_count", "edge_categories",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_float(v: object) -> float:
    if v is None:
        return np.nan
    try:
        f = float(v)
        return np.nan if f != f else f  # NaN check
    except (TypeError, ValueError):
        return np.nan


def _to_str(v: object) -> str:
    if v is None or (isinstance(v, float) and v != v):
        return ""
    return str(v).strip()


def _safe_ge(v: object, threshold: float) -> bool:
    """Return True iff v is a finite number >= threshold."""
    f = _to_float(v)
    return not (f != f) and f >= threshold


def _sorted_csv(*values: object) -> str:
    """Build sorted deduplicated comma-separated string from non-empty string values."""
    parts = sorted({_to_str(v) for v in values if _to_str(v)})
    return ", ".join(parts)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="V6: Ecosystem graph / theme graph analysis."
    )
    p.add_argument("--db", required=True)
    p.add_argument("--raw-report-dir", required=True)
    p.add_argument("--residual-report-dir", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--theme-name", default="datacenter")
    p.add_argument("--seed-tickers", default=DEFAULT_SEED_TICKERS)
    p.add_argument("--min-raw-correlation", type=float, default=0.50)
    p.add_argument("--min-raw-rolling-mean", type=float, default=0.40)
    p.add_argument("--min-residual-correlation", type=float, default=0.30)
    p.add_argument("--min-residual-rolling-mean", type=float, default=0.20)
    p.add_argument("--min-combined-score", type=float, default=0.30)
    p.add_argument("--min-component-size", type=int, default=3)
    p.add_argument("--min-clique-size", type=int, default=3)
    p.add_argument("--max-clique-size", type=int, default=6)
    p.add_argument("--max-cliques", type=int, default=1000)
    p.add_argument("--max-seed-distance", type=int, default=2)
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
        if "instruments" not in tables:
            raise SystemExit("ERROR: required table 'instruments' not found")
        existing = {
            row[1]
            for row in conn.execute("PRAGMA table_info(instruments);").fetchall()
        }
        for col in ("ticker", "sector", "industry"):
            if col not in existing:
                raise SystemExit(
                    f"ERROR: table 'instruments' missing column '{col}'"
                )
    finally:
        conn.close()


def validate_input_dirs(
    raw_dir: Path, residual_dir: Path
) -> tuple[list[Path], list[Path]]:
    if not raw_dir.exists():
        raise SystemExit(f"ERROR: raw report directory not found: {raw_dir}")
    if not residual_dir.exists():
        raise SystemExit(
            f"ERROR: residual report directory not found: {residual_dir}"
        )
    raw_found = [raw_dir / f for f in RAW_REPORT_FILES if (raw_dir / f).exists()]
    res_found = [
        residual_dir / f
        for f in RESIDUAL_REPORT_FILES
        if (residual_dir / f).exists()
    ]
    if not raw_found and not res_found:
        raise SystemExit(
            "ERROR: no expected candidate files found in either directory"
        )
    return raw_found, res_found


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
    instruments: dict[str, dict] = {}
    for ticker, sector, industry in rows:
        instruments[_to_str(ticker)] = {
            "sector": _to_str(sector),
            "industry": _to_str(industry),
        }
    return instruments


# ---------------------------------------------------------------------------
# Load and normalize candidate edges
# ---------------------------------------------------------------------------

def load_candidate_edges(
    raw_found: list[Path], residual_found: list[Path]
) -> tuple[pd.DataFrame, int]:
    """
    Read candidate pairs from all found files.
    Returns (combined_df, raw_row_count).
    Each row has: ticker_1, ticker_2, family, source,
    raw_correlation, raw_rolling_corr_mean, raw_rolling_corr_latest,
    residual_correlation, residual_rolling_corr_mean, residual_rolling_corr_latest,
    residual_delta_vs_raw.
    """
    all_rows: list[dict] = []
    raw_count = 0

    def _read(paths: list[Path], family: str) -> None:
        nonlocal raw_count
        for path in paths:
            source = path.stem
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
                if not t1 or not t2 or t1 == t2:
                    continue

                # Canonical alphabetical order
                ct1, ct2 = min(t1, t2), max(t1, t2)

                # Raw metrics: full_period_correlation preferred, fall back to raw_correlation
                raw_corr = _to_float(row.get("full_period_correlation", np.nan))
                if np.isnan(raw_corr):
                    raw_corr = _to_float(row.get("raw_correlation", np.nan))

                raw_rolling_mean = _to_float(row.get("rolling_corr_mean", np.nan))
                raw_rolling_latest = _to_float(row.get("rolling_corr_latest", np.nan))

                # Residual metrics
                res_corr = _to_float(row.get("sector_residual_correlation", np.nan))
                res_rolling_mean = _to_float(row.get("rolling_residual_corr_mean", np.nan))
                res_rolling_latest = _to_float(row.get("rolling_residual_corr_latest", np.nan))
                res_delta = _to_float(row.get("correlation_delta_vs_raw", np.nan))

                all_rows.append(
                    {
                        "canonical_ticker_1": ct1,
                        "canonical_ticker_2": ct2,
                        "_family": family,
                        "_source": source,
                        "raw_correlation": raw_corr,
                        "raw_rolling_corr_mean": raw_rolling_mean,
                        "raw_rolling_corr_latest": raw_rolling_latest,
                        "residual_correlation": res_corr,
                        "residual_rolling_corr_mean": res_rolling_mean,
                        "residual_rolling_corr_latest": res_rolling_latest,
                        "residual_delta_vs_raw": res_delta,
                    }
                )

    _read(raw_found, "RAW")
    _read(residual_found, "RESIDUAL")

    return pd.DataFrame(all_rows), raw_count


def normalize_and_deduplicate_edges(
    raw_df: pd.DataFrame, instruments: dict[str, dict]
) -> tuple[pd.DataFrame, int]:
    """
    Deduplicate by canonical pair. For duplicate rows: combine source/family
    (sorted comma-sep), take max of numeric metrics.
    Enrich with sector/industry metadata from instruments DB.
    Returns (deduped_df, skipped_missing_metadata_count).
    """
    if raw_df.empty:
        return pd.DataFrame(), 0

    numeric_cols = [
        "raw_correlation", "raw_rolling_corr_mean", "raw_rolling_corr_latest",
        "residual_correlation", "residual_rolling_corr_mean",
        "residual_rolling_corr_latest", "residual_delta_vs_raw",
    ]
    pair_key = ["canonical_ticker_1", "canonical_ticker_2"]

    rows: list[dict] = []
    skipped = 0

    for (ct1, ct2), group in raw_df.groupby(pair_key, sort=True):
        # Skip pairs where either ticker is not in instruments
        if ct1 not in instruments or ct2 not in instruments:
            skipped += group.shape[0]
            continue

        sources = sorted({s for s in group["_source"].tolist() if s})
        families = sorted({f for f in group["_family"].tolist() if f})

        row: dict = {
            "canonical_ticker_1": ct1,
            "canonical_ticker_2": ct2,
            "source_report": ", ".join(sources),
            "source_family": ", ".join(families),
        }

        # Take max of each numeric metric across duplicate rows
        for col in numeric_cols:
            if col in group.columns:
                vals = group[col].dropna()
                row[col] = float(vals.max()) if len(vals) > 0 else np.nan
            else:
                row[col] = np.nan

        # Sector/industry from DB (canonical ticker order: ct1 < ct2 alphabetically)
        meta1 = instruments[ct1]
        meta2 = instruments[ct2]
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

        row.update(
            {
                "sector_1": s1, "industry_1": i1,
                "sector_2": s2, "industry_2": i2,
                "same_sector": same_sector, "same_industry": same_industry,
                "edge_category": edge_cat,
                "cross_sector_edge": not same_sector and bool(s1 and s2),
                "cross_industry_edge": not same_industry and bool(i1 and i2),
            }
        )
        rows.append(row)

    return pd.DataFrame(rows), skipped


# ---------------------------------------------------------------------------
# Combined score
# ---------------------------------------------------------------------------

def compute_combined_score(
    raw_corr: float,
    raw_rolling_mean: float,
    res_corr: float,
    res_rolling_mean: float,
) -> float:
    """
    Weighted average of available metrics. Missing metrics are removed and
    remaining weights are rescaled to sum to 1.
    Returns NaN if all metrics are missing.
    """
    candidates = [
        ("raw_correlation", raw_corr),
        ("raw_rolling_corr_mean", raw_rolling_mean),
        ("residual_correlation", res_corr),
        ("residual_rolling_corr_mean", res_rolling_mean),
    ]
    available = [
        (BASE_WEIGHTS[name], v)
        for name, v in candidates
        if not (v != v) and v is not None  # v != v catches NaN
    ]
    if not available:
        return np.nan
    total_w = sum(w for w, _ in available)
    return sum(w * v for w, v in available) / total_w


# ---------------------------------------------------------------------------
# Edge filtering
# ---------------------------------------------------------------------------

def filter_edges(
    df: pd.DataFrame,
    seed_set: set[str],
    min_raw_corr: float,
    min_raw_rolling: float,
    min_res_corr: float,
    min_res_rolling: float,
    min_combined: float,
) -> tuple[pd.DataFrame, int]:
    """
    Keep edges passing condition A, B, or C, plus combined_score threshold.
    Returns (kept_df, skipped_below_threshold_count).
    """
    if df.empty:
        return df, 0

    # Compute combined_score for all edges
    df = df.copy()
    df["combined_score"] = df.apply(
        lambda r: compute_combined_score(
            r["raw_correlation"],
            r["raw_rolling_corr_mean"],
            r["residual_correlation"],
            r["residual_rolling_corr_mean"],
        ),
        axis=1,
    )

    kept_rows = []
    skipped = 0

    for _, row in df.iterrows():
        ct1 = row["canonical_ticker_1"]
        ct2 = row["canonical_ticker_2"]
        rc = _to_float(row.get("raw_correlation"))
        rrm = _to_float(row.get("raw_rolling_corr_mean"))
        resc = _to_float(row.get("residual_correlation"))
        resrm = _to_float(row.get("residual_rolling_corr_mean"))
        cs = _to_float(row.get("combined_score"))

        cond_a = _safe_ge(rc, min_raw_corr) and _safe_ge(rrm, min_raw_rolling)
        cond_b = _safe_ge(resc, min_res_corr) and _safe_ge(resrm, min_res_rolling)
        cond_c = (ct1 in seed_set or ct2 in seed_set) and (
            _safe_ge(rc, min_raw_corr) or _safe_ge(resc, min_res_corr)
        )

        if (cond_a or cond_b or cond_c) and _safe_ge(cs, min_combined):
            kept_rows.append(row)
        else:
            skipped += 1

    if not kept_rows:
        return pd.DataFrame(columns=df.columns), skipped

    return pd.DataFrame(kept_rows).reset_index(drop=True), skipped


# ---------------------------------------------------------------------------
# Build graph
# ---------------------------------------------------------------------------

def build_graph(
    edges_df: pd.DataFrame, seed_set: set[str]
) -> tuple[dict[str, set[str]], dict[tuple[str, str], dict]]:
    """
    Build adjacency list and edge_map from filtered edges DataFrame.

    adj: dict[ticker -> set of neighbors]
    edge_map: dict[(ct1, ct2) -> dict of edge attributes]
    Canonical pair: ct1 < ct2 alphabetically.
    """
    adj: dict[str, set[str]] = {}
    edge_map: dict[tuple[str, str], dict] = {}

    for _, row in edges_df.iterrows():
        ct1 = row["canonical_ticker_1"]
        ct2 = row["canonical_ticker_2"]

        adj.setdefault(ct1, set()).add(ct2)
        adj.setdefault(ct2, set()).add(ct1)

        key = (ct1, ct2)
        edge_map[key] = {
            "ticker_1": ct1,
            "ticker_2": ct2,
            "combined_score": _to_float(row.get("combined_score")),
            "raw_correlation": _to_float(row.get("raw_correlation")),
            "raw_rolling_corr_mean": _to_float(row.get("raw_rolling_corr_mean")),
            "raw_rolling_corr_latest": _to_float(row.get("raw_rolling_corr_latest")),
            "residual_correlation": _to_float(row.get("residual_correlation")),
            "residual_rolling_corr_mean": _to_float(row.get("residual_rolling_corr_mean")),
            "residual_rolling_corr_latest": _to_float(row.get("residual_rolling_corr_latest")),
            "residual_delta_vs_raw": _to_float(row.get("residual_delta_vs_raw")),
            "edge_category": _to_str(row.get("edge_category", "UNKNOWN")),
            "cross_sector_edge": bool(row.get("cross_sector_edge", False)),
            "cross_industry_edge": bool(row.get("cross_industry_edge", False)),
            "source_family": _to_str(row.get("source_family", "")),
            "source_report": _to_str(row.get("source_report", "")),
            "sector_1": _to_str(row.get("sector_1", "")),
            "industry_1": _to_str(row.get("industry_1", "")),
            "sector_2": _to_str(row.get("sector_2", "")),
            "industry_2": _to_str(row.get("industry_2", "")),
            "same_sector": bool(row.get("same_sector", False)),
            "same_industry": bool(row.get("same_industry", False)),
            "ticker_1_is_seed": ct1 in seed_set,
            "ticker_2_is_seed": ct2 in seed_set,
        }

    return adj, edge_map


# ---------------------------------------------------------------------------
# Connected components (deterministic BFS)
# ---------------------------------------------------------------------------

def compute_connected_components(
    adj: dict[str, set[str]]
) -> tuple[list[list[str]], dict[str, int], dict[int, int]]:
    """
    Find connected components using BFS. Processes nodes in sorted order.
    Returns:
        components:   list of sorted ticker lists, sorted by min-ticker alphabetically
        node_to_comp: dict ticker -> component index (0-based)
        comp_sizes:   dict component_index -> size
    """
    visited: set[str] = set()
    comp_list: list[list[str]] = []

    for start in sorted(adj.keys()):
        if start in visited:
            continue
        # BFS
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

    # Sort components by their alphabetically first ticker
    comp_list.sort(key=lambda c: c[0])

    node_to_comp: dict[str, int] = {}
    comp_sizes: dict[int, int] = {}
    for idx, comp in enumerate(comp_list):
        comp_sizes[idx] = len(comp)
        for ticker in comp:
            node_to_comp[ticker] = idx

    return comp_list, node_to_comp, comp_sizes


# ---------------------------------------------------------------------------
# Seed distances (multi-source BFS)
# ---------------------------------------------------------------------------

def compute_seed_distances(
    adj: dict[str, set[str]], seed_set: set[str]
) -> tuple[dict[str, int], dict[str, set[str]]]:
    """
    Level-by-level multi-source BFS from all seed tickers in the graph.
    Returns:
        distances:   dict ticker -> min graph distance to any seed (0 for seeds)
        seed_reach:  dict ticker -> set of seeds at the minimum distance
    """
    graph_seeds = sorted(seed_set & set(adj.keys()))
    distances: dict[str, int] = {}
    seed_reach: dict[str, set[str]] = {}

    for s in graph_seeds:
        distances[s] = 0
        seed_reach[s] = {s}

    current_level = list(graph_seeds)
    d = 0

    while current_level:
        next_level_map: dict[str, set[str]] = {}
        for node in current_level:
            for nb in adj[node]:
                if nb not in distances:
                    if nb not in next_level_map:
                        next_level_map[nb] = set()
                    next_level_map[nb].update(seed_reach[node])

        next_level = sorted(next_level_map.keys())
        d += 1
        for nb in next_level:
            distances[nb] = d
            seed_reach[nb] = next_level_map[nb]

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
    instruments: dict[str, dict],
) -> pd.DataFrame:
    rows: list[dict] = []

    for ticker in sorted(adj.keys()):
        neighbors = sorted(adj[ticker])
        degree = len(neighbors)

        weighted_degree = 0.0
        raw_edge_deg = 0
        res_edge_deg = 0
        cross_sector_deg = 0
        cross_industry_deg = 0

        for nb in neighbors:
            key = (min(ticker, nb), max(ticker, nb))
            edge = edge_map.get(key, {})
            cs = _to_float(edge.get("combined_score"))
            if not (cs != cs):
                weighted_degree += cs
            if not (_to_float(edge.get("raw_correlation")) != _to_float(edge.get("raw_correlation"))):
                raw_edge_deg += 1
            if not (_to_float(edge.get("residual_correlation")) != _to_float(edge.get("residual_correlation"))):
                res_edge_deg += 1
            if edge.get("cross_sector_edge", False):
                cross_sector_deg += 1
            if edge.get("cross_industry_edge", False):
                cross_industry_deg += 1

        comp_idx = node_to_comp.get(ticker, -1)
        sd = distances.get(ticker, None)

        # nearest_seed_tickers: seeds from BFS reach at min distance
        nearest = sorted(seed_reach.get(ticker, set()) & seed_set)
        nearest_str = ", ".join(nearest)

        # direct seed neighbors: adjacent tickers that are seeds
        direct_seeds = sorted([nb for nb in neighbors if nb in seed_set])

        info = instruments.get(ticker, {"sector": "", "industry": ""})

        rows.append(
            {
                "ticker": ticker,
                "sector": info["sector"],
                "industry": info["industry"],
                "is_seed": ticker in seed_set,
                "component_id": comp_idx + 1 if comp_idx >= 0 else None,
                "component_size": comp_sizes.get(comp_idx, 0) if comp_idx >= 0 else 0,
                "degree": degree,
                "weighted_degree": weighted_degree,
                "raw_edge_degree": raw_edge_deg,
                "residual_edge_degree": res_edge_deg,
                "cross_sector_degree": cross_sector_deg,
                "cross_industry_degree": cross_industry_deg,
                "seed_distance": sd,
                "nearest_seed_tickers": nearest_str,
                "direct_seed_neighbor_count": len(direct_seeds),
                "direct_seed_neighbors": ", ".join(direct_seeds),
            }
        )

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Component reports
# ---------------------------------------------------------------------------

def compute_component_reports(
    components: list[list[str]],
    node_metrics_df: pd.DataFrame,
    edge_map: dict[tuple[str, str], dict],
    seed_set: set[str],
    instruments: dict[str, dict],
    min_comp_size: int,
) -> pd.DataFrame:
    node_wd = (
        node_metrics_df.set_index("ticker")["weighted_degree"].to_dict()
        if not node_metrics_df.empty
        else {}
    )

    rows: list[dict] = []
    for idx, comp in enumerate(components):
        if len(comp) < min_comp_size:
            continue

        comp_set = set(comp)

        # Edges within component
        comp_edges = [
            ed
            for (ct1, ct2), ed in edge_map.items()
            if ct1 in comp_set and ct2 in comp_set
        ]

        edge_count = len(comp_edges)
        scores = [_to_float(e["combined_score"]) for e in comp_edges]
        valid_scores = [s for s in scores if not (s != s)]
        avg_score = float(np.mean(valid_scores)) if valid_scores else np.nan
        max_score = float(max(valid_scores)) if valid_scores else np.nan

        cross_sector_cnt = sum(1 for e in comp_edges if e.get("cross_sector_edge", False))
        cross_industry_cnt = sum(1 for e in comp_edges if e.get("cross_industry_edge", False))

        sectors = sorted(
            {
                instruments.get(t, {}).get("sector", "")
                for t in comp
                if instruments.get(t, {}).get("sector", "")
            }
        )
        industries = sorted(
            {
                instruments.get(t, {}).get("industry", "")
                for t in comp
                if instruments.get(t, {}).get("industry", "")
            }
        )
        seed_in_comp = sorted(comp_set & seed_set)

        # Top 10 by weighted_degree then ticker
        by_wd = sorted(comp, key=lambda t: (-node_wd.get(t, 0.0), t))
        top10 = by_wd[:10]

        rows.append(
            {
                "component_id": idx + 1,
                "ticker_count": len(comp),
                "seed_count": len(seed_in_comp),
                "sector_count": len(sectors),
                "industry_count": len(industries),
                "edge_count": edge_count,
                "average_edge_score": avg_score,
                "max_edge_score": max_score,
                "cross_sector_edge_count": cross_sector_cnt,
                "cross_industry_edge_count": cross_industry_cnt,
                "sectors": ", ".join(sectors),
                "industries": ", ".join(industries),
                "tickers": ", ".join(sorted(comp)),
                "seed_tickers": ", ".join(seed_in_comp),
                "top_weighted_degree_tickers": ", ".join(top10),
            }
        )

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Clique finding (Bron-Kerbosch with pivoting, deterministic)
# ---------------------------------------------------------------------------

def find_cliques(
    adj: dict[str, set[str]],
    edge_map: dict[tuple[str, str], dict],
    min_size: int,
    max_size: int,
    max_total: int,
) -> list[list[str]]:
    """
    Find maximal cliques of size in [min_size, max_size] using Bron-Kerbosch
    with Tomita pivoting. Nodes are processed in sorted (alphabetical) order
    for full determinism. Results are deduplicated by sorted tuple.

    For maximal cliques larger than max_size, they will not appear since the
    algorithm prunes branches once |R| == max_size and reports R as a
    size-limited clique.
    """
    nodes_sorted = sorted(adj.keys())
    # Frozen sets for O(1) set operations
    adj_f: dict[str, frozenset] = {v: frozenset(adj[v]) for v in nodes_sorted}

    found: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()

    # Safety limit: stop collecting after this many raw finds to avoid huge runtimes
    _safety_limit = max(max_total * 20, 20000)

    def bk(R: frozenset, P: frozenset, X: frozenset) -> None:
        if len(found) >= _safety_limit:
            return

        # Pruning: cannot reach min_size
        if len(R) + len(P) < min_size:
            return

        # At max_size: report R as a size-limited clique, do not expand
        if len(R) == max_size:
            key = tuple(sorted(R))
            if key not in seen:
                seen.add(key)
                found.append(list(key))
            return

        # Maximal clique found (no expansion possible)
        if not P and not X:
            if len(R) >= min_size:
                key = tuple(sorted(R))
                if key not in seen:
                    seen.add(key)
                    found.append(list(key))
            return

        # Pivot: choose u from P ∪ X maximizing |N(u) ∩ P|
        PX = P | X
        pivot = max(sorted(PX), key=lambda u: len(adj_f.get(u, frozenset()) & P))

        # Iterate over P \ N(pivot) in sorted order for determinism
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
    edges_df: pd.DataFrame,
    node_df: pd.DataFrame,
    comp_df: pd.DataFrame,
    cliques_raw: list[list[str]],
    edge_map: dict[tuple[str, str], dict],
    seed_set: set[str],
    instruments: dict[str, dict],
    theme_name: str,
    max_seed_dist: int,
    min_comp_size: int,
    min_clique_size: int,
    max_cliques: int,
) -> dict[str, int]:
    """Write all CSV output files. Returns dict of counts."""
    counts: dict[str, int] = {}

    # ------------------------------------------------------------------
    # 1. ecosystem_edges.csv
    # ------------------------------------------------------------------
    e_rows = []
    for _, ed in edge_map.items():
        e_rows.append(ed)
    edf = pd.DataFrame(e_rows)
    if not edf.empty:
        edf = edf.sort_values(
            ["combined_score", "ticker_1", "ticker_2"],
            ascending=[False, True, True],
        ).reset_index(drop=True)
    out_cols = [c for c in EDGE_COLS if c in edf.columns] if not edf.empty else EDGE_COLS
    (edf[out_cols] if not edf.empty else pd.DataFrame(columns=EDGE_COLS)).to_csv(
        out_dir / "ecosystem_edges.csv", index=False
    )
    counts["edges_kept"] = len(edf)

    # ------------------------------------------------------------------
    # 2. ecosystem_nodes.csv
    # ------------------------------------------------------------------
    if not node_df.empty:
        ndf = node_df.copy()
        ndf["_sd_sort"] = ndf["seed_distance"].fillna(9999)
        ndf = ndf.sort_values(
            ["is_seed", "_sd_sort", "weighted_degree", "ticker"],
            ascending=[False, True, False, True],
        ).drop(columns=["_sd_sort"]).reset_index(drop=True)
        nout = [c for c in NODE_COLS if c in ndf.columns]
        ndf[nout].to_csv(out_dir / "ecosystem_nodes.csv", index=False)
        counts["nodes"] = len(ndf)
    else:
        pd.DataFrame(columns=NODE_COLS).to_csv(out_dir / "ecosystem_nodes.csv", index=False)
        counts["nodes"] = 0

    # ------------------------------------------------------------------
    # 3. ecosystem_components.csv
    # ------------------------------------------------------------------
    if not comp_df.empty:
        cdf = comp_df.sort_values(
            ["seed_count", "ticker_count", "average_edge_score", "component_id"],
            ascending=[False, False, False, True],
        ).reset_index(drop=True)
        cdf[[c for c in COMP_COLS if c in cdf.columns]].to_csv(
            out_dir / "ecosystem_components.csv", index=False
        )
        counts["components"] = len(cdf)
    else:
        pd.DataFrame(columns=COMP_COLS).to_csv(out_dir / "ecosystem_components.csv", index=False)
        counts["components"] = 0

    # ------------------------------------------------------------------
    # 4. ecosystem_seed_expansion.csv
    # ------------------------------------------------------------------
    if not node_df.empty:
        # Non-seed nodes with seed_distance <= max_seed_dist
        node_wd = node_df.set_index("ticker")["weighted_degree"].to_dict()
        seed_exp_rows = []
        for _, row in node_df.iterrows():
            ticker = row["ticker"]
            if ticker in seed_set:
                continue
            sd = row.get("seed_distance")
            if sd is None or (isinstance(sd, float) and sd != sd):
                continue
            if sd > max_seed_dist:
                continue

            # Best direct seed edge
            direct_seeds = [nb for nb in (row.get("direct_seed_neighbors", "") or "").split(", ") if nb]
            best_seed_nb = ""
            best_seed_score = np.nan
            for s in direct_seeds:
                key = (min(ticker, s), max(ticker, s))
                edge = edge_map.get(key, {})
                cs = _to_float(edge.get("combined_score"))
                if not (cs != cs) and (best_seed_score != best_seed_score or cs > best_seed_score):
                    best_seed_score = cs
                    best_seed_nb = s

            if sd == 1:
                evidence = "DIRECT_SEED_NEIGHBOR"
            elif sd == 2:
                evidence = "SECOND_ORDER_NEIGHBOR"
            else:
                evidence = "OTHER_WITHIN_MAX_DISTANCE"

            seed_exp_rows.append(
                {
                    "theme_name": theme_name,
                    "ticker": ticker,
                    "sector": row.get("sector", ""),
                    "industry": row.get("industry", ""),
                    "seed_distance": sd,
                    "nearest_seed_tickers": row.get("nearest_seed_tickers", ""),
                    "direct_seed_neighbor_count": row.get("direct_seed_neighbor_count", 0),
                    "direct_seed_neighbors": row.get("direct_seed_neighbors", ""),
                    "component_id": row.get("component_id"),
                    "component_size": row.get("component_size", 0),
                    "degree": row.get("degree", 0),
                    "weighted_degree": row.get("weighted_degree", 0.0),
                    "cross_sector_degree": row.get("cross_sector_degree", 0),
                    "direct_edge_score_to_best_seed": best_seed_score,
                    "best_seed_neighbor": best_seed_nb,
                    "evidence_level": evidence,
                }
            )

        sedf = pd.DataFrame(seed_exp_rows)
        if not sedf.empty:
            sedf["_dss_sort"] = sedf["direct_edge_score_to_best_seed"].fillna(-np.inf)
            sedf = sedf.sort_values(
                ["seed_distance", "_dss_sort", "weighted_degree", "ticker"],
                ascending=[True, False, False, True],
            ).drop(columns=["_dss_sort"]).reset_index(drop=True)
        out_se = [c for c in SEED_EXP_COLS if c in sedf.columns] if not sedf.empty else SEED_EXP_COLS
        (sedf[out_se] if not sedf.empty else pd.DataFrame(columns=SEED_EXP_COLS)).to_csv(
            out_dir / "ecosystem_seed_expansion.csv", index=False
        )
        counts["seed_expansion_rows"] = len(sedf)
    else:
        pd.DataFrame(columns=SEED_EXP_COLS).to_csv(
            out_dir / "ecosystem_seed_expansion.csv", index=False
        )
        counts["seed_expansion_rows"] = 0

    # ------------------------------------------------------------------
    # 5. ecosystem_seed_direct_edges.csv
    # ------------------------------------------------------------------
    seed_edge_rows = []
    for (ct1, ct2), ed in edge_map.items():
        t1_seed = ed.get("ticker_1_is_seed", False)
        t2_seed = ed.get("ticker_2_is_seed", False)
        if not t1_seed and not t2_seed:
            continue
        row = dict(ed)
        if t1_seed and t2_seed:
            row["seed_ticker"] = _sorted_csv(ct1, ct2)
            row["non_seed_ticker"] = ""
        elif t1_seed:
            row["seed_ticker"] = ct1
            row["non_seed_ticker"] = ct2
        else:
            row["seed_ticker"] = ct2
            row["non_seed_ticker"] = ct1
        seed_edge_rows.append(row)

    sde_cols = EDGE_COLS + ["seed_ticker", "non_seed_ticker"]
    if seed_edge_rows:
        sdedf = pd.DataFrame(seed_edge_rows)
        sdedf["_ns_sort"] = sdedf["non_seed_ticker"].fillna("").astype(str)
        sdedf = sdedf.sort_values(
            ["combined_score", "seed_ticker", "_ns_sort"],
            ascending=[False, True, True],
        ).drop(columns=["_ns_sort"]).reset_index(drop=True)
        out_sde = [c for c in sde_cols if c in sdedf.columns]
        sdedf[out_sde].to_csv(out_dir / "ecosystem_seed_direct_edges.csv", index=False)
        counts["seed_direct_edges"] = len(sdedf)
    else:
        pd.DataFrame(columns=sde_cols).to_csv(
            out_dir / "ecosystem_seed_direct_edges.csv", index=False
        )
        counts["seed_direct_edges"] = 0

    # ------------------------------------------------------------------
    # 6. ecosystem_cross_sector_bridges.csv
    # ------------------------------------------------------------------
    cross_rows = [ed for ed in edge_map.values() if ed.get("cross_sector_edge", False)]
    if cross_rows:
        csdf = pd.DataFrame(cross_rows)
        csdf["_rc"] = csdf["raw_correlation"].fillna(-np.inf)
        csdf["_resc"] = csdf["residual_correlation"].fillna(-np.inf)
        csdf = csdf.sort_values(
            ["combined_score", "_resc", "_rc", "ticker_1", "ticker_2"],
            ascending=[False, False, False, True, True],
        ).drop(columns=["_rc", "_resc"]).reset_index(drop=True)
        out_cs = [c for c in EDGE_COLS if c in csdf.columns]
        csdf[out_cs].to_csv(out_dir / "ecosystem_cross_sector_bridges.csv", index=False)
        counts["cross_sector_edges"] = len(csdf)
    else:
        pd.DataFrame(columns=EDGE_COLS).to_csv(
            out_dir / "ecosystem_cross_sector_bridges.csv", index=False
        )
        counts["cross_sector_edges"] = 0

    # ------------------------------------------------------------------
    # 7. ecosystem_cliques_3plus.csv
    # ------------------------------------------------------------------
    clique_rows = []
    for clique_tickers in cliques_raw:
        n = len(clique_tickers)
        clique_set = set(clique_tickers)
        # All edges within clique
        pairs = [(clique_tickers[i], clique_tickers[j])
                 for i in range(n) for j in range(i + 1, n)]
        c_edges = []
        for (a, b) in pairs:
            key = (min(a, b), max(a, b))
            ed = edge_map.get(key)
            if ed:
                c_edges.append(ed)

        c_scores = [_to_float(e["combined_score"]) for e in c_edges]
        valid_c = [s for s in c_scores if not (s != s)]

        c_sectors = sorted({instruments.get(t, {}).get("sector", "") for t in clique_tickers if instruments.get(t, {}).get("sector", "")})
        c_inds = sorted({instruments.get(t, {}).get("industry", "") for t in clique_tickers if instruments.get(t, {}).get("industry", "")})
        c_seed = sorted(clique_set & seed_set)
        c_cats = sorted({e["edge_category"] for e in c_edges if e.get("edge_category")})

        clique_rows.append(
            {
                "clique_size": n,
                "tickers": ", ".join(sorted(clique_tickers)),
                "seed_count": len(c_seed),
                "seed_tickers": ", ".join(c_seed),
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
            }
        )

    if clique_rows:
        cldf = pd.DataFrame(clique_rows)
        cldf["_avg"] = cldf["average_combined_score"].fillna(-np.inf)
        cldf = cldf.sort_values(
            ["clique_size", "_avg", "seed_count", "tickers"],
            ascending=[False, False, False, True],
        ).drop(columns=["_avg"]).reset_index(drop=True)
        # Apply max_cliques limit
        cldf = cldf.head(max_cliques).reset_index(drop=True)
        cldf.insert(0, "clique_id", range(1, len(cldf) + 1))
        out_cl = [c for c in CLIQUE_COLS if c in cldf.columns]
        cldf[out_cl].to_csv(out_dir / "ecosystem_cliques_3plus.csv", index=False)
        counts["cliques_found"] = len(clique_rows)
        counts["cliques_written"] = len(cldf)
    else:
        pd.DataFrame(columns=CLIQUE_COLS).to_csv(
            out_dir / "ecosystem_cliques_3plus.csv", index=False
        )
        counts["cliques_found"] = 0
        counts["cliques_written"] = 0

    # ------------------------------------------------------------------
    # 8. ecosystem_seed_components.csv
    # ------------------------------------------------------------------
    if not comp_df.empty:
        sc_df = comp_df[comp_df["seed_count"] > 0].copy()
        sc_df = sc_df.sort_values(
            ["seed_count", "ticker_count", "average_edge_score", "component_id"],
            ascending=[False, False, False, True],
        ).reset_index(drop=True)
        sc_df[[c for c in COMP_COLS if c in sc_df.columns]].to_csv(
            out_dir / "ecosystem_seed_components.csv", index=False
        )
        counts["seed_components"] = len(sc_df)
    else:
        pd.DataFrame(columns=COMP_COLS).to_csv(
            out_dir / "ecosystem_seed_components.csv", index=False
        )
        counts["seed_components"] = 0

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
    # ecosystem_theme_summary.md
    summary_lines = [
        f"# Ecosystem Theme: {theme_name}",
        "",
        f"Input database: `{db_path}`",
        f"Raw report directory: `{raw_dir}`",
        f"Residual report directory: `{residual_dir}`",
        f"Output directory: `{out_dir}`",
        "",
        f"Theme name: `{theme_name}`",
        f"Seed tickers ({len(seed_tickers)}): {', '.join(seed_tickers)}",
        "",
        "## Thresholds",
        "",
        f"- Min raw correlation: `{args.min_raw_correlation}`",
        f"- Min raw rolling mean: `{args.min_raw_rolling_mean}`",
        f"- Min residual correlation: `{args.min_residual_correlation}`",
        f"- Min residual rolling mean: `{args.min_residual_rolling_mean}`",
        f"- Min combined score: `{args.min_combined_score}`",
        f"- Min component size: `{args.min_component_size}`",
        f"- Min / max clique size: `{args.min_clique_size}` / `{args.max_clique_size}`",
        f"- Max seed distance: `{args.max_seed_distance}`",
        "",
        "## Summary Statistics",
        "",
        f"- Graph nodes: `{stats.get('nodes', 0)}`",
        f"- Graph edges: `{stats.get('edges_kept', 0)}`",
        f"- Connected components (total): `{stats.get('components_total', 0)}`",
        f"- Seed components: `{stats.get('seed_components', 0)}`",
        f"- Cliques found (size {args.min_clique_size}+): `{stats.get('cliques_found', 0)}`",
        f"- Cliques written: `{stats.get('cliques_written', 0)}`",
        f"- Direct seed edges: `{stats.get('seed_direct_edges', 0)}`",
        f"- Seed expansion candidates: `{stats.get('seed_expansion_rows', 0)}`",
        f"- Cross-sector edges: `{stats.get('cross_sector_edges', 0)}`",
        f"- Seed tickers found in graph: `{stats.get('seed_tickers_found_in_graph', 0)}`",
        "",
        "## Graph Construction",
        "",
        "Nodes are individual stock tickers. Edges are statistical similarity relationships",
        "derived from daily close-change percentage residual correlations (V4) and raw",
        "Pearson correlations (V3). An edge exists when the pair passes filtering thresholds",
        "on raw correlation, rolling correlation mean, residual correlation, and/or seed adjacency.",
        "",
        "## Combined Score",
        "",
        "Each edge has a `combined_score` computed as a weighted average of up to four",
        "metrics: raw_correlation (0.25), raw_rolling_corr_mean (0.20), residual_correlation (0.35),",
        "residual_rolling_corr_mean (0.20). Missing metrics are removed and remaining weights",
        "are rescaled to sum to 1.",
        "",
        "## Connected Components",
        "",
        "Connected components group all tickers reachable from each other via the similarity",
        "edge graph. A component may span multiple sectors if cross-sector edges exist.",
        "Component size is the number of tickers. Only components with size >= min_component_size",
        "are reported in ecosystem_components.csv.",
        "",
        "## Cliques",
        "",
        "A clique is a group of tickers where every pair has a direct similarity edge.",
        "Cliques of size >= min_clique_size are reported. Large cliques indicate groups",
        "with strong mutual statistical co-movement.",
        "",
        "## Seed Expansion",
        "",
        "Seed tickers define the theme (e.g. datacenter ecosystem). The seed expansion",
        "report lists non-seed tickers within max_seed_distance graph hops from any seed.",
        "Distance 1 = direct edge to a seed. Distance 2 = connected via one intermediate node.",
        "",
        f"## Datacenter Ecosystem Context",
        "",
        "The datacenter theme includes chips, semiconductor equipment, networking,",
        "optical/connectivity, electrical equipment, power infrastructure, utilities,",
        "datacenter REITs, cooling, and construction/engineering. These sectors are",
        "economically linked by the build-out of AI and cloud infrastructure.",
        "",
        "## Important Caveats",
        "",
        "Graph connections are statistical similarity measures, not proof of business",
        "relationship or causality. Two stocks may have high correlation for reasons",
        "unrelated to their business connection: shared macro sensitivity, sector rotation,",
        "or statistical coincidence.",
        "",
        "Datacenter ecosystem interpretation requires business validation. Not all stocks",
        "in the graph are directly linked to the datacenter theme. The graph reflects",
        "co-movement patterns over the analyzed period, which may not persist.",
    ]
    (out_dir / "ecosystem_theme_summary.md").write_text(
        "\n".join(summary_lines) + "\n", encoding="utf-8"
    )

    # ecosystem_readme.md
    readme_lines = [
        "# Ecosystem Graph Output Files",
        "",
        "Generated by `analysis/find_ecosystem_graph.py`.",
        "",
        "## Files",
        "",
        "### ecosystem_edges.csv",
        "One row per graph edge. Contains combined_score, raw/residual correlation metrics,",
        "edge category (SAME_INDUSTRY / SAME_SECTOR_CROSS_INDUSTRY / CROSS_SECTOR / UNKNOWN),",
        "sector/industry metadata, and seed flags.",
        "",
        "### ecosystem_nodes.csv",
        "One row per ticker node. Contains sector/industry, is_seed flag, component ID,",
        "degree, weighted_degree, seed_distance, nearest_seed_tickers, and direct seed neighbor info.",
        "",
        "### ecosystem_components.csv",
        f"Connected components with >= {args.min_component_size} tickers.",
        "Contains component summary: ticker/seed counts, edge stats, sector/industry lists.",
        "",
        "### ecosystem_seed_expansion.csv",
        f"Non-seed tickers within {args.max_seed_distance} graph hops of any seed ticker.",
        "Useful for discovering theme-adjacent stocks.",
        "",
        "### ecosystem_seed_direct_edges.csv",
        "Edges where at least one ticker is a seed. Sorted by combined_score descending.",
        "",
        "### ecosystem_cross_sector_bridges.csv",
        "Cross-sector edges only. These edges connect tickers from different sectors,",
        "potentially indicating cross-sector statistical linkages.",
        "",
        "### ecosystem_cliques_3plus.csv",
        f"All cliques of size {args.min_clique_size}–{args.max_clique_size}.",
        "A clique is a fully connected subgraph: every pair within the group has a direct edge.",
        "",
        "### ecosystem_seed_components.csv",
        "Components that contain at least one seed ticker.",
        "",
        "### ecosystem_theme_summary.md",
        "Detailed Markdown report with statistics and methodology explanation.",
        "",
        "### ecosystem_readme.md",
        "This file.",
    ]
    (out_dir / "ecosystem_readme.md").write_text(
        "\n".join(readme_lines) + "\n", encoding="utf-8"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    db_path = Path(args.db)
    raw_dir = Path(args.raw_report_dir)
    residual_dir = Path(args.residual_report_dir)
    out_dir = Path(args.output_dir)

    seed_tickers: list[str] = sorted(
        {t.strip() for t in args.seed_tickers.split(",") if t.strip()}
    )
    seed_set: set[str] = set(seed_tickers)

    validate_database(db_path)
    raw_found, res_found = validate_input_dirs(raw_dir, residual_dir)
    instruments = load_instruments(db_path)

    raw_combined, raw_count = load_candidate_edges(raw_found, res_found)
    deduped, skipped_meta = normalize_and_deduplicate_edges(raw_combined, instruments)
    unique_pairs = len(deduped)

    def _no_graph(reason: str) -> None:
        print(f"SUMMARY db={db_path}")
        print(f"SUMMARY raw_report_dir={raw_dir}")
        print(f"SUMMARY residual_report_dir={residual_dir}")
        print(f"SUMMARY output_dir={out_dir}")
        print(f"SUMMARY theme_name={args.theme_name}")
        print(f"SUMMARY seed_tickers_requested={len(seed_tickers)}")
        print(f"SUMMARY raw_candidate_files_found={len(raw_found)}")
        print(f"SUMMARY residual_candidate_files_found={len(res_found)}")
        print(f"SUMMARY candidate_rows_raw={raw_count}")
        print(f"SUMMARY candidate_pairs_unique={unique_pairs}")
        print(f"SUMMARY skipped_pairs_missing_metadata={skipped_meta}")
        print("SUMMARY status=NO_USABLE_GRAPH")
        print(f"SUMMARY reason={reason}")

    if deduped.empty:
        _no_graph("no_valid_pairs_after_deduplication")
        return

    filtered, skipped_threshold = filter_edges(
        deduped, seed_set,
        args.min_raw_correlation, args.min_raw_rolling_mean,
        args.min_residual_correlation, args.min_residual_rolling_mean,
        args.min_combined_score,
    )

    if filtered.empty:
        _no_graph("no_pairs_survived_edge_filtering")
        return

    adj, edge_map = build_graph(filtered, seed_set)

    if not adj:
        _no_graph("empty_graph_after_build")
        return

    out_dir.mkdir(parents=True, exist_ok=True)

    components, node_to_comp, comp_sizes = compute_connected_components(adj)
    distances, seed_reach = compute_seed_distances(adj, seed_set)
    node_df = compute_node_metrics(
        adj, edge_map, node_to_comp, comp_sizes,
        distances, seed_reach, seed_set, instruments,
    )
    comp_df = compute_component_reports(
        components, node_df, edge_map, seed_set, instruments, args.min_component_size
    )
    cliques_raw = find_cliques(
        adj, edge_map, args.min_clique_size, args.max_clique_size, args.max_cliques
    )

    seed_in_graph = sorted(seed_set & set(adj.keys()))

    counts = write_reports(
        out_dir, filtered, node_df, comp_df, cliques_raw, edge_map,
        seed_set, instruments, args.theme_name, args.max_seed_distance,
        args.min_component_size, args.min_clique_size, args.max_cliques,
    )

    comp_count_all = len(components)
    cross_industry_count = sum(
        1 for ed in edge_map.values() if ed.get("cross_industry_edge", False)
    )

    write_markdown_reports(
        out_dir, db_path, raw_dir, residual_dir,
        args.theme_name, seed_tickers, args,
        {**counts, "components_total": comp_count_all, "seed_tickers_found_in_graph": len(seed_in_graph)},
    )

    print(f"SUMMARY db={db_path}")
    print(f"SUMMARY raw_report_dir={raw_dir}")
    print(f"SUMMARY residual_report_dir={residual_dir}")
    print(f"SUMMARY output_dir={out_dir}")
    print(f"SUMMARY theme_name={args.theme_name}")
    print(f"SUMMARY seed_tickers_requested={len(seed_tickers)}")
    print(f"SUMMARY seed_tickers_found_in_graph={len(seed_in_graph)}")
    print(f"SUMMARY raw_candidate_files_found={len(raw_found)}")
    print(f"SUMMARY residual_candidate_files_found={len(res_found)}")
    print(f"SUMMARY candidate_rows_raw={raw_count}")
    print(f"SUMMARY candidate_pairs_unique={unique_pairs}")
    print(f"SUMMARY edges_kept={counts.get('edges_kept', 0)}")
    print(f"SUMMARY nodes_total={counts.get('nodes', 0)}")
    print(f"SUMMARY components_total={comp_count_all}")
    print(f"SUMMARY components_min_size={args.min_component_size}")
    print(f"SUMMARY seed_components={counts.get('seed_components', 0)}")
    print(f"SUMMARY cross_sector_edges={counts.get('cross_sector_edges', 0)}")
    print(f"SUMMARY cross_industry_edges={cross_industry_count}")
    print(f"SUMMARY cliques_found={counts.get('cliques_found', 0)}")
    print(f"SUMMARY cliques_written={counts.get('cliques_written', 0)}")
    print(f"SUMMARY seed_direct_edges={counts.get('seed_direct_edges', 0)}")
    print(f"SUMMARY seed_expansion_rows={counts.get('seed_expansion_rows', 0)}")
    print(f"SUMMARY skipped_pairs_missing_metadata={skipped_meta}")
    print(f"SUMMARY skipped_pairs_below_threshold={skipped_threshold}")
    print("SUMMARY status=OK")


if __name__ == "__main__":
    main()
