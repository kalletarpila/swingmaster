"""
V7: Datacenter network centrality / connectivity analysis.

Reads V6b graph outputs and V6c focused reports and produces deterministic
network centrality reports without rebuilding correlations or querying SQLite.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import pandas as pd


OPERATING_DATACENTER_SUBTHEMES = {
    "SEMICONDUCTORS_AI_CHIPS",
    "SEMICONDUCTOR_EQUIPMENT",
    "SERVER_STORAGE_HARDWARE",
    "NETWORKING_OPTICAL_CONNECTIVITY",
    "ELECTRICAL_POWER_EQUIPMENT",
    "POWER_GENERATION_UTILITIES",
    "DATACENTER_REIT",
    "ENGINEERING_CONSTRUCTION_INFRA",
}

FINANCIAL_OR_FUND_INDUSTRY_TERMS = [
    "asset management",
    "capital markets",
    "banks",
    "credit services",
    "insurance",
]

NON_CORE_CONSUMER_REAL_ESTATE_TERMS = [
    "reit - hotel & motel",
    "auto & truck dealerships",
    "recreational vehicles",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="V7 datacenter network centrality reporting CLI"
    )
    parser.add_argument("--graph-dir", required=True)
    parser.add_argument("--focused-report-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--theme-name", default="datacenter")
    parser.add_argument("--top-n", type=int, default=300)
    parser.add_argument("--min-network-importance-score", type=float, default=0.30)
    parser.add_argument("--min-seed-neighbor-count", type=int, default=2)
    parser.add_argument("--min-core-neighbor-count", type=int, default=3)
    parser.add_argument("--min-neighbor-subtheme-count", type=int, default=2)
    parser.add_argument("--min-cross-sector-degree", type=int, default=2)
    parser.add_argument("--min-clique-count", type=int, default=1)
    return parser.parse_args()


def _to_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _to_float(value: object) -> float:
    if pd.isna(value):
        return np.nan
    try:
        return float(value)
    except (TypeError, ValueError):
        return np.nan


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        return False
    txt = str(value).strip().lower()
    return txt in {"true", "1", "yes", "y", "t"}


def _sorted_join(values: list[object]) -> str:
    cleaned = sorted({
        _to_text(v)
        for v in values
        if _to_text(v)
    })
    return ", ".join(cleaned)


def _industry_contains_any(industry: object, terms: list[str]) -> bool:
    il = _to_text(industry).lower()
    return any(term in il for term in terms)


def normalize_booleans(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    bool_cols = [
        "is_seed",
        "seed_edge",
        "both_seed_edge",
        "cross_sector_edge",
        "cross_industry_edge",
        "ticker_1_is_seed",
        "ticker_2_is_seed",
        "found_in_graph",
    ]
    for col in bool_cols:
        if col in out.columns:
            out[col] = out[col].map(_to_bool)
    return out


def parse_list_column(value: object) -> list[str]:
    if pd.isna(value):
        return []
    parts = [part.strip() for part in str(value).split(",")]
    parts = [part for part in parts if part]
    return sorted(set(parts))


def validate_input_files(graph_dir: Path, focused_report_dir: Path) -> dict[str, Path]:
    if not graph_dir.exists():
        raise SystemExit(f"ERROR: graph directory not found: {graph_dir}")
    if not focused_report_dir.exists():
        raise SystemExit(f"ERROR: focused report directory not found: {focused_report_dir}")

    required = {
        "seed_ecosystem_edges.csv": graph_dir / "seed_ecosystem_edges.csv",
        "seed_ecosystem_nodes.csv": graph_dir / "seed_ecosystem_nodes.csv",
        "seed_ecosystem_cliques_3plus.csv": graph_dir / "seed_ecosystem_cliques_3plus.csv",
        "datacenter_core_validation_shortlist.csv": focused_report_dir / "datacenter_core_validation_shortlist.csv",
        "datacenter_broad_beta_candidates.csv": focused_report_dir / "datacenter_broad_beta_candidates.csv",
        "datacenter_priority_candidates.csv": focused_report_dir / "datacenter_priority_candidates.csv",
    }
    missing = [name for name, path in required.items() if not path.exists()]
    if missing:
        raise SystemExit("ERROR: missing required input files: " + ", ".join(sorted(missing)))

    optional = {
        "datacenter_subtheme_groups.csv": focused_report_dir / "datacenter_subtheme_groups.csv",
        "multi_seed_connected_candidates.csv": focused_report_dir / "multi_seed_connected_candidates.csv",
        "datacenter_cross_sector_bridges_top.csv": focused_report_dir / "datacenter_cross_sector_bridges_top.csv",
        "datacenter_validation_shortlist.csv": focused_report_dir / "datacenter_validation_shortlist.csv",
    }

    file_map: dict[str, Path] = {}
    file_map.update(required)
    for name, path in optional.items():
        if path.exists():
            file_map[name] = path

    return file_map


def load_inputs(file_map: dict[str, Path]) -> dict[str, pd.DataFrame]:
    data: dict[str, pd.DataFrame] = {}
    for name, path in file_map.items():
        if path.suffix.lower() == ".csv":
            data[name] = normalize_booleans(pd.read_csv(path))
    return data


def build_subtheme_map(
    core_df: pd.DataFrame,
    broad_df: pd.DataFrame,
    priority_df: pd.DataFrame,
    nodes_df: pd.DataFrame,
) -> dict[str, str]:
    out: dict[str, str] = {}

    for _, row in priority_df.iterrows():
        ticker = _to_text(row.get("ticker"))
        subtheme = _to_text(row.get("subtheme_guess"))
        if ticker and subtheme:
            out[ticker] = subtheme

    for _, row in broad_df.iterrows():
        ticker = _to_text(row.get("ticker"))
        subtheme = _to_text(row.get("subtheme_guess"))
        if ticker and subtheme:
            out[ticker] = subtheme

    for _, row in core_df.iterrows():
        ticker = _to_text(row.get("ticker"))
        subtheme = _to_text(row.get("subtheme_guess"))
        if ticker and subtheme:
            out[ticker] = subtheme

    for _, row in nodes_df.iterrows():
        ticker = _to_text(row.get("ticker"))
        if not ticker:
            continue
        if ticker not in out:
            if _to_bool(row.get("is_seed")):
                out[ticker] = "SEED_UNCLASSIFIED"
            else:
                out[ticker] = "OTHER_OR_UNCLASSIFIED"

    return out


def build_membership_map(
    nodes_df: pd.DataFrame,
    core_df: pd.DataFrame,
    broad_df: pd.DataFrame,
    priority_df: pd.DataFrame,
) -> dict[str, str]:
    seeds = {
        _to_text(row.get("ticker"))
        for _, row in nodes_df.iterrows()
        if _to_bool(row.get("is_seed")) and _to_text(row.get("ticker"))
    }
    core_set = set(core_df.get("ticker", pd.Series([], dtype=str)).astype(str))
    broad_set = set(broad_df.get("ticker", pd.Series([], dtype=str)).astype(str))
    priority_set = set(priority_df.get("ticker", pd.Series([], dtype=str)).astype(str))

    out: dict[str, str] = {}
    for _, row in nodes_df.iterrows():
        ticker = _to_text(row.get("ticker"))
        if not ticker:
            continue
        if ticker in seeds:
            out[ticker] = "SEED"
        elif ticker in core_set:
            out[ticker] = "CORE_VALIDATION"
        elif ticker in broad_set:
            out[ticker] = "BROAD_BETA"
        elif ticker in priority_set:
            out[ticker] = "PRIORITY_ONLY"
        else:
            out[ticker] = "GRAPH_ONLY"

    return out


def build_adjacency_metrics(
    edges_df: pd.DataFrame,
    tickers: list[str],
) -> dict[str, dict[str, object]]:
    adj: dict[str, list[dict[str, object]]] = {t: [] for t in tickers}

    for _, row in edges_df.iterrows():
        t1 = _to_text(row.get("ticker_1"))
        t2 = _to_text(row.get("ticker_2"))
        if not t1 or not t2:
            continue
        score = _to_float(row.get("combined_score"))
        if pd.isna(score):
            score = 0.0

        rec12 = {
            "neighbor": t2,
            "combined_score": score,
            "cross_sector_edge": _to_bool(row.get("cross_sector_edge")),
            "cross_industry_edge": _to_bool(row.get("cross_industry_edge")),
            "neighbor_sector": _to_text(row.get("sector_2")),
            "neighbor_industry": _to_text(row.get("industry_2")),
        }
        rec21 = {
            "neighbor": t1,
            "combined_score": score,
            "cross_sector_edge": _to_bool(row.get("cross_sector_edge")),
            "cross_industry_edge": _to_bool(row.get("cross_industry_edge")),
            "neighbor_sector": _to_text(row.get("sector_1")),
            "neighbor_industry": _to_text(row.get("industry_1")),
        }

        if t1 not in adj:
            adj[t1] = []
        if t2 not in adj:
            adj[t2] = []
        adj[t1].append(rec12)
        adj[t2].append(rec21)

    out: dict[str, dict[str, object]] = {}
    for ticker in sorted(adj.keys()):
        rows = adj[ticker]
        neighbors = sorted({_to_text(r["neighbor"]) for r in rows if _to_text(r["neighbor"])})
        degree = len(neighbors)
        scores = [float(r["combined_score"]) for r in rows]
        weighted = float(np.sum(scores)) if scores else 0.0
        avg_score = float(np.mean(scores)) if scores else 0.0
        max_score = float(np.max(scores)) if scores else 0.0
        min_score = float(np.min(scores)) if scores else 0.0

        out[ticker] = {
            "neighbors": neighbors,
            "rows": rows,
            "degree_from_edges": degree,
            "weighted_degree_from_edges": weighted,
            "average_edge_score": avg_score,
            "max_edge_score": max_score,
            "min_edge_score": min_score,
        }

    return out


def compute_seed_connectivity(
    adjacency_metrics: dict[str, dict[str, object]],
    seed_set: set[str],
) -> dict[str, dict[str, object]]:
    out: dict[str, dict[str, object]] = {}
    for ticker, rec in adjacency_metrics.items():
        seed_neighbors = []
        seed_scores = []
        for row in rec["rows"]:
            nbr = _to_text(row.get("neighbor"))
            if nbr in seed_set:
                seed_neighbors.append(nbr)
                seed_scores.append(float(row.get("combined_score", 0.0)))

        seed_neighbors = sorted(set(seed_neighbors))
        best_neighbor = ""
        if seed_scores:
            best_idx = int(np.argmax(seed_scores))
            best_neighbor = sorted(set([seed_neighbors[best_idx]]) if seed_neighbors else [""])[0] if seed_neighbors else ""

        out[ticker] = {
            "seed_neighbor_count_from_edges": len(seed_neighbors),
            "seed_neighbors_from_edges": _sorted_join(seed_neighbors),
            "average_seed_edge_score": float(np.mean(seed_scores)) if seed_scores else 0.0,
            "max_seed_edge_score": float(np.max(seed_scores)) if seed_scores else 0.0,
            "best_seed_neighbor_from_edges": best_neighbor,
        }

    return out


def compute_core_connectivity(
    adjacency_metrics: dict[str, dict[str, object]],
    core_set: set[str],
) -> dict[str, dict[str, object]]:
    out: dict[str, dict[str, object]] = {}
    for ticker, rec in adjacency_metrics.items():
        core_neighbors = []
        core_scores = []
        for row in rec["rows"]:
            nbr = _to_text(row.get("neighbor"))
            if nbr in core_set:
                core_neighbors.append(nbr)
                core_scores.append(float(row.get("combined_score", 0.0)))

        core_neighbors = sorted(set(core_neighbors))
        best_neighbor = ""
        if core_scores and core_neighbors:
            best_pair = sorted(
                [(n, s) for n, s in zip(core_neighbors, core_scores)],
                key=lambda x: (-x[1], x[0]),
            )
            best_neighbor = best_pair[0][0]

        out[ticker] = {
            "core_neighbor_count": len(core_neighbors),
            "core_neighbors": _sorted_join(core_neighbors),
            "average_core_edge_score": float(np.mean(core_scores)) if core_scores else 0.0,
            "max_core_edge_score": float(np.max(core_scores)) if core_scores else 0.0,
            "best_core_neighbor": best_neighbor,
        }

    return out


def compute_subtheme_connectivity(
    adjacency_metrics: dict[str, dict[str, object]],
    subtheme_map: dict[str, str],
) -> dict[str, dict[str, object]]:
    out: dict[str, dict[str, object]] = {}
    for ticker, rec in adjacency_metrics.items():
        nbr_subthemes = []
        for row in rec["rows"]:
            nbr = _to_text(row.get("neighbor"))
            st = _to_text(subtheme_map.get(nbr, ""))
            if st:
                nbr_subthemes.append(st)

        unique_subthemes = sorted(set(nbr_subthemes))
        counter = Counter(nbr_subthemes)
        dominant_subtheme = ""
        dominant_count = 0
        if counter:
            dominant_subtheme, dominant_count = sorted(
                counter.items(), key=lambda kv: (-kv[1], kv[0])
            )[0]

        out[ticker] = {
            "neighbor_subtheme_count": len(unique_subthemes),
            "neighbor_subthemes": _sorted_join(unique_subthemes),
            "dominant_neighbor_subtheme": dominant_subtheme,
            "dominant_neighbor_subtheme_count": int(dominant_count),
        }

    return out


def compute_sector_industry_connectivity(
    adjacency_metrics: dict[str, dict[str, object]],
    nodes_df: pd.DataFrame,
) -> dict[str, dict[str, object]]:
    out: dict[str, dict[str, object]] = {}

    node_index = nodes_df.set_index("ticker") if "ticker" in nodes_df.columns else pd.DataFrame()
    has_node_cross = (
        "cross_sector_degree" in nodes_df.columns and "cross_industry_degree" in nodes_df.columns
    )

    for ticker, rec in adjacency_metrics.items():
        sectors = sorted({
            _to_text(r.get("neighbor_sector"))
            for r in rec["rows"]
            if _to_text(r.get("neighbor_sector"))
        })
        industries = sorted({
            _to_text(r.get("neighbor_industry"))
            for r in rec["rows"]
            if _to_text(r.get("neighbor_industry"))
        })

        computed_cross_sector = int(sum(1 for r in rec["rows"] if _to_bool(r.get("cross_sector_edge"))))
        computed_cross_industry = int(sum(1 for r in rec["rows"] if _to_bool(r.get("cross_industry_edge"))))

        cross_sector_degree = computed_cross_sector
        cross_industry_degree = computed_cross_industry
        if has_node_cross and ticker in node_index.index:
            node_cross_sector = _to_float(node_index.loc[ticker].get("cross_sector_degree"))
            node_cross_industry = _to_float(node_index.loc[ticker].get("cross_industry_degree"))
            if not pd.isna(node_cross_sector):
                cross_sector_degree = int(node_cross_sector)
            if not pd.isna(node_cross_industry):
                cross_industry_degree = int(node_cross_industry)

        out[ticker] = {
            "connected_sector_count": len(sectors),
            "connected_sectors": _sorted_join(sectors),
            "connected_industry_count": len(industries),
            "connected_industries": _sorted_join(industries),
            "cross_sector_degree": int(cross_sector_degree),
            "cross_industry_degree": int(cross_industry_degree),
        }

    return out


def compute_clique_metrics(
    cliques_df: pd.DataFrame,
    tickers: list[str],
) -> dict[str, dict[str, object]]:
    out = {
        t: {
            "clique_count": 0,
            "max_clique_size": 0,
            "average_clique_score": 0.0,
            "seed_clique_count": 0,
            "cross_sector_clique_count": 0,
            "cross_industry_clique_count": 0,
        }
        for t in tickers
    }

    if cliques_df.empty:
        return out

    scores_by_ticker: dict[str, list[float]] = defaultdict(list)
    for _, row in cliques_df.iterrows():
        clique_tickers = parse_list_column(row.get("tickers"))
        if not clique_tickers:
            continue
        clique_size = int(_to_float(row.get("clique_size")) if not pd.isna(_to_float(row.get("clique_size"))) else len(clique_tickers))
        avg_score = _to_float(row.get("average_combined_score"))
        if pd.isna(avg_score):
            avg_score = 0.0
        seed_count = int(_to_float(row.get("seed_count")) if not pd.isna(_to_float(row.get("seed_count"))) else 0)
        sector_count = int(_to_float(row.get("sector_count")) if not pd.isna(_to_float(row.get("sector_count"))) else 0)
        industry_count = int(_to_float(row.get("industry_count")) if not pd.isna(_to_float(row.get("industry_count"))) else 0)

        for t in clique_tickers:
            if t not in out:
                out[t] = {
                    "clique_count": 0,
                    "max_clique_size": 0,
                    "average_clique_score": 0.0,
                    "seed_clique_count": 0,
                    "cross_sector_clique_count": 0,
                    "cross_industry_clique_count": 0,
                }
            out[t]["clique_count"] += 1
            out[t]["max_clique_size"] = max(out[t]["max_clique_size"], clique_size)
            if seed_count > 0:
                out[t]["seed_clique_count"] += 1
            if sector_count > 1:
                out[t]["cross_sector_clique_count"] += 1
            if industry_count > 1:
                out[t]["cross_industry_clique_count"] += 1
            scores_by_ticker[t].append(float(avg_score))

    for t, scores in scores_by_ticker.items():
        out[t]["average_clique_score"] = float(np.mean(scores)) if scores else 0.0

    return out


def compute_bridge_scores(node_scores: pd.DataFrame) -> pd.DataFrame:
    out = node_scores.copy()

    norm_neighbor_subtheme_count = (out["neighbor_subtheme_count"].fillna(0.0).astype(float) / 5.0).clip(upper=1.0)
    norm_average_edge_score = out["average_edge_score"].fillna(0.0).astype(float).clip(lower=0.0, upper=1.0)
    norm_cross_sector_degree = (out["cross_sector_degree"].fillna(0.0).astype(float) / 25.0).clip(upper=1.0)
    norm_seed_neighbor_count = (out["seed_neighbor_count_from_edges"].fillna(0.0).astype(float) / 10.0).clip(upper=1.0)
    norm_core_neighbor_count = (out["core_neighbor_count"].fillna(0.0).astype(float) / 20.0).clip(upper=1.0)

    out["cross_subtheme_bridge_score"] = norm_neighbor_subtheme_count * norm_average_edge_score
    out["cross_sector_bridge_score"] = norm_cross_sector_degree * norm_average_edge_score
    out["seed_to_core_bridge_score"] = (
        0.40 * norm_seed_neighbor_count
        + 0.40 * norm_core_neighbor_count
        + 0.20 * norm_cross_sector_degree
    )

    return out


def compute_network_importance_scores(node_scores: pd.DataFrame) -> pd.DataFrame:
    out = node_scores.copy()

    n_weighted = (out["weighted_degree_from_edges"].fillna(0.0).astype(float) / 50.0).clip(upper=1.0)
    n_seed_neighbors = (out["seed_neighbor_count_from_edges"].fillna(0.0).astype(float) / 10.0).clip(upper=1.0)
    n_core_neighbors = (out["core_neighbor_count"].fillna(0.0).astype(float) / 20.0).clip(upper=1.0)
    n_cross_sector = (out["cross_sector_degree"].fillna(0.0).astype(float) / 25.0).clip(upper=1.0)
    n_neighbor_subthemes = (out["neighbor_subtheme_count"].fillna(0.0).astype(float) / 5.0).clip(upper=1.0)
    n_cliques = (out["clique_count"].fillna(0.0).astype(float) / 10.0).clip(upper=1.0)
    n_avg_edge = out["average_edge_score"].fillna(0.0).astype(float).clip(lower=0.0, upper=1.0)

    out["network_importance_score"] = (
        0.20 * n_weighted
        + 0.20 * n_seed_neighbors
        + 0.15 * n_core_neighbors
        + 0.15 * n_cross_sector
        + 0.10 * n_neighbor_subthemes
        + 0.10 * n_cliques
        + 0.10 * n_avg_edge
    )

    out["network_importance_tier"] = np.where(
        out["network_importance_score"] >= 0.70,
        "HIGH",
        np.where(out["network_importance_score"] >= 0.45, "MEDIUM", "LOW"),
    )

    return out


def classify_network_role(row: pd.Series, args: argparse.Namespace) -> str:
    is_seed = _to_bool(row.get("is_seed"))
    membership = _to_text(row.get("ecosystem_membership_class"))
    score = _to_float(row.get("network_importance_score"))
    if pd.isna(score):
        score = 0.0

    seed_neighbors = int(_to_float(row.get("seed_neighbor_count_from_edges")) if not pd.isna(_to_float(row.get("seed_neighbor_count_from_edges"))) else 0)
    core_neighbors = int(_to_float(row.get("core_neighbor_count")) if not pd.isna(_to_float(row.get("core_neighbor_count"))) else 0)
    neighbor_subthemes = int(_to_float(row.get("neighbor_subtheme_count")) if not pd.isna(_to_float(row.get("neighbor_subtheme_count"))) else 0)
    cross_sector = int(_to_float(row.get("cross_sector_degree")) if not pd.isna(_to_float(row.get("cross_sector_degree"))) else 0)

    if is_seed:
        return "SEED_ANCHOR"

    if (
        membership == "CORE_VALIDATION"
        and core_neighbors >= args.min_core_neighbor_count
        and score >= args.min_network_importance_score
    ):
        return "CORE_CONNECTOR"

    if (
        seed_neighbors >= args.min_seed_neighbor_count
        and score >= args.min_network_importance_score
    ):
        return "MULTI_SEED_CONNECTOR"

    if (
        neighbor_subthemes >= args.min_neighbor_subtheme_count
        and score >= args.min_network_importance_score
    ):
        return "CROSS_SUBTHEME_BRIDGE"

    if (
        cross_sector >= args.min_cross_sector_degree
        and score >= args.min_network_importance_score
    ):
        return "CROSS_SECTOR_BRIDGE"

    if (
        membership == "CORE_VALIDATION"
        and neighbor_subthemes <= 2
        and score >= args.min_network_importance_score
    ):
        return "SUBTHEME_SPECIALIST"

    if membership == "BROAD_BETA":
        return "BROAD_BETA_NODE"

    return "PERIPHERAL_NODE"


def is_operating_datacenter_candidate(row: pd.Series) -> bool:
    return (
        (not _to_bool(row.get("is_seed")))
        and (_to_text(row.get("ecosystem_membership_class")) == "CORE_VALIDATION")
        and (_to_text(row.get("sector")) != "Financial Services")
        and (_to_text(row.get("subtheme_guess")) in OPERATING_DATACENTER_SUBTHEMES)
    )


def is_broad_beta_or_financial_candidate(row: pd.Series) -> bool:
    membership = _to_text(row.get("ecosystem_membership_class"))
    sector = _to_text(row.get("sector"))
    subtheme = _to_text(row.get("subtheme_guess"))
    industry = _to_text(row.get("industry"))

    return (
        (membership == "BROAD_BETA")
        or (sector == "Financial Services")
        or (subtheme in {"OTHER_OR_UNCLASSIFIED", "BROAD_TECH_OR_INDUSTRIAL_BETA", "SOFTWARE_PLATFORM_ADJACENT"})
        or _industry_contains_any(industry, FINANCIAL_OR_FUND_INDUSTRY_TERMS + NON_CORE_CONSUMER_REAL_ESTATE_TERMS)
    )


def build_seed_anchor_reason(row: pd.Series, args: argparse.Namespace) -> str:
    reasons = {"SEED"}
    if _to_text(row.get("network_importance_tier")) == "HIGH":
        reasons.add("HIGH_NETWORK_IMPORTANCE")
    if _to_float(row.get("seed_neighbor_count_from_edges")) >= float(args.min_seed_neighbor_count):
        reasons.add("MULTI_SEED_CONNECTED")
    if _to_float(row.get("core_neighbor_count")) >= float(args.min_core_neighbor_count):
        reasons.add("CORE_CONNECTED")
    if _to_float(row.get("neighbor_subtheme_count")) >= float(args.min_neighbor_subtheme_count):
        reasons.add("CROSS_SUBTHEME_BRIDGE")
    if _to_float(row.get("cross_sector_degree")) >= float(args.min_cross_sector_degree):
        reasons.add("CROSS_SECTOR_BRIDGE")
    if _to_float(row.get("clique_count")) >= float(args.min_clique_count):
        reasons.add("CLIQUE_PARTICIPANT")
    return _sorted_join(sorted(reasons))


def build_operating_connector_reason(row: pd.Series, args: argparse.Namespace) -> str:
    reasons = {"CORE_VALIDATION", "OPERATING_DATACENTER_SUBTHEME"}
    if _to_text(row.get("network_importance_tier")) == "HIGH":
        reasons.add("HIGH_NETWORK_IMPORTANCE")
    if _to_float(row.get("seed_neighbor_count_from_edges")) >= float(args.min_seed_neighbor_count):
        reasons.add("MULTI_SEED_CONNECTED")
    if _to_float(row.get("core_neighbor_count")) >= float(args.min_core_neighbor_count):
        reasons.add("CORE_CONNECTED")
    if _to_float(row.get("neighbor_subtheme_count")) >= float(args.min_neighbor_subtheme_count):
        reasons.add("CROSS_SUBTHEME_BRIDGE")
    if _to_float(row.get("cross_sector_degree")) >= float(args.min_cross_sector_degree):
        reasons.add("CROSS_SECTOR_BRIDGE")
    if _to_float(row.get("clique_count")) >= float(args.min_clique_count):
        reasons.add("CLIQUE_PARTICIPANT")
    return _sorted_join(sorted(reasons))


def build_broad_beta_network_reason(row: pd.Series, args: argparse.Namespace) -> str:
    reasons = set()
    membership = _to_text(row.get("ecosystem_membership_class"))
    sector = _to_text(row.get("sector"))
    subtheme = _to_text(row.get("subtheme_guess"))
    industry = _to_text(row.get("industry"))

    if membership == "BROAD_BETA":
        reasons.add("BROAD_BETA_MEMBERSHIP")
    if sector == "Financial Services":
        reasons.add("FINANCIAL_SERVICES")
    if subtheme == "OTHER_OR_UNCLASSIFIED":
        reasons.add("OTHER_OR_UNCLASSIFIED")
    if subtheme == "BROAD_TECH_OR_INDUSTRIAL_BETA":
        reasons.add("BROAD_TECH_OR_INDUSTRIAL_BETA")
    if subtheme == "SOFTWARE_PLATFORM_ADJACENT":
        reasons.add("SOFTWARE_PLATFORM_ADJACENT")
    if _industry_contains_any(industry, FINANCIAL_OR_FUND_INDUSTRY_TERMS):
        reasons.add("FINANCIAL_OR_FUND_INDUSTRY")
    if _industry_contains_any(industry, NON_CORE_CONSUMER_REAL_ESTATE_TERMS):
        reasons.add("NON_CORE_CONSUMER_OR_REAL_ESTATE")
    if _to_text(row.get("network_importance_tier")) == "HIGH":
        reasons.add("HIGH_NETWORK_IMPORTANCE")
    if _to_float(row.get("seed_neighbor_count_from_edges")) >= float(args.min_seed_neighbor_count):
        reasons.add("MULTI_SEED_CONNECTED")
    if _to_float(row.get("core_neighbor_count")) >= float(args.min_core_neighbor_count):
        reasons.add("CORE_CONNECTED")
    if _to_float(row.get("cross_sector_degree")) >= float(args.min_cross_sector_degree):
        reasons.add("CROSS_SECTOR_BRIDGE")
    return _sorted_join(sorted(reasons))


def write_seed_anchor_report(
    node_scores: pd.DataFrame,
    args: argparse.Namespace,
    output_dir: Path,
) -> pd.DataFrame:
    out = node_scores[node_scores["is_seed"].map(_to_bool)].copy()
    out["seed_anchor_reason"] = out.apply(lambda r: build_seed_anchor_reason(r, args), axis=1)
    out = out.sort_values(
        by=[
            "network_importance_score",
            "weighted_degree_from_edges",
            "seed_neighbor_count_from_edges",
            "core_neighbor_count",
            "ticker",
        ],
        ascending=[False, False, False, False, True],
        kind="mergesort",
    ).head(args.top_n)
    out.to_csv(output_dir / "datacenter_top_seed_anchors.csv", index=False)
    return out


def write_operating_non_seed_connector_report(
    node_scores: pd.DataFrame,
    args: argparse.Namespace,
    output_dir: Path,
) -> pd.DataFrame:
    out = node_scores[node_scores.apply(is_operating_datacenter_candidate, axis=1)].copy()
    out = out[out["network_importance_score"] >= args.min_network_importance_score].copy()
    out["operating_connector_reason"] = out.apply(lambda r: build_operating_connector_reason(r, args), axis=1)
    out = out.sort_values(
        by=[
            "network_importance_score",
            "seed_neighbor_count_from_edges",
            "core_neighbor_count",
            "weighted_degree_from_edges",
            "ticker",
        ],
        ascending=[False, False, False, False, True],
        kind="mergesort",
    ).head(args.top_n)
    out.to_csv(output_dir / "datacenter_top_operating_non_seed_connectors.csv", index=False)
    return out


def write_broad_beta_network_connector_report(
    node_scores: pd.DataFrame,
    args: argparse.Namespace,
    output_dir: Path,
) -> pd.DataFrame:
    out = node_scores[node_scores.apply(is_broad_beta_or_financial_candidate, axis=1)].copy()
    out = out[out["network_importance_score"] >= args.min_network_importance_score].copy()
    out["broad_beta_network_reason"] = out.apply(lambda r: build_broad_beta_network_reason(r, args), axis=1)
    out = out.sort_values(
        by=[
            "network_importance_score",
            "weighted_degree_from_edges",
            "seed_neighbor_count_from_edges",
            "ticker",
        ],
        ascending=[False, False, False, True],
        kind="mergesort",
    ).head(args.top_n)
    out.to_csv(output_dir / "datacenter_broad_beta_network_connectors.csv", index=False)
    return out


def write_operating_connector_by_subtheme(
    operating_connectors: pd.DataFrame,
    theme_name: str,
    output_dir: Path,
) -> pd.DataFrame:
    rows = []
    for subtheme, grp in operating_connectors.groupby("subtheme_guess", sort=True):
        g = grp.copy()
        top_operating = g.sort_values(
            by=["network_importance_score", "ticker"],
            ascending=[False, True],
            kind="mergesort",
        ).head(15)
        top_multi_seed = g.sort_values(
            by=["seed_neighbor_count_from_edges", "network_importance_score", "ticker"],
            ascending=[False, False, True],
            kind="mergesort",
        ).head(15)
        top_core_connected = g.sort_values(
            by=["core_neighbor_count", "network_importance_score", "ticker"],
            ascending=[False, False, True],
            kind="mergesort",
        ).head(15)

        rows.append(
            {
                "theme_name": theme_name,
                "subtheme_guess": subtheme,
                "ticker_count": int(len(g)),
                "high_network_importance_count": int((g["network_importance_tier"] == "HIGH").sum()),
                "medium_network_importance_count": int((g["network_importance_tier"] == "MEDIUM").sum()),
                "low_network_importance_count": int((g["network_importance_tier"] == "LOW").sum()),
                "average_network_importance_score": float(g["network_importance_score"].mean()) if not g.empty else 0.0,
                "median_network_importance_score": float(g["network_importance_score"].median()) if not g.empty else 0.0,
                "average_seed_neighbor_count": float(g["seed_neighbor_count_from_edges"].mean()) if not g.empty else 0.0,
                "average_core_neighbor_count": float(g["core_neighbor_count"].mean()) if not g.empty else 0.0,
                "average_cross_sector_degree": float(g["cross_sector_degree"].mean()) if not g.empty else 0.0,
                "top_operating_connectors": _sorted_join(top_operating["ticker"].tolist()),
                "top_multi_seed_operating_connectors": _sorted_join(top_multi_seed["ticker"].tolist()),
                "top_core_connected_operating_connectors": _sorted_join(top_core_connected["ticker"].tolist()),
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        out = pd.DataFrame(
            columns=[
                "theme_name",
                "subtheme_guess",
                "ticker_count",
                "high_network_importance_count",
                "medium_network_importance_count",
                "low_network_importance_count",
                "average_network_importance_score",
                "median_network_importance_score",
                "average_seed_neighbor_count",
                "average_core_neighbor_count",
                "average_cross_sector_degree",
                "top_operating_connectors",
                "top_multi_seed_operating_connectors",
                "top_core_connected_operating_connectors",
            ]
        )
    out = out.sort_values(
        by=["average_network_importance_score", "ticker_count", "subtheme_guess"],
        ascending=[False, False, True],
        kind="mergesort",
    )
    out.to_csv(output_dir / "datacenter_operating_connector_by_subtheme.csv", index=False)
    return out


def write_final_manual_review_queue(
    node_scores: pd.DataFrame,
    args: argparse.Namespace,
    output_dir: Path,
) -> pd.DataFrame:
    df = node_scores.copy()

    seed_anchor = df["is_seed"].map(_to_bool)
    operating = df.apply(is_operating_datacenter_candidate, axis=1) & (df["network_importance_score"] >= args.min_network_importance_score)
    high_signal_broad = (
        df.apply(is_broad_beta_or_financial_candidate, axis=1)
        & (df["network_importance_score"] >= 0.70)
        & (df["seed_neighbor_count_from_edges"] >= args.min_seed_neighbor_count)
        & (df["core_neighbor_count"] >= args.min_core_neighbor_count)
    )

    out = df[seed_anchor | operating | high_signal_broad].copy()

    def _bucket(row: pd.Series) -> str:
        if _to_bool(row.get("is_seed")):
            return "SEED_ANCHOR"
        if is_operating_datacenter_candidate(row):
            return "OPERATING_NON_SEED_CONNECTOR"
        if (
            is_broad_beta_or_financial_candidate(row)
            and _to_float(row.get("network_importance_score")) >= 0.70
            and _to_float(row.get("seed_neighbor_count_from_edges")) >= float(args.min_seed_neighbor_count)
            and _to_float(row.get("core_neighbor_count")) >= float(args.min_core_neighbor_count)
        ):
            return "HIGH_SIGNAL_BROAD_BETA"
        return "OTHER_INCLUDED"

    def _reason(row: pd.Series) -> str:
        reasons = set()
        if _to_bool(row.get("is_seed")):
            reasons.add("SEED")
        if is_operating_datacenter_candidate(row):
            reasons.add("OPERATING_DATACENTER_CANDIDATE")
        if (
            is_broad_beta_or_financial_candidate(row)
            and _to_float(row.get("network_importance_score")) >= 0.70
            and _to_float(row.get("seed_neighbor_count_from_edges")) >= float(args.min_seed_neighbor_count)
            and _to_float(row.get("core_neighbor_count")) >= float(args.min_core_neighbor_count)
        ):
            reasons.add("HIGH_SIGNAL_BROAD_BETA")
        if _to_text(row.get("network_importance_tier")) == "HIGH":
            reasons.add("HIGH_NETWORK_IMPORTANCE")
        if _to_float(row.get("seed_neighbor_count_from_edges")) >= float(args.min_seed_neighbor_count):
            reasons.add("MULTI_SEED_CONNECTED")
        if _to_float(row.get("core_neighbor_count")) >= float(args.min_core_neighbor_count):
            reasons.add("CORE_CONNECTED")
        if _to_float(row.get("neighbor_subtheme_count")) >= float(args.min_neighbor_subtheme_count):
            reasons.add("CROSS_SUBTHEME_BRIDGE")
        if _to_float(row.get("cross_sector_degree")) >= float(args.min_cross_sector_degree):
            reasons.add("CROSS_SECTOR_BRIDGE")
        if _to_float(row.get("clique_count")) >= float(args.min_clique_count):
            reasons.add("CLIQUE_PARTICIPANT")
        return _sorted_join(sorted(reasons))

    out["manual_review_bucket"] = out.apply(_bucket, axis=1)
    out["manual_review_reason"] = out.apply(_reason, axis=1)
    bucket_order = {
        "SEED_ANCHOR": 0,
        "OPERATING_NON_SEED_CONNECTOR": 1,
        "HIGH_SIGNAL_BROAD_BETA": 2,
        "OTHER_INCLUDED": 3,
    }
    out["_bucket_rank"] = out["manual_review_bucket"].map(lambda x: bucket_order.get(_to_text(x), 9))
    out = out.sort_values(
        by=[
            "_bucket_rank",
            "network_importance_score",
            "seed_neighbor_count_from_edges",
            "core_neighbor_count",
            "ticker",
        ],
        ascending=[True, False, False, False, True],
        kind="mergesort",
    ).drop(columns=["_bucket_rank"]).head(args.top_n)
    out.to_csv(output_dir / "datacenter_final_manual_review_queue.csv", index=False)
    return out


def _markdown_table(df: pd.DataFrame, columns: list[str], top_n: int = 10) -> str:
    if df.empty:
        return "(no rows)"
    view = df[columns].head(top_n).copy()
    lines = []
    lines.append("| " + " | ".join(columns) + " |")
    lines.append("| " + " | ".join(["---"] * len(columns)) + " |")
    for _, row in view.iterrows():
        cells = []
        for c in columns:
            v = row.get(c)
            if isinstance(v, float) and not pd.isna(v):
                txt = str(v)
            else:
                txt = _to_text(v)
            cells.append(txt.replace("|", "\\|"))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def write_node_scores(node_scores: pd.DataFrame, output_dir: Path) -> pd.DataFrame:
    out = node_scores.sort_values(
        by=[
            "network_importance_score",
            "weighted_degree_from_edges",
            "seed_neighbor_count_from_edges",
            "ticker",
        ],
        ascending=[False, False, False, True],
        kind="mergesort",
    )
    out.to_csv(output_dir / "datacenter_network_node_scores.csv", index=False)
    return out


def write_top_reports(
    node_scores: pd.DataFrame,
    theme_name: str,
    args: argparse.Namespace,
    output_dir: Path,
) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}

    top_connectors = node_scores[
        node_scores["network_importance_score"] >= args.min_network_importance_score
    ].copy()
    top_connectors = top_connectors.sort_values(
        by=["network_importance_score", "weighted_degree_from_edges", "ticker"],
        ascending=[False, False, True],
        kind="mergesort",
    ).head(args.top_n)
    top_connectors.to_csv(output_dir / "datacenter_top_network_connectors.csv", index=False)
    out["top_network_connectors"] = top_connectors

    top_non_seed = node_scores[
        (~node_scores["is_seed"].map(_to_bool))
        & (node_scores["network_importance_score"] >= args.min_network_importance_score)
    ].copy()
    top_non_seed = top_non_seed.sort_values(
        by=[
            "network_importance_score",
            "seed_neighbor_count_from_edges",
            "core_neighbor_count",
            "ticker",
        ],
        ascending=[False, False, False, True],
        kind="mergesort",
    ).head(args.top_n)
    top_non_seed.to_csv(output_dir / "datacenter_top_non_seed_connectors.csv", index=False)
    out["top_non_seed_connectors"] = top_non_seed

    multi_seed = node_scores[
        (~node_scores["is_seed"].map(_to_bool))
        & (node_scores["seed_neighbor_count_from_edges"] >= args.min_seed_neighbor_count)
    ].copy()
    multi_seed_cols = [
        "theme_name",
        "ticker",
        "sector",
        "industry",
        "subtheme_guess",
        "ecosystem_membership_class",
        "seed_neighbor_count_from_edges",
        "seed_neighbors_from_edges",
        "average_seed_edge_score",
        "max_seed_edge_score",
        "best_seed_neighbor_from_edges",
        "core_neighbor_count",
        "weighted_degree_from_edges",
        "average_edge_score",
        "network_importance_score",
        "network_importance_tier",
        "network_role",
    ]
    multi_seed = multi_seed.sort_values(
        by=[
            "seed_neighbor_count_from_edges",
            "average_seed_edge_score",
            "network_importance_score",
            "ticker",
        ],
        ascending=[False, False, False, True],
        kind="mergesort",
    )
    multi_seed = multi_seed[multi_seed_cols]
    multi_seed.to_csv(output_dir / "datacenter_multi_seed_connectors.csv", index=False)
    out["multi_seed_connectors"] = multi_seed

    cross_subtheme = node_scores[
        (node_scores["neighbor_subtheme_count"] >= args.min_neighbor_subtheme_count)
        & (node_scores["network_importance_score"] >= args.min_network_importance_score)
    ].copy()
    cross_subtheme_cols = [
        "theme_name",
        "ticker",
        "sector",
        "industry",
        "is_seed",
        "subtheme_guess",
        "ecosystem_membership_class",
        "neighbor_subtheme_count",
        "neighbor_subthemes",
        "dominant_neighbor_subtheme",
        "dominant_neighbor_subtheme_count",
        "cross_subtheme_bridge_score",
        "cross_sector_degree",
        "cross_industry_degree",
        "seed_neighbor_count_from_edges",
        "core_neighbor_count",
        "network_importance_score",
        "network_role",
    ]
    cross_subtheme = cross_subtheme.sort_values(
        by=[
            "cross_subtheme_bridge_score",
            "neighbor_subtheme_count",
            "network_importance_score",
            "ticker",
        ],
        ascending=[False, False, False, True],
        kind="mergesort",
    ).head(args.top_n)
    cross_subtheme = cross_subtheme[cross_subtheme_cols]
    cross_subtheme.to_csv(output_dir / "datacenter_cross_subtheme_bridges.csv", index=False)
    out["cross_subtheme_bridges"] = cross_subtheme

    cross_sector = node_scores[
        (node_scores["cross_sector_degree"] >= args.min_cross_sector_degree)
        & (node_scores["network_importance_score"] >= args.min_network_importance_score)
    ].copy()
    cross_sector_cols = [
        "theme_name",
        "ticker",
        "sector",
        "industry",
        "is_seed",
        "subtheme_guess",
        "ecosystem_membership_class",
        "cross_sector_degree",
        "cross_industry_degree",
        "connected_sector_count",
        "connected_sectors",
        "connected_industry_count",
        "connected_industries",
        "cross_sector_bridge_score",
        "seed_neighbor_count_from_edges",
        "core_neighbor_count",
        "network_importance_score",
        "network_role",
    ]
    cross_sector = cross_sector.sort_values(
        by=[
            "cross_sector_degree",
            "cross_sector_bridge_score",
            "network_importance_score",
            "ticker",
        ],
        ascending=[False, False, False, True],
        kind="mergesort",
    ).head(args.top_n)
    cross_sector = cross_sector[cross_sector_cols]
    cross_sector.to_csv(output_dir / "datacenter_cross_sector_network_bridges.csv", index=False)
    out["cross_sector_network_bridges"] = cross_sector

    clique_connectors = node_scores[
        node_scores["clique_count"] >= args.min_clique_count
    ].copy()
    clique_cols = [
        "theme_name",
        "ticker",
        "sector",
        "industry",
        "is_seed",
        "subtheme_guess",
        "ecosystem_membership_class",
        "clique_count",
        "max_clique_size",
        "average_clique_score",
        "seed_clique_count",
        "cross_sector_clique_count",
        "cross_industry_clique_count",
        "network_importance_score",
        "network_role",
    ]
    clique_connectors = clique_connectors.sort_values(
        by=[
            "clique_count",
            "average_clique_score",
            "network_importance_score",
            "ticker",
        ],
        ascending=[False, False, False, True],
        kind="mergesort",
    ).head(args.top_n)
    clique_connectors = clique_connectors[clique_cols]
    clique_connectors.to_csv(output_dir / "datacenter_clique_connectors.csv", index=False)
    out["clique_connectors"] = clique_connectors

    return out


def write_subtheme_summary(
    node_scores: pd.DataFrame,
    theme_name: str,
    output_dir: Path,
) -> pd.DataFrame:
    rows = []
    for subtheme, grp in node_scores.groupby("subtheme_guess", sort=True):
        g = grp.copy()
        top_connectors = g.sort_values(
            by=["network_importance_score", "ticker"],
            ascending=[False, True],
            kind="mergesort",
        ).head(15)
        top_non_seed = g[~g["is_seed"].map(_to_bool)].sort_values(
            by=["network_importance_score", "ticker"],
            ascending=[False, True],
            kind="mergesort",
        ).head(15)
        top_multi_seed = g.sort_values(
            by=["seed_neighbor_count_from_edges", "network_importance_score", "ticker"],
            ascending=[False, False, True],
            kind="mergesort",
        ).head(15)

        connected_subthemes_set = set()
        for value in g["neighbor_subthemes"].tolist():
            connected_subthemes_set.update(parse_list_column(value))

        rows.append(
            {
                "theme_name": theme_name,
                "subtheme_guess": subtheme,
                "ticker_count": int(len(g)),
                "seed_count": int(g["is_seed"].map(_to_bool).sum()),
                "non_seed_count": int((~g["is_seed"].map(_to_bool)).sum()),
                "core_validation_count": int((g["ecosystem_membership_class"] == "CORE_VALIDATION").sum()),
                "broad_beta_count": int((g["ecosystem_membership_class"] == "BROAD_BETA").sum()),
                "average_network_importance_score": float(g["network_importance_score"].mean()) if not g.empty else 0.0,
                "median_network_importance_score": float(g["network_importance_score"].median()) if not g.empty else 0.0,
                "average_weighted_degree": float(g["weighted_degree_from_edges"].mean()) if not g.empty else 0.0,
                "average_seed_neighbor_count": float(g["seed_neighbor_count_from_edges"].mean()) if not g.empty else 0.0,
                "average_core_neighbor_count": float(g["core_neighbor_count"].mean()) if not g.empty else 0.0,
                "average_cross_sector_degree": float(g["cross_sector_degree"].mean()) if not g.empty else 0.0,
                "top_connectors": _sorted_join(top_connectors["ticker"].tolist()),
                "top_non_seed_connectors": _sorted_join(top_non_seed["ticker"].tolist()),
                "top_multi_seed_connectors": _sorted_join(top_multi_seed["ticker"].tolist()),
                "connected_subthemes": _sorted_join(sorted(connected_subthemes_set)),
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        out = pd.DataFrame(
            columns=[
                "theme_name",
                "subtheme_guess",
                "ticker_count",
                "seed_count",
                "non_seed_count",
                "core_validation_count",
                "broad_beta_count",
                "average_network_importance_score",
                "median_network_importance_score",
                "average_weighted_degree",
                "average_seed_neighbor_count",
                "average_core_neighbor_count",
                "average_cross_sector_degree",
                "top_connectors",
                "top_non_seed_connectors",
                "top_multi_seed_connectors",
                "connected_subthemes",
            ]
        )

    out = out.sort_values(
        by=["average_network_importance_score", "ticker_count", "subtheme_guess"],
        ascending=[False, False, True],
        kind="mergesort",
    )
    out.to_csv(output_dir / "datacenter_subtheme_network_summary.csv", index=False)
    return out


def write_filtered_edges(
    edges_df: pd.DataFrame,
    node_scores: pd.DataFrame,
    top_n: int,
    output_dir: Path,
) -> pd.DataFrame:
    score_map = dict(zip(node_scores["ticker"], node_scores["network_importance_score"]))
    role_map = dict(zip(node_scores["ticker"], node_scores["network_role"]))
    subtheme_map = dict(zip(node_scores["ticker"], node_scores["subtheme_guess"]))

    top_tickers = set(
        node_scores.sort_values(
            by=["network_importance_score", "ticker"],
            ascending=[False, True],
            kind="mergesort",
        ).head(top_n)["ticker"].tolist()
    )

    df = edges_df.copy()
    df["ticker_1_network_importance_score"] = df["ticker_1"].map(lambda t: _to_float(score_map.get(_to_text(t), 0.0)))
    df["ticker_2_network_importance_score"] = df["ticker_2"].map(lambda t: _to_float(score_map.get(_to_text(t), 0.0)))
    df["ticker_1_network_role"] = df["ticker_1"].map(lambda t: _to_text(role_map.get(_to_text(t))))
    df["ticker_2_network_role"] = df["ticker_2"].map(lambda t: _to_text(role_map.get(_to_text(t))))
    df["ticker_1_subtheme_guess"] = df["ticker_1"].map(lambda t: _to_text(subtheme_map.get(_to_text(t))))
    df["ticker_2_subtheme_guess"] = df["ticker_2"].map(lambda t: _to_text(subtheme_map.get(_to_text(t))))

    score = df["combined_score"].fillna(0.0).astype(float)
    cse = df.get("cross_sector_edge", pd.Series(False, index=df.index)).map(_to_bool)
    cie = df.get("cross_industry_edge", pd.Series(False, index=df.index)).map(_to_bool)

    in_top = df["ticker_1"].astype(str).isin(top_tickers) & df["ticker_2"].astype(str).isin(top_tickers)
    keep = (
        (score >= 0.55)
        | in_top
        | (cse & (score >= 0.45))
        | (cie & (score >= 0.45))
    )

    out = df[keep].copy()
    cols = [
        "ticker_1",
        "ticker_2",
        "combined_score",
        "raw_correlation",
        "residual_correlation",
        "residual_rolling_corr_mean",
        "edge_category",
        "cross_sector_edge",
        "cross_industry_edge",
        "seed_edge",
        "both_seed_edge",
        "ticker_1_network_importance_score",
        "ticker_2_network_importance_score",
        "ticker_1_network_role",
        "ticker_2_network_role",
        "ticker_1_subtheme_guess",
        "ticker_2_subtheme_guess",
        "sector_1",
        "industry_1",
        "sector_2",
        "industry_2",
        "source_family",
        "source_file",
    ]
    for col in cols:
        if col not in out.columns:
            out[col] = np.nan

    out = out[cols].sort_values(
        by=["combined_score", "ticker_1", "ticker_2"],
        ascending=[False, True, True],
        kind="mergesort",
    )
    out.to_csv(output_dir / "datacenter_edge_filtered_strong_network.csv", index=False)
    return out


def write_validation_shortlist(
    node_scores: pd.DataFrame,
    args: argparse.Namespace,
    output_dir: Path,
) -> pd.DataFrame:
    df = node_scores.copy()

    include = (
        (df["ecosystem_membership_class"] == "CORE_VALIDATION")
        | (df["network_role"].isin([
            "CORE_CONNECTOR",
            "MULTI_SEED_CONNECTOR",
            "CROSS_SUBTHEME_BRIDGE",
            "CROSS_SECTOR_BRIDGE",
        ]))
        | (df["network_importance_tier"] == "HIGH")
        | (df["seed_neighbor_count_from_edges"] >= args.min_seed_neighbor_count)
        | (df["core_neighbor_count"] >= args.min_core_neighbor_count)
    )

    exclude = (
        (df["ecosystem_membership_class"] == "BROAD_BETA")
        & (df["network_importance_score"] < 0.70)
        & (df["seed_neighbor_count_from_edges"] < args.min_seed_neighbor_count)
    )

    out = df[include & (~exclude)].copy()

    reasons: dict[str, str] = {}
    for _, row in out.iterrows():
        rs = set()
        ticker = _to_text(row.get("ticker"))

        if _to_text(row.get("ecosystem_membership_class")) == "CORE_VALIDATION":
            rs.add("CORE_VALIDATION")
        if _to_text(row.get("network_importance_tier")) == "HIGH":
            rs.add("HIGH_NETWORK_IMPORTANCE")
        if _to_float(row.get("seed_neighbor_count_from_edges")) >= float(args.min_seed_neighbor_count):
            rs.add("MULTI_SEED_CONNECTED")
        if _to_float(row.get("core_neighbor_count")) >= float(args.min_core_neighbor_count):
            rs.add("CORE_CONNECTED")
        if _to_text(row.get("network_role")) == "CROSS_SUBTHEME_BRIDGE":
            rs.add("CROSS_SUBTHEME_BRIDGE")
        if _to_text(row.get("network_role")) == "CROSS_SECTOR_BRIDGE":
            rs.add("CROSS_SECTOR_BRIDGE")
        if _to_text(row.get("network_role")) == "SEED_ANCHOR":
            rs.add("SEED_ANCHOR")

        reasons[ticker] = _sorted_join(sorted(rs))

    out["validation_reason"] = out["ticker"].map(lambda t: reasons.get(_to_text(t), ""))
    out["_is_seed_sort"] = out["is_seed"].map(_to_bool).astype(int)
    out = out.sort_values(
        by=[
            "_is_seed_sort",
            "network_importance_score",
            "seed_neighbor_count_from_edges",
            "core_neighbor_count",
            "ticker",
        ],
        ascending=[False, False, False, False, True],
        kind="mergesort",
    ).drop(columns=["_is_seed_sort"]).head(args.top_n)

    out.to_csv(output_dir / "datacenter_network_validation_shortlist.csv", index=False)
    return out


def write_markdown_report(
    graph_dir: Path,
    focused_report_dir: Path,
    output_dir: Path,
    theme_name: str,
    node_scores: pd.DataFrame,
    top_reports: dict[str, pd.DataFrame],
    subtheme_summary: pd.DataFrame,
    top_seed_anchors: pd.DataFrame,
    top_operating_non_seed_connectors: pd.DataFrame,
    broad_beta_network_connectors: pd.DataFrame,
    operating_connector_by_subtheme: pd.DataFrame,
    final_manual_review_queue: pd.DataFrame,
) -> None:
    seed_nodes = int(node_scores["is_seed"].map(_to_bool).sum())
    core_nodes = int((node_scores["ecosystem_membership_class"] == "CORE_VALIDATION").sum())
    broad_nodes = int((node_scores["ecosystem_membership_class"] == "BROAD_BETA").sum())

    lines = []
    lines.append(f"# {theme_name.title()} Network Centrality Report")
    lines.append("")
    lines.append("## Inputs")
    lines.append(f"- graph_dir: {graph_dir}")
    lines.append(f"- focused_report_dir: {focused_report_dir}")
    lines.append(f"- output_dir: {output_dir}")
    lines.append(f"- theme_name: {theme_name}")
    lines.append("")
    lines.append("## Counts")
    lines.append(f"- input_nodes: {len(node_scores)}")
    lines.append(f"- input_edges: {int(node_scores['degree_from_edges'].sum() / 2)}")
    lines.append(f"- seed_nodes: {seed_nodes}")
    lines.append(f"- core_validation_nodes: {core_nodes}")
    lines.append(f"- broad_beta_nodes: {broad_nodes}")
    lines.append("")

    lines.append("## Top 10 Overall Network Connectors")
    lines.append(_markdown_table(
        top_reports["top_network_connectors"],
        ["ticker", "subtheme_guess", "network_importance_score", "network_importance_tier", "network_role"],
        top_n=10,
    ))
    lines.append("")

    lines.append("## Top 10 Non-Seed Network Connectors")
    lines.append(_markdown_table(
        top_reports["top_non_seed_connectors"],
        ["ticker", "subtheme_guess", "network_importance_score", "seed_neighbor_count_from_edges", "network_role"],
        top_n=10,
    ))
    lines.append("")

    lines.append("## Top 10 Multi-Seed Connectors")
    lines.append(_markdown_table(
        top_reports["multi_seed_connectors"],
        ["ticker", "subtheme_guess", "seed_neighbor_count_from_edges", "average_seed_edge_score", "network_importance_score"],
        top_n=10,
    ))
    lines.append("")

    lines.append("## Top 10 Cross-Subtheme Bridges")
    lines.append(_markdown_table(
        top_reports["cross_subtheme_bridges"],
        ["ticker", "neighbor_subtheme_count", "cross_subtheme_bridge_score", "network_importance_score", "network_role"],
        top_n=10,
    ))
    lines.append("")

    lines.append("## Top 10 Cross-Sector Bridges")
    lines.append(_markdown_table(
        top_reports["cross_sector_network_bridges"],
        ["ticker", "cross_sector_degree", "cross_sector_bridge_score", "network_importance_score", "network_role"],
        top_n=10,
    ))
    lines.append("")

    lines.append("## Top 10 Seed Anchors")
    lines.append(_markdown_table(
        top_seed_anchors,
        ["ticker", "network_importance_score", "seed_neighbor_count_from_edges", "core_neighbor_count", "network_role"],
        top_n=10,
    ))
    lines.append("")

    lines.append("## Top 10 Operating Non-Seed Datacenter Connectors")
    lines.append(_markdown_table(
        top_operating_non_seed_connectors,
        ["ticker", "subtheme_guess", "network_importance_score", "seed_neighbor_count_from_edges", "core_neighbor_count"],
        top_n=10,
    ))
    lines.append("")

    lines.append("## Top 10 Broad Beta / Financial Network Connectors")
    lines.append(_markdown_table(
        broad_beta_network_connectors,
        ["ticker", "subtheme_guess", "network_importance_score", "seed_neighbor_count_from_edges", "core_neighbor_count"],
        top_n=10,
    ))
    lines.append("")

    lines.append("## Operating Connector Summary By Subtheme")
    lines.append(_markdown_table(
        operating_connector_by_subtheme,
        ["subtheme_guess", "ticker_count", "average_network_importance_score", "average_seed_neighbor_count", "average_core_neighbor_count"],
        top_n=10,
    ))
    lines.append("")

    lines.append("## Subtheme Network Summary")
    lines.append(_markdown_table(
        subtheme_summary,
        ["subtheme_guess", "ticker_count", "average_network_importance_score", "average_seed_neighbor_count", "average_core_neighbor_count"],
        top_n=10,
    ))
    lines.append("")

    lines.append("## Scoring Notes")
    lines.append("- network_importance_score combines weighted degree, seed/core connectivity, cross-sector connectivity, subtheme breadth, clique participation, and average edge strength.")
    lines.append("- network_role labels nodes by deterministic priority: seed anchor, core connector, multi-seed connector, bridge roles, specialist, broad beta, or peripheral.")
    lines.append("- Warning: network centrality reflects statistical connectivity and is not proof of business relationship or investment attractiveness.")
    lines.append("- datacenter_top_operating_non_seed_connectors.csv is the preferred report for finding new operational datacenter candidates.")
    lines.append("- datacenter_broad_beta_network_connectors.csv is intentionally separated because high connectivity can reflect broad risk-factor behavior rather than operational datacenter exposure.")
    lines.append("- datacenter_final_manual_review_queue.csv is the cleanest manual review starting point.")

    (output_dir / "datacenter_network_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_readme(output_dir: Path) -> None:
    lines = [
        "# Datacenter Network Centrality Outputs",
        "",
        "This directory contains V7 centrality/connectivity reports generated from V6b + V6c outputs.",
        "",
        "## Files",
        "- datacenter_network_node_scores.csv: full node-level network metrics for all graph nodes.",
        "- datacenter_top_network_connectors.csv: top nodes by network importance score.",
        "- datacenter_top_non_seed_connectors.csv: top non-seed nodes by network importance score.",
        "- datacenter_multi_seed_connectors.csv: nodes connected to multiple seed tickers.",
        "- datacenter_cross_subtheme_bridges.csv: nodes bridging multiple neighbor subthemes.",
        "- datacenter_cross_sector_network_bridges.csv: nodes with strong cross-sector connectivity.",
        "- datacenter_clique_connectors.csv: nodes with meaningful clique participation.",
        "- datacenter_subtheme_network_summary.csv: subtheme-level centrality summary.",
        "- datacenter_edge_filtered_strong_network.csv: filtered strong edge list for manual review.",
        "- datacenter_network_validation_shortlist.csv: centrality-aware manual validation shortlist.",
        "- datacenter_top_seed_anchors.csv: ranked seed anchor nodes with deterministic reason tags.",
        "- datacenter_top_operating_non_seed_connectors.csv: ranked operating non-seed datacenter connectors.",
        "- datacenter_broad_beta_network_connectors.csv: ranked broad beta/financial/statistical connectors.",
        "- datacenter_operating_connector_by_subtheme.csv: operating connector summary grouped by subtheme.",
        "- datacenter_final_manual_review_queue.csv: combined manual review queue across seed/core/high-signal broad-beta.",
        "- datacenter_network_report.md: compact markdown summary with top sections.",
        "- datacenter_network_readme.md: this file.",
    ]
    (output_dir / "datacenter_network_readme.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    graph_dir = Path(args.graph_dir)
    focused_report_dir = Path(args.focused_report_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    file_map = validate_input_files(graph_dir, focused_report_dir)
    data = load_inputs(file_map)

    edges_df = data["seed_ecosystem_edges.csv"].copy()
    nodes_df = data["seed_ecosystem_nodes.csv"].copy()
    cliques_df = data["seed_ecosystem_cliques_3plus.csv"].copy()
    core_df = data["datacenter_core_validation_shortlist.csv"].copy()
    broad_df = data["datacenter_broad_beta_candidates.csv"].copy()
    priority_df = data["datacenter_priority_candidates.csv"].copy()

    nodes_df["ticker"] = nodes_df["ticker"].astype(str)
    tickers = sorted(set(nodes_df["ticker"].tolist()))
    seed_set = {
        _to_text(r.get("ticker"))
        for _, r in nodes_df.iterrows()
        if _to_bool(r.get("is_seed"))
    }

    core_set = set(core_df.get("ticker", pd.Series([], dtype=str)).astype(str).tolist())
    core_set = core_set.union(seed_set)

    subtheme_map = build_subtheme_map(core_df, broad_df, priority_df, nodes_df)
    membership_map = build_membership_map(nodes_df, core_df, broad_df, priority_df)
    adjacency_metrics = build_adjacency_metrics(edges_df, tickers)

    seed_conn = compute_seed_connectivity(adjacency_metrics, seed_set)
    core_conn = compute_core_connectivity(adjacency_metrics, core_set)
    subtheme_conn = compute_subtheme_connectivity(adjacency_metrics, subtheme_map)
    sector_industry_conn = compute_sector_industry_connectivity(adjacency_metrics, nodes_df)
    clique_metrics = compute_clique_metrics(cliques_df, tickers)

    node_meta = nodes_df.drop_duplicates(subset=["ticker"]).set_index("ticker")
    priority_meta = priority_df.drop_duplicates(subset=["ticker"]).set_index("ticker") if not priority_df.empty else pd.DataFrame()

    rows = []
    for ticker in tickers:
        m = node_meta.loc[ticker] if ticker in node_meta.index else pd.Series(dtype=object)
        p = priority_meta.loc[ticker] if (not priority_meta.empty and ticker in priority_meta.index) else pd.Series(dtype=object)
        a = adjacency_metrics.get(ticker, {})
        s = seed_conn.get(ticker, {})
        c = core_conn.get(ticker, {})
        st = subtheme_conn.get(ticker, {})
        si = sector_industry_conn.get(ticker, {})
        cl = clique_metrics.get(ticker, {})

        rows.append(
            {
                "theme_name": args.theme_name,
                "ticker": ticker,
                "sector": _to_text(m.get("sector")),
                "industry": _to_text(m.get("industry")),
                "is_seed": _to_bool(m.get("is_seed")),
                "seed_status": _to_text(m.get("seed_status")),
                "ecosystem_membership_class": membership_map.get(ticker, "GRAPH_ONLY"),
                "subtheme_guess": subtheme_map.get(ticker, "OTHER_OR_UNCLASSIFIED"),
                "priority_score": _to_float(p.get("priority_score")),
                "priority_tier": _to_text(p.get("priority_tier")),
                "component_id": _to_float(m.get("component_id")),
                "component_size": _to_float(m.get("component_size")),
                "degree_from_edges": int(a.get("degree_from_edges", 0)),
                "weighted_degree_from_edges": float(a.get("weighted_degree_from_edges", 0.0)),
                "average_edge_score": float(a.get("average_edge_score", 0.0)),
                "max_edge_score": float(a.get("max_edge_score", 0.0)),
                "min_edge_score": float(a.get("min_edge_score", 0.0)),
                "seed_neighbor_count_from_edges": int(s.get("seed_neighbor_count_from_edges", 0)),
                "seed_neighbors_from_edges": _to_text(s.get("seed_neighbors_from_edges")),
                "average_seed_edge_score": float(s.get("average_seed_edge_score", 0.0)),
                "max_seed_edge_score": float(s.get("max_seed_edge_score", 0.0)),
                "best_seed_neighbor_from_edges": _to_text(s.get("best_seed_neighbor_from_edges")),
                "core_neighbor_count": int(c.get("core_neighbor_count", 0)),
                "core_neighbors": _to_text(c.get("core_neighbors")),
                "average_core_edge_score": float(c.get("average_core_edge_score", 0.0)),
                "max_core_edge_score": float(c.get("max_core_edge_score", 0.0)),
                "best_core_neighbor": _to_text(c.get("best_core_neighbor")),
                "cross_sector_degree": int(si.get("cross_sector_degree", 0)),
                "cross_industry_degree": int(si.get("cross_industry_degree", 0)),
                "connected_sector_count": int(si.get("connected_sector_count", 0)),
                "connected_sectors": _to_text(si.get("connected_sectors")),
                "connected_industry_count": int(si.get("connected_industry_count", 0)),
                "connected_industries": _to_text(si.get("connected_industries")),
                "neighbor_subtheme_count": int(st.get("neighbor_subtheme_count", 0)),
                "neighbor_subthemes": _to_text(st.get("neighbor_subthemes")),
                "dominant_neighbor_subtheme": _to_text(st.get("dominant_neighbor_subtheme")),
                "dominant_neighbor_subtheme_count": int(st.get("dominant_neighbor_subtheme_count", 0)),
                "clique_count": int(cl.get("clique_count", 0)),
                "max_clique_size": int(cl.get("max_clique_size", 0)),
                "average_clique_score": float(cl.get("average_clique_score", 0.0)),
                "seed_clique_count": int(cl.get("seed_clique_count", 0)),
                "cross_sector_clique_count": int(cl.get("cross_sector_clique_count", 0)),
                "cross_industry_clique_count": int(cl.get("cross_industry_clique_count", 0)),
            }
        )

    node_scores = pd.DataFrame(rows)
    node_scores = compute_bridge_scores(node_scores)
    node_scores = compute_network_importance_scores(node_scores)
    node_scores["network_role"] = node_scores.apply(lambda r: classify_network_role(r, args), axis=1)

    node_scores = write_node_scores(node_scores, output_dir)
    top_reports = write_top_reports(node_scores, args.theme_name, args, output_dir)
    subtheme_summary = write_subtheme_summary(node_scores, args.theme_name, output_dir)
    filtered_edges = write_filtered_edges(edges_df, node_scores, args.top_n, output_dir)
    validation_shortlist = write_validation_shortlist(node_scores, args, output_dir)
    top_seed_anchors = write_seed_anchor_report(node_scores, args, output_dir)
    top_operating_non_seed_connectors = write_operating_non_seed_connector_report(node_scores, args, output_dir)
    broad_beta_network_connectors = write_broad_beta_network_connector_report(node_scores, args, output_dir)
    operating_connector_by_subtheme = write_operating_connector_by_subtheme(top_operating_non_seed_connectors, args.theme_name, output_dir)
    final_manual_review_queue = write_final_manual_review_queue(node_scores, args, output_dir)
    write_markdown_report(
        graph_dir=graph_dir,
        focused_report_dir=focused_report_dir,
        output_dir=output_dir,
        theme_name=args.theme_name,
        node_scores=node_scores,
        top_reports=top_reports,
        subtheme_summary=subtheme_summary,
        top_seed_anchors=top_seed_anchors,
        top_operating_non_seed_connectors=top_operating_non_seed_connectors,
        broad_beta_network_connectors=broad_beta_network_connectors,
        operating_connector_by_subtheme=operating_connector_by_subtheme,
        final_manual_review_queue=final_manual_review_queue,
    )
    write_readme(output_dir)

    print(f"SUMMARY graph_dir={graph_dir}")
    print(f"SUMMARY focused_report_dir={focused_report_dir}")
    print(f"SUMMARY output_dir={output_dir}")
    print(f"SUMMARY theme_name={args.theme_name}")
    print(f"SUMMARY input_edges={len(edges_df)}")
    print(f"SUMMARY input_nodes={len(nodes_df)}")
    print(f"SUMMARY seed_nodes={int(nodes_df['is_seed'].map(_to_bool).sum())}")
    print(f"SUMMARY core_validation_nodes={len(core_df)}")
    print(f"SUMMARY broad_beta_nodes={len(broad_df)}")
    print(f"SUMMARY rows_network_node_scores={len(node_scores)}")
    print(f"SUMMARY rows_top_network_connectors={len(top_reports['top_network_connectors'])}")
    print(f"SUMMARY rows_top_non_seed_connectors={len(top_reports['top_non_seed_connectors'])}")
    print(f"SUMMARY rows_multi_seed_connectors={len(top_reports['multi_seed_connectors'])}")
    print(f"SUMMARY rows_cross_subtheme_bridges={len(top_reports['cross_subtheme_bridges'])}")
    print(f"SUMMARY rows_cross_sector_network_bridges={len(top_reports['cross_sector_network_bridges'])}")
    print(f"SUMMARY rows_clique_connectors={len(top_reports['clique_connectors'])}")
    print(f"SUMMARY rows_subtheme_network_summary={len(subtheme_summary)}")
    print(f"SUMMARY rows_edge_filtered_strong_network={len(filtered_edges)}")
    print(f"SUMMARY rows_network_validation_shortlist={len(validation_shortlist)}")
    print(f"SUMMARY rows_top_seed_anchors={len(top_seed_anchors)}")
    print(f"SUMMARY rows_top_operating_non_seed_connectors={len(top_operating_non_seed_connectors)}")
    print(f"SUMMARY rows_broad_beta_network_connectors={len(broad_beta_network_connectors)}")
    print(f"SUMMARY rows_operating_connector_by_subtheme={len(operating_connector_by_subtheme)}")
    print(f"SUMMARY rows_final_manual_review_queue={len(final_manual_review_queue)}")
    print("SUMMARY status=OK")


if __name__ == "__main__":
    main()
