"""
V6c: Focused reporting for seed ecosystem graph outputs.

This script reads V6b output files and creates focused CSV/Markdown reports
without recomputing correlations or rebuilding the graph from raw market data.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd


REQUIRED_INPUT_FILES = [
    "seed_ecosystem_edges.csv",
    "seed_ecosystem_nodes.csv",
    "seed_ecosystem_direct_seed_edges.csv",
    "seed_ecosystem_first_hop_candidates.csv",
    "seed_ecosystem_second_hop_candidates.csv",
    "seed_ecosystem_cross_sector_bridges.csv",
    "seed_ecosystem_components.csv",
    "seed_ecosystem_datacenter_summary.csv",
]

OPTIONAL_INPUT_FILES = [
    "seed_ecosystem_cliques_3plus.csv",
    "seed_ecosystem_theme_summary.md",
    "seed_ecosystem_readme.md",
]

CORE_SUBTHEMES = {
    "SEMICONDUCTORS_AI_CHIPS",
    "SEMICONDUCTOR_EQUIPMENT",
    "SERVER_STORAGE_HARDWARE",
    "NETWORKING_OPTICAL_CONNECTIVITY",
    "ELECTRICAL_POWER_EQUIPMENT",
    "POWER_GENERATION_UTILITIES",
    "DATACENTER_REIT",
    "ENGINEERING_CONSTRUCTION_INFRA",
}

FINANCIAL_INDUSTRY_TERMS = [
    "asset management",
    "capital markets",
    "banks",
    "credit services",
    "insurance",
]

NON_CORE_INDUSTRY_TERMS = [
    "reit - hotel & motel",
    "auto & truck dealerships",
    "recreational vehicles",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="V6c: focused reporting for seed ecosystem graph outputs."
    )
    p.add_argument("--input-dir", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--theme-name", default="datacenter")
    p.add_argument("--top-neighbors-per-seed", type=int, default=30)
    p.add_argument("--top-priority-candidates", type=int, default=300)
    p.add_argument("--top-cross-sector-bridges", type=int, default=300)
    p.add_argument("--top-multi-seed-candidates", type=int, default=300)
    p.add_argument("--min-best-seed-edge-score", type=float, default=0.35)
    p.add_argument("--min-seed-neighbor-count", type=int, default=2)
    p.add_argument("--min-weighted-degree", type=float, default=1.0)
    return p.parse_args()


def _to_text(v: object) -> str:
    if pd.isna(v):
        return ""
    return str(v).strip()


def _to_float(v: object) -> float:
    if pd.isna(v):
        return np.nan
    try:
        return float(v)
    except (TypeError, ValueError):
        return np.nan


def _to_bool(v: object) -> bool:
    if isinstance(v, bool):
        return v
    if pd.isna(v):
        return False
    t = str(v).strip().lower()
    return t in {"true", "1", "yes", "y", "t"}


def _sorted_join(values: list[object]) -> str:
    parts = sorted({
        _to_text(v)
        for v in values
        if _to_text(v)
    })
    return ", ".join(parts)


def parse_list_column(value: object) -> list[str]:
    if pd.isna(value):
        return []
    text = str(value)
    items = [part.strip() for part in text.split(",")]
    items = [part for part in items if part]
    return sorted(set(items))


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


def validate_input_files(input_dir: Path) -> dict[str, Path]:
    if not input_dir.exists():
        raise SystemExit(f"ERROR: input directory not found: {input_dir}")

    files: dict[str, Path] = {}
    missing: list[str] = []
    for name in REQUIRED_INPUT_FILES:
        path = input_dir / name
        if not path.exists():
            missing.append(name)
        else:
            files[name] = path

    if missing:
        raise SystemExit("ERROR: missing required input files: " + ", ".join(sorted(missing)))

    for name in OPTIONAL_INPUT_FILES:
        path = input_dir / name
        if path.exists():
            files[name] = path

    return files


def load_inputs(file_map: dict[str, Path]) -> dict[str, pd.DataFrame]:
    data: dict[str, pd.DataFrame] = {}
    for name, path in file_map.items():
        if path.suffix.lower() == ".csv":
            data[name] = normalize_booleans(pd.read_csv(path))
    return data


def classify_subtheme(
    ticker: object,
    sector: object,
    industry: object,
    seed_neighbors: object,
    nearest_seed_tickers: object,
    seed_status: object,
) -> str:
    ticker_u = _to_text(ticker).upper()
    sector_t = _to_text(sector)
    industry_t = _to_text(industry)
    sector_l = sector_t.lower()
    industry_l = industry_t.lower()

    # Deterministic priority order required by V6c update.
    if ticker_u in {"EQIX", "DLR", "AMT", "CCI", "SBAC"} or "reit - specialty" in industry_l:
        return "DATACENTER_REIT"

    if "utilities" in sector_l or ticker_u in {
        "CEG", "VST", "NEE", "TLN", "PEG", "NRG", "SO", "DUK", "PCG", "XEL",
        "ETR", "EXC", "PPL", "SRE", "DTE", "AEP", "ED",
    }:
        return "POWER_GENERATION_UTILITIES"

    if ticker_u in {
        "PWR", "FIX", "STRL", "MYRG", "MTZ", "EME", "FLR", "J", "ACM", "DY",
        "PRIM", "IESC", "ACA", "ECG",
    }:
        return "ENGINEERING_CONSTRUCTION_INFRA"
    if "engineering & construction" in industry_l or "infrastructure operations" in industry_l:
        return "ENGINEERING_CONSTRUCTION_INFRA"

    if ticker_u in {
        "VRT", "ETN", "NVT", "HUBB", "POWL", "GNRC", "EMR", "GEV", "TT", "DOV",
        "AEIS", "PH", "GTES", "NPO", "IEX", "NDSN", "ITT", "GGG", "CMI", "RRX",
        "CR", "PNR", "IR", "FLS", "AME", "CSW", "JCI", "SPXC", "ROK", "ITW", "KAI",
    }:
        return "ELECTRICAL_POWER_EQUIPMENT"
    if (
        "electrical equipment" in industry_l
        or "electrical equipment & parts" in industry_l
        or "specialty industrial machinery" in industry_l
        or "building products & equipment" in industry_l
    ):
        return "ELECTRICAL_POWER_EQUIPMENT"

    if ticker_u in {"DELL", "HPE", "SMCI", "NTAP", "PSTG", "WDC", "STX", "HPQ"}:
        return "SERVER_STORAGE_HARDWARE"
    if "computer hardware" in industry_l or "data storage" in industry_l:
        return "SERVER_STORAGE_HARDWARE"

    if ticker_u in {
        "ANET", "CIEN", "GLW", "APH", "TEL", "COMM", "COHR", "LITE", "FN", "AAOI",
        "NTCT", "CSCO", "JNPR", "FLEX", "JBL", "LFUS", "PLXS", "TTMI", "BDC", "BHE",
        "CTS", "BELFB", "ROG", "OLED", "KN", "CLS", "ZBRA",
    }:
        return "NETWORKING_OPTICAL_CONNECTIVITY"
    if (
        "communication equipment" in industry_l
        or "electronic components" in industry_l
        or "optical" in industry_l
        or "connectivity" in industry_l
    ):
        return "NETWORKING_OPTICAL_CONNECTIVITY"

    if ticker_u in {
        "ASML", "AMAT", "LRCX", "KLAC", "MKSI", "NVMI", "UCTT", "VECO", "ICHR", "ENTG",
        "COHU", "ONTO", "TER", "FORM", "CAMT", "ACLS", "AMKR",
    } or "semiconductor equipment" in industry_l:
        return "SEMICONDUCTOR_EQUIPMENT"

    if "semiconductors" in industry_l or ticker_u in {
        "NVDA", "AMD", "AVGO", "TSM", "ARM", "MRVL", "MPWR", "ADI", "MCHP", "NXPI",
        "LSCC", "MU", "QCOM", "TXN", "QRVO", "SITM", "SWKS", "RMBS", "MTSI", "CRUS",
        "DIOD", "ON", "STM", "POWI", "SYNA", "ASX",
    }:
        return "SEMICONDUCTORS_AI_CHIPS"

    if (
        "technology" in sector_l
        and ("software" in industry_l or "information technology services" in industry_l)
    ):
        return "SOFTWARE_PLATFORM_ADJACENT"

    if sector_t in {"Technology", "Industrials"}:
        return "BROAD_TECH_OR_INDUSTRIAL_BETA"

    return "OTHER_OR_UNCLASSIFIED"


def _industry_contains_any(industry: object, terms: list[str]) -> bool:
    il = _to_text(industry).lower()
    return any(term in il for term in terms)


def compute_priority_scores(nodes: pd.DataFrame) -> pd.DataFrame:
    out = nodes.copy()

    if "best_seed_edge_score" not in out.columns:
        out["best_seed_edge_score"] = np.nan
    if "seed_neighbor_count" not in out.columns:
        out["seed_neighbor_count"] = 0
    if "weighted_degree" not in out.columns:
        out["weighted_degree"] = 0.0
    if "cross_sector_degree" not in out.columns:
        out["cross_sector_degree"] = 0
    if "cross_industry_degree" not in out.columns:
        out["cross_industry_degree"] = 0

    best_seed = out["best_seed_edge_score"].apply(_to_float).fillna(0.0).clip(lower=0.0, upper=1.0)
    seed_neighbors = out["seed_neighbor_count"].apply(_to_float).fillna(0.0).clip(lower=0.0)
    weighted_degree = out["weighted_degree"].apply(_to_float).fillna(0.0).clip(lower=0.0)
    cross_sector_degree = out["cross_sector_degree"].apply(_to_float).fillna(0.0).clip(lower=0.0)
    cross_industry_degree = out["cross_industry_degree"].apply(_to_float).fillna(0.0).clip(lower=0.0)

    norm_best_seed = best_seed
    norm_seed_neighbors = (seed_neighbors / 5.0).clip(upper=1.0)
    norm_weighted_degree = (weighted_degree / 20.0).clip(upper=1.0)
    norm_cross_sector = (cross_sector_degree / 10.0).clip(upper=1.0)
    norm_cross_industry = (cross_industry_degree / 20.0).clip(upper=1.0)

    status = out.get("seed_status", pd.Series("", index=out.index)).fillna("").astype(str).str.upper()
    first_hop_bonus = np.where(status == "FIRST_HOP", 1.0, np.where(status == "SECOND_HOP", 0.5, 0.0))

    out["priority_score"] = (
        0.35 * norm_best_seed
        + 0.20 * norm_seed_neighbors
        + 0.15 * norm_weighted_degree
        + 0.10 * norm_cross_sector
        + 0.10 * norm_cross_industry
        + 0.10 * first_hop_bonus
    )

    out["priority_tier"] = np.where(
        out["priority_score"] >= 0.70,
        "HIGH",
        np.where(out["priority_score"] >= 0.45, "MEDIUM", "LOW"),
    )
    return out


def _build_node_maps(nodes: pd.DataFrame) -> dict[str, dict[str, object]]:
    out: dict[str, dict[str, object]] = {}
    for _, row in nodes.iterrows():
        ticker = _to_text(row.get("ticker"))
        if ticker:
            out[ticker] = row.to_dict()
    return out


def _with_subtheme_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["subtheme_guess"] = out.apply(
        lambda r: classify_subtheme(
            r.get("ticker"),
            r.get("sector"),
            r.get("industry"),
            r.get("seed_neighbors"),
            r.get("nearest_seed_tickers"),
            r.get("seed_status"),
        ),
        axis=1,
    )
    return out


def write_top_seed_neighbors(
    direct_edges: pd.DataFrame,
    theme_name: str,
    top_neighbors_per_seed: int,
    output_dir: Path,
    node_maps: dict[str, dict[str, object]],
) -> pd.DataFrame:
    df = direct_edges.copy()

    if "seed_ticker" not in df.columns:
        df["seed_ticker"] = ""
    if "non_seed_ticker" not in df.columns:
        df["non_seed_ticker"] = ""

    if "seed_ticker" in df.columns and "non_seed_ticker" in df.columns:
        need_derive = (df["seed_ticker"].astype(str).str.strip() == "") | (
            df["non_seed_ticker"].astype(str).str.strip() == ""
        )
        if need_derive.any():
            def _derive_pair(r: pd.Series) -> tuple[str, str]:
                t1 = _to_text(r.get("ticker_1"))
                t2 = _to_text(r.get("ticker_2"))
                t1_seed = _to_bool(r.get("ticker_1_is_seed"))
                t2_seed = _to_bool(r.get("ticker_2_is_seed"))
                if t1_seed and not t2_seed:
                    return t1, t2
                if t2_seed and not t1_seed:
                    return t2, t1
                return "", ""

            derived = df.apply(_derive_pair, axis=1, result_type="expand")
            df.loc[need_derive, "seed_ticker"] = derived.loc[need_derive, 0]
            df.loc[need_derive, "non_seed_ticker"] = derived.loc[need_derive, 1]

    df = df[(df["seed_ticker"].astype(str).str.strip() != "") & (df["non_seed_ticker"].astype(str).str.strip() != "")].copy()

    def _seed_meta(r: pd.Series) -> tuple[str, str, str, str]:
        seed = _to_text(r.get("seed_ticker"))
        non_seed = _to_text(r.get("non_seed_ticker"))
        srow = node_maps.get(seed, {})
        nrow = node_maps.get(non_seed, {})
        return (
            _to_text(srow.get("sector")),
            _to_text(srow.get("industry")),
            _to_text(nrow.get("sector")),
            _to_text(nrow.get("industry")),
        )

    meta = df.apply(_seed_meta, axis=1, result_type="expand")
    df["seed_sector"] = meta[0]
    df["seed_industry"] = meta[1]
    df["non_seed_sector"] = meta[2]
    df["non_seed_industry"] = meta[3]
    df["non_seed_subtheme_guess"] = df.apply(
        lambda r: classify_subtheme(
            r.get("non_seed_ticker"),
            r.get("non_seed_sector"),
            r.get("non_seed_industry"),
            None,
            r.get("seed_ticker"),
            "FIRST_HOP",
        ),
        axis=1,
    )

    df["theme_name"] = theme_name

    sort_seed = [True, False, True]
    df = df.sort_values(
        by=["seed_ticker", "combined_score", "non_seed_ticker"],
        ascending=sort_seed,
        kind="mergesort",
    )
    df = df.groupby("seed_ticker", as_index=False, group_keys=False).head(top_neighbors_per_seed)

    cols = [
        "theme_name",
        "seed_ticker",
        "non_seed_ticker",
        "combined_score",
        "raw_correlation",
        "raw_rolling_corr_mean",
        "residual_correlation",
        "residual_rolling_corr_mean",
        "edge_category",
        "cross_sector_edge",
        "cross_industry_edge",
        "seed_sector",
        "seed_industry",
        "non_seed_sector",
        "non_seed_industry",
        "non_seed_subtheme_guess",
        "source_family",
        "source_file",
    ]
    for col in cols:
        if col not in df.columns:
            df[col] = np.nan

    out = df[cols].copy()
    out = out.sort_values(
        by=["seed_ticker", "combined_score", "non_seed_ticker"],
        ascending=[True, False, True],
        kind="mergesort",
    )
    out.to_csv(output_dir / "top_seed_neighbors_by_seed.csv", index=False)
    return out


def write_multi_seed_candidates(
    nodes: pd.DataFrame,
    theme_name: str,
    min_seed_neighbor_count: int,
    min_best_seed_edge_score: float,
    top_multi_seed_candidates: int,
    output_dir: Path,
) -> pd.DataFrame:
    df = nodes.copy()
    df = _with_subtheme_columns(df)

    is_seed = df.get("is_seed", pd.Series(False, index=df.index)).map(_to_bool)
    seed_distance = df.get("seed_distance", pd.Series(np.nan, index=df.index)).apply(_to_float)
    seed_neighbors = df.get("seed_neighbor_count", pd.Series(0, index=df.index)).apply(_to_float)
    best_seed = df.get("best_seed_edge_score", pd.Series(np.nan, index=df.index)).apply(_to_float)

    mask = (
        (~is_seed)
        & (seed_distance == 1)
        & (seed_neighbors >= float(min_seed_neighbor_count))
        & (best_seed >= float(min_best_seed_edge_score))
    )
    out = df[mask].copy()
    out["theme_name"] = theme_name

    out = out.sort_values(
        by=[
            "seed_neighbor_count",
            "priority_score",
            "best_seed_edge_score",
            "weighted_degree",
            "ticker",
        ],
        ascending=[False, False, False, False, True],
        kind="mergesort",
    ).head(top_multi_seed_candidates)

    cols = [
        "theme_name",
        "ticker",
        "sector",
        "industry",
        "subtheme_guess",
        "seed_neighbor_count",
        "seed_neighbors",
        "best_seed_edge_score",
        "best_seed_neighbor",
        "weighted_degree",
        "degree",
        "cross_sector_degree",
        "cross_industry_degree",
        "component_id",
        "component_size",
        "priority_score",
        "priority_tier",
    ]
    for col in cols:
        if col not in out.columns:
            out[col] = np.nan

    out = out[cols].copy()
    out.to_csv(output_dir / "multi_seed_connected_candidates.csv", index=False)
    return out


def write_cross_sector_bridges(
    bridges: pd.DataFrame,
    nodes: pd.DataFrame,
    theme_name: str,
    top_cross_sector_bridges: int,
    output_dir: Path,
) -> pd.DataFrame:
    df = bridges.copy()
    node_maps = _build_node_maps(nodes)

    if "cross_sector_edge" in df.columns:
        df = df[df["cross_sector_edge"].map(_to_bool)].copy()

    def _is_seed_or_first_hop(t: str) -> bool:
        r = node_maps.get(t, {})
        if _to_bool(r.get("is_seed")):
            return True
        d = _to_float(r.get("seed_distance"))
        return not pd.isna(d) and d <= 1.0

    keep = []
    for _, row in df.iterrows():
        t1 = _to_text(row.get("ticker_1"))
        t2 = _to_text(row.get("ticker_2"))
        keep.append(_is_seed_or_first_hop(t1) or _is_seed_or_first_hop(t2))
    df = df[pd.Series(keep, index=df.index)].copy()

    def _subtheme_for_ticker(t: str) -> str:
        r = node_maps.get(t, {})
        return classify_subtheme(
            t,
            r.get("sector"),
            r.get("industry"),
            r.get("seed_neighbors"),
            r.get("nearest_seed_tickers"),
            r.get("seed_status"),
        )

    df["ticker_1_subtheme_guess"] = df["ticker_1"].map(lambda x: _subtheme_for_ticker(_to_text(x)))
    df["ticker_2_subtheme_guess"] = df["ticker_2"].map(lambda x: _subtheme_for_ticker(_to_text(x)))
    df["ticker_1_seed_distance"] = df["ticker_1"].map(lambda x: _to_float(node_maps.get(_to_text(x), {}).get("seed_distance")))
    df["ticker_2_seed_distance"] = df["ticker_2"].map(lambda x: _to_float(node_maps.get(_to_text(x), {}).get("seed_distance")))
    df["theme_name"] = theme_name

    df = df.sort_values(
        by=["combined_score", "residual_correlation", "raw_correlation", "ticker_1", "ticker_2"],
        ascending=[False, False, False, True, True],
        na_position="last",
        kind="mergesort",
    ).head(top_cross_sector_bridges)

    cols = [
        "theme_name",
        "ticker_1",
        "ticker_2",
        "combined_score",
        "raw_correlation",
        "residual_correlation",
        "residual_rolling_corr_mean",
        "edge_category",
        "sector_1",
        "industry_1",
        "sector_2",
        "industry_2",
        "ticker_1_is_seed",
        "ticker_2_is_seed",
        "ticker_1_subtheme_guess",
        "ticker_2_subtheme_guess",
        "ticker_1_seed_distance",
        "ticker_2_seed_distance",
        "source_family",
        "source_file",
    ]
    for col in cols:
        if col not in df.columns:
            df[col] = np.nan

    out = df[cols].copy()
    out.to_csv(output_dir / "datacenter_cross_sector_bridges_top.csv", index=False)
    return out


def write_priority_candidates(
    nodes: pd.DataFrame,
    theme_name: str,
    min_weighted_degree: float,
    min_best_seed_edge_score: float,
    top_priority_candidates: int,
    output_dir: Path,
) -> pd.DataFrame:
    df = nodes.copy()
    df = _with_subtheme_columns(df)

    is_seed = df.get("is_seed", pd.Series(False, index=df.index)).map(_to_bool)
    status = df.get("seed_status", pd.Series("", index=df.index)).fillna("").astype(str).str.upper()
    weighted = df.get("weighted_degree", pd.Series(0.0, index=df.index)).apply(_to_float)
    best_seed = df.get("best_seed_edge_score", pd.Series(np.nan, index=df.index)).apply(_to_float)
    best_first_hop = df.get("best_first_hop_edge_score", pd.Series(np.nan, index=df.index)).apply(_to_float)

    first_ok = (status == "FIRST_HOP") & (best_seed >= min_best_seed_edge_score)
    second_ok = (status == "SECOND_HOP") & (best_first_hop >= min_best_seed_edge_score)

    mask = (
        (~is_seed)
        & (status.isin(["FIRST_HOP", "SECOND_HOP"]))
        & (weighted >= min_weighted_degree)
        & (first_ok | second_ok)
    )

    out = df[mask].copy()
    out["theme_name"] = theme_name
    out["seed_status_order"] = out["seed_status"].map({"FIRST_HOP": 0, "SECOND_HOP": 1}).fillna(9)

    out = out.sort_values(
        by=["priority_score", "seed_status_order", "best_seed_edge_score", "weighted_degree", "ticker"],
        ascending=[False, True, False, False, True],
        na_position="last",
        kind="mergesort",
    ).head(top_priority_candidates)

    cols = [
        "theme_name",
        "ticker",
        "sector",
        "industry",
        "subtheme_guess",
        "seed_status",
        "seed_distance",
        "nearest_seed_tickers",
        "seed_neighbor_count",
        "seed_neighbors",
        "best_seed_edge_score",
        "best_seed_neighbor",
        "best_first_hop_edge_score",
        "best_first_hop_neighbor",
        "degree",
        "weighted_degree",
        "cross_sector_degree",
        "cross_industry_degree",
        "component_id",
        "component_size",
        "priority_score",
        "priority_tier",
    ]
    for col in cols:
        if col not in out.columns:
            out[col] = np.nan

    out = out[cols].copy()
    out.to_csv(output_dir / "datacenter_priority_candidates.csv", index=False)
    return out


def write_subtheme_groups(
    priority_candidates: pd.DataFrame,
    theme_name: str,
    output_dir: Path,
) -> pd.DataFrame:
    df = priority_candidates.copy()
    if df.empty:
        out_cols = [
            "theme_name",
            "subtheme_guess",
            "candidate_count",
            "high_priority_count",
            "medium_priority_count",
            "low_priority_count",
            "sector_count",
            "industry_count",
            "sectors",
            "industries",
            "top_priority_tickers",
            "top_weighted_degree_tickers",
            "top_seed_connected_tickers",
        ]
        empty = pd.DataFrame(columns=out_cols)
        empty.to_csv(output_dir / "datacenter_subtheme_groups.csv", index=False)
        return empty

    rows: list[dict[str, object]] = []
    for subtheme, grp in df.groupby("subtheme_guess", sort=True):
        g = grp.copy()
        top_priority = g.sort_values(
            by=["priority_score", "ticker"],
            ascending=[False, True],
            kind="mergesort",
        ).head(15)
        top_weighted = g.sort_values(
            by=["weighted_degree", "ticker"],
            ascending=[False, True],
            kind="mergesort",
        ).head(15)
        top_seed_connected = g.sort_values(
            by=["seed_neighbor_count", "best_seed_edge_score", "ticker"],
            ascending=[False, False, True],
            na_position="last",
            kind="mergesort",
        ).head(15)

        rows.append(
            {
                "theme_name": theme_name,
                "subtheme_guess": subtheme,
                "candidate_count": int(len(g)),
                "high_priority_count": int((g["priority_tier"] == "HIGH").sum()),
                "medium_priority_count": int((g["priority_tier"] == "MEDIUM").sum()),
                "low_priority_count": int((g["priority_tier"] == "LOW").sum()),
                "sector_count": int(g["sector"].fillna("").astype(str).str.strip().replace("", np.nan).nunique(dropna=True)),
                "industry_count": int(g["industry"].fillna("").astype(str).str.strip().replace("", np.nan).nunique(dropna=True)),
                "sectors": _sorted_join(g["sector"].tolist()),
                "industries": _sorted_join(g["industry"].tolist()),
                "top_priority_tickers": _sorted_join(top_priority["ticker"].tolist()),
                "top_weighted_degree_tickers": _sorted_join(top_weighted["ticker"].tolist()),
                "top_seed_connected_tickers": _sorted_join(top_seed_connected["ticker"].tolist()),
            }
        )

    out = pd.DataFrame(rows)
    out = out.sort_values(
        by=["high_priority_count", "candidate_count", "subtheme_guess"],
        ascending=[False, False, True],
        kind="mergesort",
    )
    out.to_csv(output_dir / "datacenter_subtheme_groups.csv", index=False)
    return out


def write_seed_summary_compact(
    seed_summary: pd.DataFrame,
    top_seed_neighbors: pd.DataFrame,
    theme_name: str,
    output_dir: Path,
) -> pd.DataFrame:
    df = seed_summary.copy()
    df["theme_name"] = theme_name

    if "seed_ticker" not in df.columns and "ticker" in df.columns:
        df["seed_ticker"] = df["ticker"]

    df["subtheme_guess"] = df.apply(
        lambda r: classify_subtheme(
            r.get("seed_ticker"),
            r.get("sector"),
            r.get("industry"),
            r.get("direct_non_seed_neighbors"),
            r.get("seed_ticker"),
            "SEED",
        ),
        axis=1,
    )

    neighbors_map: dict[str, str] = {}
    if not top_seed_neighbors.empty:
        t = top_seed_neighbors.copy()
        t = t.sort_values(
            by=["seed_ticker", "combined_score", "non_seed_ticker"],
            ascending=[True, False, True],
            kind="mergesort",
        )
        for seed, grp in t.groupby("seed_ticker", sort=True):
            neighbors_map[seed] = _sorted_join(grp.head(20)["non_seed_ticker"].tolist())

    df["top_direct_neighbors"] = df["seed_ticker"].map(lambda x: neighbors_map.get(_to_text(x), ""))

    cols = [
        "theme_name",
        "seed_ticker",
        "found_in_graph",
        "sector",
        "industry",
        "subtheme_guess",
        "degree",
        "weighted_degree",
        "direct_non_seed_neighbor_count",
        "cross_sector_direct_edge_count",
        "cross_industry_direct_edge_count",
        "best_direct_edge_score",
        "best_direct_neighbor",
        "top_direct_neighbors",
    ]
    for col in cols:
        if col not in df.columns:
            df[col] = np.nan

    out = df[cols].copy()
    found = out.get("found_in_graph", pd.Series(False, index=out.index)).map(_to_bool)
    out["_found_order"] = np.where(found, 0, 1)
    out = out.sort_values(
        by=["_found_order", "weighted_degree", "seed_ticker"],
        ascending=[True, False, True],
        na_position="last",
        kind="mergesort",
    ).drop(columns=["_found_order"])
    out.to_csv(output_dir / "datacenter_seed_summary_compact.csv", index=False)
    return out


def write_cliques_top(
    cliques: pd.DataFrame | None,
    nodes: pd.DataFrame,
    theme_name: str,
    output_dir: Path,
) -> pd.DataFrame:
    cols = [
        "theme_name",
        "clique_id",
        "clique_size",
        "tickers",
        "seed_count",
        "seed_tickers",
        "first_hop_count",
        "second_hop_count",
        "sector_count",
        "industry_count",
        "sectors",
        "industries",
        "average_combined_score",
        "min_combined_score",
        "max_combined_score",
        "cross_sector_edge_count",
        "cross_industry_edge_count",
        "edge_categories",
        "subtheme_guesses",
    ]

    if cliques is None:
        out = pd.DataFrame(columns=cols)
        out.to_csv(output_dir / "datacenter_cliques_top.csv", index=False)
        return out

    node_maps = _build_node_maps(nodes)
    first_hops = {
        _to_text(r.get("ticker"))
        for _, r in nodes.iterrows()
        if _to_text(r.get("seed_status")).upper() == "FIRST_HOP"
    }

    rows: list[dict[str, object]] = []
    for _, row in cliques.iterrows():
        tickers = parse_list_column(row.get("tickers"))
        seed_count = int(_to_float(row.get("seed_count")) if not pd.isna(_to_float(row.get("seed_count"))) else 0)
        has_first_hop = any(t in first_hops for t in tickers)
        if not (seed_count >= 1 or has_first_hop):
            continue

        subthemes = []
        for t in tickers:
            nr = node_maps.get(t, {})
            subthemes.append(
                classify_subtheme(
                    t,
                    nr.get("sector"),
                    nr.get("industry"),
                    nr.get("seed_neighbors"),
                    nr.get("nearest_seed_tickers"),
                    nr.get("seed_status"),
                )
            )

        out_row = row.to_dict()
        out_row["theme_name"] = theme_name
        out_row["subtheme_guesses"] = _sorted_join(subthemes)
        rows.append(out_row)

    if rows:
        out = pd.DataFrame(rows)
    else:
        out = pd.DataFrame(columns=cols)

    for col in cols:
        if col not in out.columns:
            out[col] = np.nan

    out = out[cols].copy()
    out = out.sort_values(
        by=["seed_count", "clique_size", "average_combined_score", "tickers"],
        ascending=[False, False, False, True],
        kind="mergesort",
    ).head(300)
    out.to_csv(output_dir / "datacenter_cliques_top.csv", index=False)
    return out


def write_validation_shortlist(
    nodes: pd.DataFrame,
    priority_candidates: pd.DataFrame,
    multi_seed_candidates: pd.DataFrame,
    bridges_top: pd.DataFrame,
    theme_name: str,
    min_seed_neighbor_count: int,
    output_dir: Path,
) -> pd.DataFrame:
    base = nodes.copy()
    base = base[~base.get("is_seed", pd.Series(False, index=base.index)).map(_to_bool)].copy()
    base = _with_subtheme_columns(base)

    multi_set = set(multi_seed_candidates.get("ticker", pd.Series([], dtype=str)).astype(str).tolist())
    bridge_tickers = set()
    if not bridges_top.empty:
        bridge_tickers.update(bridges_top.get("ticker_1", pd.Series([], dtype=str)).astype(str).tolist())
        bridge_tickers.update(bridges_top.get("ticker_2", pd.Series([], dtype=str)).astype(str).tolist())

    # Small deterministic choice for ambiguous source-union behavior:
    # start from all non-seed nodes, then mark reasons using the three source reports.
    reason_map: dict[str, set[str]] = defaultdict(set)
    for _, row in base.iterrows():
        t = _to_text(row.get("ticker"))
        if not t:
            continue

        tier = _to_text(row.get("priority_tier")).upper()
        if tier == "HIGH":
            reason_map[t].add("HIGH_PRIORITY")

        if _to_float(row.get("seed_neighbor_count")) >= float(min_seed_neighbor_count):
            reason_map[t].add("MULTI_SEED_CONNECTED")

        if t in multi_set:
            reason_map[t].add("MULTI_SEED_CONNECTED")

        if t in bridge_tickers:
            reason_map[t].add("CROSS_SECTOR_BRIDGE")

        if _to_text(row.get("subtheme_guess")) in CORE_SUBTHEMES:
            reason_map[t].add("CORE_DATACENTER_SUBTHEME")

    keep_tickers = sorted([t for t, reasons in reason_map.items() if reasons])
    out = base[base["ticker"].astype(str).isin(keep_tickers)].copy()
    out["theme_name"] = theme_name
    out["validation_reason"] = out["ticker"].map(lambda t: _sorted_join(sorted(reason_map.get(_to_text(t), set()))))

    cols = [
        "theme_name",
        "ticker",
        "sector",
        "industry",
        "subtheme_guess",
        "priority_score",
        "priority_tier",
        "seed_status",
        "seed_distance",
        "seed_neighbor_count",
        "seed_neighbors",
        "best_seed_edge_score",
        "best_seed_neighbor",
        "best_first_hop_edge_score",
        "best_first_hop_neighbor",
        "weighted_degree",
        "cross_sector_degree",
        "cross_industry_degree",
        "validation_reason",
    ]
    for col in cols:
        if col not in out.columns:
            out[col] = np.nan

    tier_rank = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    out["_tier_rank"] = out["priority_tier"].map(lambda x: tier_rank.get(_to_text(x), 9))
    out = out.sort_values(
        by=["_tier_rank", "priority_score", "seed_neighbor_count", "ticker"],
        ascending=[True, False, False, True],
        kind="mergesort",
    ).drop(columns=["_tier_rank"]).head(300)

    out = out[cols].copy()
    out.to_csv(output_dir / "datacenter_validation_shortlist.csv", index=False)
    return out


def write_core_and_broad_shortlists(
    validation_shortlist: pd.DataFrame,
    theme_name: str,
    min_seed_neighbor_count: int,
    output_dir: Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    src = validation_shortlist.copy()

    # Core shortlist: operating datacenter ecosystem candidates.
    core = src[src["subtheme_guess"].isin(sorted(CORE_SUBTHEMES))].copy()
    core = core[core.get("sector", "").astype(str) != "Financial Services"].copy()
    core = core[~core["industry"].map(lambda v: _industry_contains_any(v, FINANCIAL_INDUSTRY_TERMS + NON_CORE_INDUSTRY_TERMS))].copy()

    core_reason_map: dict[str, str] = {}
    for _, row in core.iterrows():
        reasons = {
            "CORE_DATACENTER_SUBTHEME",
            "NON_FINANCIAL_OPERATING_COMPANY",
        }
        status = _to_text(row.get("seed_status")).upper()
        if status == "FIRST_HOP":
            reasons.add("FIRST_HOP")
        if status == "SECOND_HOP":
            reasons.add("SECOND_HOP")
        if _to_float(row.get("seed_neighbor_count")) >= float(min_seed_neighbor_count):
            reasons.add("MULTI_SEED_CONNECTED")
        if _to_float(row.get("cross_sector_degree")) > 0:
            reasons.add("CROSS_SECTOR_RELEVANT")
        core_reason_map[_to_text(row.get("ticker"))] = _sorted_join(sorted(reasons))

    core["theme_name"] = theme_name
    core["core_validation_reason"] = core["ticker"].map(lambda t: core_reason_map.get(_to_text(t), ""))
    core["_tier_rank"] = core["priority_tier"].map({"HIGH": 0, "MEDIUM": 1, "LOW": 2}).fillna(9)
    core = core.sort_values(
        by=["_tier_rank", "priority_score", "seed_neighbor_count", "best_seed_edge_score", "ticker"],
        ascending=[True, False, False, False, True],
        na_position="last",
        kind="mergesort",
    ).drop(columns=["_tier_rank"]).head(300)

    core_cols = [
        "theme_name",
        "ticker",
        "sector",
        "industry",
        "subtheme_guess",
        "priority_score",
        "priority_tier",
        "seed_status",
        "seed_distance",
        "seed_neighbor_count",
        "seed_neighbors",
        "best_seed_edge_score",
        "best_seed_neighbor",
        "best_first_hop_edge_score",
        "best_first_hop_neighbor",
        "weighted_degree",
        "cross_sector_degree",
        "cross_industry_degree",
        "validation_reason",
        "core_validation_reason",
    ]
    for col in core_cols:
        if col not in core.columns:
            core[col] = np.nan
    core = core[core_cols].copy()
    core.to_csv(output_dir / "datacenter_core_validation_shortlist.csv", index=False)

    # Broad beta shortlist: statistically connected but less directly operational names.
    broad = src.copy()
    broad_mask = (
        broad["subtheme_guess"].isin([
            "OTHER_OR_UNCLASSIFIED",
            "BROAD_TECH_OR_INDUSTRIAL_BETA",
            "SOFTWARE_PLATFORM_ADJACENT",
        ])
        | (broad.get("sector", "").astype(str) == "Financial Services")
        | broad["industry"].map(lambda v: _industry_contains_any(v, FINANCIAL_INDUSTRY_TERMS + NON_CORE_INDUSTRY_TERMS))
    )
    broad = broad[broad_mask].copy()

    broad_reason_map: dict[str, str] = {}
    for _, row in broad.iterrows():
        reasons = {"STATISTICAL_BETA_CANDIDATE"}
        subtheme = _to_text(row.get("subtheme_guess"))
        sector = _to_text(row.get("sector"))
        industry = _to_text(row.get("industry"))
        if subtheme == "OTHER_OR_UNCLASSIFIED":
            reasons.add("OTHER_OR_UNCLASSIFIED")
        if subtheme == "BROAD_TECH_OR_INDUSTRIAL_BETA":
            reasons.add("BROAD_TECH_OR_INDUSTRIAL_BETA")
        if subtheme == "SOFTWARE_PLATFORM_ADJACENT":
            reasons.add("SOFTWARE_PLATFORM_ADJACENT")
        if sector == "Financial Services":
            reasons.add("FINANCIAL_SERVICES")
        if _industry_contains_any(industry, FINANCIAL_INDUSTRY_TERMS):
            reasons.add("FINANCIAL_OR_FUND_INDUSTRY")
        if _industry_contains_any(industry, NON_CORE_INDUSTRY_TERMS):
            reasons.add("NON_CORE_CONSUMER_OR_REAL_ESTATE")
        broad_reason_map[_to_text(row.get("ticker"))] = _sorted_join(sorted(reasons))

    broad["theme_name"] = theme_name
    broad["broad_beta_reason"] = broad["ticker"].map(lambda t: broad_reason_map.get(_to_text(t), ""))
    broad = broad.sort_values(
        by=["priority_score", "seed_neighbor_count", "weighted_degree", "ticker"],
        ascending=[False, False, False, True],
        kind="mergesort",
    ).head(300)

    broad_cols = [
        "theme_name",
        "ticker",
        "sector",
        "industry",
        "subtheme_guess",
        "priority_score",
        "priority_tier",
        "seed_status",
        "seed_distance",
        "seed_neighbor_count",
        "seed_neighbors",
        "best_seed_edge_score",
        "best_seed_neighbor",
        "best_first_hop_edge_score",
        "best_first_hop_neighbor",
        "weighted_degree",
        "cross_sector_degree",
        "cross_industry_degree",
        "validation_reason",
        "broad_beta_reason",
    ]
    for col in broad_cols:
        if col not in broad.columns:
            broad[col] = np.nan
    broad = broad[broad_cols].copy()
    broad.to_csv(output_dir / "datacenter_broad_beta_candidates.csv", index=False)

    return core, broad


def _markdown_table(df: pd.DataFrame, columns: list[str], top_n: int = 10) -> str:
    if df.empty:
        return "(no rows)"
    show = df[columns].head(top_n).copy()
    lines = []
    header = "| " + " | ".join(columns) + " |"
    sep = "| " + " | ".join(["---"] * len(columns)) + " |"
    lines.append(header)
    lines.append(sep)
    for _, row in show.iterrows():
        cells = []
        for c in columns:
            v = row.get(c)
            if isinstance(v, float) and not pd.isna(v):
                text = str(v)
            else:
                text = _to_text(v)
            text = text.replace("|", "\\|")
            cells.append(text)
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def write_markdown_report(
    input_dir: Path,
    output_dir: Path,
    theme_name: str,
    args: argparse.Namespace,
    seed_summary_compact: pd.DataFrame,
    priority_candidates: pd.DataFrame,
    multi_seed_candidates: pd.DataFrame,
    bridges_top: pd.DataFrame,
    subtheme_groups: pd.DataFrame,
    core_validation_shortlist: pd.DataFrame,
    broad_beta_candidates: pd.DataFrame,
) -> None:
    seeds_found = int(seed_summary_compact.get("found_in_graph", pd.Series([], dtype=bool)).map(_to_bool).sum())
    lines: list[str] = []
    lines.append(f"# {theme_name.title()} Focused Ecosystem Report")
    lines.append("")
    lines.append("## Run Context")
    lines.append(f"- input_dir: {input_dir}")
    lines.append(f"- output_dir: {output_dir}")
    lines.append(f"- theme_name: {theme_name}")
    lines.append("- thresholds used:")
    lines.append(f"  - min_best_seed_edge_score: {args.min_best_seed_edge_score}")
    lines.append(f"  - min_seed_neighbor_count: {args.min_seed_neighbor_count}")
    lines.append(f"  - min_weighted_degree: {args.min_weighted_degree}")
    lines.append(f"  - top_neighbors_per_seed: {args.top_neighbors_per_seed}")
    lines.append(f"  - top_priority_candidates: {args.top_priority_candidates}")
    lines.append(f"  - top_cross_sector_bridges: {args.top_cross_sector_bridges}")
    lines.append(f"  - top_multi_seed_candidates: {args.top_multi_seed_candidates}")
    lines.append("")
    lines.append("## Counts")
    lines.append(f"- seeds_found_in_graph: {seeds_found}")
    lines.append(f"- priority_candidates: {len(priority_candidates)}")
    lines.append(f"- multi_seed_candidates: {len(multi_seed_candidates)}")
    lines.append(f"- cross_sector_bridges: {len(bridges_top)}")
    lines.append(f"- subthemes: {subtheme_groups['subtheme_guess'].nunique() if not subtheme_groups.empty else 0}")
    lines.append(f"- core_validation_candidates: {len(core_validation_shortlist)}")
    lines.append(f"- broad_beta_candidates: {len(broad_beta_candidates)}")
    lines.append("")

    lines.append("## Top 10 Priority Candidates")
    lines.append(_markdown_table(
        priority_candidates.sort_values(
            by=["priority_score", "ticker"],
            ascending=[False, True],
            kind="mergesort",
        ),
        ["ticker", "subtheme_guess", "priority_score", "priority_tier", "seed_status", "seed_neighbor_count"],
        top_n=10,
    ))
    lines.append("")

    lines.append("## Top 10 Multi-Seed Candidates")
    lines.append(_markdown_table(
        multi_seed_candidates.sort_values(
            by=["seed_neighbor_count", "priority_score", "ticker"],
            ascending=[False, False, True],
            kind="mergesort",
        ),
        ["ticker", "subtheme_guess", "seed_neighbor_count", "best_seed_edge_score", "priority_score"],
        top_n=10,
    ))
    lines.append("")

    lines.append("## Top 10 Cross-Sector Bridges")
    lines.append(_markdown_table(
        bridges_top.sort_values(
            by=["combined_score", "ticker_1", "ticker_2"],
            ascending=[False, True, True],
            kind="mergesort",
        ),
        ["ticker_1", "ticker_2", "combined_score", "ticker_1_subtheme_guess", "ticker_2_subtheme_guess"],
        top_n=10,
    ))
    lines.append("")

    lines.append("## Subtheme Summary Counts")
    lines.append(_markdown_table(
        subtheme_groups.sort_values(
            by=["high_priority_count", "candidate_count", "subtheme_guess"],
            ascending=[False, False, True],
            kind="mergesort",
        ),
        ["subtheme_guess", "candidate_count", "high_priority_count", "medium_priority_count", "low_priority_count"],
        top_n=10,
    ))
    lines.append("")

    lines.append("## Top 10 Core Validation Candidates")
    lines.append(_markdown_table(
        core_validation_shortlist.sort_values(
            by=["priority_score", "ticker"],
            ascending=[False, True],
            kind="mergesort",
        ),
        ["ticker", "subtheme_guess", "priority_score", "priority_tier", "seed_status", "seed_neighbor_count"],
        top_n=10,
    ))
    lines.append("")

    lines.append("## Top 10 Broad Beta Candidates")
    lines.append(_markdown_table(
        broad_beta_candidates.sort_values(
            by=["priority_score", "ticker"],
            ascending=[False, True],
            kind="mergesort",
        ),
        ["ticker", "subtheme_guess", "priority_score", "priority_tier", "seed_status", "seed_neighbor_count"],
        top_n=10,
    ))
    lines.append("")

    lines.append("## Important Notes")
    lines.append("- These outputs are statistical candidate signals, not proof of business relationships.")
    lines.append("- All candidates require manual business validation before investment use.")
    lines.append("- datacenter_core_validation_shortlist.csv is the preferred manual validation starting point.")
    lines.append("- datacenter_broad_beta_candidates.csv contains statistically connected but less directly operational candidates.")
    lines.append("- Financial Services and fund-like/market-beta candidates are intentionally separated from the operating datacenter ecosystem list.")

    (output_dir / "datacenter_focused_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_readme(output_dir: Path) -> None:
    lines = [
        "# Datacenter Focused Report Outputs",
        "",
        "This folder contains V6c focused reports generated from V6b graph outputs.",
        "",
        "## Files",
        "- top_seed_neighbors_by_seed.csv: top direct non-seed neighbors for each seed ticker.",
        "- multi_seed_connected_candidates.csv: non-seed first-hop candidates connected to multiple seeds.",
        "- datacenter_cross_sector_bridges_top.csv: top cross-sector bridge edges involving seed/first-hop endpoints.",
        "- datacenter_priority_candidates.csv: ranked first-hop/second-hop non-seed candidates using deterministic priority_score.",
        "- datacenter_subtheme_groups.csv: grouped summary by deterministic subtheme_guess.",
        "- datacenter_seed_summary_compact.csv: compact summary of each seed and top direct neighbors.",
        "- datacenter_cliques_top.csv: strongest cliques containing at least one seed or first-hop ticker.",
        "- datacenter_validation_shortlist.csv: concise manual validation list with reason tags.",
        "- datacenter_core_validation_shortlist.csv: clean operating-company shortlist for manual validation.",
        "- datacenter_broad_beta_candidates.csv: broad beta/statistical candidates separated from the core list.",
        "- datacenter_focused_report.md: compact markdown summary (top 10 sections).",
        "- datacenter_report_readme.md: this file.",
    ]
    (output_dir / "datacenter_report_readme.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    file_map = validate_input_files(input_dir)
    data = load_inputs(file_map)

    nodes = data["seed_ecosystem_nodes.csv"].copy()
    nodes = compute_priority_scores(nodes)

    edges = data["seed_ecosystem_edges.csv"]
    direct_edges = data["seed_ecosystem_direct_seed_edges.csv"]
    bridges = data["seed_ecosystem_cross_sector_bridges.csv"]
    seed_summary = data["seed_ecosystem_datacenter_summary.csv"]

    node_maps = _build_node_maps(nodes)

    top_seed_neighbors = write_top_seed_neighbors(
        direct_edges=direct_edges,
        theme_name=args.theme_name,
        top_neighbors_per_seed=args.top_neighbors_per_seed,
        output_dir=output_dir,
        node_maps=node_maps,
    )

    multi_seed_candidates = write_multi_seed_candidates(
        nodes=nodes,
        theme_name=args.theme_name,
        min_seed_neighbor_count=args.min_seed_neighbor_count,
        min_best_seed_edge_score=args.min_best_seed_edge_score,
        top_multi_seed_candidates=args.top_multi_seed_candidates,
        output_dir=output_dir,
    )

    bridges_top = write_cross_sector_bridges(
        bridges=bridges,
        nodes=nodes,
        theme_name=args.theme_name,
        top_cross_sector_bridges=args.top_cross_sector_bridges,
        output_dir=output_dir,
    )

    priority_candidates = write_priority_candidates(
        nodes=nodes,
        theme_name=args.theme_name,
        min_weighted_degree=args.min_weighted_degree,
        min_best_seed_edge_score=args.min_best_seed_edge_score,
        top_priority_candidates=args.top_priority_candidates,
        output_dir=output_dir,
    )

    subtheme_groups = write_subtheme_groups(
        priority_candidates=priority_candidates,
        theme_name=args.theme_name,
        output_dir=output_dir,
    )

    seed_summary_compact = write_seed_summary_compact(
        seed_summary=seed_summary,
        top_seed_neighbors=top_seed_neighbors,
        theme_name=args.theme_name,
        output_dir=output_dir,
    )

    cliques_df = data.get("seed_ecosystem_cliques_3plus.csv")
    cliques_top = write_cliques_top(
        cliques=cliques_df,
        nodes=nodes,
        theme_name=args.theme_name,
        output_dir=output_dir,
    )

    validation_shortlist = write_validation_shortlist(
        nodes=nodes,
        priority_candidates=priority_candidates,
        multi_seed_candidates=multi_seed_candidates,
        bridges_top=bridges_top,
        theme_name=args.theme_name,
        min_seed_neighbor_count=args.min_seed_neighbor_count,
        output_dir=output_dir,
    )

    core_validation_shortlist, broad_beta_candidates = write_core_and_broad_shortlists(
        validation_shortlist=validation_shortlist,
        theme_name=args.theme_name,
        min_seed_neighbor_count=args.min_seed_neighbor_count,
        output_dir=output_dir,
    )

    write_markdown_report(
        input_dir=input_dir,
        output_dir=output_dir,
        theme_name=args.theme_name,
        args=args,
        seed_summary_compact=seed_summary_compact,
        priority_candidates=priority_candidates,
        multi_seed_candidates=multi_seed_candidates,
        bridges_top=bridges_top,
        subtheme_groups=subtheme_groups,
        core_validation_shortlist=core_validation_shortlist,
        broad_beta_candidates=broad_beta_candidates,
    )
    write_readme(output_dir)

    print(f"SUMMARY input_dir={input_dir}")
    print(f"SUMMARY output_dir={output_dir}")
    print(f"SUMMARY theme_name={args.theme_name}")
    print(f"SUMMARY top_neighbors_per_seed={args.top_neighbors_per_seed}")
    print(f"SUMMARY min_best_seed_edge_score={args.min_best_seed_edge_score}")
    print(f"SUMMARY min_seed_neighbor_count={args.min_seed_neighbor_count}")
    print(f"SUMMARY min_weighted_degree={args.min_weighted_degree}")
    print(f"SUMMARY input_edges={len(edges)}")
    print(f"SUMMARY input_nodes={len(nodes)}")
    print(f"SUMMARY input_direct_seed_edges={len(direct_edges)}")
    print(f"SUMMARY rows_top_seed_neighbors_by_seed={len(top_seed_neighbors)}")
    print(f"SUMMARY rows_multi_seed_connected_candidates={len(multi_seed_candidates)}")
    print(f"SUMMARY rows_datacenter_cross_sector_bridges_top={len(bridges_top)}")
    print(f"SUMMARY rows_datacenter_priority_candidates={len(priority_candidates)}")
    print(f"SUMMARY rows_datacenter_subtheme_groups={len(subtheme_groups)}")
    print(f"SUMMARY rows_datacenter_seed_summary_compact={len(seed_summary_compact)}")
    print(f"SUMMARY rows_datacenter_cliques_top={len(cliques_top)}")
    print(f"SUMMARY rows_datacenter_validation_shortlist={len(validation_shortlist)}")
    print(f"SUMMARY rows_datacenter_core_validation_shortlist={len(core_validation_shortlist)}")
    print(f"SUMMARY rows_datacenter_broad_beta_candidates={len(broad_beta_candidates)}")
    print("SUMMARY status=OK")


if __name__ == "__main__":
    main()
