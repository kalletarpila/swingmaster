#!/usr/bin/env python3
"""
Datacenter Network Interpretation (V7b) Reporting Script

Purpose:
Read V7 (datacenter_network_centrality) outputs and produce interpretable,
prioritized reports for manual business validation.

V7b is a reporting layer only. It does not:
- Recompute graph edges
- Recompute correlations
- Recompute centrality from raw data
- Access SQLite
- Access external services

Input:
- V7 output directory (--v7-dir)

Output:
- 8 CSV files with operating relevance and ecosystem bridge metrics
- 2 markdown files (summary and readme)
- Machine-readable SUMMARY lines

Dependencies:
- argparse, pathlib, pandas, numpy, collections
"""

import argparse
import sys
from pathlib import Path
from collections import defaultdict
from typing import Optional

import numpy as np
import pandas as pd


# =============================================================================
# CONSTANTS
# =============================================================================

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

BROAD_NON_CORE_SUBTHEMES = {
    "OTHER_OR_UNCLASSIFIED",
    "BROAD_TECH_OR_INDUSTRIAL_BETA",
    "SOFTWARE_PLATFORM_ADJACENT",
    "SEED_UNCLASSIFIED",
}

SEED_SUBTHEME_MAPPING = {
    # DATACENTER_REIT
    "EQIX": "DATACENTER_REIT",
    "DLR": "DATACENTER_REIT",
    "AMT": "DATACENTER_REIT",
    "CCI": "DATACENTER_REIT",
    "SBAC": "DATACENTER_REIT",
    # POWER_GENERATION_UTILITIES
    "CEG": "POWER_GENERATION_UTILITIES",
    "VST": "POWER_GENERATION_UTILITIES",
    "NEE": "POWER_GENERATION_UTILITIES",
    "TLN": "POWER_GENERATION_UTILITIES",
    "NRG": "POWER_GENERATION_UTILITIES",
    "PEG": "POWER_GENERATION_UTILITIES",
    "SO": "POWER_GENERATION_UTILITIES",
    "DUK": "POWER_GENERATION_UTILITIES",
    "PCG": "POWER_GENERATION_UTILITIES",
    "XEL": "POWER_GENERATION_UTILITIES",
    "ETR": "POWER_GENERATION_UTILITIES",
    "EXC": "POWER_GENERATION_UTILITIES",
    "PPL": "POWER_GENERATION_UTILITIES",
    "SRE": "POWER_GENERATION_UTILITIES",
    "DTE": "POWER_GENERATION_UTILITIES",
    "AEP": "POWER_GENERATION_UTILITIES",
    "ED": "POWER_GENERATION_UTILITIES",
    # ENGINEERING_CONSTRUCTION_INFRA
    "PWR": "ENGINEERING_CONSTRUCTION_INFRA",
    "MTZ": "ENGINEERING_CONSTRUCTION_INFRA",
    "FIX": "ENGINEERING_CONSTRUCTION_INFRA",
    "EME": "ENGINEERING_CONSTRUCTION_INFRA",
    "STRL": "ENGINEERING_CONSTRUCTION_INFRA",
    "PRIM": "ENGINEERING_CONSTRUCTION_INFRA",
    "IESC": "ENGINEERING_CONSTRUCTION_INFRA",
    "ACA": "ENGINEERING_CONSTRUCTION_INFRA",
    "ECG": "ENGINEERING_CONSTRUCTION_INFRA",
    "MYRG": "ENGINEERING_CONSTRUCTION_INFRA",
    "FLR": "ENGINEERING_CONSTRUCTION_INFRA",
    "ACM": "ENGINEERING_CONSTRUCTION_INFRA",
    "DY": "ENGINEERING_CONSTRUCTION_INFRA",
    "J": "ENGINEERING_CONSTRUCTION_INFRA",
    # ELECTRICAL_POWER_EQUIPMENT
    "VRT": "ELECTRICAL_POWER_EQUIPMENT",
    "ETN": "ELECTRICAL_POWER_EQUIPMENT",
    "NVT": "ELECTRICAL_POWER_EQUIPMENT",
    "HUBB": "ELECTRICAL_POWER_EQUIPMENT",
    "POWL": "ELECTRICAL_POWER_EQUIPMENT",
    "GNRC": "ELECTRICAL_POWER_EQUIPMENT",
    "EMR": "ELECTRICAL_POWER_EQUIPMENT",
    "GEV": "ELECTRICAL_POWER_EQUIPMENT",
    "TT": "ELECTRICAL_POWER_EQUIPMENT",
    "DOV": "ELECTRICAL_POWER_EQUIPMENT",
    "AEIS": "ELECTRICAL_POWER_EQUIPMENT",
    "PH": "ELECTRICAL_POWER_EQUIPMENT",
    "GTES": "ELECTRICAL_POWER_EQUIPMENT",
    "NPO": "ELECTRICAL_POWER_EQUIPMENT",
    "IEX": "ELECTRICAL_POWER_EQUIPMENT",
    "NDSN": "ELECTRICAL_POWER_EQUIPMENT",
    "ITT": "ELECTRICAL_POWER_EQUIPMENT",
    "GGG": "ELECTRICAL_POWER_EQUIPMENT",
    "CMI": "ELECTRICAL_POWER_EQUIPMENT",
    "RRX": "ELECTRICAL_POWER_EQUIPMENT",
    "CR": "ELECTRICAL_POWER_EQUIPMENT",
    "PNR": "ELECTRICAL_POWER_EQUIPMENT",
    "IR": "ELECTRICAL_POWER_EQUIPMENT",
    "FLS": "ELECTRICAL_POWER_EQUIPMENT",
    "AME": "ELECTRICAL_POWER_EQUIPMENT",
    "CSW": "ELECTRICAL_POWER_EQUIPMENT",
    "JCI": "ELECTRICAL_POWER_EQUIPMENT",
    "SPXC": "ELECTRICAL_POWER_EQUIPMENT",
    "ROK": "ELECTRICAL_POWER_EQUIPMENT",
    "ITW": "ELECTRICAL_POWER_EQUIPMENT",
    "KAI": "ELECTRICAL_POWER_EQUIPMENT",
    # SERVER_STORAGE_HARDWARE
    "DELL": "SERVER_STORAGE_HARDWARE",
    "HPE": "SERVER_STORAGE_HARDWARE",
    "SMCI": "SERVER_STORAGE_HARDWARE",
    "NTAP": "SERVER_STORAGE_HARDWARE",
    "PSTG": "SERVER_STORAGE_HARDWARE",
    "WDC": "SERVER_STORAGE_HARDWARE",
    "STX": "SERVER_STORAGE_HARDWARE",
    # NETWORKING_OPTICAL_CONNECTIVITY
    "ANET": "NETWORKING_OPTICAL_CONNECTIVITY",
    "CIEN": "NETWORKING_OPTICAL_CONNECTIVITY",
    "GLW": "NETWORKING_OPTICAL_CONNECTIVITY",
    "APH": "NETWORKING_OPTICAL_CONNECTIVITY",
    "TEL": "NETWORKING_OPTICAL_CONNECTIVITY",
    "COHR": "NETWORKING_OPTICAL_CONNECTIVITY",
    "LITE": "NETWORKING_OPTICAL_CONNECTIVITY",
    "AAOI": "NETWORKING_OPTICAL_CONNECTIVITY",
    "FN": "NETWORKING_OPTICAL_CONNECTIVITY",
    "FLEX": "NETWORKING_OPTICAL_CONNECTIVITY",
    "JBL": "NETWORKING_OPTICAL_CONNECTIVITY",
    "PLXS": "NETWORKING_OPTICAL_CONNECTIVITY",
    "TTMI": "NETWORKING_OPTICAL_CONNECTIVITY",
    "BDC": "NETWORKING_OPTICAL_CONNECTIVITY",
    "BHE": "NETWORKING_OPTICAL_CONNECTIVITY",
    "CTS": "NETWORKING_OPTICAL_CONNECTIVITY",
    "CLS": "NETWORKING_OPTICAL_CONNECTIVITY",
    "CSCO": "NETWORKING_OPTICAL_CONNECTIVITY",
    "NTCT": "NETWORKING_OPTICAL_CONNECTIVITY",
    "BELFB": "NETWORKING_OPTICAL_CONNECTIVITY",
    "ROG": "NETWORKING_OPTICAL_CONNECTIVITY",
    "LFUS": "NETWORKING_OPTICAL_CONNECTIVITY",
    "ZBRA": "NETWORKING_OPTICAL_CONNECTIVITY",
    # SEMICONDUCTOR_EQUIPMENT
    "ASML": "SEMICONDUCTOR_EQUIPMENT",
    "AMAT": "SEMICONDUCTOR_EQUIPMENT",
    "LRCX": "SEMICONDUCTOR_EQUIPMENT",
    "KLAC": "SEMICONDUCTOR_EQUIPMENT",
    "MKSI": "SEMICONDUCTOR_EQUIPMENT",
    "NVMI": "SEMICONDUCTOR_EQUIPMENT",
    "ONTO": "SEMICONDUCTOR_EQUIPMENT",
    "ENTG": "SEMICONDUCTOR_EQUIPMENT",
    "UCTT": "SEMICONDUCTOR_EQUIPMENT",
    "COHU": "SEMICONDUCTOR_EQUIPMENT",
    "VECO": "SEMICONDUCTOR_EQUIPMENT",
    "CAMT": "SEMICONDUCTOR_EQUIPMENT",
    "TER": "SEMICONDUCTOR_EQUIPMENT",
    "FORM": "SEMICONDUCTOR_EQUIPMENT",
    "ACLS": "SEMICONDUCTOR_EQUIPMENT",
    "AMKR": "SEMICONDUCTOR_EQUIPMENT",
    "ICHR": "SEMICONDUCTOR_EQUIPMENT",
    "KLIC": "SEMICONDUCTOR_EQUIPMENT",
    "PLAB": "SEMICONDUCTOR_EQUIPMENT",
    # SEMICONDUCTORS_AI_CHIPS
    "NVDA": "SEMICONDUCTORS_AI_CHIPS",
    "AMD": "SEMICONDUCTORS_AI_CHIPS",
    "AVGO": "SEMICONDUCTORS_AI_CHIPS",
    "MRVL": "SEMICONDUCTORS_AI_CHIPS",
    "MPWR": "SEMICONDUCTORS_AI_CHIPS",
    "TSM": "SEMICONDUCTORS_AI_CHIPS",
    "ARM": "SEMICONDUCTORS_AI_CHIPS",
    "MU": "SEMICONDUCTORS_AI_CHIPS",
    "QCOM": "SEMICONDUCTORS_AI_CHIPS",
    "NXPI": "SEMICONDUCTORS_AI_CHIPS",
    "MCHP": "SEMICONDUCTORS_AI_CHIPS",
    "ADI": "SEMICONDUCTORS_AI_CHIPS",
    "TXN": "SEMICONDUCTORS_AI_CHIPS",
    "LSCC": "SEMICONDUCTORS_AI_CHIPS",
    "RMBS": "SEMICONDUCTORS_AI_CHIPS",
    "ASX": "SEMICONDUCTORS_AI_CHIPS",
    "STM": "SEMICONDUCTORS_AI_CHIPS",
    "ON": "SEMICONDUCTORS_AI_CHIPS",
    "POWI": "SEMICONDUCTORS_AI_CHIPS",
    "GFS": "SEMICONDUCTORS_AI_CHIPS",
    "MTSI": "SEMICONDUCTORS_AI_CHIPS",
    "TSEM": "SEMICONDUCTORS_AI_CHIPS",
    "SITM": "SEMICONDUCTORS_AI_CHIPS",
    "ALGM": "SEMICONDUCTORS_AI_CHIPS",
    "VSH": "SEMICONDUCTORS_AI_CHIPS",
    "DIOD": "SEMICONDUCTORS_AI_CHIPS",
}

FINANCIAL_INDUSTRY_TERMS = [
    "asset management",
    "capital markets",
    "banks",
    "credit services",
    "insurance",
    "reit - hotel & motel",
    "auto & truck dealerships",
    "recreational vehicles",
]


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    return False


def _to_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _to_float(value: object) -> float:
    if pd.isna(value):
        return 0.0
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0


def _sorted_join(values: list[object]) -> str:
    cleaned = [
        _to_text(v)
        for v in values
        if pd.notna(v) and _to_text(v).strip()
    ]
    return ", ".join(sorted(set(cleaned)))


def _industry_contains_any(industry: object, terms: list[str]) -> bool:
    il = _to_text(industry).lower()
    return any(term.lower() in il for term in terms)


def parse_list_column(value: object) -> list[str]:
    if pd.isna(value):
        return []
    text = _to_text(value)
    if not text:
        return []
    return [
        s.strip()
        for s in text.split(",")
        if s.strip()
    ]


# =============================================================================
# ARGUMENT PARSING
# =============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Datacenter Network Interpretation (V7b) Reporting",
    )
    parser.add_argument(
        "--v7-dir",
        required=True,
        help="Path to V7 output directory",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Path for V7b output directory",
    )
    parser.add_argument(
        "--theme-name",
        default="datacenter",
        help="Theme name (default: datacenter)",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=300,
        help="Max rows for top reports (default: 300)",
    )
    parser.add_argument(
        "--min-operating-relevance-score",
        type=float,
        default=0.35,
        help="Minimum operating relevance score (default: 0.35)",
    )
    parser.add_argument(
        "--min-network-importance-score",
        type=float,
        default=0.30,
        help="Minimum V7 network importance score (default: 0.30)",
    )
    parser.add_argument(
        "--min-seed-subtheme-count",
        type=int,
        default=2,
        help="Minimum seed subtheme count for reports (default: 2)",
    )
    parser.add_argument(
        "--min-core-neighbor-count",
        type=int,
        default=3,
        help="Minimum core neighbor count (default: 3)",
    )
    parser.add_argument(
        "--min-cross-sector-degree",
        type=int,
        default=2,
        help="Minimum cross-sector degree (default: 2)",
    )
    parser.add_argument(
        "--min-bridge-score",
        type=float,
        default=0.20,
        help="Minimum bridge score (default: 0.20)",
    )
    return parser.parse_args()


# =============================================================================
# FILE VALIDATION AND LOADING
# =============================================================================

def validate_input_files(v7_dir: Path) -> dict[str, Path]:
    """Validate that required V7 input files exist."""
    required = [
        "datacenter_network_node_scores.csv",
        "datacenter_top_seed_anchors.csv",
        "datacenter_top_operating_non_seed_connectors.csv",
        "datacenter_broad_beta_network_connectors.csv",
        "datacenter_final_manual_review_queue.csv",
        "datacenter_edge_filtered_strong_network.csv",
    ]

    file_map = {}
    for fname in required:
        fpath = v7_dir / fname
        if not fpath.exists():
            raise FileNotFoundError(f"Required file missing: {fpath}")
        file_map[fname] = fpath

    optional = [
        "datacenter_subtheme_network_summary.csv",
        "datacenter_multi_seed_connectors.csv",
        "datacenter_cross_subtheme_bridges.csv",
        "datacenter_cross_sector_network_bridges.csv",
        "datacenter_clique_connectors.csv",
    ]

    for fname in optional:
        fpath = v7_dir / fname
        if fpath.exists():
            file_map[fname] = fpath

    return file_map


def load_inputs(file_map: dict[str, Path]) -> dict[str, pd.DataFrame]:
    """Load CSV files, normalizing booleans."""
    data = {}

    for key, fpath in file_map.items():
        df = pd.read_csv(fpath, dtype=str, keep_default_na=False)
        # Normalize booleans
        for col in df.columns:
            if col in [
                "is_seed",
                "cross_sector_edge",
                "cross_industry_edge",
                "seed_edge",
                "both_seed_edge",
            ]:
                df[col] = df[col].map(_to_bool)
        data[key] = df

    return data


# =============================================================================
# SEED SUBTHEME DERIVATION
# =============================================================================

def derive_seed_subtheme(row: pd.Series) -> str:
    """Derive seed_subtheme from subtheme_guess or ticker/sector/industry."""
    subtheme = _to_text(row.get("subtheme_guess"))

    # Use explicit mapping if available and not unclassified
    if subtheme and subtheme not in BROAD_NON_CORE_SUBTHEMES:
        return subtheme

    # Check SEED_SUBTHEME_MAPPING by ticker
    ticker = _to_text(row.get("ticker"))
    if ticker in SEED_SUBTHEME_MAPPING:
        return SEED_SUBTHEME_MAPPING[ticker]

    # Check sector/industry
    sector = _to_text(row.get("sector")).lower()
    industry = _to_text(row.get("industry")).lower()

    if "reit" in industry and "specialty" in industry:
        return "DATACENTER_REIT"
    if "utilities" in sector:
        return "POWER_GENERATION_UTILITIES"
    if "engineering" in industry or "construction" in industry:
        return "ENGINEERING_CONSTRUCTION_INFRA"
    if any(term in industry for term in ["electrical equipment", "specialty industrial machinery", "building products"]):
        return "ELECTRICAL_POWER_EQUIPMENT"
    if any(term in industry for term in ["computer hardware", "data storage"]):
        return "SERVER_STORAGE_HARDWARE"
    if any(term in industry for term in ["communication equipment", "electronic components"]):
        return "NETWORKING_OPTICAL_CONNECTIVITY"
    if "semiconductor equipment" in industry:
        return "SEMICONDUCTOR_EQUIPMENT"
    if "semiconductor" in industry:
        return "SEMICONDUCTORS_AI_CHIPS"

    return "OTHER_OR_UNCLASSIFIED"


def build_seed_subtheme_map(
    node_scores: pd.DataFrame,
) -> dict[str, str]:
    """Build ticker -> seed_subtheme mapping for all nodes."""
    out = {}
    for _, row in node_scores.iterrows():
        ticker = _to_text(row.get("ticker"))
        if _to_bool(row.get("is_seed")):
            out[ticker] = derive_seed_subtheme(row)
        else:
            out[ticker] = ""
    return out


# =============================================================================
# CONNECTIVITY COMPUTATION
# =============================================================================

def compute_seed_subtheme_connectivity(
    node_scores: pd.DataFrame,
    seed_subtheme_map: dict[str, str],
) -> dict[str, dict[str, object]]:
    """Compute seed subtheme connectivity for each node."""
    out = {}

    for _, row in node_scores.iterrows():
        ticker = _to_text(row.get("ticker"))
        seed_neighbors_str = _to_text(row.get("seed_neighbors_from_edges"))
        seed_neighbors = parse_list_column(seed_neighbors_str)

        # Map seed neighbors to seed subthemes
        seed_subthemes_set = set()
        for sn in seed_neighbors:
            st = seed_subtheme_map.get(_to_text(sn), "")
            if st:
                seed_subthemes_set.add(st)

        seed_subthemes_list = sorted(seed_subthemes_set)
        dominant = None
        dominant_count = 0

        if seed_subthemes_list:
            # Count occurrences
            seed_subtheme_counts = defaultdict(int)
            for sn in seed_neighbors:
                st = seed_subtheme_map.get(_to_text(sn), "")
                if st:
                    seed_subtheme_counts[st] += 1

            # Find dominant (alphabetically tie-break)
            max_count = max(seed_subtheme_counts.values()) if seed_subtheme_counts else 0
            candidates = [st for st, cnt in seed_subtheme_counts.items() if cnt == max_count]
            if candidates:
                dominant = sorted(candidates)[0]
                dominant_count = max_count

        out[ticker] = {
            "seed_subtheme_count": len(seed_subthemes_list),
            "seed_subthemes": seed_subthemes_list,
            "dominant_seed_subtheme": dominant if dominant else "",
            "dominant_seed_subtheme_count": dominant_count,
        }

    return out


# =============================================================================
# SCORING FUNCTIONS
# =============================================================================

def compute_operating_relevance_scores(
    node_scores: pd.DataFrame,
    seed_subtheme_connectivity: dict[str, dict[str, object]],
) -> dict[str, dict[str, float]]:
    """Compute operating relevance score for each node."""
    out = {}

    for ticker, row in node_scores.iterrows():
        subtheme = _to_text(row.get("subtheme_guess"))
        is_seed = _to_bool(row.get("is_seed"))

        # Operating subtheme score
        if subtheme in OPERATING_DATACENTER_SUBTHEMES:
            op_subtheme_score = 1.00
        elif is_seed and subtheme == "OTHER_OR_UNCLASSIFIED":
            # Seed with unclassified might still be operating
            seed_subtheme_info = seed_subtheme_connectivity.get(ticker, {})
            seed_st = _to_text(seed_subtheme_info.get("dominant_seed_subtheme", ""))
            if seed_st in OPERATING_DATACENTER_SUBTHEMES:
                op_subtheme_score = 0.60
            else:
                op_subtheme_score = 0.00
        elif subtheme == "SOFTWARE_PLATFORM_ADJACENT":
            op_subtheme_score = 0.30
        elif subtheme == "BROAD_TECH_OR_INDUSTRIAL_BETA":
            op_subtheme_score = 0.20
        else:
            op_subtheme_score = 0.00

        # Normalize metrics
        net_score = _to_float(row.get("network_importance_score", 0))
        norm_net_score = min(max(net_score, 0.0), 1.0)

        core_neighbors = _to_float(row.get("core_neighbor_count", 0))
        norm_core_neighbors = min(core_neighbors / 20.0, 1.0)

        seed_subtheme_info = seed_subtheme_connectivity.get(ticker, {})
        seed_subtheme_cnt = seed_subtheme_info.get("seed_subtheme_count", 0)
        norm_seed_subtheme_cnt = min(seed_subtheme_cnt / 4.0, 1.0)

        cross_sector = _to_float(row.get("cross_sector_degree", 0))
        norm_cross_sector = min(cross_sector / 25.0, 1.0)

        avg_core_score = _to_float(row.get("average_core_edge_score", 0))
        norm_avg_core_score = min(max(avg_core_score, 0.0), 1.0)

        # Operating relevance score formula
        op_rel_score = (
            0.30 * op_subtheme_score
            + 0.20 * norm_net_score
            + 0.15 * norm_core_neighbors
            + 0.15 * norm_seed_subtheme_cnt
            + 0.10 * norm_cross_sector
            + 0.10 * norm_avg_core_score
        )

        # Tier
        if op_rel_score >= 0.70:
            tier = "HIGH"
        elif op_rel_score >= 0.45:
            tier = "MEDIUM"
        else:
            tier = "LOW"

        out[ticker] = {
            "operating_relevance_score": op_rel_score,
            "operating_relevance_tier": tier,
        }

    return out


def compute_ecosystem_bridge_scores(
    node_scores: pd.DataFrame,
    seed_subtheme_connectivity: dict[str, dict[str, object]],
) -> dict[str, dict[str, object]]:
    """Compute ecosystem bridge score and type for each node."""
    out = {}

    for ticker, row in node_scores.iterrows():
        # Normalize metrics
        seed_subtheme_info = seed_subtheme_connectivity.get(ticker, {})
        seed_subtheme_cnt = seed_subtheme_info.get("seed_subtheme_count", 0)
        norm_seed_subtheme_cnt = min(seed_subtheme_cnt / 4.0, 1.0)

        neighbor_subtheme_cnt = _to_float(row.get("neighbor_subtheme_count", 0))
        norm_neighbor_subtheme_cnt = min(neighbor_subtheme_cnt / 5.0, 1.0)

        cross_sector = _to_float(row.get("cross_sector_degree", 0))
        norm_cross_sector = min(cross_sector / 25.0, 1.0)

        core_neighbors = _to_float(row.get("core_neighbor_count", 0))
        norm_core_neighbors = min(core_neighbors / 20.0, 1.0)

        avg_score = _to_float(row.get("average_edge_score", 0))
        norm_avg_score = min(max(avg_score, 0.0), 1.0)

        # Ecosystem bridge score formula
        eco_bridge_score = (
            0.30 * norm_seed_subtheme_cnt
            + 0.25 * norm_neighbor_subtheme_cnt
            + 0.20 * norm_cross_sector
            + 0.15 * norm_core_neighbors
            + 0.10 * norm_avg_score
        )

        out[ticker] = {
            "ecosystem_bridge_score": eco_bridge_score,
        }

    return out


def assign_bridge_types(
    node_scores: pd.DataFrame,
    seed_subtheme_connectivity: dict[str, dict[str, object]],
    args: argparse.Namespace,
) -> dict[str, str]:
    """Assign bridge type to each node."""
    out = {}

    for ticker, row in node_scores.iterrows():
        seed_subtheme_info = seed_subtheme_connectivity.get(ticker, {})
        seed_subtheme_cnt = seed_subtheme_info.get("seed_subtheme_count", 0)
        neighbor_subtheme_cnt = _to_float(row.get("neighbor_subtheme_count", 0))
        cross_sector = _to_float(row.get("cross_sector_degree", 0))
        core_neighbors = _to_float(row.get("core_neighbor_count", 0))

        bridge_type = "NOT_BRIDGE"

        if seed_subtheme_cnt >= args.min_seed_subtheme_count:
            bridge_type = "SEED_SUBTHEME_BRIDGE"
        elif neighbor_subtheme_cnt >= 3:
            bridge_type = "CROSS_SUBTHEME_BRIDGE"
        elif (cross_sector >= args.min_cross_sector_degree and
              core_neighbors >= args.min_core_neighbor_count):
            bridge_type = "CROSS_SECTOR_CORE_BRIDGE"
        elif core_neighbors >= args.min_core_neighbor_count:
            bridge_type = "CORE_DENSE_CONNECTOR"

        out[ticker] = bridge_type

    return out


def assign_interpretation_flags(
    node_scores: pd.DataFrame,
    operating_relevance_scores: dict[str, dict[str, object]],
    ecosystem_bridge_scores: dict[str, dict[str, object]],
    bridge_types: dict[str, str],
    args: argparse.Namespace,
) -> dict[str, str]:
    """Assign interpretation flag to each node."""
    out = {}

    for ticker, row in node_scores.iterrows():
        is_seed = _to_bool(row.get("is_seed"))
        subtheme = _to_text(row.get("subtheme_guess"))
        membership = _to_text(row.get("ecosystem_membership_class"))
        sector = _to_text(row.get("sector"))
        industry = _to_text(row.get("industry"))

        op_rel_score = operating_relevance_scores.get(ticker, {}).get("operating_relevance_score", 0)
        eco_bridge_score = ecosystem_bridge_scores.get(ticker, {}).get("ecosystem_bridge_score", 0)
        bridge_type = bridge_types.get(ticker, "NOT_BRIDGE")

        flag = "LOW_SPECIFICITY"

        # Priority order
        if is_seed:
            flag = "SEED_ANCHOR"
        elif (subtheme in OPERATING_DATACENTER_SUBTHEMES and
              membership == "CORE_VALIDATION" and
              op_rel_score >= args.min_operating_relevance_score):
            flag = "OPERATING_CORE"
        elif (eco_bridge_score >= args.min_bridge_score and
              bridge_type != "NOT_BRIDGE" and
              subtheme in OPERATING_DATACENTER_SUBTHEMES):
            flag = "ECOSYSTEM_BRIDGE"
        elif subtheme in OPERATING_DATACENTER_SUBTHEMES:
            flag = "OPERATING_ADJACENT"
        elif (sector == "Financial Services" or
              _industry_contains_any(industry, FINANCIAL_INDUSTRY_TERMS)):
            flag = "FINANCIAL_OR_FUND_NOISE"
        elif (subtheme in BROAD_NON_CORE_SUBTHEMES or
              membership == "BROAD_BETA"):
            flag = "BROAD_BETA_RISK"

        out[ticker] = flag

    return out


# =============================================================================
# OUTPUT WRITERS
# =============================================================================

def write_operating_relevance_scores(
    node_scores: pd.DataFrame,
    seed_subtheme_connectivity: dict[str, dict[str, object]],
    operating_relevance_scores: dict[str, dict[str, object]],
    ecosystem_bridge_scores: dict[str, dict[str, object]],
    bridge_types: dict[str, str],
    interpretation_flags: dict[str, str],
    output_dir: Path,
) -> pd.DataFrame:
    """Write full operating relevance scores file."""
    out = node_scores.copy()

    # Add new columns using map
    out["seed_subtheme"] = out["ticker"].astype(str).map(
        lambda t: seed_subtheme_connectivity.get(_to_text(t), {}).get("dominant_seed_subtheme", "")
    )
    out["seed_subtheme_count"] = out["ticker"].astype(str).map(
        lambda t: seed_subtheme_connectivity.get(_to_text(t), {}).get("seed_subtheme_count", 0)
    ).astype(float)
    out["seed_subthemes"] = out["ticker"].astype(str).map(
        lambda t: _sorted_join(seed_subtheme_connectivity.get(_to_text(t), {}).get("seed_subthemes", []))
    )
    out["dominant_seed_subtheme"] = out["ticker"].astype(str).map(
        lambda t: seed_subtheme_connectivity.get(_to_text(t), {}).get("dominant_seed_subtheme", "")
    )
    out["dominant_seed_subtheme_count"] = out["ticker"].astype(str).map(
        lambda t: seed_subtheme_connectivity.get(_to_text(t), {}).get("dominant_seed_subtheme_count", 0)
    ).astype(float)

    out["operating_relevance_score"] = out["ticker"].astype(str).map(
        lambda t: operating_relevance_scores.get(_to_text(t), {}).get("operating_relevance_score", 0)
    ).astype(float)
    out["operating_relevance_tier"] = out["ticker"].astype(str).map(
        lambda t: operating_relevance_scores.get(_to_text(t), {}).get("operating_relevance_tier", "")
    )

    out["ecosystem_bridge_score"] = out["ticker"].astype(str).map(
        lambda t: ecosystem_bridge_scores.get(_to_text(t), {}).get("ecosystem_bridge_score", 0)
    ).astype(float)
    out["ecosystem_bridge_type"] = out["ticker"].astype(str).map(lambda t: bridge_types.get(_to_text(t), ""))
    out["interpretation_flag"] = out["ticker"].astype(str).map(lambda t: interpretation_flags.get(_to_text(t), ""))

    # Ensure numeric columns are float
    numeric_cols = [
        "network_importance_score",
        "degree_from_edges",
        "weighted_degree_from_edges",
        "average_edge_score",
        "seed_neighbor_count_from_edges",
        "core_neighbor_count",
        "average_core_edge_score",
        "cross_sector_degree",
        "cross_industry_degree",
        "neighbor_subtheme_count",
        "clique_count",
        "max_clique_size",
    ]

    for col in numeric_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0)

    # Select columns
    cols = [
        "theme_name",
        "ticker",
        "sector",
        "industry",
        "is_seed",
        "seed_status",
        "ecosystem_membership_class",
        "subtheme_guess",
        "seed_subtheme",
        "network_importance_score",
        "network_importance_tier",
        "network_role",
        "operating_relevance_score",
        "operating_relevance_tier",
        "interpretation_flag",
        "ecosystem_bridge_score",
        "ecosystem_bridge_type",
        "degree_from_edges",
        "weighted_degree_from_edges",
        "average_edge_score",
        "seed_neighbor_count_from_edges",
        "seed_neighbors_from_edges",
        "seed_subtheme_count",
        "seed_subthemes",
        "dominant_seed_subtheme",
        "dominant_seed_subtheme_count",
        "core_neighbor_count",
        "core_neighbors",
        "average_core_edge_score",
        "cross_sector_degree",
        "cross_industry_degree",
        "neighbor_subtheme_count",
        "neighbor_subthemes",
        "dominant_neighbor_subtheme",
        "clique_count",
        "max_clique_size",
    ]

    for col in cols:
        if col not in out.columns:
            out[col] = ""

    out = out[cols]

    # Sort
    out = out.sort_values(
        by=[
            "operating_relevance_score",
            "network_importance_score",
            "seed_subtheme_count",
            "core_neighbor_count",
            "ticker",
        ],
        ascending=[False, False, False, False, True],
        kind="mergesort",
    )

    out.to_csv(output_dir / "datacenter_operating_relevance_scores.csv", index=False)
    return out


def write_top_operating_relevance(
    operating_relevance_scores: pd.DataFrame,
    args: argparse.Namespace,
    output_dir: Path,
) -> pd.DataFrame:
    """Write top operating relevance report."""
    out = operating_relevance_scores[
        operating_relevance_scores["operating_relevance_score"] >= args.min_operating_relevance_score
    ].copy()

    out = out.sort_values(
        by=[
            "operating_relevance_score",
            "network_importance_score",
            "ticker",
        ],
        ascending=[False, False, True],
        kind="mergesort",
    ).head(args.top_n)

    out.to_csv(output_dir / "datacenter_top_operating_relevance.csv", index=False)
    return out


def write_top_non_seed_operating_relevance(
    operating_relevance_scores: pd.DataFrame,
    args: argparse.Namespace,
    output_dir: Path,
) -> pd.DataFrame:
    """Write top non-seed operating relevance report."""
    out = operating_relevance_scores[
        (~operating_relevance_scores["is_seed"]) &
        (operating_relevance_scores["operating_relevance_score"] >= args.min_operating_relevance_score)
    ].copy()

    out = out.sort_values(
        by=[
            "operating_relevance_score",
            "network_importance_score",
            "seed_subtheme_count",
            "core_neighbor_count",
            "ticker",
        ],
        ascending=[False, False, False, False, True],
        kind="mergesort",
    ).head(args.top_n)

    out.to_csv(output_dir / "datacenter_top_non_seed_operating_relevance.csv", index=False)
    return out


def write_connectors_by_subtheme(
    operating_relevance_scores: pd.DataFrame,
    output_dir: Path,
) -> pd.DataFrame:
    """Write top connectors by subtheme."""
    rows = []

    for subtheme, grp in operating_relevance_scores.groupby("subtheme_guess", sort=True):
        g = grp.copy()
        top_25 = g.sort_values(
            by=["operating_relevance_score", "network_importance_score", "ticker"],
            ascending=[False, False, True],
            kind="mergesort",
        ).head(25)

        for rank, (_, row) in enumerate(top_25.iterrows(), start=1):
            rows.append(
                {
                    "theme_name": _to_text(row.get("theme_name")),
                    "subtheme_guess": subtheme,
                    "rank_within_subtheme": rank,
                    "ticker": _to_text(row.get("ticker")),
                    "sector": _to_text(row.get("sector")),
                    "industry": _to_text(row.get("industry")),
                    "is_seed": _to_bool(row.get("is_seed")),
                    "ecosystem_membership_class": _to_text(row.get("ecosystem_membership_class")),
                    "network_importance_score": _to_float(row.get("network_importance_score")),
                    "operating_relevance_score": _to_float(row.get("operating_relevance_score")),
                    "operating_relevance_tier": _to_text(row.get("operating_relevance_tier")),
                    "interpretation_flag": _to_text(row.get("interpretation_flag")),
                    "seed_subtheme_count": _to_float(row.get("seed_subtheme_count")),
                    "seed_subthemes": _to_text(row.get("seed_subthemes")),
                    "core_neighbor_count": _to_float(row.get("core_neighbor_count")),
                    "cross_sector_degree": _to_float(row.get("cross_sector_degree")),
                    "ecosystem_bridge_type": _to_text(row.get("ecosystem_bridge_type")),
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        out = pd.DataFrame(
            columns=[
                "theme_name",
                "subtheme_guess",
                "rank_within_subtheme",
                "ticker",
                "sector",
                "industry",
                "is_seed",
                "ecosystem_membership_class",
                "network_importance_score",
                "operating_relevance_score",
                "operating_relevance_tier",
                "interpretation_flag",
                "seed_subtheme_count",
                "seed_subthemes",
                "core_neighbor_count",
                "cross_sector_degree",
                "ecosystem_bridge_type",
            ]
        )

    out = out.sort_values(
        by=["subtheme_guess", "rank_within_subtheme"],
        ascending=[True, True],
        kind="mergesort",
    )

    out.to_csv(output_dir / "datacenter_top_connectors_by_subtheme.csv", index=False)
    return out


def write_seed_subtheme_connectivity(
    operating_relevance_scores: pd.DataFrame,
    args: argparse.Namespace,
    output_dir: Path,
) -> pd.DataFrame:
    """Write seed subtheme connectivity report."""
    out = operating_relevance_scores[
        operating_relevance_scores["seed_subtheme_count"] >= args.min_seed_subtheme_count
    ].copy()

    cols = [
        "theme_name",
        "ticker",
        "sector",
        "industry",
        "is_seed",
        "ecosystem_membership_class",
        "subtheme_guess",
        "seed_subtheme",
        "seed_subtheme_count",
        "seed_subthemes",
        "dominant_seed_subtheme",
        "dominant_seed_subtheme_count",
        "seed_neighbor_count_from_edges",
        "seed_neighbors_from_edges",
        "core_neighbor_count",
        "network_importance_score",
        "operating_relevance_score",
        "ecosystem_bridge_score",
        "ecosystem_bridge_type",
        "interpretation_flag",
    ]

    for col in cols:
        if col not in out.columns:
            out[col] = ""

    out = out[cols]

    out = out.sort_values(
        by=[
            "seed_subtheme_count",
            "ecosystem_bridge_score",
            "operating_relevance_score",
            "ticker",
        ],
        ascending=[False, False, False, True],
        kind="mergesort",
    ).head(args.top_n)

    out.to_csv(output_dir / "datacenter_seed_subtheme_connectivity.csv", index=False)
    return out


def write_best_ecosystem_bridges(
    operating_relevance_scores: pd.DataFrame,
    args: argparse.Namespace,
    output_dir: Path,
) -> pd.DataFrame:
    """Write best ecosystem bridges report."""
    out = operating_relevance_scores[
        (operating_relevance_scores["ecosystem_bridge_score"] >= args.min_bridge_score) &
        (operating_relevance_scores["ecosystem_bridge_type"] != "NOT_BRIDGE")
    ].copy()

    cols = [
        "theme_name",
        "ticker",
        "sector",
        "industry",
        "is_seed",
        "ecosystem_membership_class",
        "subtheme_guess",
        "seed_subtheme",
        "ecosystem_bridge_score",
        "ecosystem_bridge_type",
        "seed_subtheme_count",
        "seed_subthemes",
        "neighbor_subtheme_count",
        "neighbor_subthemes",
        "cross_sector_degree",
        "cross_industry_degree",
        "core_neighbor_count",
        "core_neighbors",
        "network_importance_score",
        "operating_relevance_score",
        "interpretation_flag",
    ]

    for col in cols:
        if col not in out.columns:
            out[col] = ""

    out = out[cols]

    out = out.sort_values(
        by=[
            "ecosystem_bridge_score",
            "seed_subtheme_count",
            "neighbor_subtheme_count",
            "operating_relevance_score",
            "ticker",
        ],
        ascending=[False, False, False, False, True],
        kind="mergesort",
    ).head(args.top_n)

    out.to_csv(output_dir / "datacenter_best_ecosystem_bridges.csv", index=False)
    return out


def write_interpretation_flags(
    operating_relevance_scores: pd.DataFrame,
    output_dir: Path,
) -> pd.DataFrame:
    """Write interpretation flags summary."""
    rows = []

    # Ensure numeric columns are numeric
    op_rel_copy = operating_relevance_scores.copy()
    op_rel_copy["operating_relevance_score"] = pd.to_numeric(op_rel_copy["operating_relevance_score"], errors="coerce").fillna(0)
    op_rel_copy["network_importance_score"] = pd.to_numeric(op_rel_copy["network_importance_score"], errors="coerce").fillna(0)
    op_rel_copy["is_seed"] = op_rel_copy["is_seed"].map(_to_bool)

    for flag, grp in op_rel_copy.groupby("interpretation_flag", sort=True):
        g = grp.copy()

        top_20 = g.sort_values(
            by=["operating_relevance_score", "ticker"],
            ascending=[False, True],
            kind="mergesort",
        ).head(20)
        top_non_seed = g[~g["is_seed"]].sort_values(
            by=["operating_relevance_score", "ticker"],
            ascending=[False, True],
            kind="mergesort",
        ).head(20)

        rows.append(
            {
                "theme_name": _to_text(op_rel_copy.iloc[0].get("theme_name")),
                "interpretation_flag": flag,
                "ticker_count": len(g),
                "seed_count": int(g["is_seed"].sum()),
                "non_seed_count": len(g) - int(g["is_seed"].sum()),
                "average_operating_relevance_score": float(g["operating_relevance_score"].mean()),
                "average_network_importance_score": float(g["network_importance_score"].mean()),
                "top_tickers": _sorted_join(top_20["ticker"].tolist()),
                "top_non_seed_tickers": _sorted_join(top_non_seed["ticker"].tolist()),
                "subthemes": _sorted_join(g["subtheme_guess"].unique().tolist()),
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        out = pd.DataFrame(
            columns=[
                "theme_name",
                "interpretation_flag",
                "ticker_count",
                "seed_count",
                "non_seed_count",
                "average_operating_relevance_score",
                "average_network_importance_score",
                "top_tickers",
                "top_non_seed_tickers",
                "subthemes",
            ]
        )

    out = out.sort_values(
        by=[
            "average_operating_relevance_score",
            "ticker_count",
            "interpretation_flag",
        ],
        ascending=[False, False, True],
        kind="mergesort",
    )

    out.to_csv(output_dir / "datacenter_interpretation_flags.csv", index=False)
    return out


def write_final_research_queue(
    operating_relevance_scores: pd.DataFrame,
    args: argparse.Namespace,
    output_dir: Path,
) -> pd.DataFrame:
    """Write final research queue."""
    df = operating_relevance_scores.copy()

    # Inclusion conditions
    is_seed = df["is_seed"]
    op_core = df["interpretation_flag"] == "OPERATING_CORE"
    eco_bridge = df["interpretation_flag"] == "ECOSYSTEM_BRIDGE"
    op_adjacent = df["interpretation_flag"] == "OPERATING_ADJACENT"
    high_score = (
        (df["operating_relevance_score"] >= args.min_operating_relevance_score) &
        (df["network_importance_score"] >= args.min_network_importance_score)
    )
    multi_seed_bridge = (
        (df["seed_subtheme_count"] >= args.min_seed_subtheme_count) &
        (df["ecosystem_bridge_score"] >= args.min_bridge_score)
    )

    include = is_seed | op_core | eco_bridge | op_adjacent | high_score | multi_seed_bridge

    # Exclude conditions
    is_noise = (
        (df["interpretation_flag"].isin(["FINANCIAL_OR_FUND_NOISE", "BROAD_BETA_RISK"])) &
        (df["operating_relevance_score"] < 0.70)
    )

    out = df[include & (~is_noise)].copy()

    def _reason(row: pd.Series) -> str:
        reasons = set()
        if _to_bool(row.get("is_seed")):
            reasons.add("SEED")
        if _to_text(row.get("interpretation_flag")) == "OPERATING_CORE":
            reasons.add("OPERATING_CORE")
        if _to_text(row.get("interpretation_flag")) == "OPERATING_ADJACENT":
            reasons.add("OPERATING_ADJACENT")
        if _to_text(row.get("interpretation_flag")) == "ECOSYSTEM_BRIDGE":
            reasons.add("ECOSYSTEM_BRIDGE")
        if _to_float(row.get("operating_relevance_score")) >= args.min_operating_relevance_score:
            reasons.add("HIGH_OPERATING_RELEVANCE")
        if _to_float(row.get("network_importance_score")) >= args.min_network_importance_score:
            reasons.add("HIGH_NETWORK_IMPORTANCE")
        if _to_float(row.get("seed_subtheme_count")) >= args.min_seed_subtheme_count:
            reasons.add("MULTI_SEED_SUBTHEME_CONNECTED")
        if _to_float(row.get("core_neighbor_count")) >= args.min_core_neighbor_count:
            reasons.add("CORE_CONNECTED")
        if _to_float(row.get("cross_sector_degree")) >= args.min_cross_sector_degree:
            reasons.add("CROSS_SECTOR_CONNECTED")
        return _sorted_join(sorted(reasons))

    out["v7b_research_reason"] = out.apply(_reason, axis=1)

    out = out.sort_values(
        by=[
            "is_seed",
            "operating_relevance_score",
            "ecosystem_bridge_score",
            "network_importance_score",
            "ticker",
        ],
        ascending=[False, False, False, False, True],
        kind="mergesort",
    ).head(args.top_n)

    out.to_csv(output_dir / "datacenter_v7b_final_research_queue.csv", index=False)
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
                txt = f"{v:.4f}" if v != int(v) else str(int(v))
            else:
                txt = _to_text(v)
            cells.append(txt.replace("|", "\\|"))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def write_markdown_summary(
    v7_dir: Path,
    output_dir: Path,
    theme_name: str,
    operating_relevance_scores: pd.DataFrame,
    top_operating_relevance: pd.DataFrame,
    top_non_seed_operating_relevance: pd.DataFrame,
    seed_subtheme_connectivity: pd.DataFrame,
    best_ecosystem_bridges: pd.DataFrame,
    interpretation_flags: pd.DataFrame,
    args: argparse.Namespace,
) -> None:
    """Write markdown summary."""
    lines = []

    lines.append(f"# {theme_name.title()} Network Interpretation Summary (V7b)")
    lines.append("")

    lines.append("## Inputs and Parameters")
    lines.append(f"- v7_dir: {v7_dir}")
    lines.append(f"- output_dir: {output_dir}")
    lines.append(f"- theme_name: {theme_name}")
    lines.append("")
    lines.append("## Thresholds")
    lines.append(f"- min_operating_relevance_score: {args.min_operating_relevance_score}")
    lines.append(f"- min_network_importance_score: {args.min_network_importance_score}")
    lines.append(f"- min_seed_subtheme_count: {args.min_seed_subtheme_count}")
    lines.append(f"- min_core_neighbor_count: {args.min_core_neighbor_count}")
    lines.append(f"- min_cross_sector_degree: {args.min_cross_sector_degree}")
    lines.append(f"- min_bridge_score: {args.min_bridge_score}")
    lines.append("")

    lines.append("## Summary Counts")
    lines.append(f"- Total nodes: {len(operating_relevance_scores)}")
    lines.append(f"- HIGH operating relevance: {int((operating_relevance_scores['operating_relevance_tier'] == 'HIGH').sum())}")
    lines.append(f"- MEDIUM operating relevance: {int((operating_relevance_scores['operating_relevance_tier'] == 'MEDIUM').sum())}")
    lines.append(f"- LOW operating relevance: {int((operating_relevance_scores['operating_relevance_tier'] == 'LOW').sum())}")
    lines.append(f"- Seed nodes: {int(operating_relevance_scores['is_seed'].sum())}")
    lines.append(f"- OPERATING_CORE nodes: {int((operating_relevance_scores['interpretation_flag'] == 'OPERATING_CORE').sum())}")
    lines.append(f"- ECOSYSTEM_BRIDGE nodes: {int((operating_relevance_scores['interpretation_flag'] == 'ECOSYSTEM_BRIDGE').sum())}")
    lines.append("")

    lines.append("## Top 10 Operating Relevance Tickers")
    lines.append(_markdown_table(
        top_operating_relevance,
        ["ticker", "subtheme_guess", "operating_relevance_score", "network_importance_score", "interpretation_flag"],
        top_n=10,
    ))
    lines.append("")

    lines.append("## Top 10 Non-Seed Operating Relevance Tickers")
    lines.append(_markdown_table(
        top_non_seed_operating_relevance,
        ["ticker", "subtheme_guess", "operating_relevance_score", "seed_subtheme_count", "core_neighbor_count"],
        top_n=10,
    ))
    lines.append("")

    lines.append("## Top 10 Seed Subtheme Connectors")
    lines.append(_markdown_table(
        seed_subtheme_connectivity,
        ["ticker", "seed_subtheme_count", "seed_subthemes", "ecosystem_bridge_score", "operating_relevance_score"],
        top_n=10,
    ))
    lines.append("")

    lines.append("## Top 10 Ecosystem Bridges")
    lines.append(_markdown_table(
        best_ecosystem_bridges,
        ["ticker", "ecosystem_bridge_type", "seed_subtheme_count", "neighbor_subtheme_count", "ecosystem_bridge_score"],
        top_n=10,
    ))
    lines.append("")

    lines.append("## Interpretation Flag Summary")
    lines.append(_markdown_table(
        interpretation_flags,
        ["interpretation_flag", "ticker_count", "seed_count", "average_operating_relevance_score"],
        top_n=10,
    ))
    lines.append("")

    lines.append("## Scoring Notes")
    lines.append("")
    lines.append("### operating_relevance_score")
    lines.append("Combines operating subtheme classification, network importance, core connectivity, seed subtheme diversity, and cross-sector reach.")
    lines.append("Range: 0.00 to 1.00. Tiers: HIGH (>=0.70), MEDIUM (>=0.45), LOW (<0.45).")
    lines.append("")
    lines.append("### seed_subtheme_count")
    lines.append("Number of distinct operating datacenter seed subthemes this node connects to.")
    lines.append("Higher values indicate connectors that bridge multiple parts of the ecosystem.")
    lines.append("")
    lines.append("### ecosystem_bridge_score")
    lines.append("Weights seed subtheme diversity, cross-subtheme connections, cross-sector reach, core density, and edge strength.")
    lines.append("Identifies companies that bridge fragmented parts of the ecosystem.")
    lines.append("")
    lines.append("## Warnings")
    lines.append("- These are statistical/network prioritization outputs, not proof of business exposure.")
    lines.append("- Manual business review is required before any investment decisions.")
    lines.append("- Network connectivity does not imply causality or strong business relationships.")

    (output_dir / "datacenter_v7b_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_readme(output_dir: Path) -> None:
    """Write V7b readme."""
    lines = [
        "# Datacenter Network Interpretation Outputs (V7b)",
        "",
        "V7b reads V7 (network centrality) outputs and produces interpretable prioritization reports.",
        "",
        "## Output Files",
        "",
        "### datacenter_operating_relevance_scores.csv",
        "Full node table with V7b metrics: operating relevance score, seed subtheme connectivity, ecosystem bridge scores.",
        "One row per ticker.",
        "",
        "### datacenter_top_operating_relevance.csv",
        "Top nodes by operating relevance score, filtered by threshold.",
        "Includes both seeds and non-seeds.",
        "",
        "### datacenter_top_non_seed_operating_relevance.csv",
        "Top non-seed nodes by operating relevance score.",
        "Best candidates for new operating datacenter exposure.",
        "",
        "### datacenter_top_connectors_by_subtheme.csv",
        "Top 25 connectors within each subtheme.",
        "Helps identify best-in-class companies per operational focus area.",
        "",
        "### datacenter_seed_subtheme_connectivity.csv",
        "Companies connected to multiple seed subthemes.",
        "Bridges across different parts of the operating datacenter ecosystem.",
        "",
        "### datacenter_best_ecosystem_bridges.csv",
        "Companies with high ecosystem bridge scores.",
        "Key nodes for understanding how different ecosystem areas connect.",
        "",
        "### datacenter_interpretation_flags.csv",
        "Summary of companies by interpretation flag (OPERATING_CORE, ECOSYSTEM_BRIDGE, BROAD_BETA_RISK, etc.).",
        "Helps identify and quantify different roles in the ecosystem.",
        "",
        "### datacenter_v7b_final_research_queue.csv",
        "Clean research queue combining seeds, operating core, ecosystem bridges, and high-relevance candidates.",
        "Primary output for manual business validation workflows.",
        "",
        "### datacenter_v7b_summary.md",
        "Executive summary with key metrics, top lists, and scoring explanations.",
        "",
        "### datacenter_v7b_readme.md",
        "This file.",
    ]

    (output_dir / "datacenter_v7b_readme.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    args = parse_args()

    v7_dir = Path(args.v7_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Validate and load inputs
    try:
        file_map = validate_input_files(v7_dir)
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    data = load_inputs(file_map)

    node_scores = data["datacenter_network_node_scores.csv"].copy()
    node_scores = node_scores.drop_duplicates(subset=["ticker"]).set_index("ticker")

    # Build seed subtheme map
    seed_subtheme_map = build_seed_subtheme_map(node_scores.reset_index())

    # Compute connectivity and scores
    seed_subtheme_connectivity = compute_seed_subtheme_connectivity(
        node_scores.reset_index(),
        seed_subtheme_map,
    )

    operating_relevance_scores = compute_operating_relevance_scores(
        node_scores,
        seed_subtheme_connectivity,
    )

    ecosystem_bridge_scores = compute_ecosystem_bridge_scores(
        node_scores,
        seed_subtheme_connectivity,
    )

    bridge_types = assign_bridge_types(
        node_scores,
        seed_subtheme_connectivity,
        args,
    )

    interpretation_flags = assign_interpretation_flags(
        node_scores,
        operating_relevance_scores,
        ecosystem_bridge_scores,
        bridge_types,
        args,
    )

    # Write full operating relevance scores
    node_scores_reset = node_scores.reset_index()
    full_scores = write_operating_relevance_scores(
        node_scores_reset,
        seed_subtheme_connectivity,
        operating_relevance_scores,
        ecosystem_bridge_scores,
        bridge_types,
        interpretation_flags,
        output_dir,
    )

    # Write additional reports
    top_op_rel = write_top_operating_relevance(full_scores, args, output_dir)
    top_non_seed_op_rel = write_top_non_seed_operating_relevance(full_scores, args, output_dir)
    connectors_by_subtheme = write_connectors_by_subtheme(full_scores, output_dir)
    seed_subtheme_conn = write_seed_subtheme_connectivity(full_scores, args, output_dir)
    best_bridges = write_best_ecosystem_bridges(full_scores, args, output_dir)
    interp_flags = write_interpretation_flags(full_scores, output_dir)
    final_queue = write_final_research_queue(full_scores, args, output_dir)

    # Write markdown and readme
    write_markdown_summary(
        v7_dir,
        output_dir,
        args.theme_name,
        full_scores,
        top_op_rel,
        top_non_seed_op_rel,
        seed_subtheme_conn,
        best_bridges,
        interp_flags,
        args,
    )
    write_readme(output_dir)

    # Print SUMMARY
    print(f"SUMMARY v7_dir={v7_dir}")
    print(f"SUMMARY output_dir={output_dir}")
    print(f"SUMMARY theme_name={args.theme_name}")
    print(f"SUMMARY input_nodes={len(full_scores)}")
    print(f"SUMMARY rows_operating_relevance_scores={len(full_scores)}")
    print(f"SUMMARY rows_top_operating_relevance={len(top_op_rel)}")
    print(f"SUMMARY rows_top_non_seed_operating_relevance={len(top_non_seed_op_rel)}")
    print(f"SUMMARY rows_top_connectors_by_subtheme={len(connectors_by_subtheme)}")
    print(f"SUMMARY rows_seed_subtheme_connectivity={len(seed_subtheme_conn)}")
    print(f"SUMMARY rows_best_ecosystem_bridges={len(best_bridges)}")
    print(f"SUMMARY rows_interpretation_flags={len(interp_flags)}")
    print(f"SUMMARY rows_v7b_final_research_queue={len(final_queue)}")
    print(f"SUMMARY high_operating_relevance_rows={int((full_scores['operating_relevance_tier'] == 'HIGH').sum())}")
    print(f"SUMMARY medium_operating_relevance_rows={int((full_scores['operating_relevance_tier'] == 'MEDIUM').sum())}")
    print(f"SUMMARY low_operating_relevance_rows={int((full_scores['operating_relevance_tier'] == 'LOW').sum())}")
    print("SUMMARY status=OK")


if __name__ == "__main__":
    main()
