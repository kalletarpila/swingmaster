from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd


MODULE_PATH = (
    Path(__file__).resolve().parents[2]
    / "analysis"
    / "report_datacenter_network_interpretation.py"
)
SPEC = importlib.util.spec_from_file_location(
    "report_datacenter_network_interpretation",
    MODULE_PATH,
)
assert SPEC is not None
assert SPEC.loader is not None
v7b = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(v7b)


def test_derive_seed_subtheme_uses_ticker_mapping_for_unclassified_seed() -> None:
    row = pd.Series(
        {
            "ticker": "ANET",
            "subtheme_guess": "OTHER_OR_UNCLASSIFIED",
            "sector": "Technology",
            "industry": "Unknown",
        }
    )

    assert v7b.derive_seed_subtheme(row) == "NETWORKING_OPTICAL_CONNECTIVITY"


def test_compute_seed_subtheme_connectivity_counts_distinct_subthemes_and_breaks_ties_alphabetically() -> None:
    node_scores = pd.DataFrame(
        [
            {
                "ticker": "BRDG",
                "seed_neighbors_from_edges": "EQIX, DLR, ANET, CSCO",
            }
        ]
    )
    seed_subtheme_map = {
        "EQIX": "DATACENTER_REIT",
        "DLR": "DATACENTER_REIT",
        "ANET": "NETWORKING_OPTICAL_CONNECTIVITY",
        "CSCO": "NETWORKING_OPTICAL_CONNECTIVITY",
    }

    out = v7b.compute_seed_subtheme_connectivity(node_scores, seed_subtheme_map)

    assert out["BRDG"]["seed_subtheme_count"] == 2
    assert out["BRDG"]["seed_subthemes"] == [
        "DATACENTER_REIT",
        "NETWORKING_OPTICAL_CONNECTIVITY",
    ]
    assert out["BRDG"]["dominant_seed_subtheme"] == "DATACENTER_REIT"
    assert out["BRDG"]["dominant_seed_subtheme_count"] == 2


def test_assign_interpretation_flags_respects_priority_order() -> None:
    node_scores = pd.DataFrame(
        [
            {
                "ticker": "EQIX",
                "is_seed": True,
                "subtheme_guess": "DATACENTER_REIT",
                "ecosystem_membership_class": "CORE_VALIDATION",
                "sector": "Real Estate",
                "industry": "REIT - Specialty",
            },
            {
                "ticker": "VRT",
                "is_seed": False,
                "subtheme_guess": "ELECTRICAL_POWER_EQUIPMENT",
                "ecosystem_membership_class": "CORE_VALIDATION",
                "sector": "Industrials",
                "industry": "Electrical Equipment",
            },
            {
                "ticker": "BLK",
                "is_seed": False,
                "subtheme_guess": "OTHER_OR_UNCLASSIFIED",
                "ecosystem_membership_class": "ADJACENT",
                "sector": "Financial Services",
                "industry": "Asset Management",
            },
            {
                "ticker": "QQQX",
                "is_seed": False,
                "subtheme_guess": "BROAD_TECH_OR_INDUSTRIAL_BETA",
                "ecosystem_membership_class": "BROAD_BETA",
                "sector": "Technology",
                "industry": "Software",
            },
        ]
    ).set_index("ticker")
    operating_relevance_scores = {
        "EQIX": {"operating_relevance_score": 0.95},
        "VRT": {"operating_relevance_score": 0.60},
        "BLK": {"operating_relevance_score": 0.15},
        "QQQX": {"operating_relevance_score": 0.25},
    }
    ecosystem_bridge_scores = {
        "EQIX": {"ecosystem_bridge_score": 0.40},
        "VRT": {"ecosystem_bridge_score": 0.10},
        "BLK": {"ecosystem_bridge_score": 0.30},
        "QQQX": {"ecosystem_bridge_score": 0.30},
    }
    bridge_types = {
        "EQIX": "SEED_SUBTHEME_BRIDGE",
        "VRT": "NOT_BRIDGE",
        "BLK": "CROSS_SECTOR_CORE_BRIDGE",
        "QQQX": "SEED_SUBTHEME_BRIDGE",
    }
    args = type(
        "Args",
        (),
        {
            "min_operating_relevance_score": 0.35,
            "min_bridge_score": 0.20,
        },
    )()

    out = v7b.assign_interpretation_flags(
        node_scores,
        operating_relevance_scores,
        ecosystem_bridge_scores,
        bridge_types,
        args,
    )

    assert out["EQIX"] == "SEED_ANCHOR"
    assert out["VRT"] == "OPERATING_CORE"
    assert out["BLK"] == "FINANCIAL_OR_FUND_NOISE"
    assert out["QQQX"] == "BROAD_BETA_RISK"


def test_write_final_research_queue_filters_low_score_noise_but_keeps_high_conviction_rows(
    tmp_path: Path,
) -> None:
    operating_relevance_scores = pd.DataFrame(
        [
            {
                "theme_name": "datacenter",
                "ticker": "EQIX",
                "is_seed": True,
                "interpretation_flag": "SEED_ANCHOR",
                "operating_relevance_score": 0.82,
                "ecosystem_bridge_score": 0.44,
                "network_importance_score": 0.91,
                "seed_subtheme_count": 2.0,
                "core_neighbor_count": 6.0,
                "cross_sector_degree": 3.0,
            },
            {
                "theme_name": "datacenter",
                "ticker": "NOISY",
                "is_seed": False,
                "interpretation_flag": "BROAD_BETA_RISK",
                "operating_relevance_score": 0.60,
                "ecosystem_bridge_score": 0.33,
                "network_importance_score": 0.45,
                "seed_subtheme_count": 3.0,
                "core_neighbor_count": 4.0,
                "cross_sector_degree": 3.0,
            },
            {
                "theme_name": "datacenter",
                "ticker": "STRONG",
                "is_seed": False,
                "interpretation_flag": "OPERATING_ADJACENT",
                "operating_relevance_score": 0.76,
                "ecosystem_bridge_score": 0.31,
                "network_importance_score": 0.52,
                "seed_subtheme_count": 2.0,
                "core_neighbor_count": 5.0,
                "cross_sector_degree": 2.0,
            },
        ]
    )
    args = type(
        "Args",
        (),
        {
            "min_operating_relevance_score": 0.35,
            "min_network_importance_score": 0.30,
            "min_seed_subtheme_count": 2,
            "min_core_neighbor_count": 3,
            "min_cross_sector_degree": 2,
            "min_bridge_score": 0.20,
            "top_n": 300,
        },
    )()

    out = v7b.write_final_research_queue(
        operating_relevance_scores,
        args,
        tmp_path,
    )

    assert out["ticker"].tolist() == ["EQIX", "STRONG"]

    reasons = dict(zip(out["ticker"], out["v7b_research_reason"]))
    assert "SEED" in reasons["EQIX"]
    assert "HIGH_OPERATING_RELEVANCE" in reasons["STRONG"]
    assert "HIGH_NETWORK_IMPORTANCE" in reasons["STRONG"]
