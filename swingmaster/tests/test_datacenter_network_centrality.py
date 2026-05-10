from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd


MODULE_PATH = (
    Path(__file__).resolve().parents[2]
    / "analysis"
    / "report_datacenter_network_centrality.py"
)
SPEC = importlib.util.spec_from_file_location(
    "report_datacenter_network_centrality",
    MODULE_PATH,
)
assert SPEC is not None
assert SPEC.loader is not None
v7 = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(v7)


def _args() -> object:
    return type(
        "Args",
        (),
        {
            "min_network_importance_score": 0.30,
            "min_seed_neighbor_count": 2,
            "min_core_neighbor_count": 3,
            "min_neighbor_subtheme_count": 2,
            "min_cross_sector_degree": 2,
            "min_clique_count": 1,
            "top_n": 300,
        },
    )()


def test_classify_network_role_respects_priority_order() -> None:
    args = _args()

    seed_row = pd.Series(
        {
            "is_seed": True,
            "ecosystem_membership_class": "CORE_VALIDATION",
            "network_importance_score": 0.95,
            "seed_neighbor_count_from_edges": 5,
            "core_neighbor_count": 6,
            "neighbor_subtheme_count": 4,
            "cross_sector_degree": 4,
        }
    )
    core_row = pd.Series(
        {
            "is_seed": False,
            "ecosystem_membership_class": "CORE_VALIDATION",
            "network_importance_score": 0.55,
            "seed_neighbor_count_from_edges": 5,
            "core_neighbor_count": 3,
            "neighbor_subtheme_count": 4,
            "cross_sector_degree": 4,
        }
    )
    multi_seed_row = pd.Series(
        {
            "is_seed": False,
            "ecosystem_membership_class": "ADJACENT",
            "network_importance_score": 0.40,
            "seed_neighbor_count_from_edges": 2,
            "core_neighbor_count": 1,
            "neighbor_subtheme_count": 4,
            "cross_sector_degree": 4,
        }
    )
    broad_beta_row = pd.Series(
        {
            "is_seed": False,
            "ecosystem_membership_class": "BROAD_BETA",
            "network_importance_score": 0.10,
            "seed_neighbor_count_from_edges": 0,
            "core_neighbor_count": 0,
            "neighbor_subtheme_count": 0,
            "cross_sector_degree": 0,
        }
    )

    assert v7.classify_network_role(seed_row, args) == "SEED_ANCHOR"
    assert v7.classify_network_role(core_row, args) == "CORE_CONNECTOR"
    assert v7.classify_network_role(multi_seed_row, args) == "MULTI_SEED_CONNECTOR"
    assert v7.classify_network_role(broad_beta_row, args) == "BROAD_BETA_NODE"


def test_operating_and_broad_beta_candidate_filters_split_rows_correctly() -> None:
    operating_row = pd.Series(
        {
            "is_seed": False,
            "ecosystem_membership_class": "CORE_VALIDATION",
            "sector": "Industrials",
            "industry": "Electrical Equipment",
            "subtheme_guess": "ELECTRICAL_POWER_EQUIPMENT",
        }
    )
    financial_row = pd.Series(
        {
            "is_seed": False,
            "ecosystem_membership_class": "ADJACENT",
            "sector": "Financial Services",
            "industry": "Asset Management",
            "subtheme_guess": "OTHER_OR_UNCLASSIFIED",
        }
    )

    assert v7.is_operating_datacenter_candidate(operating_row) is True
    assert v7.is_broad_beta_or_financial_candidate(operating_row) is False
    assert v7.is_operating_datacenter_candidate(financial_row) is False
    assert v7.is_broad_beta_or_financial_candidate(financial_row) is True


def test_build_broad_beta_network_reason_collects_expected_tags() -> None:
    row = pd.Series(
        {
            "ecosystem_membership_class": "BROAD_BETA",
            "sector": "Financial Services",
            "industry": "Asset Management",
            "subtheme_guess": "SOFTWARE_PLATFORM_ADJACENT",
            "network_importance_tier": "HIGH",
            "seed_neighbor_count_from_edges": 3,
            "core_neighbor_count": 4,
            "cross_sector_degree": 2,
        }
    )

    out = v7.build_broad_beta_network_reason(row, _args())

    assert "BROAD_BETA_MEMBERSHIP" in out
    assert "FINANCIAL_SERVICES" in out
    assert "FINANCIAL_OR_FUND_INDUSTRY" in out
    assert "SOFTWARE_PLATFORM_ADJACENT" in out
    assert "HIGH_NETWORK_IMPORTANCE" in out
    assert "MULTI_SEED_CONNECTED" in out
    assert "CORE_CONNECTED" in out
    assert "CROSS_SECTOR_BRIDGE" in out


def test_write_final_manual_review_queue_keeps_seed_operating_and_high_signal_broad_beta(
    tmp_path: Path,
) -> None:
    node_scores = pd.DataFrame(
        [
            {
                "ticker": "EQIX",
                "is_seed": True,
                "ecosystem_membership_class": "CORE_VALIDATION",
                "sector": "Real Estate",
                "industry": "REIT - Specialty",
                "subtheme_guess": "DATACENTER_REIT",
                "network_importance_score": 0.92,
                "network_importance_tier": "HIGH",
                "seed_neighbor_count_from_edges": 5,
                "core_neighbor_count": 6,
                "neighbor_subtheme_count": 4,
                "cross_sector_degree": 3,
                "clique_count": 2,
            },
            {
                "ticker": "VRT",
                "is_seed": False,
                "ecosystem_membership_class": "CORE_VALIDATION",
                "sector": "Industrials",
                "industry": "Electrical Equipment",
                "subtheme_guess": "ELECTRICAL_POWER_EQUIPMENT",
                "network_importance_score": 0.61,
                "network_importance_tier": "MEDIUM",
                "seed_neighbor_count_from_edges": 2,
                "core_neighbor_count": 4,
                "neighbor_subtheme_count": 2,
                "cross_sector_degree": 2,
                "clique_count": 1,
            },
            {
                "ticker": "BLK",
                "is_seed": False,
                "ecosystem_membership_class": "BROAD_BETA",
                "sector": "Financial Services",
                "industry": "Asset Management",
                "subtheme_guess": "OTHER_OR_UNCLASSIFIED",
                "network_importance_score": 0.74,
                "network_importance_tier": "HIGH",
                "seed_neighbor_count_from_edges": 3,
                "core_neighbor_count": 3,
                "neighbor_subtheme_count": 1,
                "cross_sector_degree": 2,
                "clique_count": 0,
            },
            {
                "ticker": "WEAK",
                "is_seed": False,
                "ecosystem_membership_class": "BROAD_BETA",
                "sector": "Financial Services",
                "industry": "Asset Management",
                "subtheme_guess": "OTHER_OR_UNCLASSIFIED",
                "network_importance_score": 0.55,
                "network_importance_tier": "MEDIUM",
                "seed_neighbor_count_from_edges": 1,
                "core_neighbor_count": 1,
                "neighbor_subtheme_count": 0,
                "cross_sector_degree": 0,
                "clique_count": 0,
            },
        ]
    )

    out = v7.write_final_manual_review_queue(node_scores, _args(), tmp_path)

    assert out["ticker"].tolist() == ["EQIX", "VRT", "BLK"]

    buckets = dict(zip(out["ticker"], out["manual_review_bucket"]))
    assert buckets["EQIX"] == "SEED_ANCHOR"
    assert buckets["VRT"] == "OPERATING_NON_SEED_CONNECTOR"
    assert buckets["BLK"] == "HIGH_SIGNAL_BROAD_BETA"

    reasons = dict(zip(out["ticker"], out["manual_review_reason"]))
    assert "SEED" in reasons["EQIX"]
    assert "OPERATING_DATACENTER_CANDIDATE" in reasons["VRT"]
    assert "HIGH_SIGNAL_BROAD_BETA" in reasons["BLK"]
