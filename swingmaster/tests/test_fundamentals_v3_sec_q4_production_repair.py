from __future__ import annotations

from swingmaster.fundamentals.v3_sec_q4_production_repair import canonical_q4_year, collision_resolution, nearest_next_q4


def test_q4_year_anchors_january_to_prior_canonical_year() -> None:
    assert canonical_q4_year("2026-01-31") == 2025


def test_q4_year_keeps_june_fiscal_year_end_in_period_year() -> None:
    assert canonical_q4_year("2026-06-30") == 2026


def test_nearest_next_q4_anchors_explicit_q_to_same_cycle() -> None:
    q4 = [{"period_end_date": "2025-12-31"}, {"period_end_date": "2026-12-31"}]

    assert nearest_next_q4("2025-09-30", q4)["period_end_date"] == "2025-12-31"


def test_collision_resolution_deletes_lower_priority_repaired_row() -> None:
    all_rows = [
        {"quarter_id": 1, "company_id": 10, "fiscal_year": 2026, "fiscal_quarter": "Q4", "period_end_date": "2025-12-31", "accepted_source_provider": "LEGACY"},
        {"quarter_id": 2, "company_id": 10, "fiscal_year": 2025, "fiscal_quarter": "Q4", "period_end_date": "2025-12-31", "accepted_source_provider": "YAHOO"},
    ]

    deletes, collisions = collision_resolution(all_rows, {1: 2025})

    assert deletes == {1}
    assert collisions[0]["collision_result"] == "SAME_RESULT_COLLISION"
