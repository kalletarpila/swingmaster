from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

from swingmaster.fundamentals.v3_schema import apply_v3_schema
from swingmaster.fundamentals.v3_yahoo_canonical_seed import (
    apply_company_baseline,
    build_final_metadata_map,
    load_company_baseline,
)


NOW = "2026-08-22T00:00:00Z"


def test_company_baseline_applies_active_and_inactive_rows(tmp_path: Path) -> None:
    baseline_path = tmp_path / "phase3_company_active_baseline.csv"
    _write_csv(
        baseline_path,
        ["market", "ticker", "recommended_v3_company_active", "activity_classification", "last_price_date", "trading_sessions_stale"],
        [
            ["usa", "AAA", "1", "ACTIVE", "2026-08-21", "0"],
            ["usa", "OLD", "0", "DELISTED_OR_INACTIVE", "2024-01-01", "500"],
        ],
    )
    conn = sqlite3.connect(":memory:")
    apply_v3_schema(conn)
    conn.row_factory = sqlite3.Row

    summary = apply_company_baseline(conn, load_company_baseline(baseline_path), now_utc=NOW)

    assert summary == {
        "approved_source_companies": 2,
        "company_rows": 2,
        "active_rows": 1,
        "inactive_rows": 1,
        "duplicate_market_ticker": 0,
        "reconciles": True,
    }
    assert conn.execute("SELECT active FROM v3_company WHERE ticker='OLD'").fetchone()[0] == 0


def test_metadata_priority_handles_cava_neup_and_lfcr(tmp_path: Path) -> None:
    bootstrap = tmp_path / "bootstrap"
    post_a = tmp_path / "post_a"
    post_a2 = tmp_path / "post_a2"
    post_a3 = tmp_path / "post_a3"
    for path in (bootstrap, post_a, post_a2, post_a3):
        path.mkdir()
    (bootstrap / "candidates.jsonl").write_text(
        '{"ticker":"CAVA","period_end_date":"2026-04-30","fiscal_year":2026,"fiscal_quarter":"Q1","publish_date":"2026-05-20","market_availability_date":"2026-05-20","provider_details":{"fiscal_identity_source":"V2"}}\n'
        '{"ticker":"NEUP","period_end_date":"2026-03-31","fiscal_year":2026,"fiscal_quarter":"Q1","publish_date":"2026-05-15","market_availability_date":"2026-05-15","provider_details":{"fiscal_identity_source":"V2_WRONG"}}\n'
    )
    _write_csv(bootstrap / "duplicate_candidate_keys.csv", ["ticker", "period_end_date", "fiscal_year", "fiscal_quarter", "publish_date"], [])
    _write_csv(post_a / "metadata_rejection_rows.csv", ["ticker", "period_end_date"], [])
    _write_csv(post_a / "sequential_recovery_candidates.csv", ["ticker", "period_end_date", "fiscal_year", "fiscal_quarter"], [])
    _write_csv(post_a / "publication_date_recovery.csv", ["ticker", "period_end_date", "recovered_publish_date"], [])
    _write_csv(post_a2 / "additional_unresolved_recovery.csv", ["ticker", "period_end_date", "final_fiscal_year", "final_fiscal_quarter"], [])
    _write_csv(post_a2 / "resolved_anchor_conflicts.csv", ["ticker", "period_end_date", "final_fiscal_year", "final_fiscal_quarter"], [])
    _write_csv(post_a2 / "manual_fiscal_calendar_resolution.csv", ["ticker", "period_end_date", "final_fiscal_year", "final_fiscal_quarter"], [])
    _write_csv(post_a3 / "recovered_rows.csv", ["ticker", "period_end_date", "final_fiscal_year", "final_fiscal_quarter"], [])
    _write_csv(post_a3 / "company_recent_result_calendar.csv", ["ticker", "period_end_date", "fiscal_year", "fiscal_quarter", "official_result_publication_date"], [])
    _write_csv(
        post_a3 / "manual_fiscal_calendar_resolution_neup.csv",
        ["ticker", "company", "fiscal_year", "fiscal_quarter", "period_end_date", "publish_date", "evidence_quality", "notes"],
        [["NEUP", "Neuphoria", "2026", "Q3", "2026-03-31", "2026-05-15", "USER", "corrected"]],
    )
    _write_csv(
        post_a3 / "phase3_reconciliation_exceptions.csv",
        [
            "ticker",
            "yahoo_period_end_date",
            "resolved_fiscal_year",
            "resolved_fiscal_quarter",
            "official_period_end_date",
            "publication_date",
            "exception_type",
            "competing_candidate_work_unit_key",
            "competing_period_end_date",
            "field_comparison_status",
            "recommended_phase3_action",
        ],
        [
            ["CAVA", "2026-03-31", "2026", "Q1", "2026-04-19", "2026-05-19", "DUPLICATE_FISCAL_WORK_UNIT_REQUIRES_RECONCILIATION", "", "2026-04-30", "", ""],
            ["LFCR", "2025-09-30", "", "", "2025-12-31", "2026-03-16", "TRANSITION_PERIOD_DATE_VARIANT", "", "", "", ""],
        ],
    )

    metadata = build_final_metadata_map(bootstrap_root=bootstrap, post_a_root=post_a, post_a2_root=post_a2, post_a3_root=post_a3)

    assert metadata[("CAVA", "2026-03-31")].disposition == "COMPLEMENTARY_SAME_FISCAL_Q"
    assert metadata[("CAVA", "2026-04-30")].period_date_policy == "SAFE_VARIANT"
    assert metadata[("NEUP", "2026-03-31")].fiscal_quarter == "Q3"
    assert metadata[("LFCR", "2025-09-30")].candidate_can_create_quarter is False
    assert metadata[("LFCR", "2025-09-30")].candidate_issue_type == "TRANSITION_PERIOD_VARIANT"


def _write_csv(path: Path, headers: list[str], rows: list[list[str]]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows(rows)
