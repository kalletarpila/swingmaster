from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

from swingmaster.fundamentals.v3_canonical_migration import CANONICAL_FIELD_NAMES, V3CanonicalMigrationCandidate, V3CanonicalMigrationEngine
from swingmaster.fundamentals.v3_repositories import V3CompanyRepository
from swingmaster.fundamentals.v3_schema import apply_v3_schema
from swingmaster.fundamentals.v3_universe_refinement import (
    EXCLUDE_BANK,
    EXCLUDE_INSURANCE,
    EXCLUDE_OTHER_FINANCIAL,
    KEEP_FINANCIAL_INFRASTRUCTURE,
    KEEP_OTHER,
    KEEP_REIT,
    MANUAL_REVIEW_REQUIRED,
    classify_company,
    plan_refined_universe,
    retained_company_parity,
    special_case_parity,
)
from swingmaster.fundamentals.v3_v2_enrichment_policy import (
    V2_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE,
    decide_v2_publish_date_action,
    decide_v2_value_action,
)


NOW = "2026-08-22T00:00:00Z"


def test_financial_company_classification_policy() -> None:
    assert _class("BNK", sector="Financial Services", industry="Banks - Regional").review_class == EXCLUDE_BANK
    assert _class("REIN", sector="Financial Services", industry="Insurance - Reinsurance").review_class == EXCLUDE_INSURANCE
    assert _class("LIFE", sector="Financial Services", industry="Insurance - Life").review_class == EXCLUDE_INSURANCE
    assert _class("MREIT", sector="Real Estate", industry="REIT - Mortgage").review_class == KEEP_REIT
    assert _class("OREIT", sector="Real Estate", industry="REIT - Office").review_class == KEEP_REIT
    assert _class("EXCH", sector="Financial Services", industry="Financial Data & Stock Exchanges").review_class == KEEP_FINANCIAL_INFRASTRUCTURE
    assert _class("ASSET", sector="Financial Services", industry="Asset Management").review_class == EXCLUDE_OTHER_FINANCIAL
    assert _class("CREDIT", sector="Financial Services", industry="Credit Services").review_class == EXCLUDE_OTHER_FINANCIAL
    manual = _class("UNK", sector="Financial Services", industry="Specialty Finance Platform")
    assert manual.review_class == MANUAL_REVIEW_REQUIRED
    assert manual.decision == "KEEP"
    assert _class("SOFT", sector="Technology", industry="Software - Application").review_class == KEEP_OTHER
    assert _class("BNK", sector="Financial Services", industry="Banks - Regional") == _class("BNK", sector="Financial Services", industry="Banks - Regional")


def test_plan_refined_universe_excludes_only_policy_financials_and_keeps_inactive_retained(tmp_path: Path) -> None:
    v3_db = tmp_path / "v3.db"
    osake_db = tmp_path / "osake.db"
    baseline = tmp_path / "baseline.csv"
    artifacts = tmp_path / "artifacts"
    _create_v3_companies(v3_db, [("BNK", True), ("SAFE", True), ("MREIT", True), ("EXCH", True), ("OLD", False)])
    _create_ticker_meta(
        osake_db,
        [
            ("BNK", "Financial Services", "Banks - Regional"),
            ("SAFE", "Consumer Defensive", "Packaged Foods"),
            ("MREIT", "Real Estate", "REIT - Mortgage"),
            ("EXCH", "Financial Services", "Financial Data & Stock Exchanges"),
            ("OLD", "Technology", "Software - Infrastructure"),
        ],
    )
    _write_csv(
        baseline,
        ["market", "ticker", "recommended_v3_company_active", "activity_classification"],
        [
            ["usa", "BNK", "1", "ACTIVE"],
            ["usa", "SAFE", "1", "ACTIVE"],
            ["usa", "MREIT", "1", "ACTIVE"],
            ["usa", "EXCH", "1", "ACTIVE"],
            ["usa", "OLD", "0", "DELISTED_OR_INACTIVE"],
        ],
    )

    plan = plan_refined_universe(v3_db=v3_db, osakedata_db=osake_db, company_baseline_csv=baseline, artifact_root=artifacts)
    refined = list(csv.DictReader((artifacts / "refined_universe.csv").open()))

    assert plan["current_approved"] == 5
    assert plan["excluded_tickers"] == ["BNK"]
    assert [row["ticker"] for row in refined] == ["EXCH", "MREIT", "OLD", "SAFE"]
    assert {row["ticker"]: row["recommended_v3_company_active"] for row in refined}["OLD"] == "0"
    assert len(list(csv.DictReader((artifacts / "excluded_banks.csv").open()))) == 1
    assert len(list(csv.DictReader((artifacts / "kept_reits.csv").open()))) == 1


def test_retained_company_parity_allows_excluded_drop_but_blocks_value_drift(tmp_path: Path) -> None:
    before = tmp_path / "before.db"
    after = tmp_path / "after.db"
    _create_v3_with_candidates(before, [_candidate("KEEP", revenue=100.0), _candidate("DROP", revenue=50.0)])
    _create_v3_with_candidates(after, [_candidate("KEEP", revenue=100.0)])

    parity = retained_company_parity(before, after)

    assert parity["summary"] == {"retained_company_value_differences": 0, "retained_company_metadata_differences": 0}

    drift = tmp_path / "drift.db"
    _create_v3_with_candidates(drift, [_candidate("KEEP", revenue=101.0)])
    drift_parity = retained_company_parity(before, drift)
    assert drift_parity["value_differences"] == 1


def test_special_case_parity_requires_cava_neup_and_lfcr_variant_exclusion(tmp_path: Path) -> None:
    db = tmp_path / "special.db"
    _create_v3_with_candidates(
        db,
        [
            _candidate("CAVA", fiscal_year=2026, fiscal_quarter="Q1", period_end_date="2026-03-31"),
            _candidate("NEUP", fiscal_year=2026, fiscal_quarter="Q1", period_end_date="2025-09-30"),
            _candidate("NEUP", fiscal_year=2026, fiscal_quarter="Q2", period_end_date="2025-12-31", source_record_id="YAHOO:NEUP:2025-12-31"),
            _candidate("NEUP", fiscal_year=2026, fiscal_quarter="Q3", period_end_date="2026-03-31", source_record_id="YAHOO:NEUP:2026-03-31"),
        ],
    )

    result = special_case_parity(db)

    assert result["passed"] is True


def test_v2_no_overwrite_policy_for_values_and_publish_dates() -> None:
    assert V2_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE is False
    assert decide_v2_value_action(field_name="revenue", existing_v3_value=100.0, v2_value=101.0, same_quarter_confirmed=True) == "CONFLICT_NO_OVERWRITE"
    assert decide_v2_value_action(field_name="revenue", existing_v3_value=None, v2_value=101.0, same_quarter_confirmed=True) == "FILL_NULL"
    assert decide_v2_value_action(field_name="revenue", existing_v3_value=100.0, v2_value=100.0, same_quarter_confirmed=True) == "CONFIRM_ONLY"
    assert decide_v2_value_action(field_name="revenue", existing_v3_value=None, v2_value=101.0, same_quarter_confirmed=False) == "BLOCK_IDENTITY_NOT_CONFIRMED"
    assert decide_v2_publish_date_action(existing_publish_date="2026-04-20", v2_publish_date="2026-04-21", same_quarter_confirmed=True) == "CONFLICT_NO_OVERWRITE"
    assert decide_v2_publish_date_action(existing_publish_date=None, v2_publish_date="2026-04-21", same_quarter_confirmed=True) == "FILL_NULL"
    assert decide_v2_publish_date_action(existing_publish_date="2026-04-20", v2_publish_date="2026-04-21", same_quarter_confirmed=False) == "BLOCK_IDENTITY_NOT_CONFIRMED"

    for field_name in CANONICAL_FIELD_NAMES:
        assert decide_v2_value_action(field_name=field_name, existing_v3_value=1.0, v2_value=2.0, same_quarter_confirmed=True) == "CONFLICT_NO_OVERWRITE"


def _class(ticker: str, *, sector: str, industry: str):
    return classify_company(ticker=ticker, company_name=ticker, active=1, sector=sector, industry=industry)


def _create_v3_companies(path: Path, rows: list[tuple[str, bool]]) -> None:
    with sqlite3.connect(path) as conn:
        apply_v3_schema(conn)
        repo = V3CompanyRepository(conn)
        for ticker, active in rows:
            repo.admit_company(market="usa", ticker=ticker, active=active, now_utc=NOW)
        conn.commit()


def _create_v3_with_candidates(path: Path, candidates: list[V3CanonicalMigrationCandidate]) -> None:
    with sqlite3.connect(path) as conn:
        apply_v3_schema(conn)
        conn.row_factory = sqlite3.Row
        V3CanonicalMigrationEngine(conn).apply_source_batch(candidates, source="YAHOO", migration_run_id="RUN", now_utc=NOW)
        conn.commit()


def _create_ticker_meta(path: Path, rows: list[tuple[str, str, str]]) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE ticker_meta (market TEXT NOT NULL, ticker TEXT NOT NULL, sector TEXT, industry TEXT)")
        conn.executemany("INSERT INTO ticker_meta VALUES ('usa', ?, ?, ?)", rows)
        conn.commit()


def _candidate(
    ticker: str,
    *,
    fiscal_year: int = 2026,
    fiscal_quarter: str = "Q1",
    period_end_date: str = "2026-03-31",
    source_record_id: str | None = None,
    revenue: float = 100.0,
) -> V3CanonicalMigrationCandidate:
    return V3CanonicalMigrationCandidate(
        source_system="YAHOO",
        source_record_id=source_record_id or f"YAHOO:{ticker}:{period_end_date}",
        migration_run_id="RUN",
        market="usa",
        ticker=ticker,
        fiscal_year=fiscal_year,
        fiscal_quarter=fiscal_quarter,
        period_end_date=period_end_date,
        publish_date="2026-04-20",
        values={"revenue": revenue, "ebitda": 10.0, "free_cashflow": 5.0, "cash": 20.0, "total_debt": 3.0, "shares_outstanding": 1.0},
        approved_company_active=True,
    )


def _write_csv(path: Path, headers: list[str], rows: list[list[str]]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows(rows)
