from __future__ import annotations

import json
import sqlite3

from swingmaster.fundamentals_v2.phase9_sec_normalization import (
    SecObservation,
    V2Quarter,
    _apply_sec_revenue_rows,
    _build_sec_revenue_recoverability,
    _classify_sec_revenue_companies,
    classify_sec_context,
    parse_sec_fact_field_name,
)


def test_parse_sec_fact_field_name_extracts_context_metadata() -> None:
    meta = parse_sec_fact_field_name(
        "RevenueFromContractWithCustomerExcludingAssessedTax|form=10-Q|unit=USD|fy=2025|fp=Q1|frame=CY2025Q1|start=2025-01-01|filed=2025-05-01"
    )

    assert meta["concept"] == "RevenueFromContractWithCustomerExcludingAssessedTax"
    assert meta["form"] == "10-Q"
    assert meta["unit"] == "USD"
    assert meta["fp"] == "Q1"


def test_classify_sec_context_marks_q1_duration_as_direct_quarter() -> None:
    context = classify_sec_context(
        statement_type="income",
        period_type="sec_fact",
        field_name="RevenueFromContractWithCustomerExcludingAssessedTax|form=10-Q|unit=USD|fy=2025|fp=Q1|frame=CY2025Q1|start=2025-01-01|filed=2025-05-01",
        period_end_date="2025-03-31",
    )

    assert context["duration_class"] == "DIRECT_QUARTER"
    assert context["duration_days"] == 90


def test_company_scoped_revenue_requires_clean_overlap() -> None:
    v2_rows = [
        _v2("ABC", idx, f"2025-0{idx}-30", revenue=100.0 + idx)
        for idx in range(1, 5)
    ]
    observations = {
        "revenue": {
            (row.ticker, row.report_date): _sec_obs(row.ticker, row.report_date, float(row.values["revenue"]))
            for row in v2_rows
        }
    }

    quality, tiers = _classify_sec_revenue_companies(v2_rows, observations)

    assert tiers["ABC"] == "SAFE_SCOPED"
    assert [row for row in quality if row["ticker"] == "ABC"][0]["within_1_pct"] == 4


def test_revenue_recoverability_rejects_ambiguous_or_unvalidated_company() -> None:
    v2_rows = [_v2("ABC", 1, "2025-03-31", revenue=None)]
    observations = {"revenue": {("ABC", "2025-03-31"): _sec_obs("ABC", "2025-03-31", 123.0)}}

    rows = _build_sec_revenue_recoverability(v2_rows, observations, {"ABC": "NEEDS_MORE_VALIDATION"}, {"ABC": "2025-03-31"})

    assert rows[0]["category"] == "NEEDS_MORE_VALIDATION"


def test_apply_sec_revenue_rows_fills_null_with_context_formula_provenance() -> None:
    conn = _memory_v2()

    result = _apply_sec_revenue_rows(conn, [_eligible_row()], run_id="phase9_test", dry_run=False, now="2026-01-01T00:00:00Z")

    assert result[0]["action"] == "FILLED"
    assert conn.execute("SELECT revenue FROM rc_v2_fundamental_quarterly WHERE quarter_id=1").fetchone()[0] == 123.0
    source = conn.execute("SELECT source_value FROM rc_v2_fundamental_field_source WHERE quarter_id=1").fetchone()[0]
    payload = json.loads(source)
    assert payload["validation_tier"] == "SAFE_SCOPED"
    assert payload["provider"] == "SEC"
    assert payload["formula"] == "none"
    assert payload["sec_context"]["duration_class"] == "DIRECT_QUARTER"


def test_apply_sec_revenue_rows_does_not_overwrite_existing_revenue() -> None:
    conn = _memory_v2(existing_revenue=99.0)

    result = _apply_sec_revenue_rows(conn, [_eligible_row()], run_id="phase9_test", dry_run=False)

    assert result[0]["action"] == "CONFLICT_EXISTING_DIFFERENT"
    assert conn.execute("SELECT revenue FROM rc_v2_fundamental_quarterly WHERE quarter_id=1").fetchone()[0] == 99.0
    assert conn.execute("SELECT COUNT(*) FROM rc_v2_fundamental_field_source").fetchone()[0] == 0


def _v2(ticker: str, quarter_id: int, report_date: str, *, revenue: float | None) -> V2Quarter:
    return V2Quarter(
        ticker=ticker,
        company_id=1,
        quarter_id=quarter_id,
        fiscal_year=2025,
        fiscal_period="Q1",
        report_date=report_date,
        values={
            "revenue": revenue,
            "ebitda": None,
            "free_cashflow": None,
            "operating_cashflow": None,
            "capex": None,
            "shares_outstanding": None,
            "cash": None,
            "total_debt": None,
            "ebit": None,
        },
    )


def _sec_obs(ticker: str, date: str, value: float) -> SecObservation:
    return SecObservation(
        ticker=ticker,
        period_end_date=date,
        field_name="Total Revenue",
        value=value,
        statement_type="income",
        period_type="quarterly",
        currency="USD",
        source="sec_edgar",
        retrieved_at_utc="2026-01-01T00:00:00Z",
        run_id="legacy",
    )


def _eligible_row() -> dict[str, object]:
    return {
        "ticker": "ABC",
        "company_id": 1,
        "quarter_id": 1,
        "fiscal_year": 2025,
        "fiscal_period": "Q1",
        "report_date": "2025-03-31",
        "target_field": "revenue",
        "provider": "SEC",
        "field_concept": "Total Revenue",
        "candidate_value": 123.0,
        "provider_date": "2025-03-31",
        "match_mode": "EXACT_DATE_INFERRED_FISCAL",
        "date_offset_days": 0,
        "fiscal_identity_verified": 0,
        "risk_tier": "SAFE_SCOPED",
        "category": "SAFE_SCOPED_RECOVERY",
        "validation_rule": "company-scoped SEC reconstructed quarterly Total Revenue",
        "sec_context": "DIRECT_QUARTER_RECONSTRUCTED",
        "is_latest": 1,
        "legacy_run_id": "legacy",
        "retrieved_at_utc": "2026-01-01T00:00:00Z",
    }


def _memory_v2(*, existing_revenue: float | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE rc_v2_fundamental_quarterly (
            quarter_id INTEGER PRIMARY KEY,
            revenue REAL,
            available_canonical_field_count INTEGER NOT NULL DEFAULT 0,
            updated_at_utc TEXT
        );
        CREATE TABLE rc_v2_import_run (
            import_run_id TEXT PRIMARY KEY,
            market TEXT NOT NULL,
            simfin_dir TEXT NOT NULL,
            builder_version TEXT NOT NULL,
            started_at_utc TEXT NOT NULL,
            finished_at_utc TEXT
        );
        CREATE TABLE rc_v2_fundamental_field_source (
            quarter_id INTEGER NOT NULL,
            field_name TEXT NOT NULL,
            provider TEXT NOT NULL,
            provider_field TEXT NOT NULL,
            source_dataset TEXT NOT NULL,
            source_file TEXT NOT NULL,
            source_file_sha256 TEXT NOT NULL,
            transformation TEXT NOT NULL,
            source_value TEXT,
            import_run_id TEXT NOT NULL,
            created_at_utc TEXT NOT NULL,
            PRIMARY KEY (quarter_id, field_name, provider)
        );
        """
    )
    conn.execute(
        "INSERT INTO rc_v2_fundamental_quarterly (quarter_id, revenue, available_canonical_field_count, updated_at_utc) VALUES (1, ?, 0, '')",
        (existing_revenue,),
    )
    return conn
