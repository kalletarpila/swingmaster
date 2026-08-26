from __future__ import annotations

import sqlite3

from swingmaster.fundamentals.v3_phase8a9_period_end_apply import (
    ORIGINAL_QUEUE_COLUMNS,
    apply_repairs,
    current_v3_reconciliation,
    freeze_repairs,
    reconcile_original_columns,
    sequence_guards,
    validate_verified_input,
)


def setup_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY, ticker TEXT, active INTEGER);
        CREATE TABLE v3_quarter(
            quarter_id INTEGER PRIMARY KEY,
            company_id INTEGER,
            fiscal_year INTEGER,
            fiscal_quarter TEXT,
            period_end_date TEXT,
            publish_date TEXT
        );
        CREATE TABLE v3_quarter_fundamentals(quarter_id INTEGER PRIMARY KEY, revenue REAL);
        INSERT INTO v3_company VALUES (1,'OKCO',1),(2,'DUPCO',1);
        INSERT INTO v3_quarter VALUES
            (10,1,2024,'Q3','2024-03-31',NULL),
            (11,1,2024,'Q4','2024-12-31',NULL),
            (12,1,2025,'Q1','2024-09-30',NULL),
            (20,2,2024,'Q4','2025-03-29',NULL),
            (21,2,2025,'Q4','2026-03-31',NULL);
        INSERT INTO v3_quarter_fundamentals VALUES (10,1),(11,1),(12,1),(20,1),(21,1);
        """
    )
    return conn


def verified_row(ticker: str, request_id: str, fy: str, fq: str, old: str, new: str, sources: str = "1") -> dict[str, str]:
    row = {column: "" for column in ORIGINAL_QUEUE_COLUMNS}
    row.update(
        {
            "Request ID": request_id,
            "Ticker": ticker,
            "Fiscal Year": fy,
            "Fiscal Q": fq,
            "Issue Type": "PERIOD_END",
            "Field": "period_end_date",
            "Period End": old,
            "Current Value": old,
            "Candidate Value": new,
            "Verified Period End": new,
            "Status": "DIFFERENT",
            "Confidence": "HIGH",
            "Source Count": sources,
            "Primary Source": "https://example.test/source",
            "Primary Source Type": "ISSUER_EARNINGS_RELEASE",
            "Verification Method": "DIRECT_PERIOD_CONFIRMATION",
        }
    )
    return row


def test_validate_verified_input_enforces_locked_counts() -> None:
    rows = [
        verified_row(f"T{i}", f"R{i}", "2025", "Q4", "2026-03-31", "2025-03-31", "2" if i < 12 else "1")
        for i in range(18)
    ]
    summary, checks = validate_verified_input(rows)
    assert summary["rows"] == 18
    assert summary["source_count_2plus"] == 12
    assert all(row["status"] == "PASS" for row in checks)


def test_original_column_reconciliation_rejects_substitution() -> None:
    row = verified_row("OKCO", "R1", "2024", "Q4", "2024-12-31", "2024-06-30")
    original = dict(row)
    original["Ticker"] = "OTHER"
    try:
        reconcile_original_columns([row], [original])
    except RuntimeError as exc:
        assert "no longer matches" in str(exc)
    else:
        raise AssertionError("expected original-column reconciliation failure")


def test_sequence_guard_allows_clean_period_end_repair_and_apply() -> None:
    conn = setup_db()
    row = verified_row("OKCO", "R1", "2024", "Q4", "2024-12-31", "2024-06-30")
    reconciled = current_v3_reconciliation(conn, [row])
    guarded = sequence_guards(conn, reconciled, {"R1": row})
    assert guarded[0]["sequence_guard"] == "VALID"
    repairs = freeze_repairs(guarded, {"R1": row})
    audit = apply_repairs(conn, repairs)
    assert audit[0]["apply_status"] == "APPLIED"
    assert conn.execute("SELECT period_end_date FROM v3_quarter WHERE quarter_id=11").fetchone()[0] == "2024-06-30"


def test_sequence_guard_blocks_same_company_period_end_collision() -> None:
    conn = setup_db()
    row = verified_row("DUPCO", "R1", "2025", "Q4", "2026-03-31", "2025-03-29", "2")
    guarded = sequence_guards(conn, current_v3_reconciliation(conn, [row]), {"R1": row})
    assert guarded[0]["sequence_guard"] == "COLLISION"
    assert freeze_repairs(guarded, {"R1": row}) == []
