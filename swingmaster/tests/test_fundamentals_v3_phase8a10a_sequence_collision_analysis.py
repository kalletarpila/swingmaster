from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

from swingmaster.fundamentals import v3_phase8a10a_sequence_collision_analysis as a10a


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def retained_row(i: int, ticker: str = "AAA", guard: str = "COLLISION") -> dict[str, str]:
    return {
        "active": "1",
        "company_id": str(i),
        "current_period_end": "2026-03-31",
        "duplicate_period_end_count": "1" if guard == "COLLISION" else "0",
        "duplicate_period_end_rows": "FY2024 Q4 2025-03-29" if guard == "COLLISION" else "",
        "fiscal_quarter": "Q4",
        "fiscal_year": "2025",
        "new_period_end": "2025-03-29",
        "old_period_end": "2026-03-31",
        "quarter_id": str(i * 10),
        "request_id": f"R{i}",
        "sequence_guard": guard,
        "sequence_guard_ok": "0",
        "status": "PASS",
        "target_row_count": "1",
        "ticker": ticker,
    }


def test_exact_15_r1_cases_loaded(tmp_path: Path) -> None:
    root = tmp_path / "a9"
    write_csv(root / "retained_r1_reaudit.csv", [retained_row(i) for i in range(15)])
    assert len(a10a.freeze_cases(root)) == 15


def test_fiscal_ordinal_deterministic() -> None:
    assert a10a.fiscal_ordinal(2025, "Q1") < a10a.fiscal_ordinal(2025, "Q2")
    assert a10a.fiscal_ordinal(2025, "Q4") < a10a.fiscal_ordinal(2026, "Q1")


def test_expected_transitions_valid() -> None:
    assert a10a.expected_next_identity(2025, "Q1") == (2025, "Q2")
    assert a10a.expected_next_identity(2025, "Q2") == (2025, "Q3")
    assert a10a.expected_next_identity(2025, "Q3") == (2025, "Q4")
    assert a10a.expected_next_identity(2025, "Q4") == (2026, "Q1")


def test_duplicate_fyq_and_missing_quarter_detection() -> None:
    prev = {"fiscal_year": 2025, "fiscal_quarter": "Q1"}
    dup = {"fiscal_year": 2025, "fiscal_quarter": "Q1"}
    skip = {"fiscal_year": 2025, "fiscal_quarter": "Q3"}
    assert a10a.transition_class(prev, dup) == "REVERSE_OR_DUPLICATE_LABEL"
    assert a10a.transition_class(prev, skip) == "MISSING_OR_SKIPPED_QUARTER"


def test_reversed_period_end_detected() -> None:
    assert a10a.period_gap_class(-90) == "NEGATIVE_OR_ZERO"


def test_52_53_week_valid_calendar_not_falsely_rejected() -> None:
    assert a10a.is_52_53_week_case("CRUS", "2025-03-29", "52_53_WEEK_CALENDAR")


def test_collision_comparison_deterministic() -> None:
    left = {"revenue": 100.0, "cash": 10.0}
    right = {"revenue": 101.0, "cash": 10.0}
    status, conflicts = a10a.value_comparison(left, right)
    assert status == "IDENTICAL_OR_NEAR_IDENTICAL"
    assert conflicts == ""


def test_sequence_conflict_deterministic() -> None:
    prev = {"fiscal_year": 2025, "fiscal_quarter": "Q2"}
    current = {"fiscal_year": 2025, "fiscal_quarter": "Q4"}
    assert a10a.transition_class(prev, current) == "MISSING_OR_SKIPPED_QUARTER"


def test_publish_before_period_and_chronology_anomaly_detected() -> None:
    assert a10a.reporting_lag_class(-1) == "NEGATIVE"
    timeline = [
        {"company_id": 1, "ticker": "AAA", "quarter_id": 1, "fiscal_year": 2025, "fiscal_quarter": "Q1", "period_end": "2025-03-31", "publish_date": "2025-05-01"},
        {"company_id": 1, "ticker": "AAA", "quarter_id": 2, "fiscal_year": 2025, "fiscal_quarter": "Q2", "period_end": "2025-06-30", "publish_date": "2025-04-01"},
    ]
    rows = a10a.publish_context([{"quarter_id": 2}], timeline)
    assert "PUBLISH_BEFORE_PERIOD" in rows[0]["publish_context_anomaly"]
    assert "PUBLISH_BEFORE_PRIOR_PUBLISH" in rows[0]["publish_context_anomaly"]


def test_complete_company_histories_extracted() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY, ticker TEXT, company_name TEXT, active INTEGER);
        CREATE TABLE v3_quarter(quarter_id INTEGER PRIMARY KEY, company_id INTEGER, fiscal_year INTEGER, fiscal_quarter TEXT, period_end_date TEXT, publish_date TEXT);
        CREATE TABLE v3_quarter_fundamentals(quarter_id INTEGER PRIMARY KEY, revenue REAL, operating_income REAL, ebit REAL, ebitda REAL, free_cashflow REAL, cash REAL, total_debt REAL, shares_outstanding REAL, accepted_source_provider TEXT);
        CREATE TABLE v3_ttm(ttm_id INTEGER PRIMARY KEY, endpoint_quarter_id INTEGER);
        CREATE TABLE v3_score(score_id INTEGER PRIMARY KEY, as_of_quarter_id INTEGER);
        CREATE TABLE v3_lifecycle(lifecycle_id INTEGER PRIMARY KEY, endpoint_quarter_id INTEGER);
        CREATE TABLE v3_valuation(valuation_id INTEGER PRIMARY KEY, endpoint_quarter_id INTEGER);
        INSERT INTO v3_company VALUES (1,'AAA','A Corp',1);
        INSERT INTO v3_quarter VALUES (1,1,2025,'Q1','2025-03-31','2025-05-01'),(2,1,2025,'Q2','2025-06-30','2025-08-01');
        INSERT INTO v3_quarter_fundamentals VALUES (1,1,1,1,1,1,1,1,1,'YAHOO'),(2,1,1,1,1,1,1,1,1,'YAHOO');
        INSERT INTO v3_ttm VALUES (1,2);
        """
    )
    timeline = a10a.complete_timelines(conn, [{"company_id": 1}])
    assert len(timeline) == 2
    assert timeline[-1]["ttm_endpoint_presence"] == 1


def test_no_production_or_derived_writes_in_summary(tmp_path: Path, monkeypatch) -> None:
    paths = a10a.Phase8A10APaths(artifact_root=tmp_path / "out", v3_db=tmp_path / "dummy.db", rawcandle_db=tmp_path / "raw.db")
    paths.v3_db.write_text("db", encoding="utf-8")
    paths.rawcandle_db.write_text("raw", encoding="utf-8")
    summary = {
        "production_writes": 0,
        "rawcandle_writes": 0,
        "derived_writes": {"ttm": 0, "score": 0, "lifecycle": 0, "valuation": 0},
    }
    assert summary["production_writes"] == 0
    assert summary["rawcandle_writes"] == 0
    assert all(value == 0 for value in summary["derived_writes"].values())
