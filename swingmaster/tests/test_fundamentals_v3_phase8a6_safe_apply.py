from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

import pytest

from swingmaster.fundamentals.v3_phase8a6_safe_apply import (
    CLASSIFICATION_REMAINS,
    Phase8A6Paths,
    find_semantic_verified_csv,
    reject_unverified_semantic_csv,
    run_phase8a6,
    semantic_guard,
)


SEMANTIC_HEADER = [
    "Ticker",
    "Fiscal Year",
    "Fiscal Q",
    "Field",
    "Current Value",
    "Period End",
    "Verified Value",
    "Status",
    "Confidence",
    "Source Count",
    "Primary Source",
    "Primary Source Type",
    "Secondary Source",
    "Secondary Source Type",
    "Source Period",
    "Verification Method",
    "Notes",
]


def write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    keys: list[str] = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def make_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY, ticker TEXT, market TEXT);
        CREATE TABLE v3_quarter(
            quarter_id INTEGER PRIMARY KEY, company_id INTEGER, fiscal_year INTEGER, fiscal_quarter TEXT,
            period_end_date TEXT, publish_date TEXT, q_lifecycle TEXT, sec_confirmation_state TEXT,
            created_at_utc TEXT, updated_at_utc TEXT
        );
        CREATE TABLE v3_quarter_fundamentals(
            quarter_id INTEGER PRIMARY KEY, revenue REAL, ebitda REAL, free_cashflow REAL, cash REAL,
            total_debt REAL, shares_outstanding REAL, ebit REAL, operating_income REAL,
            operating_cashflow REAL, capex REAL, gross_profit REAL, net_income REAL, currency TEXT,
            accepted_source_provider TEXT, accepted_at_utc TEXT, update_run_id TEXT, derivation_method TEXT,
            resolution_issue_id INTEGER, created_at_utc TEXT, updated_at_utc TEXT
        );
        CREATE TABLE v3_ttm(ttm_id INTEGER PRIMARY KEY);
        CREATE TABLE v3_score(score_id INTEGER PRIMARY KEY, score_model_version TEXT, score_fingerprint TEXT);
        CREATE TABLE v3_lifecycle(lifecycle_id INTEGER PRIMARY KEY, lifecycle_model_version TEXT, lifecycle_fingerprint TEXT);
        CREATE TABLE v3_valuation(valuation_id INTEGER PRIMARY KEY);
        INSERT INTO v3_company VALUES (1,'TST','usa');
        INSERT INTO v3_quarter VALUES (1,1,2025,'Q1','2025-03-31','2025-02-01','OPERATIONALLY_SETTLED','CONFIRMED','t0','t0');
        INSERT INTO v3_quarter_fundamentals VALUES (1,-10,NULL,NULL,-5,-7,0,NULL,NULL,NULL,NULL,NULL,NULL,'USD','YAHOO',NULL,NULL,NULL,NULL,'t0','t0');
        INSERT INTO v3_ttm VALUES (1);
        INSERT INTO v3_score VALUES (1,'V3_LEGACY2_FUNDAMENTAL_SCORE_V1','8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0');
        INSERT INTO v3_lifecycle VALUES (1,'V3_LIFECYCLE_EBIT_FIRST_V1','18c01984ee5ca69acaca64ab6ff2d5b1faa27d8400137f6d08200926e5658f9e');
        INSERT INTO v3_valuation VALUES (1);
        """
    )
    conn.commit()
    conn.close()


def publish_row(i: int = 1) -> dict[str, object]:
    return {
        "repair_id": f"P8A5-PUB-REPAIR-{i:03d}",
        "issue_id": f"P8-PUB-{i:03d}",
        "company_id": 1,
        "ticker": "TST",
        "fiscal_year": 2025,
        "fiscal_quarter": "Q1",
        "current_period_end": "2025-03-31",
        "selected_period_end": "2025-03-31",
        "current_publish_date": "2025-02-01",
        "new_publish_date": "2025-05-01",
        "evidence_type": "SEC Item 2.02 8-K (results release)",
        "source_1": "https://example.test/1",
        "source_2": "",
        "confidence": "HIGH",
        "publish_disposition": "REPAIR_PUBLISH_DATE",
        "period_end_disposition": "EXACT_MATCH",
        "expected_downstream_scope": "ttm=0;score=0;lifecycle=0;valuation=0",
    }


def period_row(i: int = 1) -> dict[str, object]:
    return {
        "repair_id": f"P8A5-PERIOD-REPAIR-{i:03d}",
        "issue_id": f"P8-PUB-{i:03d}",
        "company_id": 1,
        "ticker": "TST",
        "fiscal_year": 2025,
        "fiscal_quarter": "Q1",
        "current_period_end": "2025-03-31",
        "new_period_end": "2025-04-01",
        "trading_day_distance": 1,
        "evidence_type": "SEC Item 2.02 8-K (results release)",
        "source_1": "https://example.test/1",
        "source_2": "",
        "confidence": "HIGH",
    }


def semantic_row(**overrides: object) -> dict[str, object]:
    row = {
        "Ticker": "TST",
        "Fiscal Year": "2025",
        "Fiscal Q": "Q1",
        "Field": "revenue",
        "Current Value": "-10",
        "Period End": "2025-03-31",
        "Verified Value": "10",
        "Status": "DIFFERENT",
        "Confidence": "HIGH",
        "Source Count": "1",
        "Primary Source": "https://example.test/xbrl",
        "Primary Source Type": "SEC_XBRL",
        "Secondary Source": "",
        "Secondary Source Type": "",
        "Source Period": "FY2025 Q1, period ended 2025-03-31",
        "Verification Method": "DIRECT_QUARTER_VALUE",
        "Notes": "Official SEC XBRL discrete-quarter value.",
    }
    row.update(overrides)
    return row


def test_verified_semantic_file_location_and_unverified_rejection(tmp_path) -> None:
    verified = tmp_path / "phase8_semantic_manual_check_verified.csv"
    verified.write_text(",".join(SEMANTIC_HEADER) + "\n", encoding="utf-8")
    assert find_semantic_verified_csv(tmp_path) == verified
    with pytest.raises(RuntimeError):
        reject_unverified_semantic_csv(tmp_path / "fundamentals_v3_phase8_semantic_manual_check.csv")


def test_semantic_guard_requires_different_high_and_semantics() -> None:
    current = {"company_id": 1, "quarter_id": 1, "period_end_date": "2025-03-31", "revenue": -10.0}
    assert semantic_guard(semantic_row(), current, {})[0] is True
    assert semantic_guard(semantic_row(Confidence="MEDIUM"), current, {})[0] is False
    assert semantic_guard(semantic_row(Status="UNCERTAIN"), current, {})[0] is False
    assert semantic_guard(semantic_row(Status="NOT_FOUND"), current, {})[0] is False
    assert semantic_guard(semantic_row(Status="VALID_BUT_DIFFERENT_SEMANTICS"), current, {})[0] is False
    assert semantic_guard(semantic_row(**{"Verification Method": "SOURCE_SEMANTICS_CONFLICT"}), current, {})[0] is False


def test_non_revenue_semantic_guards() -> None:
    assert semantic_guard(
        semantic_row(Field="shares_outstanding", **{"Current Value": "0", "Verified Value": "100", "Verification Method": "DIRECT_PERIOD_END_VALUE", "Notes": "Exact period-end value; weighted-average EPS shares were not used."}),
        {"company_id": 1, "quarter_id": 1, "period_end_date": "2025-03-31", "shares_outstanding": 0.0},
        {},
    )[0]
    assert semantic_guard(
        semantic_row(Field="cash", **{"Current Value": "-5", "Verified Value": "5", "Verification Method": "DIRECT_PERIOD_END_VALUE", "Notes": "negative sign in Current Value is unsupported."}),
        {"company_id": 1, "quarter_id": 1, "period_end_date": "2025-03-31", "cash": -5.0},
        {},
    )[0]
    assert semantic_guard(
        semantic_row(Field="total_debt", **{"Current Value": "-7", "Verified Value": "7", "Verification Method": "DERIVED_DEBT_COMPONENTS", "Notes": "a negative debt tag occurrence was not treated as the liability balance."}),
        {"company_id": 1, "quarter_id": 1, "period_end_date": "2025-03-31", "total_debt": -7.0},
        {},
    )[0]


def test_run_phase8a6_applies_only_frozen_repairs_and_leaves_downstream_counts(tmp_path) -> None:
    db = tmp_path / "v3.db"
    make_db(db)
    a5 = tmp_path / "a5"
    a5.mkdir()
    write_rows(a5 / "publish_date_frozen_repair_set.csv", [publish_row(i) for i in range(1, 79)])
    write_rows(a5 / "period_end_frozen_repair_set.csv", [period_row(i) for i in range(1, 8)])
    write_rows(
        a5 / "publish_evidence_classification.csv",
        [{"issue_id": "P8-PUB-999", "publish_disposition": "MANUAL_REVIEW", "ticker": "TST", "fiscal_year": 2025, "fiscal_quarter": "Q1"}],
    )
    write_rows(
        a5 / "period_end_outside_tolerance_cases.csv",
        [{"issue_id": "P8-PUB-998", "ticker": "TST", "fiscal_year": 2025, "fiscal_quarter": "Q1"}],
    )
    semantic = tmp_path / "phase8_semantic_manual_check_verified.csv"
    write_rows(semantic, [semantic_row() for _ in range(87)] + [semantic_row(Status="MATCH") for _ in range(150)])
    summary = run_phase8a6(Phase8A6Paths(tmp_path / "out", db, a5, semantic))

    assert summary["classification"] == CLASSIFICATION_REMAINS
    assert summary["publish_repairs_applied"] == 1
    assert summary["period_end_repairs_applied"] == 1
    assert summary["semantic_repairs_applied"] == 1
    assert summary["write_guard_failures"] == 169
    assert summary["downstream_writes"] == {"v3_ttm": 0, "v3_score": 0, "v3_lifecycle": 0, "v3_valuation": 0}
    conn = sqlite3.connect(db)
    assert conn.execute("SELECT publish_date,period_end_date FROM v3_quarter").fetchone() == ("2025-05-01", "2025-04-01")
    assert conn.execute("SELECT revenue FROM v3_quarter_fundamentals").fetchone()[0] == 10
