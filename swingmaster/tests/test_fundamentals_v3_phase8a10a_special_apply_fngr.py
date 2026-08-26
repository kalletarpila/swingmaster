from __future__ import annotations

import sqlite3

from swingmaster.fundamentals import v3_phase8a10a_special_apply_fngr as fngr


def apply_rows() -> list[dict[str, str]]:
    return [
        {
            "ticker": "FNGR",
            "transformation_group_id": "P8A10A-SPECIAL-FNGR",
            "current_canonical_quarter_id": "37082",
            "current_fy": "2024",
            "current_fq": "Q2",
            "current_period_end": "2024-05-31",
            "current_publish_date": "2023-10-16",
            "field": "period_end",
            "old_value": "2024-05-31",
            "new_value": "2023-08-31",
            "operation": "UPDATE_PERIOD_END",
        },
        {
            "ticker": "FNGR",
            "transformation_group_id": "P8A10A-SPECIAL-FNGR",
            "current_canonical_quarter_id": "37082",
            "current_fy": "2024",
            "current_fq": "Q2",
            "current_period_end": "2024-05-31",
            "current_publish_date": "2023-10-16",
            "field": "publish_date",
            "old_value": "2023-10-16",
            "new_value": "2023-10-13",
            "operation": "UPDATE_PUBLISH_DATE",
        },
    ]


def db(*, drift: bool = False) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    period = "2024-05-30" if drift else "2024-05-31"
    conn.executescript(
        f"""
        CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY, ticker TEXT, active INTEGER);
        CREATE TABLE v3_quarter(
            quarter_id INTEGER PRIMARY KEY, company_id INTEGER, fiscal_year INTEGER, fiscal_quarter TEXT,
            period_end_date TEXT, publish_date TEXT, market_availability_date TEXT, q_lifecycle TEXT,
            sec_confirmation_state TEXT, UNIQUE(company_id,fiscal_year,fiscal_quarter)
        );
        CREATE TABLE v3_quarter_fundamentals(
            quarter_id INTEGER PRIMARY KEY, revenue REAL, gross_profit REAL, operating_income REAL, ebit REAL,
            ebitda REAL, net_income REAL, operating_cashflow REAL, capex REAL, free_cashflow REAL, cash REAL,
            total_debt REAL, shares_outstanding REAL, currency TEXT, accepted_source_provider TEXT,
            accepted_at_utc TEXT, update_run_id TEXT, derivation_method TEXT
        );
        CREATE TABLE v3_migration_audit(audit_id INTEGER PRIMARY KEY, quarter_id INTEGER);
        CREATE TABLE v3_ttm(ttm_id INTEGER PRIMARY KEY);
        CREATE TABLE v3_score(score_id INTEGER PRIMARY KEY);
        CREATE TABLE v3_lifecycle(lifecycle_id INTEGER PRIMARY KEY);
        CREATE TABLE v3_valuation(valuation_id INTEGER PRIMARY KEY);
        INSERT INTO v3_company VALUES (913,'FNGR',1);
        INSERT INTO v3_quarter VALUES (37082,913,2024,'Q2','{period}','2023-10-16',NULL,'OPERATIONALLY_SETTLED','CONFIRMED');
        INSERT INTO v3_quarter_fundamentals VALUES (37082,8373983,681889,-1676089,-1676089,-958371,-1655904,-1409939,NULL,NULL,1064124,NULL,52660051,'USD','V2','a','r','d');
        INSERT INTO v3_migration_audit VALUES (1,37082);
        INSERT INTO v3_migration_audit VALUES (2,37082);
        """
    )
    return conn


def test_frozen_scope_only_fngr() -> None:
    fngr.validate_frozen_scope(apply_rows())


def test_one_transformation_group() -> None:
    assert {row["transformation_group_id"] for row in apply_rows()} == {"P8A10A-SPECIAL-FNGR"}


def test_quarter_id_37082() -> None:
    assert {row["current_canonical_quarter_id"] for row in apply_rows()} == {"37082"}


def test_fy2024_q2_unchanged() -> None:
    conn = db()
    before = fngr.fngr_row(conn)
    fngr.apply_fngr(conn)
    after = fngr.fngr_row(conn)
    assert (before["fiscal_year"], before["fiscal_quarter"]) == (after["fiscal_year"], after["fiscal_quarter"]) == (2024, "Q2")


def test_period_end_old_value_guard() -> None:
    guards, _ = fngr.write_guards(db(), apply_rows())
    assert [row for row in guards if row["check"] == "period_end_old"][0]["status"] == "PASS"


def test_publish_date_old_value_guard() -> None:
    guards, _ = fngr.write_guards(db(), apply_rows())
    assert [row for row in guards if row["check"] == "publish_date_old"][0]["status"] == "PASS"


def test_period_end_becomes_2023_08_31() -> None:
    conn = db()
    fngr.apply_fngr(conn)
    assert fngr.fngr_row(conn)["period_end_date"] == "2023-08-31"


def test_publish_date_becomes_2023_10_13() -> None:
    conn = db()
    fngr.apply_fngr(conn)
    assert fngr.fngr_row(conn)["publish_date"] == "2023-10-13"


def test_revenue_remains_8373983() -> None:
    conn = db()
    fngr.apply_fngr(conn)
    assert fngr.fngr_row(conn)["revenue"] == 8373983


def test_all_fundamentals_unchanged() -> None:
    conn = db()
    before = fngr.fngr_row(conn)
    fngr.apply_fngr(conn)
    after = fngr.fngr_row(conn)
    assert [before[field] for field in fngr.FUNDAMENTAL_FIELDS] == [after[field] for field in fngr.FUNDAMENTAL_FIELDS]


def test_lineage_unchanged() -> None:
    conn = db()
    before = fngr.fngr_row(conn)["lineage_refs"]
    fngr.apply_fngr(conn)
    assert fngr.fngr_row(conn)["lineage_refs"] == before


def test_no_fy_fq_change() -> None:
    conn = db()
    fngr.apply_fngr(conn)
    row = fngr.fngr_row(conn)
    assert row["fiscal_year"] == 2024 and row["fiscal_quarter"] == "Q2"


def test_changed_cells_exactly_2() -> None:
    conn = db()
    before = fngr.fngr_row(conn)
    fngr.apply_fngr(conn)
    assert set(fngr.changed_cells(before, fngr.fngr_row(conn))) == {"period_end_date", "publish_date"}


def test_sparse_history_does_not_trigger_segment_shift() -> None:
    assert fngr.structural_validation(db(), fngr.fngr_row(db()))["sparse_history_non_blocking"] is True


def test_corrected_reporting_lag_positive() -> None:
    assert fngr.structural_validation(db(), fngr.fngr_row(db()))["corrected_reporting_lag_days"] == 43


def test_fngr_exits_structural_r1() -> None:
    assert "FNGR" not in {row["ticker"] for row in fngr.post_structural_r1()}


def test_immr_remains_r1() -> None:
    assert "IMMR" in {row["ticker"] for row in fngr.post_structural_r1()}


def test_rcat_remains_r1() -> None:
    assert "RCAT" in {row["ticker"] for row in fngr.post_structural_r1()}


def test_no_new_r1() -> None:
    assert len(fngr.post_structural_r1()) == 2


def test_no_ttm_writes() -> None:
    conn = db()
    before = fngr.table_counts(conn)["v3_ttm"]
    fngr.apply_fngr(conn)
    assert fngr.table_counts(conn)["v3_ttm"] == before


def test_no_score_writes() -> None:
    conn = db()
    before = fngr.table_counts(conn)["v3_score"]
    fngr.apply_fngr(conn)
    assert fngr.table_counts(conn)["v3_score"] == before


def test_no_lifecycle_writes() -> None:
    conn = db()
    before = fngr.table_counts(conn)["v3_lifecycle"]
    fngr.apply_fngr(conn)
    assert fngr.table_counts(conn)["v3_lifecycle"] == before


def test_no_valuation_writes() -> None:
    conn = db()
    before = fngr.table_counts(conn)["v3_valuation"]
    fngr.apply_fngr(conn)
    assert fngr.table_counts(conn)["v3_valuation"] == before


def test_no_rawcandle_writes_contract() -> None:
    assert str(fngr.Phase8A10AFngrApplyPaths.__dataclass_fields__["rawcandle_db"].default).endswith("rawcandle/data/osakedata.db")


def test_rollback_on_old_value_drift() -> None:
    conn = db(drift=True)
    try:
        fngr.apply_fngr(conn)
    except RuntimeError:
        pass
    row = conn.execute("SELECT period_end_date,publish_date FROM v3_quarter WHERE quarter_id=37082").fetchone()
    assert tuple(row) == ("2024-05-30", "2023-10-16")
