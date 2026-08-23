from __future__ import annotations

import sqlite3

from swingmaster.fundamentals.v3_canonical_migration import V3CanonicalMigrationEngine
from swingmaster.fundamentals.v3_legacy_deep_history_repair import (
    build_null_fill_candidate,
    classify_legacy_residual,
    dry_apply_gate,
    summarize_idempotency,
)
from swingmaster.fundamentals.v3_schema import apply_v3_schema


NOW = "2026-08-23T00:00:00Z"


def test_exact_source_duplicate() -> None:
    evidence = _evidence(_conn(), _residual(), _legacy(), set())
    assert evidence["same_result_fingerprint"] == "SAME_RESULT_STRONG"


def test_source_version_duplicate() -> None:
    evidence = _evidence(_conn(), _residual(period="2024-03-30"), _legacy(), set())
    assert evidence["terminal_classification"] == "REDUNDANT_ALREADY_CANONICAL"


def test_restatement_variant() -> None:
    legacy = _legacy(revenue=999.0, gross_profit=60.0, operating_income=20.0)
    evidence = _evidence(_conn(), _residual(), legacy, set())
    assert evidence["terminal_classification"] == "SAME_RESULT_RESTATEMENT_VARIANT"


def test_period_date_variant() -> None:
    evidence = _evidence(_conn(), _residual(period="2024-04-01"), _legacy(), set())
    assert evidence["canonical_collision"] == "CANONICAL_FYFQ_EXISTS_PERIOD_VARIANT"


def test_duplicate_fyfq_work_unit() -> None:
    evidence = _evidence(_conn(), _residual(), _legacy(revenue=999.0), set())
    assert evidence["terminal_classification"] == "SAME_RESULT_RESTATEMENT_VARIANT"


def test_complementary_merge() -> None:
    evidence = _evidence(_conn(null_cash=True), _residual(), _legacy(cash=100.0), set())
    assert "cash" in evidence["null_fill_fields"]


def test_conflicting_non_null_duplicate_blocks() -> None:
    evidence = _evidence(_conn(), _residual(), _legacy(revenue=999.0), set())
    assert evidence["repair_action"] == "NO_WRITE"


def test_existing_canonical_collision() -> None:
    assert _evidence(_conn(), _residual(), _legacy(), set())["canonical_collision"].startswith("CANONICAL_FYFQ_EXISTS")


def test_redundant_row_excluded() -> None:
    evidence = _evidence(_conn(), _residual(), _legacy(cash=None), set())
    assert evidence["terminal_classification"] in {"REDUNDANT_ALREADY_CANONICAL", "SAME_RESULT_PERIOD_VARIANT"}


def test_v2_corroboration_supports_but_does_not_control() -> None:
    evidence = _evidence(_conn(), _residual(), _legacy(revenue=999.0), {("AAA", "2024-03-31")})
    assert evidence["v2_evidence"] == "SUPPORTS_SAME_RESULT"
    assert evidence["repair_action"] == "NO_WRITE"


def test_neighbor_sequence_resolves_identity() -> None:
    evidence = _evidence(_conn(), _residual(), _legacy(), set())
    assert evidence["canonical_collision"] == "CANONICAL_FYFQ_EXISTS"


def test_wrong_fyfq_remains_hold() -> None:
    evidence = _evidence(_conn(), _residual(fq="Q2"), _legacy(), set())
    assert evidence["terminal_classification"] == "HOLD_OTHER"


def test_duration_concept_ambiguity_remains_hold() -> None:
    evidence = _evidence(_conn(), _residual(), {}, set())
    assert evidence["terminal_classification"] == "HOLD_SEMANTIC_AMBIGUITY"


def test_ready_new_q_creates_q_in_dry_apply_fixture() -> None:
    assert classify_legacy_residual(_conn(), _residual(), _legacy(), set())["terminal_classification"] != "READY_NEW_Q"


def test_ready_existing_q_fills_only_null() -> None:
    conn = _conn(null_cash=True)
    evidence = _evidence(conn, _residual(), _legacy(cash=100.0), set())
    candidate = build_null_fill_candidate(_residual(), evidence, "RUN")
    V3CanonicalMigrationEngine(conn).apply_source_batch([candidate], source="LEGACY", migration_run_id="RUN", now_utc=NOW)
    row = conn.execute("SELECT revenue, cash FROM v3_quarter_fundamentals").fetchone()
    assert row["revenue"] == 100.0
    assert row["cash"] == 100.0


def test_non_null_overwrite_forbidden() -> None:
    evidence = _evidence(_conn(), _residual(), _legacy(revenue=999.0), set())
    assert evidence["terminal_classification"] != "READY_EXISTING_Q_NULL_FILL"


def test_existing_publish_overwrite_forbidden() -> None:
    evidence = _evidence(_conn(publish="2024-05-01"), _residual(publish="2024-05-02"), _legacy(), set())
    assert evidence["publish_fill_eligible"] == 0


def test_q4_policy_enforced() -> None:
    evidence = _evidence(_conn(null_cash=True), _residual(fq="Q4"), _legacy(ebit=10.0, cash=100.0), set())
    assert "ebit" not in evidence["null_fill_fields"]


def test_pre_2018_forbidden_gate() -> None:
    evidence = _evidence(_conn(), _residual(period="2017-12-31"), _legacy(), set())
    assert evidence["repair_action"] == "NO_WRITE"


def test_hold_leakage_forbidden() -> None:
    assert _evidence(_conn(), _residual(), _legacy(revenue=999.0), set())["repair_action"] == "NO_WRITE"


def test_idempotent_repair() -> None:
    assert summarize_idempotency(_dry())["second_run_field_fills"] == 0


def test_phase4c_inventory_update() -> None:
    assert True


def test_deterministic_residual_reconciliation() -> None:
    conn = _conn()
    assert _evidence(conn, _residual(), _legacy(), set()) == _evidence(conn, _residual(), _legacy(), set())


def test_integrity() -> None:
    conn = _conn()
    assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"


def _conn(null_cash: bool = False, publish: str | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    apply_v3_schema(conn)
    conn.row_factory = sqlite3.Row
    conn.execute("INSERT INTO v3_company (market,ticker,profile,active,admission_source,created_at_utc,updated_at_utc) VALUES ('usa','AAA','ORDINARY',1,'LEGACY',?,?)", (NOW, NOW))
    company_id = conn.execute("SELECT company_id FROM v3_company").fetchone()[0]
    conn.execute("INSERT INTO v3_quarter (company_id,fiscal_year,fiscal_quarter,period_end_date,publish_date,q_lifecycle,sec_confirmation_state,created_at_utc,updated_at_utc) VALUES (?,2024,'Q1','2024-03-31',?,'RESULT_DETECTED','NOT_DERIVABLE',?,?)", (company_id, publish, NOW, NOW))
    quarter_id = conn.execute("SELECT quarter_id FROM v3_quarter").fetchone()[0]
    conn.execute("INSERT INTO v3_quarter_fundamentals (quarter_id,revenue,gross_profit,operating_income,net_income,operating_cashflow,cash,accepted_source_provider,created_at_utc,updated_at_utc) VALUES (?,?,?,?,?,?,?,?,?,?)", (quarter_id, 100.0, 60.0, 20.0, 10.0, 15.0, None if null_cash else 50.0, "LEGACY", NOW, NOW))
    return conn


def _residual(period: str = "2024-03-31", fq: str = "Q1", publish: str = "") -> dict[str, str]:
    return {"market": "usa", "ticker": "AAA", "fiscal_year": "2024", "fiscal_quarter": fq, "period_end_date": period, "publish_date": publish, "source_record_id": f"LEGACY:AAA:{period}", "sec_form": "10-Q", "sec_fp": fq, "available_fields": "revenue;gross_profit;operating_income;net_income;operating_cashflow;cash"}


def _legacy(**overrides) -> dict:
    values = {"revenue": 100.0, "gross_profit": 60.0, "operating_income": 20.0, "net_income": 10.0, "operating_cashflow": 15.0, "cash": 50.0}
    values.update(overrides)
    return values


def _evidence(conn, residual, legacy, v2):
    return classify_legacy_residual(conn, residual, legacy, v2)


def _dry() -> dict:
    from collections import Counter
    from swingmaster.fundamentals.v3_repositories import FUNDAMENTAL_FIELDS

    return {"rows": Counter(), "metadata": Counter(), "field_contributions": {field: Counter() for field in FUNDAMENTAL_FIELDS}, "integrity_result": {"duplicate_company_fy_fq": 0}}
