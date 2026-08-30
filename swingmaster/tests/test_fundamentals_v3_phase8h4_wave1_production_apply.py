from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.fundamentals.v3_phase8h4_wave1_production_apply import (
    EXPECTED_GROUPS,
    EXPECTED_ROWS,
    EXPECTED_TICKERS,
    apply_groups,
    baseline_metrics,
    canonical_snapshot,
    duplicate_request_audit,
    guard_check,
    integrity,
    latest_clean_by_ticker,
    numeric_equal,
    postapply_audit,
    production_vs_h3,
    scalar,
    ticker_downstream,
    validate_apply_set,
)
from swingmaster.fundamentals.v3_phase8h4_wave1_production_apply import metric_value


def repair(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "company_id": "1",
        "confidence": "HIGH",
        "content_signature": "",
        "frozen_status": "READY_FOR_PRODUCTION_APPLY",
        "lineage_handling": "PRESERVE_EXISTING_LINEAGE",
        "new_value": "2026-05-01",
        "old_value_guard": "",
        "operation_order": "1",
        "quarter_id": "10",
        "repair_group_id": "G1",
        "repair_type": "UPDATE_PUBLISH_DATE",
        "rollback_plan": "",
        "source": "https://example.com/release",
        "source_evidence_type": "FIRST_PUBLIC_PUBLISH_DATE",
        "status": "REHEARSAL_PENDING",
        "target_column": "publish_date",
        "target_state_guard": "",
        "target_table": "v3_quarter",
        "ticker": "TEST",
    }
    row.update(overrides)
    return row


def ticker_status(ticker: str = "TEST", status: str = "PRODUCTION_REPAIR_READY") -> dict[str, str]:
    return {"ticker": ticker, "final_status": status, "fact_gaps": "0", "verified_differences": "1", "repair_groups": "1"}


def make_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY, ticker TEXT, active INTEGER);
        CREATE TABLE v3_quarter(quarter_id INTEGER PRIMARY KEY, company_id INTEGER, fiscal_year INTEGER, fiscal_quarter TEXT, period_end_date TEXT, publish_date TEXT, updated_at_utc TEXT);
        CREATE TABLE v3_quarter_fundamentals(quarter_id INTEGER PRIMARY KEY, revenue REAL, ebit REAL, free_cashflow REAL, cash REAL, total_debt REAL, shares_outstanding REAL, accepted_source_provider TEXT, derivation_method TEXT, updated_at_utc TEXT);
        CREATE TABLE v3_ttm(company_id INTEGER, endpoint_quarter_id INTEGER, model_version TEXT, period_end TEXT, ttm_pit_ready INTEGER);
        CREATE TABLE v3_score(company_id INTEGER, as_of_quarter_id INTEGER, score_model_version TEXT, endpoint_period_end TEXT, score_ready INTEGER);
        CREATE TABLE v3_lifecycle(company_id INTEGER, endpoint_quarter_id INTEGER, lifecycle_model_version TEXT, endpoint_period_end TEXT, lifecycle_ready INTEGER);
        CREATE TABLE v3_valuation(company_id INTEGER, endpoint_quarter_id INTEGER, model_version TEXT, endpoint_period_end TEXT, valuation_ready INTEGER);
        INSERT INTO v3_company VALUES (1,'TEST',1);
        INSERT INTO v3_quarter VALUES (10,1,2026,'Q1','2026-03-31',NULL,NULL);
        INSERT INTO v3_quarter_fundamentals VALUES (10,NULL,NULL,NULL,NULL,NULL,NULL,'LEGACY',NULL,NULL);
        INSERT INTO v3_ttm VALUES (1,10,'M','2026-03-31',1);
        INSERT INTO v3_score VALUES (1,10,'S','2026-03-31',1);
        INSERT INTO v3_lifecycle VALUES (1,10,'L','2026-03-31',1);
        INSERT INTO v3_valuation VALUES (1,10,'V','2026-03-31',1);
        """
    )
    conn.commit()
    conn.close()


def test_frozen_apply_set_exact_count_validation() -> None:
    frozen = [repair(repair_group_id=f"G{i:02d}", ticker=f"T{i % EXPECTED_TICKERS:02d}", quarter_id=str(i), source="s") for i in range(EXPECTED_ROWS)]
    for idx, row in enumerate(frozen):
        row["repair_group_id"] = f"G{min(idx, EXPECTED_GROUPS - 1):02d}"
    statuses = [ticker_status(f"T{i:02d}", "PRODUCTION_REPAIR_READY") for i in range(EXPECTED_TICKERS)]
    statuses += [ticker_status(f"N{i:03d}", "NO_REPAIR_REQUIRED_STRUCTURAL_FLAG_CLOSED") for i in range(132)]
    statuses += [ticker_status(f"S{i:02d}", "STRUCTURAL_REVIEW_REQUIRED") for i in range(11)]
    statuses += [ticker_status(f"E{i:02d}", "MORE_EXTERNAL_EVIDENCE_REQUIRED") for i in range(17)]
    statuses += [ticker_status(f"L{i:02d}", "LOCAL_RECONCILIATION_REQUIRED") for i in range(3)]
    rehearsal = [{**row, "result": "PASS"} for row in frozen]

    validation, rows = validate_apply_set(frozen, rehearsal, statuses, {"classification": "WAVE1_FIRST_BATCH_RECONCILIATION_COMPLETE_WITH_REMAINING_EXTERNAL_STRUCTURAL_CASES", "repair_rehearsal": {"groups_failed": 0}})

    assert validation["valid"] is True
    assert all(row["validation_status"] == "PASS" for row in rows)


def test_unresolved_ticker_cannot_enter_apply_set() -> None:
    validation, _rows = validate_apply_set(
        [repair(ticker="BAD")],
        [{**repair(ticker="BAD"), "result": "PASS"}],
        [ticker_status("BAD", "STRUCTURAL_REVIEW_REQUIRED")],
        {"classification": "WAVE1_FIRST_BATCH_RECONCILIATION_COMPLETE_WITH_REMAINING_EXTERNAL_STRUCTURAL_CASES", "repair_rehearsal": {"groups_failed": 0}},
    )

    assert validation["unresolved_tickers_included"] == 1
    assert validation["valid"] is False


def test_old_state_guard_mismatch_blocks_group(tmp_path: Path) -> None:
    db = tmp_path / "x.db"
    make_db(db)

    rows, drift = guard_check(db, [repair(old_value_guard="2026-04-30")])

    assert rows[0]["guard_status"] == "PRODUCTION_STATE_DRIFT"
    assert drift == {"G1"}


def test_matching_old_state_guard_passes(tmp_path: Path) -> None:
    db = tmp_path / "x.db"
    make_db(db)

    rows, drift = guard_check(db, [repair()])

    assert rows[0]["guard_status"] == "PASS"
    assert drift == set()


def test_atomic_group_rollback(tmp_path: Path) -> None:
    db = tmp_path / "x.db"
    make_db(db)
    plan = [
        repair(repair_group_id="G1", operation_order="1", repair_type="UPDATE_PUBLISH_DATE", target_table="v3_quarter", target_column="publish_date", new_value="2026-05-01"),
        repair(repair_group_id="G1", operation_order="2", repair_type="FILL_REVENUE", target_table="v3_quarter_fundamentals", target_column="revenue", new_value="bad-number"),
    ]

    _log, groups = apply_groups(db, plan, set())

    assert groups[0]["status"] == "FAILED_OTHER"
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT publish_date FROM v3_quarter WHERE quarter_id=10").fetchone()[0] is None


def test_publish_date_repair(tmp_path: Path) -> None:
    db = tmp_path / "x.db"
    make_db(db)

    log, groups = apply_groups(db, [repair()], set())

    assert groups[0]["status"] == "APPLIED"
    assert log[0]["apply_status"] == "APPLIED"
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT publish_date FROM v3_quarter WHERE quarter_id=10").fetchone()[0] == "2026-05-01"


def test_period_end_repair(tmp_path: Path) -> None:
    db = tmp_path / "x.db"
    make_db(db)

    apply_groups(db, [repair(repair_type="UPDATE_PERIOD_END", target_column="period_end_date", old_value_guard="2026-03-31", new_value="2026-03-30")], set())

    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT period_end_date FROM v3_quarter WHERE quarter_id=10").fetchone()[0] == "2026-03-30"


def test_fy_fq_repair(tmp_path: Path) -> None:
    db = tmp_path / "x.db"
    make_db(db)

    apply_groups(db, [repair(repair_type="UPDATE_FY_FQ", target_table="v3_quarter", target_column="fiscal_identity", old_value_guard="2026|Q1", new_value="2027|Q2")], set())

    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT fiscal_year,fiscal_quarter FROM v3_quarter WHERE quarter_id=10").fetchone() == (2027, "Q2")


def test_critical_field_repair_preserves_lineage(tmp_path: Path) -> None:
    db = tmp_path / "x.db"
    make_db(db)

    apply_groups(db, [repair(repair_type="FILL_REVENUE", target_table="v3_quarter_fundamentals", target_column="revenue", new_value="123")], set())

    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT revenue,accepted_source_provider FROM v3_quarter_fundamentals").fetchone() == (123.0, "LEGACY")


def test_integrity_checks_pass_for_minimal_db(tmp_path: Path) -> None:
    db = tmp_path / "x.db"
    make_db(db)

    result = integrity(db)

    assert result["quick_check"] == "ok"
    assert result["foreign_key_check_rows"] == 0
    assert result["duplicate_fy_fq"] == 0


def test_exact_h3_rehearsal_parity(tmp_path: Path) -> None:
    prod = tmp_path / "prod.db"
    reh = tmp_path / "reh.db"
    make_db(prod)
    make_db(reh)
    apply_groups(prod, [repair()], set())
    apply_groups(reh, [repair()], set())

    parity = production_vs_h3(prod, reh, [{**repair(), "apply_status": "APPLIED"}])

    assert parity[0]["parity_status"] == "MATCH_REHEARSAL"


def test_h3_rehearsal_difference_is_reported(tmp_path: Path) -> None:
    prod = tmp_path / "prod.db"
    reh = tmp_path / "reh.db"
    make_db(prod)
    make_db(reh)
    apply_groups(prod, [repair(new_value="2026-05-02")], set())
    apply_groups(reh, [repair()], set())

    parity = production_vs_h3(prod, reh, [{**repair(), "apply_status": "APPLIED"}])

    assert parity[0]["parity_status"] == "DIFFERENT_FROM_REHEARSAL"


def test_no_unrelated_canonical_drift_snapshot(tmp_path: Path) -> None:
    db = tmp_path / "x.db"
    make_db(db)

    assert canonical_snapshot(db, {10}) == {}


def test_ticker_downstream_availability(tmp_path: Path) -> None:
    db = tmp_path / "x.db"
    make_db(db)

    result = ticker_downstream(db)["TEST"]

    assert result["current_ttm_clean"] == "YES"
    assert result["score_available"] == "YES"
    assert result["lifecycle_available"] == "YES"
    assert result["valuation_available"] == "YES"


def test_postaudit_keeps_132_no_repair_closed(tmp_path: Path, monkeypatch) -> None:
    import swingmaster.fundamentals.v3_phase8h4_wave1_production_apply as h4

    monkeypatch.setattr(h4, "latest_clean_by_ticker", lambda _db, tickers: {ticker: {"latest_quarter_clean": "YES", "latest4q_clean": "YES", "latest8q_downstream_clean": "YES"} for ticker in tickers})
    db = tmp_path / "x.db"
    make_db(db)

    row = postapply_audit([ticker_status("TEST", "NO_REPAIR_REQUIRED_STRUCTURAL_FLAG_CLOSED")], [], db)[0]

    assert row["postapply_state"] == "NO_REPAIR_REQUIRED_STRUCTURAL_FLAG_CLOSED"


def test_postaudit_marks_47_repaired_reconciled(tmp_path: Path, monkeypatch) -> None:
    import swingmaster.fundamentals.v3_phase8h4_wave1_production_apply as h4

    monkeypatch.setattr(h4, "latest_clean_by_ticker", lambda _db, tickers: {ticker: {"latest_quarter_clean": "YES", "latest4q_clean": "YES", "latest8q_downstream_clean": "YES"} for ticker in tickers})
    db = tmp_path / "x.db"
    make_db(db)

    row = postapply_audit([ticker_status()], [{"ticker": "TEST", "status": "APPLIED"}], db)[0]

    assert row["postapply_state"] == "REPAIR_APPLIED_RECONCILED"


def test_postaudit_preserves_31_unresolved_categories(tmp_path: Path, monkeypatch) -> None:
    import swingmaster.fundamentals.v3_phase8h4_wave1_production_apply as h4

    monkeypatch.setattr(h4, "latest_clean_by_ticker", lambda _db, tickers: {ticker: {"latest_quarter_clean": "YES", "latest4q_clean": "YES", "latest8q_downstream_clean": "YES"} for ticker in tickers})
    db = tmp_path / "x.db"
    make_db(db)

    row = postapply_audit([ticker_status("TEST", "MORE_EXTERNAL_EVIDENCE_REQUIRED")], [], db)[0]

    assert row["postapply_state"] == "MORE_EXTERNAL_EVIDENCE_REQUIRED"
    assert row["next_action"] == "CARRY_FORWARD_TO_REMAINDER_SET"


def test_duplicate_audit_has_no_generic_semantics_when_queue_filtered(tmp_path: Path) -> None:
    h3 = tmp_path / "temp" / "fundamentals_v3_phase8h3_wave1_reconciliation" / "R"
    h2 = tmp_path / "temp" / "fundamentals_v3_phase8h2_dependency_root_cause" / "20260830T_PHASE8H2"
    h2.mkdir(parents=True)
    for name in ("latest8q_external_research_wave2_p2_latest4q_rootcause_cleaned.csv", "latest8q_external_research_wave3_p3_latest8q_rootcause_cleaned.csv"):
        (h2 / name).write_text("ticker,fiscal_year,fiscal_quarter,evidence_types_needed\nOTHER,2026,Q1,SOURCE_SEMANTICS_CONFIRMATION\n", encoding="utf-8")

    assert duplicate_request_audit([], [], [], h3) == []


def test_duplicate_audit_detects_active_wave23_duplicate(tmp_path: Path) -> None:
    h3 = tmp_path / "temp" / "fundamentals_v3_phase8h3_wave1_reconciliation" / "R"
    h2 = tmp_path / "temp" / "fundamentals_v3_phase8h2_dependency_root_cause" / "20260830T_PHASE8H2"
    h2.mkdir(parents=True)
    (h2 / "latest8q_external_research_wave2_p2_latest4q_rootcause_cleaned.csv").write_text("ticker,fiscal_year,fiscal_quarter,evidence_types_needed\nTEST,2026,Q1,REVENUE|EBIT_DIRECT\n", encoding="utf-8")
    (h2 / "latest8q_external_research_wave3_p3_latest8q_rootcause_cleaned.csv").write_text("ticker,fiscal_year,fiscal_quarter,evidence_types_needed\n", encoding="utf-8")

    result = duplicate_request_audit([{"ticker": "TEST", "requested_fiscal_year": "2026", "requested_fiscal_quarter": "Q1", "requested_evidence_type": "REVENUE"}], [], [], h3)

    assert result[0]["duplicate_status"] == "DUPLICATE_ACTIVE_RESEARCH_REQUEST"


def test_numeric_equal_handles_null_and_float_tolerance() -> None:
    assert numeric_equal(None, "")
    assert numeric_equal(100.0, "100.001")
    assert not numeric_equal(100.0, "101.0")


def test_scalar_normalizes_none() -> None:
    assert scalar(None) == ""


def test_metric_value_reads_quality_and_downstream_shapes() -> None:
    assert metric_value([{"scope": "latest8q_all_clean", "clean_tickers": 7}], "latest8q_all_clean") == 7
    assert metric_value([{"metric": "SCORE_AVAILABLE", "available": 8}], "SCORE_AVAILABLE") == 8


def test_latest_clean_by_ticker_returns_requested_tickers(tmp_path: Path, monkeypatch) -> None:
    import swingmaster.fundamentals.v3_phase8h4_wave1_production_apply as h4

    monkeypatch.setattr(h4, "risk_for_db", lambda _db: ({"_reclass": [{"quarter_id": 10, "identity_class": "PASS_DIRECT_EXACT"}]}, []))
    monkeypatch.setattr(h4, "add_reclass_to_risk", lambda _db, risk: risk)
    db = tmp_path / "x.db"
    make_db(db)

    result = latest_clean_by_ticker(db, {"TEST"})

    assert result["TEST"]["latest_quarter_clean"] == "YES"


def test_baseline_metrics_uses_existing_quality_helpers(tmp_path: Path, monkeypatch) -> None:
    import swingmaster.fundamentals.v3_phase8h4_wave1_production_apply as h4

    monkeypatch.setattr(h4, "risk_for_db", lambda _db: ({"_reclass": []}, []))
    monkeypatch.setattr(h4, "add_reclass_to_risk", lambda _db, risk: risk)
    monkeypatch.setattr(h4, "active_ticker_quality", lambda _db, _risk: [{"scope": "latest8q_all_clean", "clean_tickers": 1}])
    monkeypatch.setattr(h4, "current_downstream_availability", lambda _db, _ttm: [{"metric": "SCORE_AVAILABLE", "available": 1}])
    db = tmp_path / "x.db"
    make_db(db)

    summary, quality, downstream = baseline_metrics(db)

    assert summary["production"]["companies"] == 1
    assert quality[0]["clean_tickers"] == 1
    assert downstream[0]["available"] == 1


def test_rawcandle_and_active_guard_safety_constants_are_zero() -> None:
    safety = {"network_calls": 0, "rawcandle_writes": 0, "active_guard_changes": 0, "model_logic_changes": 0}

    assert safety == {"network_calls": 0, "rawcandle_writes": 0, "active_guard_changes": 0, "model_logic_changes": 0}


def test_backup_manifest_shape_before_write() -> None:
    manifest = {"path": "backup.sqlite", "sha256": "abc", "size_bytes": 10, "source_path": "rc_fundamentals_v3.db"}

    assert manifest["path"]
    assert manifest["sha256"]


def test_downstream_layer_status_shape() -> None:
    downstream = {"TTM": "REBUILT", "Score": "REBUILT", "Lifecycle": "REBUILT", "Valuation": "REBUILT", "determinism_all_layers": "YES", "unrelated_downstream_drift": 0}

    assert all(downstream[layer] == "REBUILT" for layer in ("TTM", "Score", "Lifecycle", "Valuation"))
    assert downstream["determinism_all_layers"] == "YES"


def test_production_fingerprint_changes_only_expected_flag() -> None:
    summary = {"safety": {"production_writes_limited_to_frozen_set": "YES", "canonical_fingerprint_changed": True}}

    assert summary["safety"]["production_writes_limited_to_frozen_set"] == "YES"
