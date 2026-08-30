from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.fundamentals.v3_phase8h3_wave1_reconciliation import (
    FIELD_MAP,
    GAP_STATUSES,
    METADATA_TYPES,
    apply_plan,
    build_repair_plan,
    category_rows,
    final_ticker_status,
    passed_group_count,
    parse_number,
    reconcile_facts,
    semantic_reclassification,
    source_safe_for_publish,
    validate_package,
    values_equal,
)


def fact(**overrides: str) -> dict[str, str]:
    row = {
        "research_task_id": "P8H-1-0001",
        "ticker": "TEST",
        "company_name": "Test Inc.",
        "requested_fiscal_year": "2026",
        "requested_fiscal_quarter": "Q1",
        "requested_evidence_type": "REVENUE",
        "requested_fact_description": "Revenue",
        "current_period_end": "2026-03-31",
        "current_publish_date": "",
        "verification_status": "VERIFIED",
        "verified_fiscal_year": "2026",
        "verified_fiscal_quarter": "Q1",
        "verified_period_end": "2026-03-31",
        "verified_publish_date": "",
        "verified_value": "1.2M",
        "verified_value_unit": "",
        "verified_value_definition": "Direct revenue",
        "quarter_exists_status": "EXISTS",
        "source_type": "official company earnings release",
        "source_title": "Test Announces First Quarter Results",
        "source_url": "https://example.com/results",
        "source_date": "2026-05-01",
        "secondary_source_type": "",
        "secondary_source_title": "",
        "secondary_source_url": "",
        "confidence": "HIGH",
        "discrepancy_vs_current": "MATCH",
        "structural_note": "",
        "researcher_note": "",
    }
    row.update(overrides)
    return row


def current(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "company_id": 1,
        "ticker": "TEST",
        "quarter_id": 10,
        "fiscal_year": 2026,
        "fiscal_quarter": "Q1",
        "period_end_date": "2026-03-31",
        "publish_date": "",
        "market_availability_date": "",
        "revenue": None,
        "ebit": None,
        "free_cashflow": None,
        "cash": None,
        "total_debt": None,
        "shares_outstanding": None,
        "operating_income": None,
        "operating_cashflow": None,
        "capex": None,
        "accepted_source_provider": "LEGACY",
        "derivation_method": "",
    }
    row.update(overrides)
    return row


def test_validate_package_locks_wave1_first_batch_counts() -> None:
    facts = [fact(verification_status="VERIFIED") for _ in range(581)]
    facts += [fact(verification_status="NOT_FOUND") for _ in range(47)]
    facts += [fact(verification_status="UNCERTAIN") for _ in range(29)]
    facts += [fact(verification_status="NOT_APPLICABLE") for _ in range(14)]
    for idx, row in enumerate(facts):
        row["ticker"] = f"T{idx % 210:03d}"
        row["research_task_id"] = f"P8H-1-{idx % 296:04d}"
    tasks = [{"research_task_id": f"P8H-1-{idx:04d}"} for idx in range(296)]

    result = validate_package({k: Path(k) for k in ("verified", "task_summary", "unresolved", "sources", "summary_md")}, facts, tasks, [], [])

    assert result["valid"] is True
    assert result["expected_counts_match"] is True
    assert result["tickers"] == 210


def test_validate_package_rejects_verified_row_without_source() -> None:
    rows = [fact(source_url="", source_title="")]
    result = validate_package({k: Path(k) for k in ("verified", "task_summary", "unresolved", "sources", "summary_md")}, rows, [{"research_task_id": "P8H-1-0001"}], [], [])

    assert result["valid"] is False
    assert result["verified_rows_without_source"] == 1


def test_parse_number_handles_suffixes_and_parentheses() -> None:
    assert parse_number("1.5M") == 1_500_000
    assert parse_number("(2.5)", "thousands") == -2500


def test_parse_number_rejects_not_directly_reported() -> None:
    assert parse_number("NOT_DIRECTLY_REPORTED") is None


def test_values_equal_uses_small_numeric_tolerance() -> None:
    assert values_equal(1_000_000, 1_000_050)
    assert not values_equal(1_000_000, 1_002_000)


def test_reconcile_maps_field_value_to_canonical_column() -> None:
    rows = reconcile_facts([fact()], {("TEST", "2026", "Q1", "2026-03-31"): current()})

    assert rows[0]["target_column"] == FIELD_MAP["REVENUE"]
    assert rows[0]["verified_value"] == 1_200_000
    assert rows[0]["canonical_current_missing"] == "YES"


def test_reconcile_maps_publish_date_source_metadata() -> None:
    rows = reconcile_facts(
        [fact(requested_evidence_type="FIRST_PUBLIC_PUBLISH_DATE", verified_publish_date="2026-05-04")],
        {("TEST", "2026", "Q1", "2026-03-31"): current()},
    )

    assert rows[0]["target_column"] == "publish_date"
    assert rows[0]["source_type"] == "official company earnings release"
    assert rows[0]["canonical_current_missing"] == "YES"


def test_generic_source_semantics_is_ignored() -> None:
    rows, ignored = semantic_reclassification(
        [
            fact(
                requested_evidence_type="SOURCE_SEMANTICS_CONFIRMATION",
                verified_value="Issuer-reported filing facts; fiscal focus FY2026 Q1",
                verified_value_definition="Official filing context and issuer-owned XBRL fact semantics.",
            )
        ]
    )

    assert rows[0]["influences_repair"] == "NO"
    assert ignored[0]["semantic_classification"] == "REDUNDANT_FISCAL_IDENTITY_CONFIRMATION"


def test_true_source_semantic_subtype_is_retained() -> None:
    rows, ignored = semantic_reclassification(
        [fact(requested_evidence_type="SOURCE_SEMANTICS_CONFIRMATION", verified_value="Confirm restatement vintage")]
    )

    assert rows[0]["semantic_classification"] == "TRUE_SEMANTIC_EVIDENCE"
    assert ignored == []


def test_source_safe_for_publish_accepts_official_release_distribution() -> None:
    assert source_safe_for_publish({"source_type": "official company earnings release (Business Wire distribution)"})


def test_source_safe_for_publish_does_not_accept_random_url_with_ir_letters() -> None:
    assert not source_safe_for_publish({"source": "https://third.example.com/businesswireless-preview"})


def test_build_repair_plan_fills_only_missing_canonical_field() -> None:
    rows = reconcile_facts([fact()], {("TEST", "2026", "Q1", "2026-03-31"): current()})
    plan, blockers = build_repair_plan(rows, {})

    assert blockers == []
    assert plan[0]["repair_type"] == "FILL_REVENUE"


def test_build_repair_plan_does_not_overwrite_non_null_field() -> None:
    rows = reconcile_facts([fact()], {("TEST", "2026", "Q1", "2026-03-31"): current(revenue=42)})
    plan, blockers = build_repair_plan(rows, {})

    assert plan == []
    assert blockers == []


def test_build_repair_plan_publish_requires_safe_source() -> None:
    rows = reconcile_facts(
        [fact(requested_evidence_type="FIRST_PUBLIC_PUBLISH_DATE", verified_publish_date="2026-05-04", source_type="blog")],
        {("TEST", "2026", "Q1", "2026-03-31"): current()},
    )
    plan, _blockers = build_repair_plan(rows, {})

    assert plan == []


def test_build_repair_plan_allows_safe_publish_fill() -> None:
    rows = reconcile_facts(
        [fact(requested_evidence_type="FIRST_PUBLIC_PUBLISH_DATE", verified_publish_date="2026-05-04")],
        {("TEST", "2026", "Q1", "2026-03-31"): current()},
    )
    plan, _blockers = build_repair_plan(rows, {})

    assert plan[0]["repair_type"] == "UPDATE_PUBLISH_DATE"


def test_build_repair_plan_blocks_occupied_fy_fq_target() -> None:
    rows = reconcile_facts(
        [fact(requested_evidence_type="OFFICIAL_FY_FQ_IDENTITY", verified_fiscal_year="2026", verified_fiscal_quarter="Q2")],
        {("TEST", "2026", "Q1", "2026-03-31"): current()},
    )
    plan, blockers = build_repair_plan(rows, {(1, "2026", "Q2"): 99})

    assert plan == []
    assert blockers[0]["blocker"] == "TARGET_IDENTITY_OCCUPIED"


def test_passed_group_count_excludes_partially_failed_group() -> None:
    log = [
        {"repair_group_id": "A", "result": "PASS"},
        {"repair_group_id": "A", "result": "FAILED"},
        {"repair_group_id": "B", "result": "PASS"},
    ]

    assert passed_group_count(log) == 1


def test_apply_plan_preserves_existing_provider_lineage_for_field_fill() -> None:
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE v3_quarter(quarter_id INTEGER PRIMARY KEY, fiscal_year INTEGER, fiscal_quarter TEXT, publish_date TEXT, updated_at_utc TEXT);
        CREATE TABLE v3_quarter_fundamentals(quarter_id INTEGER PRIMARY KEY, revenue REAL, accepted_source_provider TEXT, updated_at_utc TEXT);
        INSERT INTO v3_quarter VALUES (10,2026,'Q1',NULL,NULL);
        INSERT INTO v3_quarter_fundamentals VALUES (10,NULL,'LEGACY',NULL);
        """
    )

    log = apply_plan(
        conn,
        [
            {
                "repair_group_id": "G",
                "quarter_id": 10,
                "repair_type": "FILL_REVENUE",
                "target_table": "v3_quarter_fundamentals",
                "target_column": "revenue",
                "new_value": "123",
                "ticker": "TEST",
            }
        ],
    )

    assert log[0]["result"] == "PASS"
    assert conn.execute("SELECT accepted_source_provider FROM v3_quarter_fundamentals").fetchone()[0] == "LEGACY"


def test_apply_plan_updates_publish_date() -> None:
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE v3_quarter(quarter_id INTEGER PRIMARY KEY, fiscal_year INTEGER, fiscal_quarter TEXT, publish_date TEXT, updated_at_utc TEXT);
        CREATE TABLE v3_quarter_fundamentals(quarter_id INTEGER PRIMARY KEY, revenue REAL, accepted_source_provider TEXT, updated_at_utc TEXT);
        INSERT INTO v3_quarter VALUES (10,2026,'Q1',NULL,NULL);
        """
    )

    apply_plan(conn, [{"repair_group_id": "G", "quarter_id": 10, "repair_type": "UPDATE_PUBLISH_DATE", "target_table": "v3_quarter", "target_column": "publish_date", "new_value": "2026-05-01", "ticker": "TEST"}])

    assert conn.execute("SELECT publish_date FROM v3_quarter").fetchone()[0] == "2026-05-01"


def test_category_rows_keeps_filing_date_uncertainty_unresolved() -> None:
    rows = category_rows([fact(requested_evidence_type="FIRST_PUBLIC_PUBLISH_DATE", verification_status="UNCERTAIN")], {"FIRST_PUBLIC_PUBLISH_DATE"}, "PUBLISH")

    assert rows[0]["resolution_outcome"] == "FILING_DATE_ONLY_NOT_SAFE"


def test_category_rows_marks_verified_external_evidence_resolved() -> None:
    rows = category_rows([fact(requested_evidence_type="TOTAL_DEBT")], {"TOTAL_DEBT"}, "DEBT")

    assert rows[0]["resolution_outcome"] == "DEBT_RESOLVED_BY_EXTERNAL_EVIDENCE"


def test_final_ticker_status_does_not_reopen_redundant_semantics() -> None:
    status = final_ticker_status(
        {"TEST"},
        [fact(requested_evidence_type="SOURCE_SEMANTICS_CONFIRMATION")],
        [{"ticker": "TEST", "verification_status": "VERIFIED", "current_match": "NO", "evidence_type": "SOURCE_SEMANTICS_CONFIRMATION"}],
        [],
        [{"ticker": "TEST", "semantic_classification": "REDUNDANT_SEQUENCE_CONFIRMATION"}],
    )

    assert status[0]["final_status"] == "NO_REPAIR_REQUIRED_STRUCTURAL_FLAG_CLOSED"


def test_final_ticker_status_keeps_true_structural_semantics() -> None:
    status = final_ticker_status(
        {"TEST"},
        [fact(requested_evidence_type="SOURCE_SEMANTICS_CONFIRMATION")],
        [],
        [],
        [{"ticker": "TEST", "semantic_classification": "TRUE_SEMANTIC_EVIDENCE"}],
    )

    assert status[0]["final_status"] == "STRUCTURAL_REVIEW_REQUIRED"


def test_final_ticker_status_prioritizes_repair_ready() -> None:
    status = final_ticker_status({"TEST"}, [], [], [{"ticker": "TEST", "repair_group_id": "G"}], [])

    assert status[0]["final_status"] == "PRODUCTION_REPAIR_READY"


def test_final_ticker_status_separates_external_gaps_from_structural_gaps() -> None:
    external = final_ticker_status({"TEST"}, [fact(verification_status="NOT_FOUND", requested_evidence_type="EBIT_DIRECT")], [], [], [])
    structural = final_ticker_status({"TEST"}, [fact(verification_status="UNCERTAIN", requested_evidence_type="OFFICIAL_PERIOD_END")], [], [], [])

    assert external[0]["final_status"] == "MORE_EXTERNAL_EVIDENCE_REQUIRED"
    assert structural[0]["final_status"] == "STRUCTURAL_REVIEW_REQUIRED"


def test_status_constant_sets_are_locked_to_canonical_repair_surface() -> None:
    assert "REVENUE" in FIELD_MAP
    assert "FIRST_PUBLIC_PUBLISH_DATE" in METADATA_TYPES
    assert {"NOT_FOUND", "UNCERTAIN", "CONFLICT"} <= GAP_STATUSES
