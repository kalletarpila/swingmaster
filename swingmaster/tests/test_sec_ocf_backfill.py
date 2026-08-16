from __future__ import annotations

import json
import sqlite3

from swingmaster.fundamentals_v2.sec_ocf_backfill import (
    OCF_DIRECT_Q1,
    OCF_RECONSTRUCTED_Q2,
    OCF_RECONSTRUCTED_Q3,
    SEC_OCF_CONCEPT,
    SecCashflowFact,
    V2OcfQuarter,
    apply_sec_ocf_rows,
    build_narrowed_ocf_candidate_inventory,
    build_ocf_company_rule_quality,
    build_ocf_candidate_inventory,
    evaluate_sec_ocf_candidate,
    validate_source_period_uniqueness,
)


def test_q1_direct_success() -> None:
    q1 = _quarter("Q1", "2026-03-31")
    result = evaluate_sec_ocf_candidate(q1, {"Q1": q1}, _facts(_fact("2026-01-01", "2026-03-31", 12.0)))

    assert result.eligible is True
    assert result.rule_type == OCF_DIRECT_Q1
    assert result.value == 12.0
    assert result.transformation == "DIRECT_QUARTER_OCF"


def test_q2_subtraction_success() -> None:
    q1 = _quarter("Q1", "2026-03-31")
    q2 = _quarter("Q2", "2026-06-30")

    result = evaluate_sec_ocf_candidate(
        q2,
        {"Q1": q1, "Q2": q2},
        _facts(
            _fact("2026-01-01", "2026-03-31", 10.0),
            _fact("2026-01-01", "2026-06-30", 25.0),
        ),
    )

    assert result.eligible is True
    assert result.rule_type == OCF_RECONSTRUCTED_Q2
    assert result.value == 15.0
    assert result.arithmetic == "25.0 - 10.0"


def test_q3_subtraction_success() -> None:
    q2 = _quarter("Q2", "2026-06-30")
    q3 = _quarter("Q3", "2026-09-30")

    result = evaluate_sec_ocf_candidate(
        q3,
        {"Q2": q2, "Q3": q3},
        _facts(
            _fact("2026-01-01", "2026-06-30", 25.0),
            _fact("2026-01-01", "2026-09-30", 45.0),
        ),
    )

    assert result.eligible is True
    assert result.rule_type == OCF_RECONSTRUCTED_Q3
    assert result.value == 20.0


def test_missing_subtraction_component_rejects() -> None:
    q2 = _quarter("Q2", "2026-06-30")

    result = evaluate_sec_ocf_candidate(q2, {"Q2": q2}, {})

    assert result.eligible is False
    assert result.rejection_reason == "SUBTRACTION_COMPONENT_MISSING"


def test_mismatched_unit_dimension_concept_and_ambiguity_reject() -> None:
    q1 = _quarter("Q1", "2026-03-31")

    unit = evaluate_sec_ocf_candidate(q1, {"Q1": q1}, _facts(_fact("2026-01-01", "2026-03-31", 1.0, unit="EUR")))
    dimension = evaluate_sec_ocf_candidate(q1, {"Q1": q1}, _facts(_fact("2026-01-01", "2026-03-31", 1.0, raw_suffix="[Axis=Member]")))
    concept = evaluate_sec_ocf_candidate(q1, {"Q1": q1}, _facts(_fact("2026-01-01", "2026-03-31", 1.0, concept="OtherConcept")))
    ambiguous = evaluate_sec_ocf_candidate(
        q1,
        {"Q1": q1},
        _facts(_fact("2026-01-01", "2026-03-31", 1.0), _fact("2026-01-01", "2026-03-31", 2.0)),
    )

    assert unit.rejection_reason == "SOURCE_FACT_MISSING"
    assert dimension.rejection_reason == "SOURCE_FACT_MISSING"
    assert concept.rejection_reason == "SOURCE_FACT_MISSING"
    assert ambiguous.rejection_reason == "MULTIPLE_SOURCE_FACTS_AMBIGUOUS"


def test_non_calendar_fiscal_year_and_negative_ocf_are_allowed_when_context_matches() -> None:
    q1 = _quarter("Q1", "2026-04-30")

    result = evaluate_sec_ocf_candidate(q1, {"Q1": q1}, _facts(_fact("2026-02-01", "2026-04-30", -3.0)))

    assert result.eligible is True
    assert result.value == -3.0


def test_q4_is_not_enabled_without_explicit_review_gate() -> None:
    q4 = _quarter("Q4", "2026-12-31")

    result = evaluate_sec_ocf_candidate(q4, {"Q4": q4}, {})

    assert result.eligible is False
    assert result.rejection_reason == "Q4_AUDIT_ONLY_NOT_APPROVED"


def test_q4_can_be_evaluated_for_audit_only_when_explicitly_allowed() -> None:
    q3 = _quarter("Q3", "2026-09-30")
    q4 = _quarter("Q4", "2026-12-31")

    result = evaluate_sec_ocf_candidate(
        q4,
        {"Q3": q3, "Q4": q4},
        _facts(
            _fact("2026-01-01", "2026-09-30", 45.0),
            _fact("2026-01-01", "2026-12-31", 70.0, form="10-K"),
        ),
        allow_q4=True,
    )

    assert result.eligible is True
    assert result.rule_type == "SAFE_RECONSTRUCTED_Q4"
    assert result.value == 25.0


def test_canonical_already_non_null_rejects_for_backfill_scope() -> None:
    q1 = _quarter("Q1", "2026-03-31", operating_cashflow=99.0)

    result = evaluate_sec_ocf_candidate(q1, {"Q1": q1}, _facts(_fact("2026-01-01", "2026-03-31", 12.0)))

    assert result.eligible is False
    assert result.rejection_reason == "CANONICAL_ALREADY_NON_NULL"


def test_apply_is_null_fill_only_and_replay_idempotent() -> None:
    conn = _memory_v2()
    q1 = _quarter("Q1", "2026-03-31")
    inventory = [
        row
        for row in build_ocf_candidate_inventory([q1], _facts(_fact("2026-01-01", "2026-03-31", 12.0)), require_null_canonical=True)
        if row["eligible"] == 1
    ]

    first = apply_sec_ocf_rows(conn, inventory, run_id="phase9i3_test", dry_run=False, now="2026-08-16T00:00:00Z")
    second = apply_sec_ocf_rows(conn, inventory, run_id="phase9i3_test", dry_run=False, now="2026-08-16T00:00:00Z")
    source = conn.execute("SELECT source_value FROM rc_v2_fundamental_field_source").fetchone()[0]
    payload = json.loads(source)

    assert first[0]["action"] == "FILLED"
    assert second[0]["action"] == "SAME_VALUE_NOOP"
    assert conn.execute("SELECT operating_cashflow FROM rc_v2_fundamental_quarterly WHERE quarter_id=1").fetchone()[0] == 12.0
    assert conn.execute("SELECT COUNT(*) FROM rc_v2_fundamental_field_source").fetchone()[0] == 1
    assert payload["source_facts"][0]["concept"] == SEC_OCF_CONCEPT
    assert payload["transformation"] == "DIRECT_QUARTER_OCF"


def test_apply_does_not_overwrite_existing_non_null() -> None:
    conn = _memory_v2(existing_ocf=99.0)
    row = {
        **build_ocf_candidate_inventory(
            [_quarter("Q1", "2026-03-31", operating_cashflow=None)],
            _facts(_fact("2026-01-01", "2026-03-31", 12.0)),
            require_null_canonical=True,
        )[0],
        "eligible": 1,
    }

    result = apply_sec_ocf_rows(conn, [row], run_id="phase9i3_test", dry_run=False)

    assert result[0]["action"] == "CONFLICT_EXISTING_DIFFERENT"
    assert conn.execute("SELECT operating_cashflow FROM rc_v2_fundamental_quarterly WHERE quarter_id=1").fetchone()[0] == 99.0
    assert conn.execute("SELECT COUNT(*) FROM rc_v2_fundamental_field_source").fetchone()[0] == 0


def test_narrowed_rule_requires_clean_company_rule_overlap() -> None:
    base = build_ocf_candidate_inventory(
        [
            _quarter("Q1", f"2026-03-{day:02d}", operating_cashflow=10.0)
            for day in (28, 29, 30, 31)
        ]
        + [_quarter("Q1", "2026-03-27", operating_cashflow=None)],
        _facts(
            _fact("2026-01-01", "2026-03-28", 10.0),
            _fact("2026-01-01", "2026-03-29", 10.0),
            _fact("2026-01-01", "2026-03-30", 10.0),
            _fact("2026-01-01", "2026-03-31", 10.0),
            _fact("2026-01-01", "2026-03-27", 11.0),
        ),
        require_null_canonical=False,
    )

    quality = build_ocf_company_rule_quality(base)
    narrowed = build_narrowed_ocf_candidate_inventory(base, quality)

    assert quality[("TEST", OCF_DIRECT_Q1)]["safe_for_production"] is True
    assert [row for row in narrowed if row["current_canonical_value"] is None][0]["narrowed_safe_for_production"] == 1


def test_narrowed_rule_rejects_company_rule_with_overlap_outlier() -> None:
    base = build_ocf_candidate_inventory(
        [
            _quarter("Q1", "2026-03-28", operating_cashflow=10.0),
            _quarter("Q1", "2026-03-29", operating_cashflow=10.0),
            _quarter("Q1", "2026-03-30", operating_cashflow=10.0),
            _quarter("Q1", "2026-03-31", operating_cashflow=99.0),
            _quarter("Q1", "2026-03-27", operating_cashflow=None),
        ],
        _facts(
            _fact("2026-01-01", "2026-03-28", 10.0),
            _fact("2026-01-01", "2026-03-29", 10.0),
            _fact("2026-01-01", "2026-03-30", 10.0),
            _fact("2026-01-01", "2026-03-31", 10.0),
            _fact("2026-01-01", "2026-03-27", 11.0),
        ),
        require_null_canonical=False,
    )

    quality = build_ocf_company_rule_quality(base)
    narrowed = build_narrowed_ocf_candidate_inventory(base, quality)

    assert quality[("TEST", OCF_DIRECT_Q1)]["safe_for_production"] is False
    assert [row for row in narrowed if row["current_canonical_value"] is None][0]["narrowed_safe_for_production"] == 0


def test_source_period_uniqueness_allows_q2_different_h1_and_q1_values() -> None:
    result = validate_source_period_uniqueness(
        "SAFE_RECONSTRUCTED_Q2_V2",
        [_fact_payload("2026-01-01", "2026-06-30", 25.0), _fact_payload("2026-01-01", "2026-03-31", 10.0)],
    )

    assert result["passes"] is True
    assert result["period_count"] == 2


def test_source_period_uniqueness_allows_q3_different_9m_and_h1_values() -> None:
    result = validate_source_period_uniqueness(
        "SAFE_RECONSTRUCTED_Q3_V2",
        [_fact_payload("2026-01-01", "2026-09-30", 45.0), _fact_payload("2026-01-01", "2026-06-30", 25.0)],
    )

    assert result["passes"] is True
    assert result["period_count"] == 2


def test_source_period_uniqueness_rejects_two_distinct_h1_values_for_q2() -> None:
    result = validate_source_period_uniqueness(
        "SAFE_RECONSTRUCTED_Q2_V2",
        [
            _fact_payload("2026-01-01", "2026-06-30", 25.0),
            _fact_payload("2026-01-01", "2026-06-30", 26.0),
            _fact_payload("2026-01-01", "2026-03-31", 10.0),
        ],
    )

    assert result["passes"] is False
    assert result["reason"] == "MULTIPLE_DISTINCT_VALUES_WITHIN_SOURCE_PERIOD"


def test_source_period_uniqueness_rejects_two_distinct_q1_values_for_q2() -> None:
    result = validate_source_period_uniqueness(
        "SAFE_RECONSTRUCTED_Q2_V2",
        [
            _fact_payload("2026-01-01", "2026-06-30", 25.0),
            _fact_payload("2026-01-01", "2026-03-31", 10.0),
            _fact_payload("2026-01-01", "2026-03-31", 11.0),
        ],
    )

    assert result["passes"] is False
    assert result["reason"] == "MULTIPLE_DISTINCT_VALUES_WITHIN_SOURCE_PERIOD"


def test_source_period_uniqueness_rejects_two_distinct_9m_values_for_q3() -> None:
    result = validate_source_period_uniqueness(
        "SAFE_RECONSTRUCTED_Q3_V2",
        [
            _fact_payload("2026-01-01", "2026-09-30", 45.0),
            _fact_payload("2026-01-01", "2026-09-30", 46.0),
            _fact_payload("2026-01-01", "2026-06-30", 25.0),
        ],
    )

    assert result["passes"] is False
    assert result["reason"] == "MULTIPLE_DISTINCT_VALUES_WITHIN_SOURCE_PERIOD"


def test_source_period_uniqueness_rejects_two_distinct_h1_values_for_q3() -> None:
    result = validate_source_period_uniqueness(
        "SAFE_RECONSTRUCTED_Q3_V2",
        [
            _fact_payload("2026-01-01", "2026-09-30", 45.0),
            _fact_payload("2026-01-01", "2026-06-30", 25.0),
            _fact_payload("2026-01-01", "2026-06-30", 26.0),
        ],
    )

    assert result["passes"] is False
    assert result["reason"] == "MULTIPLE_DISTINCT_VALUES_WITHIN_SOURCE_PERIOD"


def test_source_period_uniqueness_dedupes_identical_facts_within_period() -> None:
    result = validate_source_period_uniqueness(
        "SAFE_RECONSTRUCTED_Q2_V2",
        [
            _fact_payload("2026-01-01", "2026-06-30", 25.0),
            _fact_payload("2026-01-01", "2026-06-30", 25.0),
            _fact_payload("2026-01-01", "2026-03-31", 10.0),
        ],
    )

    assert result["passes"] is True
    assert [row for row in result["periods"] if row["context_end"] == "2026-06-30"][0]["fact_count"] == 2


def _quarter(fq: str, report_date: str, *, operating_cashflow: float | None = None) -> V2OcfQuarter:
    return V2OcfQuarter(
        ticker="TEST",
        company_id=1,
        quarter_id={"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}[fq],
        fiscal_year=2026,
        fiscal_period=fq,
        report_date=report_date,
        operating_cashflow=operating_cashflow,
    )


def _fact(
    start: str,
    end: str,
    value: float,
    *,
    concept: str = SEC_OCF_CONCEPT,
    unit: str = "USD",
    raw_suffix: str = "",
    form: str = "10-Q",
) -> SecCashflowFact:
    field_name = f"{concept}|form={form}|unit={unit}|fy=2026|fp=Q1|frame=NULL|start={start}|filed=2026-05-01{raw_suffix}"
    return SecCashflowFact(
        ticker="TEST",
        period_end_date=end,
        concept=concept,
        value=value,
        currency=unit,
        form=form,
        unit=unit,
        fiscal_year="2026",
        fiscal_period="Q1",
        frame="",
        period_start=start,
        filed="2026-05-01",
        retrieved_at_utc="2026-08-16T00:00:00Z",
        run_id="legacy",
        raw_field_name=field_name,
    )


def _fact_payload(start: str, end: str, value: float) -> dict[str, object]:
    return {
        "concept": SEC_OCF_CONCEPT,
        "context_start": start,
        "context_end": end,
        "duration_days": 90 if end.endswith("03-31") else 181 if end.endswith("06-30") else 273,
        "unit": "USD",
        "currency": "USD",
        "dimensions": "undimensioned",
        "source_value": value,
    }


def _facts(*facts: SecCashflowFact) -> dict[tuple[str, str], list[SecCashflowFact]]:
    out: dict[tuple[str, str], list[SecCashflowFact]] = {}
    for fact in facts:
        if fact.concept != SEC_OCF_CONCEPT or fact.unit != "USD" or "[" in fact.raw_field_name:
            continue
        out.setdefault((fact.ticker, fact.period_end_date), []).append(fact)
    return out


def _memory_v2(*, existing_ocf: float | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE rc_v2_fundamental_quarterly (
            quarter_id INTEGER PRIMARY KEY,
            operating_cashflow REAL,
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
        "INSERT INTO rc_v2_fundamental_quarterly (quarter_id, operating_cashflow, available_canonical_field_count, updated_at_utc) VALUES (1, ?, 0, '')",
        (existing_ocf,),
    )
    return conn
