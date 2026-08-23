from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.fundamentals.v3_canonical_migration import V3CanonicalMigrationCandidate, V3CanonicalMigrationEngine
from swingmaster.fundamentals.v3_schema import apply_v3_schema
from swingmaster.fundamentals.v3_v2_mapping_review import (
    build_existing_fill_rows,
    build_grouped_existing_candidates,
    review_v2_historical_candidate,
    validate_dry_gate,
)


NOW = "2026-08-23T00:00:00Z"


def test_probable_row_remains_hold_without_new_evidence() -> None:
    row = _review_row("PROBABLE_NEW_Q")
    terminal = review_v2_historical_candidate(row=row, v2=_v2(), context=_context(), by_period={}, by_key={})

    assert terminal["terminal_class"] == "HOLD_PROBABLE_NEW_Q"


def test_same_period_wrong_fyfq_label() -> None:
    row = _review_row("PERIOD_IDENTITY_CONFLICT", fiscal_year=2020, fiscal_quarter="Q2")
    terminal = review_v2_historical_candidate(row=row, v2=_v2(), context=_context(), by_period={("AAA", "2020-06-30"): _canonical(fiscal_quarter="Q3", cash=1.0)}, by_key={})

    assert terminal["terminal_class"] == "V2_FYFQ_LABEL_ERROR"


def test_previous_and_next_mapping_errors() -> None:
    previous = review_v2_historical_candidate(row=_review_row("POSSIBLE_WRONG_V2_MAPPING"), v2=_v2(), context=_context(adjacent="PREVIOUS_Q_LOOKALIKE"), by_period={}, by_key={})
    next_row = review_v2_historical_candidate(row=_review_row("POSSIBLE_WRONG_V2_MAPPING"), v2=_v2(), context=_context(adjacent="NEXT_Q_LOOKALIKE"), by_period={}, by_key={})

    assert previous["terminal_class"] == "V2_PREVIOUS_Q_MAPPING_ERROR"
    assert next_row["terminal_class"] == "V2_NEXT_Q_MAPPING_ERROR"


def test_period_variant_of_existing_q() -> None:
    row = _review_row("PERIOD_IDENTITY_CONFLICT")
    terminal = review_v2_historical_candidate(row=row, v2=_v2(), context=_context(), by_period={}, by_key={("AAA", 2020, "Q2"): _canonical(period_end_date="2020-07-02")})

    assert terminal["terminal_class"] == "V2_PERIOD_VARIANT"


def test_q4_duplicate_sec_q4_class() -> None:
    row = _review_row("DUPLICATE_OR_VARIANT_OF_EXISTING_Q", fiscal_quarter="Q4")
    terminal = review_v2_historical_candidate(row=row, v2=_v2(), context=_context(), by_period={("AAA", "2020-06-30"): _canonical(fiscal_quarter="Q4", cash=1.0)}, by_key={})

    assert terminal["terminal_class"] == "REDUNDANT_Q4_ALREADY_CANONICAL"


def test_ready_existing_q_null_fill_plan() -> None:
    row = _review_row("DUPLICATE_OR_VARIANT_OF_EXISTING_Q")
    canonical = _canonical(revenue=None, cash=1.0)
    terminal = review_v2_historical_candidate(row=row, v2=_v2(), context=_context(), by_period={("AAA", "2020-06-30"): canonical}, by_key={})
    fills = build_existing_fill_rows(terminal, _v2(revenue=100.0), canonical)

    assert terminal["terminal_class"] == "READY_EXISTING_Q_NULL_FILL"
    assert any(fill["field"] == "revenue" for fill in fills)


def test_non_null_overwrite_forbidden_by_engine(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [_candidate(values={"revenue": 100.0})])
    candidate = _candidate(source_system="V2", source_record_id="V2:AAA", values={"revenue": 200.0}, candidate_can_create_quarter=False)

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        summary = V3CanonicalMigrationEngine(conn).apply_source_batch([candidate], source="V2", migration_run_id="RUN", now_utc=NOW).to_dict()
        revenue = conn.execute("SELECT revenue FROM v3_quarter_fundamentals").fetchone()[0]

    assert summary["field_contributions"]["revenue"]["FIELD_CONFLICT"] == 1
    assert revenue == 100.0


def test_publish_overwrite_forbidden_by_engine(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [_candidate(publish_date="2020-05-01")])
    candidate = _candidate(source_system="V2", source_record_id="V2:AAA", values={}, publish_date="2020-05-02", candidate_can_create_quarter=False)

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        summary = V3CanonicalMigrationEngine(conn).apply_source_batch([candidate], source="V2", migration_run_id="RUN", now_utc=NOW).to_dict()

    assert summary["metadata"]["PUBLISH_DATE_CONFLICT"] == 1


def test_pre_2018_forbidden_by_engine(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [])
    candidate = _candidate(source_system="V2", source_record_id="V2:OLD", period_end_date="2017-12-31", fiscal_year=2017, candidate_can_create_quarter=True)

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        summary = V3CanonicalMigrationEngine(conn).apply_source_batch([candidate], source="V2", migration_run_id="RUN", now_utc=NOW).to_dict()

    assert summary["rows"]["candidate_rows_rejected"] == 1


def test_hold_leakage_forbidden_in_dry_gate() -> None:
    prepared = type("Prepared", (), {"candidates": [_candidate(source_system="V2", source_record_id="V2:HOLD", values={"cash": 1.0}, candidate_can_create_quarter=False)], "ready_existing_fill_rows": [{"field": "cash"}], "calibration_rows": [{"wrong": 0}]})()
    dry = {"rows": {"canonical_quarters_created": 0}, "field_contributions": {field: {} for field in ("revenue", "ebitda", "free_cashflow", "cash", "total_debt", "shares_outstanding", "ebit", "operating_income", "operating_cashflow", "capex", "gross_profit", "net_income")}, "metadata": {}}
    dry["field_contributions"]["cash"] = {"FIELD_FILLED_FROM_NULL": 1}

    assert not validate_dry_gate(prepared, dry)["passed"]


def test_grouped_existing_candidate_idempotent(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [_candidate(values={"cash": None})])
    terminal = {**_review_row("DUPLICATE_OR_VARIANT_OF_EXISTING_Q"), "terminal_class": "READY_EXISTING_Q_NULL_FILL"}
    canonical = _canonical(cash=None)
    fills = build_existing_fill_rows(terminal, _v2(cash=5.0), canonical)
    groups = {}
    from swingmaster.fundamentals.v3_v2_mapping_review import group_existing_candidate

    group_existing_candidate(groups, terminal, _v2(cash=5.0), canonical, fills)
    candidate = build_grouped_existing_candidates(groups, "RUN")[0]

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        engine = V3CanonicalMigrationEngine(conn)
        first = engine.apply_source_batch([candidate], source="V2", migration_run_id="RUN", now_utc=NOW).to_dict()
        second = engine.apply_source_batch([candidate], source="V2", migration_run_id="RUN", now_utc=NOW).to_dict()

    assert first["field_contributions"]["cash"]["FIELD_FILLED_FROM_NULL"] == 1
    assert "FIELD_FILLED_FROM_NULL" not in second["field_contributions"]["cash"]


def _context(adjacent: str = "INSUFFICIENT"):
    return {
        "final": {"final_classification": "PROBABLE_NEW_Q"},
        "adjacent": {"adjacent_fingerprint_class": adjacent},
    }


def _review_row(prior: str, **overrides):
    row = {
        "market": "usa",
        "ticker": "AAA",
        "company_id": "1",
        "active": "1",
        "v2_source_record_id": "V2:AAA:2020:Q2",
        "fiscal_year": "2020",
        "fiscal_quarter": "Q2",
        "period_end_date": "2020-06-30",
        "publish_date": "2020-08-01",
        "available_fields": "revenue;cash",
        "final_classification": prior,
    }
    row.update({key: str(value) for key, value in overrides.items()})
    return row


def _v2(**overrides):
    row = {"revenue": 100.0, "cash": 5.0, "period_end_date": "2020-06-30", "publish_date": "2020-08-01"}
    row.update(overrides)
    return row


def _canonical(**overrides):
    row = {
        "market": "usa",
        "ticker": "AAA",
        "fiscal_year": 2020,
        "fiscal_quarter": "Q2",
        "period_end_date": "2020-06-30",
        "publish_date": "2020-08-01",
        "revenue": 100.0,
        "ebitda": 1.0,
        "free_cashflow": 1.0,
        "cash": 1.0,
        "total_debt": 1.0,
        "shares_outstanding": 1.0,
        "ebit": 1.0,
        "operating_income": 1.0,
        "operating_cashflow": 1.0,
        "capex": -1.0,
        "gross_profit": 1.0,
        "net_income": 1.0,
    }
    row.update(overrides)
    return row


def _candidate(**overrides):
    base = {
        "source_system": "YAHOO",
        "source_record_id": "YAHOO:AAA:2020:Q2",
        "migration_run_id": "RUN",
        "market": "usa",
        "ticker": "AAA",
        "fiscal_year": 2020,
        "fiscal_quarter": "Q2",
        "period_end_date": "2020-06-30",
        "publish_date": None,
        "values": {"cash": 1.0},
        "approved_company_active": True,
    }
    base.update(overrides)
    return V3CanonicalMigrationCandidate(**base)


def _seed(path: Path, candidates: list[V3CanonicalMigrationCandidate]) -> None:
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        apply_v3_schema(conn)
        if candidates:
            V3CanonicalMigrationEngine(conn).apply_source_batch(candidates, source="YAHOO", migration_run_id="RUN", now_utc=NOW)
        else:
            conn.execute("INSERT INTO v3_company (market, ticker, profile, active, admission_source, created_at_utc, updated_at_utc) VALUES ('usa', 'AAA', 'ORDINARY', 1, 'TEST', ?, ?)", (NOW, NOW))
        conn.commit()
