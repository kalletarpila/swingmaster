from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

from swingmaster.fundamentals.v3_canonical_migration import V3CanonicalMigrationEngine
from swingmaster.fundamentals.v3_legacy_deep_history import (
    build_candidate_bundle,
    dry_apply_gate,
    q4_selected_values,
    reconcile_ready_plan,
    summarize_idempotency,
)
from swingmaster.fundamentals.v3_schema import apply_v3_schema


NOW = "2026-08-23T00:00:00Z"


def test_ready_explicit_legacy_q_creates_canonical_q(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, include_q4=False)
    conn = _v3_conn("AAA")
    summary = V3CanonicalMigrationEngine(conn).apply_source_batch(bundle.candidates, source="LEGACY", migration_run_id="RUN", now_utc=NOW).to_dict()

    assert summary["rows"]["canonical_quarters_created"] == 1
    assert conn.execute("SELECT revenue FROM v3_quarter_fundamentals").fetchone()[0] == 10.0


def test_ready_sec_q4_creates_canonical_q_with_policy_fields(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, include_explicit=False)
    conn = _v3_conn("AAA")
    summary = V3CanonicalMigrationEngine(conn).apply_source_batch(bundle.candidates, source="LEGACY", migration_run_id="RUN", now_utc=NOW).to_dict()

    row = conn.execute("SELECT revenue, ebitda, cash FROM v3_quarter_fundamentals").fetchone()
    assert summary["rows"]["canonical_quarters_created"] == 1
    assert row["revenue"] == 40.0
    assert row["ebitda"] is None
    assert row["cash"] == 100.0


def test_hold_row_cannot_create_q(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    ids = {candidate.source_record_id for candidate in bundle.candidates}

    assert "LEGACY:HOLD:2024-03-31" not in ids


def test_pre_2018_cannot_create_q_gate(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    bad = list(bundle.candidates)
    bad[0] = _candidate_like(bad[0], period_end_date="2017-12-31")
    dirty = type(bundle)(bad, bundle.explicit_rows, bundle.q4_rows, bundle.hold_rows, bundle.q4_field_plan)

    assert reconcile_ready_plan(dirty)["pre_2018"] == 1


def test_duplicate_fyfq_prevented_gate(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    dirty = type(bundle)(bundle.candidates + [bundle.candidates[0]], bundle.explicit_rows, bundle.q4_rows, bundle.hold_rows, bundle.q4_field_plan)

    assert reconcile_ready_plan(dirty)["identity_duplicates"] == 1


def test_explicit_q_field_mapping(tmp_path: Path) -> None:
    candidate = _bundle(tmp_path, include_q4=False).candidates[0]

    assert candidate.values["revenue"] == 10.0
    assert candidate.values["ebit"] == 4.0


def test_q4_revenue_approved_mode(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, include_explicit=False)

    assert _field_plan(bundle, "revenue")["planned_source_mode"] == "FY_MINUS_Q1_Q2_Q3"


def test_q4_ocf_approved_mode(tmp_path: Path) -> None:
    assert _field_plan(_bundle(tmp_path, include_explicit=False), "operating_cashflow")["planned_source_mode"] == "FY_MINUS_9M"


def test_q4_capex_approved_mode(tmp_path: Path) -> None:
    assert _field_plan(_bundle(tmp_path, include_explicit=False), "capex")["planned_source_mode"] == "FY_MINUS_9M"


def test_q4_fcf_approved_derivation(tmp_path: Path) -> None:
    assert _field_plan(_bundle(tmp_path, include_explicit=False), "free_cashflow")["planned_source_mode"] == "APPROVED_DERIVATION"


def test_q4_cash_fy_end_instant(tmp_path: Path) -> None:
    assert _field_plan(_bundle(tmp_path, include_explicit=False), "cash")["planned_source_mode"] == "DIRECT_FY_END_INSTANT"


def test_q4_debt_fy_end_policy(tmp_path: Path) -> None:
    assert _field_plan(_bundle(tmp_path, include_explicit=False), "total_debt")["planned_source_mode"] == "DIRECT_FY_END_INSTANT"


def test_q4_shares_fy_end_instant(tmp_path: Path) -> None:
    assert _field_plan(_bundle(tmp_path, include_explicit=False), "shares_outstanding")["planned_source_mode"] == "DIRECT_FY_END_INSTANT"


def test_instant_subtraction_impossible_by_candidate_values(tmp_path: Path) -> None:
    candidate = _bundle(tmp_path, include_explicit=False).candidates[0]

    assert "cash_q1" not in candidate.derivation_inputs
    assert candidate.values["cash"] == 100.0


def test_q4_ebit_unsafe_remains_null(tmp_path: Path) -> None:
    assert "ebit" not in _bundle(tmp_path, include_explicit=False).candidates[0].values


def test_q4_ebitda_unsafe_remains_null(tmp_path: Path) -> None:
    assert "ebitda" not in _bundle(tmp_path, include_explicit=False).candidates[0].values


def test_partial_q4_accepted(tmp_path: Path) -> None:
    candidate = _bundle(tmp_path, include_explicit=False, omit_q4_cash=True).candidates[0]

    assert candidate.values["revenue"] == 40.0
    assert "cash" not in candidate.values


def test_annual_publish_date(tmp_path: Path) -> None:
    assert _bundle(tmp_path, include_explicit=False).candidates[0].publish_date == "2025-02-20"


def test_publish_null_accepted(tmp_path: Path) -> None:
    candidate = _bundle(tmp_path, include_explicit=False, q4_publish="").candidates[0]

    assert candidate.publish_date is None


def test_incompatible_vintage_field_remains_null_placeholder(tmp_path: Path) -> None:
    assert _field_plan(_bundle(tmp_path, include_explicit=False), "ebitda")["will_populate"] == 0


def test_inactive_company_historical_q_allowed(tmp_path: Path) -> None:
    conn = _v3_conn("AAA", active=False)
    V3CanonicalMigrationEngine(conn).apply_source_batch(_bundle(tmp_path, include_q4=False).candidates, source="LEGACY", migration_run_id="RUN", now_utc=NOW)

    assert conn.execute("SELECT active FROM v3_company WHERE ticker='AAA'").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM v3_quarter").fetchone()[0] == 1


def test_no_legacy_overwrite(tmp_path: Path) -> None:
    conn = _v3_conn("AAA")
    engine = V3CanonicalMigrationEngine(conn)
    engine.apply_source_batch(_bundle(tmp_path, include_q4=False).candidates, source="LEGACY", migration_run_id="RUN", now_utc=NOW)
    changed = _bundle(tmp_path, include_q4=False, explicit_revenue=99.0).candidates
    summary = engine.apply_source_batch(changed, source="LEGACY", migration_run_id="RUN", now_utc=NOW).to_dict()

    assert summary["field_contributions"]["revenue"]["FIELD_CONFLICT"] == 1
    assert conn.execute("SELECT revenue FROM v3_quarter_fundamentals").fetchone()[0] == 10.0


def test_no_v2_contribution(tmp_path: Path) -> None:
    assert all(candidate.source_system == "LEGACY" for candidate in _bundle(tmp_path).candidates)


def test_resumable_apply(tmp_path: Path) -> None:
    conn = _v3_conn("AAA")
    candidates = _bundle(tmp_path).candidates
    engine = V3CanonicalMigrationEngine(conn)
    engine.apply_source_batch(candidates[:1], source="LEGACY", migration_run_id="RUN", now_utc=NOW)
    engine.apply_source_batch(candidates[1:], source="LEGACY", migration_run_id="RUN", now_utc=NOW)

    assert conn.execute("SELECT COUNT(*) FROM v3_quarter").fetchone()[0] == len(candidates)


def test_idempotent_second_run(tmp_path: Path) -> None:
    conn = _v3_conn("AAA")
    candidates = _bundle(tmp_path, include_q4=False).candidates
    engine = V3CanonicalMigrationEngine(conn)
    engine.apply_source_batch(candidates, source="LEGACY", migration_run_id="RUN", now_utc=NOW)
    second = engine.apply_source_batch(candidates, source="LEGACY", migration_run_id="RUN", now_utc=NOW).to_dict()

    assert summarize_idempotency(_summary_for_idempotency(second))["second_run_new_qs"] == 0


def test_hold_leakage_check(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    gate = dry_apply_gate(bundle, _empty_dry_summary(), {"counts": {"companies": 1}})

    assert gate["hold_leakage"] == 0


def test_historical_floor(tmp_path: Path) -> None:
    assert all(candidate.period_end_date >= "2018-01-01" for candidate in _bundle(tmp_path).candidates)


def test_field_source_accounting(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, include_explicit=False)

    assert sum(row["will_populate"] for row in bundle.q4_field_plan if row["field"] == "revenue") == 1


def test_phase4c_inventory_generation_contract(tmp_path: Path) -> None:
    candidate = _bundle(tmp_path, include_explicit=False).candidates[0]

    assert candidate.values.get("ebitda") is None


def test_integrity(tmp_path: Path) -> None:
    conn = _v3_conn("AAA")
    V3CanonicalMigrationEngine(conn).apply_source_batch(_bundle(tmp_path).candidates, source="LEGACY", migration_run_id="RUN", now_utc=NOW)

    assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"


def _bundle(tmp_path: Path, *, include_explicit: bool = True, include_q4: bool = True, omit_q4_cash: bool = False, explicit_revenue: float = 10.0, q4_publish: str = "2025-02-20"):
    root = tmp_path / "p3c1d"
    root.mkdir(exist_ok=True)
    rows = []
    if include_explicit:
        rows.append(_plan_row("AAA", 2024, "Q1", "2024-03-31", "LEGACY_SEC_DIRECT_QUARTER", "LEGACY:AAA:2024-03-31", "2024-05-01"))
    if include_q4:
        rows.append(_plan_row("AAA", 2024, "Q4", "2024-12-31", "LEGACY_SEC_FY_Q4_RECONSTRUCTION_PLAN", "LEGACY:AAA:2024-12-31", q4_publish))
    _write_csv(root / "phase3c2_dry_import_plan.csv", rows)
    _write_csv(root / "phase3c2_hold_rows.csv", [_plan_row("HOLD", 2024, "Q1", "2024-03-31", "HOLD", "LEGACY:HOLD:2024-03-31", "")])
    _write_csv(root / "phase3c2_q4_construction_plan.csv", [row for row in rows if row["field_source_mode"] == "LEGACY_SEC_FY_Q4_RECONSTRUCTION_PLAN"])
    policy_root = tmp_path / "p3c1e"
    policy_root.mkdir(exist_ok=True)
    legacy = {
        ("AAA", "2024-03-31"): {"revenue": explicit_revenue, "ebit": 4.0, "ebitda": 5.0, "cash": 50.0},
        ("AAA", "2024-12-31"): {"revenue": 40.0, "operating_cashflow": 8.0, "capex": -2.0, "cash": None if omit_q4_cash else 100.0, "total_debt": 30.0, "shares_outstanding": 11.0},
    }
    return build_candidate_bundle(phase3c1d_root=root, q4_policy_root=policy_root, legacy_rows=legacy, migration_run_id="RUN")


def _plan_row(ticker: str, fy: int, fq: str, period: str, mode: str, source_id: str, publish: str) -> dict[str, str]:
    q4_methods = {
        "capex": "LEGACY_SEC_FY_MINUS_Q1_Q2_Q3_PENDING_CONCEPT_CHECK",
        "cash": "LEGACY_SEC_FY_END_INSTANT",
        "ebit": "UNSUPPORTED_NULL_SEMANTICALLY_UNSAFE",
        "ebitda": "UNSUPPORTED_NULL_SEMANTICALLY_UNSAFE",
        "free_cashflow": "LEGACY_SEC_DERIVE_FROM_Q4_OCF_PLUS_CAPEX_IF_INPUTS_SAFE",
        "gross_profit": "UNSUPPORTED_NULL",
        "net_income": "UNSUPPORTED_NULL",
        "operating_cashflow": "LEGACY_SEC_FY_MINUS_Q1_Q2_Q3_PENDING_CONCEPT_CHECK",
        "operating_income": "UNSUPPORTED_NULL",
        "revenue": "LEGACY_SEC_FY_MINUS_Q1_Q2_Q3_PENDING_CONCEPT_CHECK",
        "shares_outstanding": "LEGACY_SEC_FY_END_INSTANT",
        "total_debt": "LEGACY_SEC_FY_END_INSTANT",
    }
    return {
        "market": "usa",
        "ticker": ticker,
        "fiscal_year": str(fy),
        "fiscal_quarter": fq,
        "period_end_date": period,
        "publish_date": publish,
        "field_source_mode": mode,
        "source_record_id": source_id,
        "available_fields": "revenue;ebit;ebitda;operating_cashflow;capex;free_cashflow;cash;total_debt;shares_outstanding",
        "final_disposition": "READY_SEC_Q4_STRUCTURE" if mode == "LEGACY_SEC_FY_Q4_RECONSTRUCTION_PLAN" else "READY_EXISTING_CHAIN",
        "phase3c2_recommendation": "READY_FOR_PHASE3C2_IMPORT",
        "balance_sheet_direct_instant_fields": "cash;total_debt;shares_outstanding" if mode == "LEGACY_SEC_FY_Q4_RECONSTRUCTION_PLAN" else "",
        "field_derivation_methods": json_dumps(q4_methods) if mode == "LEGACY_SEC_FY_Q4_RECONSTRUCTION_PLAN" else "{}",
    }


def json_dumps(value: dict[str, str]) -> str:
    import json

    return json.dumps(value, sort_keys=True)


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=sorted({key for row in rows for key in row}))
        writer.writeheader()
        writer.writerows(rows)


def _v3_conn(ticker: str, *, active: bool = True) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    apply_v3_schema(conn)
    conn.row_factory = sqlite3.Row
    conn.execute(
        "INSERT INTO v3_company (market,ticker,profile,active,admission_source,created_at_utc,updated_at_utc) VALUES ('usa',?,'ORDINARY',?,'LEGACY',?,?)",
        (ticker, 1 if active else 0, NOW, NOW),
    )
    return conn


def _field_plan(bundle, field: str) -> dict:
    return next(row for row in bundle.q4_field_plan if row["field"] == field)


def _candidate_like(candidate, **overrides):
    data = {
        "source_system": candidate.source_system,
        "source_record_id": candidate.source_record_id,
        "migration_run_id": candidate.migration_run_id,
        "market": candidate.market,
        "ticker": candidate.ticker,
        "fiscal_year": candidate.fiscal_year,
        "fiscal_quarter": candidate.fiscal_quarter,
        "period_end_date": candidate.period_end_date,
        "publish_date": candidate.publish_date,
        "values": candidate.values,
        "approved_company_active": candidate.approved_company_active,
    }
    data.update(overrides)
    from swingmaster.fundamentals.v3_canonical_migration import V3CanonicalMigrationCandidate

    return V3CanonicalMigrationCandidate(**data)


def _empty_dry_summary() -> dict:
    from collections import Counter
    from swingmaster.fundamentals.v3_repositories import FUNDAMENTAL_FIELDS

    return {"rows": Counter(), "metadata": Counter(), "field_contributions": {field: Counter() for field in FUNDAMENTAL_FIELDS}, "issues": Counter()}


def _summary_for_idempotency(summary: dict) -> dict:
    return {"rows": summary["rows"], "metadata": summary["metadata"], "field_contributions": summary["field_contributions"], "integrity_result": summary["integrity_result"]}
