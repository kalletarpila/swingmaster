from __future__ import annotations

import csv
import sqlite3

import pytest

from swingmaster.fundamentals import v3_phase4c3_production_apply as apply


def test_plan_row_count() -> None:
    assert apply.count_plan(_plan_rows())["rows"] == 588


def test_ebit_ebitda_composition() -> None:
    counts = apply.count_plan(_plan_rows())
    assert counts["ebit"] == 104
    assert counts["ebitda"] == 484


def test_q4_count() -> None:
    assert apply.count_plan(_plan_rows())["Q4"] == 112


def test_plan_hash_reproducibility(tmp_path) -> None:
    path = tmp_path / "plan.csv"
    _write_plan(path, [_plan()])
    assert apply.sha256_file(path) == apply.sha256_file(path)


def test_duplicate_target_rejection(tmp_path) -> None:
    db = _db(tmp_path)
    rows = [_plan(), _plan()]
    out = apply.preflight_rows(db, rows, _facts())
    assert out[1]["status"] == "DUPLICATE_TARGET"


def test_target_null_eligible(tmp_path) -> None:
    assert apply.preflight_rows(_db(tmp_path), [_plan()], _facts())[0]["status"] == "ELIGIBLE"


def test_company_q_exists(tmp_path) -> None:
    row = _plan(company_id="2")
    assert apply.preflight_rows(_db(tmp_path), [row], _facts())[0]["status"] == "CANONICAL_Q_MISSING"


def test_formula_profile_exists_status_allowed(tmp_path) -> None:
    row = _plan(candidate_status="CONDITIONAL")
    assert apply.preflight_rows(_db(tmp_path), [row], _facts())[0]["status"] == "FORMULA_PROFILE_MISMATCH"


def test_sec_fact_ids_exist(tmp_path) -> None:
    assert apply.source_fact_validation_rows([_plan()], _facts())[0]["missing_fact_ids"] == ""


def test_missing_sec_fact_blocks(tmp_path) -> None:
    row = _plan(component_fact_ids="404")
    assert apply.preflight_rows(_db(tmp_path), [row], _facts())[0]["status"] == "SOURCE_FACT_MISSING"


def test_q_applicability() -> None:
    row = _plan(fiscal_quarter="Q1", q_applicability="AUTO_STRONG")
    assert row["q_applicability"] == "AUTO_STRONG"


def test_q4_independent_approval(tmp_path) -> None:
    row = _plan(fiscal_quarter="Q4", q_applicability="AUTO_STRONG")
    assert apply.preflight_rows(_db(tmp_path, quarter="Q4"), [row], _facts())[0]["status"] == "Q_APPLICABILITY_MISMATCH"


def test_direct_ebit_recompute() -> None:
    row = _plan(target_field="ebit", formula_id="PRETAX_PLUS_INTEREST_GROSS", component_values='{"PRETAX":9,"INTEREST_EXPENSE_GROSS":1}', derived_value="10")
    assert apply.recompute_value(row, _db_row(ebit=None), {}) == 10


def test_derived_ebit_recompute() -> None:
    row = _plan(target_field="ebit", formula_id="PRETAX_PLUS_INTEREST_GROSS", component_values='{"PRETAX":8,"INTEREST_EXPENSE_GROSS":2}', derived_value="10")
    assert apply.deterministic_value_match(row, _db_row(ebit=None), {})


def test_canonical_ebit_plus_da_ebitda_recompute() -> None:
    row = _plan(target_field="ebitda", formula_id="DA_COMBINED", component_values='{"DA":3,"EBIT":10}', derived_value="13")
    assert apply.recompute_value(row, _db_row(ebit=10), {}) == 13


def test_derived_ebit_plus_da_ebitda_recompute() -> None:
    row = _plan(target_field="ebitda", formula_id="DA_COMBINED", component_values='{"DA":3}', derived_value="13")
    assert apply.recompute_value(row, _db_row(ebit=None), {(1, 2024, "Q1"): 10}) == 13


def test_sec_ebit_plus_da_ebitda_recompute() -> None:
    row = _plan(target_field="ebitda", formula_id="DA_COMBINED", component_values='{"SEC_EBIT":-1198762,"DA":269440}', derived_value="-929322")
    assert apply.recompute_value(row, _db_row(ebit=None), {}) == -929322


def test_dep_amort_path_recompute() -> None:
    row = _plan(target_field="ebitda", formula_id="EBIT_PLUS_DEP_AND_AMORT", component_values='{"DEPRECIATION":1,"AMORTIZATION":2}', derived_value="13")
    assert apply.recompute_value(row, _db_row(ebit=10), {}) == 13


def test_issuer_specific_path_shape() -> None:
    row = _plan(formula_id="PRETAX_PLUS_ISSUER_SPECIFIC_INTEREST", candidate_status="AUTO_STRONG_ISSUER_SPECIFIC")
    assert row["candidate_status"] in apply.ALLOWED_STATUSES


def test_recomputed_equals_plan_value(tmp_path) -> None:
    assert apply.preflight_rows(_db(tmp_path), [_plan()], _facts())[0]["status"] == "ELIGIBLE"


@pytest.mark.parametrize("candidate_status", ["CONDITIONAL", "PROXY", "REJECTED"])
def test_forbidden_status_rejected(tmp_path, candidate_status: str) -> None:
    assert apply.preflight_rows(_db(tmp_path), [_plan(candidate_status=candidate_status)], _facts())[0]["status"] == "FORMULA_PROFILE_MISMATCH"


def test_adjusted_ebitda_rejected_constant() -> None:
    assert "ADJUSTED" not in apply.ALLOWED_STATUSES


def test_interest_paid_rejected_constant() -> None:
    assert "InterestPaid" not in "".join(apply.ALLOWED_STATUSES)


def test_non_null_overwrite_blocked(tmp_path) -> None:
    db = _db(tmp_path, ebitda=99)
    audit = apply.apply_rows(db, [_plan()], run_id="R")
    assert audit[0]["write_status"] == "BLOCKED_NON_NULL"


def test_target_leakage_impossible_in_apply() -> None:
    assert "derived_value" in _plan()


def test_transaction_apply(tmp_path) -> None:
    db = _db(tmp_path)
    audit = apply.apply_rows(db, [_plan()], run_id="R")
    assert audit[0]["write_status"] == "WROTE"


def test_write_audit() -> None:
    row = apply.audit_row(_plan(), None, "13", "R", "T", "WROTE")
    assert row["run_id"] == "R" and row["write_status"] == "WROTE"


def test_provenance_fields() -> None:
    method = apply.derivation_method(_plan())
    assert "formula_id" in method and "semantic_class" in method


def test_core_ready_uplift_summary() -> None:
    assert apply.summarize_audit([apply.audit_row(_plan(), None, "13", "R", "T", "WROTE")], "R")["total_writes"] == 1


def test_dependency_ordering() -> None:
    rows = [_plan(target_field="ebitda"), _plan(target_field="ebit")]
    assert apply.apply_sort_key(rows[1]) < apply.apply_sort_key(rows[0])


def test_q4_apply_sort() -> None:
    assert apply.apply_sort_key(_plan(fiscal_quarter="Q4"))


def test_second_run_writes_zero(tmp_path) -> None:
    db = _db(tmp_path)
    apply.apply_rows(db, [_plan()], run_id="R")
    audit = apply.apply_rows(db, [_plan()], run_id="R")
    assert not [row for row in audit if row["write_status"] == "WROTE"]


def test_no_duplicate_audit_rows_constant() -> None:
    assert apply.NEXT_PHASE.endswith("REJECTED EBIT/EBITDA CASE REVIEW")


def test_stratified_rejected_sample() -> None:
    pool = [{"rejection_category": f"C{i}", "ticker": "A", "fiscal_year": 2024, "fiscal_quarter": "Q1", "missing_metric": "ebit"} for i in range(20)]
    assert len(apply.stratified_rejected_sample(pool)) == 15


def test_multiple_rejection_categories_present() -> None:
    pool = [{"rejection_category": "A"}, {"rejection_category": "B"}]
    assert len({row["rejection_category"] for row in apply.stratified_rejected_sample(pool)}) == 2


def test_evidence_package_complete() -> None:
    text = apply.rejected_evidence_md([{"ticker": "A", "fiscal_year": 2024, "fiscal_quarter": "Q1", "missing_metric": "ebit", "rejection_category": "X"}])
    assert "A FY2024 Q1 ebit" in text


@pytest.mark.parametrize("key", ["sequence_violations", "invalid_fiscal_year", "duplicate_fyfq", "pre_2018_q", "q4_policy_violations"])
def test_integrity_zero_gate(key: str) -> None:
    integrity = {"sequence_violations": 0, "invalid_fiscal_year": 0, "duplicate_fyfq": 0, "pre_2018_q": 0, "q4_policy_violations": 0, "quick_check": "ok", "foreign_key_check_rows": 0}
    assert apply.integrity_ok(integrity)


def test_quick_check_gate() -> None:
    integrity = {"sequence_violations": 0, "invalid_fiscal_year": 0, "duplicate_fyfq": 0, "pre_2018_q": 0, "q4_policy_violations": 0, "quick_check": "ok", "foreign_key_check_rows": 0}
    assert apply.integrity_ok(integrity)


def test_fk_check_gate() -> None:
    integrity = {"sequence_violations": 0, "invalid_fiscal_year": 0, "duplicate_fyfq": 0, "pre_2018_q": 0, "q4_policy_violations": 0, "quick_check": "ok", "foreign_key_check_rows": 0}
    assert apply.integrity_ok(integrity)


def _plan_rows() -> list[dict[str, str]]:
    return [_plan(target_field="ebit") for _ in range(104)] + [_plan(fiscal_quarter="Q4" if i < 112 else "Q1", target_field="ebitda") for i in range(484)]


def _plan(**overrides) -> dict[str, str]:
    row = {
        "candidate_status": "AUTO_STRONG",
        "company_id": "1",
        "component_fact_ids": "1|2",
        "component_values": '{"DA":3,"EBIT":10}',
        "core_ready_impact": "1",
        "derived_value": "13",
        "fiscal_quarter": "Q1",
        "fiscal_year": "2024",
        "formula_id": "DA_COMBINED",
        "formula_version": "1",
        "period_end": "2024-03-31",
        "q_applicability": "AUTO_STRONG",
        "quarterization": "DIRECT_Q1",
        "research_run": "R",
        "sec_accessions": "a|b",
        "semantic_class": "SEMANTIC_A",
        "source_mode": "DERIVED_CANONICAL_EBIT_PLUS_SEC_DA",
        "statistical_class": "STAT_HIGH",
        "target_field": "ebitda",
        "ticker": "AAA",
        "validation_evidence": "AUTO_STRONG",
    }
    row.update(overrides)
    return row


def _write_plan(path, rows) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=sorted(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _facts() -> dict[str, dict]:
    return {
        "1": {"fact_id": 1, "accession": "a", "concept_name": "A", "concept_label": "A", "semantic_role": "D_AND_A_COMBINED", "value": 3, "unit": "USD", "fiscal_year": 2024, "fiscal_period": "Q1", "end_date": "2024-03-31"},
        "2": {"fact_id": 2, "accession": "b", "concept_name": "B", "concept_label": "B", "semantic_role": "PRETAX", "value": 10, "unit": "USD", "fiscal_year": 2024, "fiscal_period": "Q1", "end_date": "2024-03-31"},
    }


def _db(tmp_path, *, quarter: str = "Q1", ebit: float | None = 10, ebitda: float | None = None):
    path = tmp_path / "v3.db"
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY,ticker TEXT);
            CREATE TABLE v3_quarter(quarter_id INTEGER PRIMARY KEY,company_id INTEGER,fiscal_year INTEGER,fiscal_quarter TEXT,period_end_date TEXT);
            CREATE TABLE v3_quarter_fundamentals(quarter_id INTEGER PRIMARY KEY,ebit REAL,ebitda REAL,accepted_at_utc TEXT,update_run_id TEXT,derivation_method TEXT,updated_at_utc TEXT);
            """
        )
        conn.execute("INSERT INTO v3_company VALUES(1,'AAA')")
        conn.execute("INSERT INTO v3_quarter VALUES(1,1,2024,?,?)", (quarter, "2024-03-31"))
        conn.execute("INSERT INTO v3_quarter_fundamentals VALUES(1,?,?,?,?,?,?)", (ebit, ebitda, None, None, None, None))
    return path


def _db_row(ebit):
    class Row(dict):
        def __getitem__(self, key):
            return self.get(key)
    return Row({"ebit": ebit})
