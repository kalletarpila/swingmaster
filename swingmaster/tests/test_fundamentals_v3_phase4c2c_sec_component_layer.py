from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from swingmaster.fundamentals import v3_sec_component_layer as sec


def test_schema_creates_component_tables(tmp_path: Path) -> None:
    with sec.connect_component_db(tmp_path / "components.db") as conn:
        sec.initialize_component_schema(conn)
        names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"sec_component_fact", "sec_component_raw_cache", "sec_component_acquisition_state", "sec_component_concept_registry"} <= names


def test_component_fact_insert_idempotent(tmp_path: Path) -> None:
    with sec.connect_component_db(tmp_path / "components.db") as conn:
        sec.initialize_component_schema(conn)
        rows = sec.normalize_companyfacts(_company(), _companyfacts(), acquired_at_utc=sec.utc_now(), source_payload_sha256="h")
        sec.upsert_facts(conn, rows)
        sec.upsert_facts(conn, rows)
        assert conn.execute("SELECT COUNT(*) FROM sec_component_fact").fetchone()[0] == len(rows)


def test_multiple_vintages_preserved(tmp_path: Path) -> None:
    payload = _companyfacts()
    facts = payload["facts"]["us-gaap"]["IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest"]["units"]["USD"]
    facts.append({**facts[0], "accn": "0001-24-2", "filed": "2024-05-03", "val": 11})
    with sec.connect_component_db(tmp_path / "components.db") as conn:
        sec.initialize_component_schema(conn)
        sec.upsert_facts(conn, sec.normalize_companyfacts(_company(), payload, acquired_at_utc=sec.utc_now(), source_payload_sha256="h"))
        assert conn.execute("SELECT COUNT(DISTINCT accession) FROM sec_component_fact WHERE semantic_role='PRETAX'").fetchone()[0] == 2


def test_issuer_extension_preserved() -> None:
    rows = sec.normalize_companyfacts(_company(), _companyfacts(include_extension=True), acquired_at_utc=sec.utc_now(), source_payload_sha256="h")
    assert any(row["standard_or_extension"] == "ISSUER_EXTENSION" for row in rows)


def test_explicit_metadata_columns_exist(tmp_path: Path) -> None:
    with sec.connect_component_db(tmp_path / "components.db") as conn:
        sec.initialize_component_schema(conn)
        cols = {row["name"] for row in conn.execute("PRAGMA table_info(sec_component_fact)")}
    assert {"accession", "filed_date", "start_date", "end_date", "fiscal_year", "fiscal_period", "frame", "dimensions_json"} <= cols


def test_accession_preserved() -> None:
    row = sec.normalize_companyfacts(_company(), _companyfacts(), acquired_at_utc=sec.utc_now(), source_payload_sha256="h")[0]
    assert row["accession"] == "0001-24-1"


def test_dimensions_context_preserved() -> None:
    row = sec.normalize_companyfacts(_company(), _companyfacts(), acquired_at_utc=sec.utc_now(), source_payload_sha256="h")[0]
    assert "frame" in json.loads(row["dimensions_json"])


def test_raw_and_normalized_separation(tmp_path: Path) -> None:
    with sec.connect_component_db(tmp_path / "components.db") as conn:
        sec.initialize_component_schema(conn)
        sec.upsert_raw_cache(conn, _company(), _companyfacts(), "FETCH_OK", {"http_status": 200}, sec.utc_now())
        assert conn.execute("SELECT COUNT(*) FROM sec_component_fact").fetchone()[0] == 0


def test_cik_mapping() -> None:
    rows = sec.map_universe_to_cik([_company(ticker="AAA")], {"AAA": "0000000001"})
    assert rows[0]["mapping_status"] == "MAPPED"


def test_cik_unmapped_not_guessed() -> None:
    rows = sec.map_universe_to_cik([_company(ticker="ZZZ")], {})
    assert rows[0]["mapping_status"] == "UNMAPPED" and rows[0]["cik"] == ""


def test_cache_hit(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "CIK0000000001.json").write_text(json.dumps(_companyfacts()), encoding="utf-8")
    status, payload, meta = sec.fetch_or_load_companyfacts(company=_company(), raw_cache_dir=cache, user_agent="ua", now=sec.utc_now(), fetcher=lambda *_: {"bad": True})
    assert status == "FETCH_OK" and payload and meta["cache_hit"] == 1


def test_fetch_success_writes_cache(tmp_path: Path) -> None:
    status, _, meta = sec.fetch_or_load_companyfacts(company=_company(), raw_cache_dir=tmp_path, user_agent="ua", now=sec.utc_now(), fetcher=lambda *_: _companyfacts(), use_cache=False)
    assert status == "FETCH_OK" and meta["cache_hit"] == 0 and (tmp_path / "CIK0000000001.json").exists()


def test_empty_response_status(tmp_path: Path) -> None:
    status, _, _ = sec.fetch_or_load_companyfacts(company=_company(), raw_cache_dir=tmp_path, user_agent="ua", now=sec.utc_now(), fetcher=lambda *_: {"facts": {}}, use_cache=False)
    assert status == "FETCH_EMPTY"


def test_retryable_failure(tmp_path: Path) -> None:
    def fail(*_):
        raise RuntimeError("temporary")

    status, _, meta = sec.fetch_or_load_companyfacts(company=_company(), raw_cache_dir=tmp_path, user_agent="ua", now=sec.utc_now(), fetcher=fail, use_cache=False)
    assert status == "FETCH_FAILED" and meta["retryable"] == 1


def test_rate_limit_failure(tmp_path: Path) -> None:
    def fail(*_):
        raise RuntimeError("429")

    status, _, meta = sec.fetch_or_load_companyfacts(company=_company(), raw_cache_dir=tmp_path, user_agent="ua", now=sec.utc_now(), fetcher=fail, use_cache=False)
    assert status == "RATE_LIMITED" and meta["http_status"] == 429


def test_resume_after_failure_updates_attempt_count(tmp_path: Path) -> None:
    with sec.connect_component_db(tmp_path / "components.db") as conn:
        sec.initialize_component_schema(conn)
        sec.update_acquisition_state(conn, _company(), "FETCH_FAILED", {"retryable": 1, "error": "x"}, [], sec.utc_now())
        sec.update_acquisition_state(conn, _company(), "FETCH_OK", {"retryable": 0}, [], sec.utc_now())
        row = conn.execute("SELECT attempt_count,status FROM sec_component_acquisition_state").fetchone()
    assert row["attempt_count"] == 2 and row["status"] == "FETCH_OK"


def test_pretax_classification() -> None:
    assert sec.classify_concept("us-gaap", "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest") == "PRETAX"


def test_gross_interest_classification() -> None:
    assert sec.classify_concept("us-gaap", "InterestExpense") == "INTEREST_EXPENSE_GROSS"


def test_net_interest_classification() -> None:
    assert sec.classify_concept("us-gaap", "InterestExpenseNonOperating", "Interest expense, net") == "INTEREST_EXPENSE_NET"


def test_interest_income_classification() -> None:
    assert sec.classify_concept("us-gaap", "InterestIncomeExpenseNonOperatingNet", "Interest income") == "INTEREST_INCOME"


def test_lease_interest_classification() -> None:
    assert sec.classify_concept("us-gaap", "FinanceLeaseInterestExpense") == "FINANCE_LEASE_INTEREST"


def test_interest_paid_excluded() -> None:
    assert sec.classify_concept("us-gaap", "InterestPaidNet") == "INTEREST_PAID_CASHFLOW_EXCLUDED"


def test_da_combined_classification() -> None:
    assert sec.classify_concept("us-gaap", "DepreciationDepletionAndAmortization") == "D_AND_A_COMBINED"


def test_depreciation_classification() -> None:
    assert sec.classify_concept("us-gaap", "Depreciation") == "DEPRECIATION"


def test_amortization_classification() -> None:
    assert sec.classify_concept("us-gaap", "AmortizationOfIntangibleAssets") == "AMORTIZATION_INTANGIBLES"


def test_issuer_extension_unknown_preserved_when_relevant() -> None:
    assert sec.looks_component_relevant("AcmeSpecialPretaxMetric", "Pretax metric")


def test_q1_direct_duration() -> None:
    assert _readiness("Q1", 91) == "DIRECT_Q1"


def test_q2_direct_duration() -> None:
    assert _readiness("Q2", 91) == "DIRECT_Q2_3M"


def test_q2_h1_minus_q1() -> None:
    assert _readiness("Q2", 181) == "Q2_H1_MINUS_Q1_READY"


def test_q3_direct_duration() -> None:
    assert _readiness("Q3", 91) == "DIRECT_Q3_3M"


def test_q3_9m_minus_h1() -> None:
    assert _readiness("Q3", 273) == "Q3_9M_MINUS_H1_READY"


def test_q4_fy_minus_9m() -> None:
    assert _readiness("FY", 365) == "Q4_FY_MINUS_9M_READY"


def test_same_vintage_requirement_in_direct_vs_ytd(tmp_path: Path) -> None:
    with sec.connect_component_db(tmp_path / "components.db") as conn:
        sec.initialize_component_schema(conn)
        sec.upsert_facts(conn, [_fact("Q1", 90, 10, "A"), _fact("Q2", 181, 30, "A"), _fact("Q2", 90, 20, "A")])
        rows = sec.direct_vs_ytd_rows(conn)
    assert rows[0]["same_accession"] == 1


def test_52_53_week_duration_support() -> None:
    assert sec.duration_compatible(98, 60, 115)


def test_dimension_conflict_classification(tmp_path: Path) -> None:
    with sec.connect_component_db(tmp_path / "components.db") as conn:
        sec.initialize_component_schema(conn)
        fact = _fact("Q1", 120, 10, "A")
        fact["dimensions_json"] = '{"dim": "Segment"}'
        sec.upsert_facts(conn, [fact])
        rows = sec.quarterization_readiness_rows(conn)
    assert rows[0]["readiness_class"] == "DIMENSION_CONFLICT"


def test_annual_reconciliation() -> None:
    assert sec.relative_error(100, 100.1) <= 0.01


def test_missing_ebit_component_coverage_summary() -> None:
    rows = [{"has_pretax": 1, "has_usable_interest": 1, "interest_candidate_count": 1, "issuer_extension_only": 0}]
    assert sec.summarize_missing_ebit(rows)["with_pretax_one_interest_candidate"] == 1


def test_missing_ebitda_component_coverage_summary() -> None:
    rows = [{"ebit": 1, "has_combined_da": 1, "has_dep_amort": 0, "has_pretax": 1, "has_usable_interest": 1, "q4_ready": 1}]
    assert sec.summarize_missing_ebitda(rows)["canonical_ebit_plus_da"] == 1


def test_no_canonical_financial_writes_in_summary_shape() -> None:
    assert sec.CLASSIFICATION_INCOMPLETE.startswith("FUNDAMENTALS_V3_PHASE4C2C")


def test_no_pre_2018_normalized_rows() -> None:
    payload = _companyfacts(end="2017-12-31")
    assert sec.normalize_companyfacts(_company(), payload, acquired_at_utc=sec.utc_now(), source_payload_sha256="h") == []


def test_acquisition_idempotency_natural_key() -> None:
    row = sec.normalize_companyfacts(_company(), _companyfacts(), acquired_at_utc=sec.utc_now(), source_payload_sha256="h")[0]
    assert len(row["natural_key"]) == 64


def test_component_coverage_counts_roles() -> None:
    assert sec.coverage_for_facts([{"semantic_role": "PRETAX"}, {"semantic_role": "PRETAX"}]) == {"PRETAX": 2}


def test_raw_cache_hash() -> None:
    assert sec.sha256_text("x") == sec.sha256_text("x")


def test_duration_days() -> None:
    assert sec.duration_days("2024-01-01", "2024-04-01") == 91


def test_payload_fact_count() -> None:
    assert sec.count_payload_facts(_companyfacts()) >= 1


def test_latest_filed_date() -> None:
    assert sec.latest_filed_date(_companyfacts()) == "2024-05-01"


def test_concept_registry_refresh(tmp_path: Path) -> None:
    with sec.connect_component_db(tmp_path / "components.db") as conn:
        sec.initialize_component_schema(conn)
        sec.upsert_facts(conn, sec.normalize_companyfacts(_company(), _companyfacts(), acquired_at_utc=sec.utc_now(), source_payload_sha256="h"))
        sec.refresh_concept_registry(conn, sec.utc_now())
        assert conn.execute("SELECT COUNT(*) FROM sec_component_concept_registry").fetchone()[0] > 0


def test_interest_paid_accepted_zero(tmp_path: Path) -> None:
    with sec.connect_component_db(tmp_path / "components.db") as conn:
        sec.initialize_component_schema(conn)
        sec.upsert_facts(conn, [_fact("Q1", 90, 1, "A", role="INTEREST_PAID_CASHFLOW_EXCLUDED")])
        assert sec.role_company_count(conn, ["INTEREST_PAID_CASHFLOW_EXCLUDED"], approved_only=True) == 0


def test_dir_size(tmp_path: Path) -> None:
    (tmp_path / "x").write_text("abc", encoding="utf-8")
    assert sec.dir_size(tmp_path) == 3


def test_write_csv_empty(tmp_path: Path) -> None:
    path = tmp_path / "x.csv"
    sec.write_csv(path, [])
    assert path.read_text(encoding="utf-8") == ""


def test_write_json(tmp_path: Path) -> None:
    path = tmp_path / "x.json"
    sec.write_json(path, {"a": 1})
    assert json.loads(path.read_text(encoding="utf-8")) == {"a": 1}


def _readiness(fp: str, days: int) -> str:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    sec.initialize_component_schema(conn)
    sec.upsert_facts(conn, [_fact(fp, days, 1, "A")])
    return sec.quarterization_readiness_rows(conn)[0]["readiness_class"]


def _company(**overrides):
    row = {"company_id": 1, "market": "usa", "ticker": "AAA", "company_name": "AAA Inc", "active": 1, "cik": "0000000001"}
    row.update(overrides)
    return row


def _companyfacts(*, include_extension: bool = False, end: str = "2024-03-31"):
    facts = {
        "us-gaap": {
            "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest": {
                "label": "Income before income taxes",
                "units": {"USD": [{"start": "2024-01-01", "end": end, "val": 10, "accn": "0001-24-1", "fy": 2024, "fp": "Q1", "form": "10-Q", "filed": "2024-05-01", "frame": "CY2024Q1"}]},
            },
            "InterestExpense": {
                "label": "Interest expense",
                "units": {"USD": [{"start": "2024-01-01", "end": end, "val": 2, "accn": "0001-24-1", "fy": 2024, "fp": "Q1", "form": "10-Q", "filed": "2024-05-01"}]},
            },
            "DepreciationDepletionAndAmortization": {
                "label": "Depreciation depletion and amortization",
                "units": {"USD": [{"start": "2024-01-01", "end": end, "val": 3, "accn": "0001-24-1", "fy": 2024, "fp": "Q1", "form": "10-Q", "filed": "2024-05-01"}]},
            },
        }
    }
    if include_extension:
        facts["acme"] = {
            "AcmeLeaseInterestExpense": {
                "label": "Lease interest expense",
                "units": {"USD": [{"start": "2024-01-01", "end": end, "val": 1, "accn": "0001-24-1", "fy": 2024, "fp": "Q1", "form": "10-Q", "filed": "2024-05-01"}]},
            }
        }
    return {"facts": facts}


def _fact(fp: str, days: int, value: float, accession: str, *, role: str = "PRETAX"):
    return {
        "company_id": 1,
        "ticker": "AAA",
        "cik": "0000000001",
        "taxonomy_namespace": "us-gaap",
        "concept_name": "IncomeLossBeforeIncomeTaxes",
        "concept_label": "Pretax",
        "semantic_role": role,
        "standard_or_extension": "STANDARD",
        "value": value,
        "value_text": str(value),
        "unit": "USD",
        "scale": None,
        "start_date": "2024-01-01",
        "end_date": "2024-03-31",
        "duration_days": days,
        "instant_or_duration": "DURATION",
        "form": "10-Q",
        "accession": accession,
        "filed_date": "2024-05-01",
        "fiscal_year": 2024,
        "fiscal_period": fp,
        "frame": None,
        "source_url": "url",
        "dimensions_json": '{"dim": null, "frame": null, "segment": null}',
        "fact_json": "{}",
        "acquired_at_utc": sec.utc_now(),
        "source_payload_sha256": "h",
        "natural_key": sec.sha256_text(f"{fp}-{days}-{value}-{accession}-{role}"),
    }
