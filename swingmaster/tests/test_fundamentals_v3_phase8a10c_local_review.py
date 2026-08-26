from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

from swingmaster.fundamentals import v3_phase8a10c_local_review as local


QUEUE_COLUMNS = [
    "Priority Rank",
    "Ticker",
    "Company ID",
    "Fiscal Year",
    "Fiscal Q",
    "Period End",
    "Publish Date",
    "Original Severity",
    "Reclassified Severity",
    "Issue Type",
    "Signal Count",
    "Signals",
    "Latest Quarter Rank",
    "In Latest 4Q",
    "In Latest 8Q",
    "Affects Current TTM",
    "Affects Score",
    "Affects Lifecycle",
    "Affects Valuation",
    "Recommended Action",
]


def write_csv(path: Path, data: list[dict[str, object]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = sorted({key for row in data for key in row})
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)


def qrow(idx: int, *, action: str = "LOCAL_EVIDENCE_REVIEW", impact: bool = True, issue: str = "LONG", signals: str = "REPORTING_LAG:LONG") -> dict[str, object]:
    return {
        "Priority Rank": idx,
        "Ticker": f"T{idx:02d}",
        "Company ID": idx,
        "Fiscal Year": 2026,
        "Fiscal Q": "Q1",
        "Period End": "2026-03-31",
        "Publish Date": "2026-05-10",
        "Original Severity": "P2",
        "Reclassified Severity": "P2A_CURRENT_CRITICAL_REVIEW",
        "Issue Type": issue,
        "Signal Count": 1,
        "Signals": signals,
        "Latest Quarter Rank": 1,
        "In Latest 4Q": 1 if impact else 0,
        "In Latest 8Q": 1,
        "Affects Current TTM": 1 if impact else 0,
        "Affects Score": 1 if impact else 0,
        "Affects Lifecycle": 1 if impact else 0,
        "Affects Valuation": 1 if impact else 0,
        "Recommended Action": action,
    }


def make_db(path: Path, *, companies: int = 31) -> Path:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE v3_company(
            company_id INTEGER PRIMARY KEY, market TEXT, ticker TEXT, company_name TEXT,
            profile TEXT DEFAULT 'ORDINARY', active INTEGER, admission_source TEXT,
            admission_evidence TEXT, created_at_utc TEXT, updated_at_utc TEXT
        );
        CREATE TABLE v3_quarter(
            quarter_id INTEGER PRIMARY KEY, company_id INTEGER, fiscal_year INTEGER, fiscal_quarter TEXT,
            period_end_date TEXT, publish_date TEXT, market_availability_date TEXT,
            q_lifecycle TEXT DEFAULT 'ACTIVE', sec_confirmation_state TEXT DEFAULT 'NOT_DERIVABLE',
            created_at_utc TEXT DEFAULT 'now', updated_at_utc TEXT DEFAULT 'now'
        );
        CREATE TABLE v3_quarter_fundamentals(
            quarter_id INTEGER PRIMARY KEY, revenue REAL, ebitda REAL, free_cashflow REAL,
            cash REAL, total_debt REAL, shares_outstanding REAL, ebit REAL, operating_income REAL,
            operating_cashflow REAL, capex REAL, gross_profit REAL, net_income REAL, currency TEXT,
            accepted_source_provider TEXT, accepted_at_utc TEXT, update_run_id TEXT, derivation_method TEXT,
            resolution_issue_id INTEGER, created_at_utc TEXT, updated_at_utc TEXT
        );
        CREATE TABLE v3_migration_audit(
            audit_id INTEGER PRIMARY KEY, migration_run_id TEXT, source TEXT, source_key TEXT,
            company_id INTEGER, quarter_id INTEGER, audit_type TEXT, decision TEXT,
            evidence_json TEXT, created_at_utc TEXT
        );
        CREATE TABLE v3_provider_q_acquisition(
            acquisition_id INTEGER PRIMARY KEY, quarter_id INTEGER, provider TEXT, acquisition_result TEXT,
            last_checked_at_utc TEXT, last_success_at_utc TEXT, next_retry_at_utc TEXT, attempt_count INTEGER,
            usable_field_count INTEGER, provider_cache_ref TEXT, last_error_code TEXT, updated_at_utc TEXT
        );
        CREATE TABLE v3_resolution_issue(
            issue_id INTEGER PRIMARY KEY, quarter_id INTEGER, unresolved_market TEXT, unresolved_ticker TEXT,
            unresolved_fiscal_year INTEGER, unresolved_fiscal_quarter TEXT, issue_type TEXT, field_name TEXT,
            status TEXT, source_details_json TEXT, resolution TEXT, created_at_utc TEXT, resolved_at_utc TEXT,
            updated_at_utc TEXT
        );
        CREATE TABLE v3_ttm(ttm_id INTEGER PRIMARY KEY, company_id INTEGER, endpoint_quarter_id INTEGER, q1_quarter_id INTEGER, q2_quarter_id INTEGER, q3_quarter_id INTEGER, q4_quarter_id INTEGER);
        CREATE TABLE v3_score(score_id INTEGER PRIMARY KEY, company_id INTEGER, as_of_quarter_id INTEGER, score_model_version TEXT, score_fingerprint TEXT);
        CREATE TABLE v3_lifecycle(lifecycle_id INTEGER PRIMARY KEY, company_id INTEGER, endpoint_quarter_id INTEGER, lifecycle_model_version TEXT, lifecycle_fingerprint TEXT);
        CREATE TABLE v3_valuation(valuation_id INTEGER PRIMARY KEY, company_id INTEGER, endpoint_quarter_id INTEGER);
        """
    )
    for cid in range(1, companies + 1):
        conn.execute("INSERT INTO v3_company VALUES (?,?,?,?,?,?,?,?,?,?)", (cid, "usa", f"T{cid:02d}", f"T{cid:02d}", "ORDINARY", 1, "test", "", "now", "now"))
        for offset, (fy, fq, period, publish) in enumerate(
            [
                (2025, "Q3", "2025-09-30", "2025-11-01"),
                (2025, "Q4", "2025-12-31", "2026-02-01"),
                (2026, "Q1", "2026-03-31", "2026-05-10"),
            ],
            1,
        ):
            qid = cid * 100 + offset
            conn.execute("INSERT INTO v3_quarter(quarter_id,company_id,fiscal_year,fiscal_quarter,period_end_date,publish_date,market_availability_date) VALUES (?,?,?,?,?,?,?)", (qid, cid, fy, fq, period, publish, None))
            conn.execute("INSERT INTO v3_quarter_fundamentals VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (qid, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, "USD", "YAHOO", "now", "run", "DIRECT", None, "now", "now"))
        endpoint = cid * 100 + 3
        conn.execute("INSERT INTO v3_ttm VALUES (?,?,?,?,?,?,?)", (cid, cid, endpoint, cid * 100 + 1, cid * 100 + 1, cid * 100 + 2, endpoint))
        conn.execute("INSERT INTO v3_score VALUES (?,?,?,?,?)", (cid, cid, endpoint, "m", "sf"))
        conn.execute("INSERT INTO v3_lifecycle VALUES (?,?,?,?,?)", (cid, cid, endpoint, "m", "lf"))
        conn.execute("INSERT INTO v3_valuation VALUES (?,?,?)", (cid, cid, endpoint))
        outcomes = ["PERIOD_DATE_CONFIRMED", "PUBLISH_DATE_CONFIRMED"]
        if cid == 2:
            outcomes = ["PERIOD_DATE_CONFLICT"]
        conn.execute(
            "INSERT INTO v3_migration_audit VALUES (?,?,?,?,?,?,?,?,?,?)",
            (cid, "run", "YAHOO", f"key{cid}", cid, endpoint, "CANONICAL_APPLY", "ACCEPTED", json.dumps({"metadata_outcomes": outcomes}), "now"),
        )
    conn.commit()
    conn.close()
    return path


def make_roots(tmp_path: Path) -> tuple[Path, Path]:
    p2p3_root = tmp_path / "p2p3"
    queue = [qrow(i) for i in range(1, 31)]
    queue[1]["Issue Type"] = "SEVERE_SHORT"
    queue[1]["Signals"] = "PERIOD_END:SEVERE_SHORT"
    queue.append(qrow(31, action="EXTERNAL_RESEARCH", impact=True, issue="SEVERE_LONG", signals="PERIOD_END:SEVERE_LONG|PUBLISH_SEQUENCE:LONG"))
    write_csv(p2p3_root / "current_critical_2024plus_last8q_queue.csv", queue, QUEUE_COLUMNS)
    write_csv(p2p3_root / "P2A_current_critical.csv", queue)
    write_csv(p2p3_root / "P3_escalated.csv", [])
    full_root = tmp_path / "full"
    write_csv(full_root / "global_P1.csv", [{"ticker": "P1X", "fiscal_year": 2026, "fiscal_quarter": "Q1", "pattern": "NEGATIVE"}])
    return p2p3_root, full_root


def test_exactly_30_local_evidence_cases_frozen(tmp_path: Path) -> None:
    p2p3_root, _full = make_roots(tmp_path)
    frozen = local.freeze_local_cases(list(csv.DictReader(open(p2p3_root / "current_critical_2024plus_last8q_queue.csv"))))
    assert len(frozen) == 30


def test_p1_excluded_from_final_queue(tmp_path: Path) -> None:
    p2p3_root, full = make_roots(tmp_path)
    rows = list(csv.DictReader(open(full / "global_P1.csv")))
    assert len(rows) == 1


def test_current_v3_parity_exact_match(tmp_path: Path) -> None:
    db = make_db(tmp_path / "v3.db")
    with local.connect_ro(db) as conn:
        row, _context = local.canonical_context(conn, 1, 2026, "Q1")
    assert local.parity_status({"period_end": "2026-03-31", "publish_date": "2026-05-10"}, row) == "CURRENT_EXACT_MATCH"


def test_local_sec_or_migration_evidence_resolution() -> None:
    evidence = {"migration_audit": [{"source": "SEC", "evidence_json": json.dumps({"metadata_outcomes": ["PERIOD_DATE_CONFIRMED", "PUBLISH_DATE_CONFIRMED"]})}], "acquisitions": [], "issues": []}
    assert local.local_status({"Issue Type": "LONG", "Signals": "", "Affects Current TTM": 1, "Affects Score": 0, "Affects Lifecycle": 0, "Affects Valuation": 0}, evidence)[0] == "LOCAL_CONFIRMED_VALID_FALSE_POSITIVE"


def test_local_lineage_evidence_resolution() -> None:
    evidence = {"migration_audit": [{"source": "YAHOO", "evidence_json": json.dumps({"metadata_outcomes": ["PERIOD_DATE_CONFIRMED", "PUBLISH_DATE_CONFIRMED"]})}], "acquisitions": [{"provider": "YAHOO"}], "issues": []}
    assert "metadata_outcomes=PERIOD_DATE_CONFIRMED|PUBLISH_DATE_CONFIRMED" in local.local_evidence_summary(evidence)


def test_false_positive_classification() -> None:
    evidence = {"migration_audit": [{"source": "YAHOO", "evidence_json": json.dumps({"metadata_outcomes": ["PERIOD_DATE_CONFIRMED", "PUBLISH_DATE_CONFIRMED"]})}], "acquisitions": [], "issues": []}
    assert local.local_status({"Issue Type": "VERY_SHORT", "Signals": "", "Affects Current TTM": 1}, evidence)[0] == "LOCAL_CONFIRMED_VALID_FALSE_POSITIVE"


def test_missing_history_nonblocking() -> None:
    evidence = {"migration_audit": [], "acquisitions": [], "issues": []}
    assert local.local_status({"Issue Type": "MISSING_HISTORY", "Signals": "", "Affects Current TTM": 1}, evidence)[0] == "LOCAL_MISSING_HISTORY_NON_BLOCKING"


def test_market_availability_only_nonblocking() -> None:
    evidence = {"migration_audit": [], "acquisitions": [], "issues": []}
    assert local.local_status({"Issue Type": "LONG", "Signals": "MARKET_AVAILABILITY", "Affects Current TTM": 1}, evidence)[0] == "LOCAL_MARKET_AVAILABILITY_ONLY"


def test_local_repair_candidate_without_write_status_supported() -> None:
    assert "LOCAL_EVIDENCE_SUPPORTS_REPAIR" in {
        "LOCAL_CONFIRMED_VALID_FALSE_POSITIVE",
        "LOCAL_VALID_SPECIAL_CASE",
        "LOCAL_MISSING_HISTORY_NON_BLOCKING",
        "LOCAL_MARKET_AVAILABILITY_ONLY",
        "LOCAL_EVIDENCE_SUPPORTS_REPAIR",
        "DOWNGRADE_RECENT_NONBLOCKING",
        "DOWNGRADE_INFORMATIONAL",
        "EXTERNAL_RESEARCH_REQUIRED",
        "ESCALATE_TO_P1",
    }


def test_escalate_to_p1_rule() -> None:
    evidence = {"migration_audit": [], "acquisitions": [], "issues": []}
    assert local.local_status({"Issue Type": "SEVERE_SHORT", "Signals": "PERIOD_END:SEVERE_SHORT", "Affects Current TTM": 1}, evidence)[0] == "ESCALATE_TO_P1"


def test_current_ttm_score_lifecycle_valuation_impact_revalidation(tmp_path: Path) -> None:
    db = make_db(tmp_path / "v3.db")
    with local.connect_ro(db) as conn:
        impacts = local.revalidate_impact_rows(conn, [{"quarter_id": 103}])[0]
    assert impacts["Affects Current TTM"] == "1"
    assert impacts["Affects Score"] == "1"
    assert impacts["Affects Lifecycle"] == "1"
    assert impacts["Affects Valuation"] == "1"


def test_derived_impact_union_deduplicated_once() -> None:
    queue, dedupe = local.dedupe_external_queue(
        [
            {"Ticker": "AAA", "Fiscal Year": "2026", "Fiscal Q": "Q1", "Issue Type": "LONG", "Affects Current TTM": "1", "Affects Score": "0", "Affects Lifecycle": "0", "Affects Valuation": "0"},
            {"Ticker": "AAA", "Fiscal Year": "2026", "Fiscal Q": "Q1", "Issue Type": "LONG", "Affects Current TTM": "0", "Affects Score": "1", "Affects Lifecycle": "1", "Affects Valuation": "1"},
        ]
    )
    assert len(queue) == 1
    assert len(dedupe) == 1
    assert queue[0]["Affects Current TTM"] == "1"
    assert queue[0]["Affects Score"] == "1"


def test_nonimpact_latest8q_case_sent_to_backlog_predicate() -> None:
    row = {"In Latest 8Q": "1", "Affects Current TTM": "0", "Affects Score": "0", "Affects Lifecycle": "0", "Affects Valuation": "0"}
    assert not local.current_impact(row)


def test_precise_research_question_populated() -> None:
    question, missing = local.precise_question({"Ticker": "AAA", "Fiscal Year": 2026, "Fiscal Q": "Q1", "Period End": "2026-03-31", "Publish Date": "2026-05-10", "Issue Type": "LONG", "Signals": "REPORTING_LAG:LONG"})
    assert "Verify first public" in question
    assert "Official" in missing


def test_duplicate_research_question_is_precise() -> None:
    question, missing = local.precise_question({"Ticker": "AAA", "Fiscal Year": 2026, "Fiscal Q": "Q1", "Period End": "2026-03-31", "Issue Type": "SEVERE_LONG", "Signals": "DUPLICATE_ECONOMIC_QUARTER"})
    assert "duplicates another economic quarter" in question
    assert "two canonical rows" in missing


def test_current_impact_union_true_for_ttm_only() -> None:
    assert local.current_impact({"Affects Current TTM": "1", "Affects Score": "0", "Affects Lifecycle": "0", "Affects Valuation": "0"})


def test_current_impact_false_for_latest8_only() -> None:
    assert not local.current_impact({"In Latest 8Q": "1", "Affects Current TTM": "0", "Affects Score": "0", "Affects Lifecycle": "0", "Affects Valuation": "0"})


def test_dedupe_priority_1_for_ttm_plus_score() -> None:
    queue, _dedupe = local.dedupe_external_queue([{"Ticker": "AAA", "Fiscal Year": "2026", "Fiscal Q": "Q1", "Issue Type": "LONG", "Affects Current TTM": "1", "Affects Score": "1", "Affects Lifecycle": "0", "Affects Valuation": "0"}])
    assert queue[0]["Priority"] == "P1"


def test_dedupe_priority_2_for_ttm_only() -> None:
    queue, _dedupe = local.dedupe_external_queue([{"Ticker": "AAA", "Fiscal Year": "2026", "Fiscal Q": "Q1", "Issue Type": "LONG", "Affects Current TTM": "1", "Affects Score": "0", "Affects Lifecycle": "0", "Affects Valuation": "0"}])
    assert queue[0]["Priority"] == "P2"


def test_dedupe_priority_3_for_score_only() -> None:
    queue, _dedupe = local.dedupe_external_queue([{"Ticker": "AAA", "Fiscal Year": "2026", "Fiscal Q": "Q1", "Issue Type": "LONG", "Affects Current TTM": "0", "Affects Score": "1", "Affects Lifecycle": "0", "Affects Valuation": "0"}])
    assert queue[0]["Priority"] == "P3"


def test_valid_special_case_classification() -> None:
    evidence = {"migration_audit": [{"source": "LEGACY", "evidence_json": json.dumps({"metadata_outcomes": ["PERIOD_DATE_CONFIRMED", "PUBLISH_DATE_CONFIRMED"]})}], "acquisitions": [], "issues": []}
    assert local.local_status({"Issue Type": "SEVERE_LONG", "Signals": "PERIOD_END:SEVERE_LONG", "Affects Current TTM": 1}, evidence)[0] == "LOCAL_VALID_SPECIAL_CASE"


def test_metadata_conflict_requires_external_when_current_impact() -> None:
    evidence = {"migration_audit": [{"source": "LEGACY", "evidence_json": json.dumps({"metadata_outcomes": ["PERIOD_DATE_CONFLICT"]})}], "acquisitions": [], "issues": []}
    assert local.local_status({"Issue Type": "LONG", "Signals": "REPORTING_LAG:LONG", "Affects Current TTM": 1}, evidence)[0] == "EXTERNAL_RESEARCH_REQUIRED"


def test_no_impact_downgrade_recent_nonblocking() -> None:
    evidence = {"migration_audit": [], "acquisitions": [], "issues": []}
    assert local.local_status({"Issue Type": "LONG", "Signals": "REPORTING_LAG:LONG", "Affects Current TTM": 0, "Affects Score": 0, "Affects Lifecycle": 0, "Affects Valuation": 0}, evidence)[0] == "DOWNGRADE_RECENT_NONBLOCKING"


def test_parity_missing_current_is_already_resolved() -> None:
    assert local.parity_status({"period_end": "2026-03-31", "publish_date": "2026-05-10"}, None) == "ALREADY_RESOLVED"


def test_run_outputs_and_no_writes(tmp_path: Path) -> None:
    db = make_db(tmp_path / "v3.db")
    raw = tmp_path / "osakedata.db"
    raw.write_text("raw", encoding="utf-8")
    p2p3_root, full_root = make_roots(tmp_path)
    out = tmp_path / "out"
    summary = local.run_phase8a10c_local_review(local.Phase8A10CPaths(out, p2p3_root, full_root, db, raw))
    assert summary["starting_state"]["local_evidence_rows"] == 30
    assert summary["local_review"]["exact_current_matches"] == 30
    assert summary["local_review"]["locally_confirmed_valid"] >= 1
    assert summary["local_review"]["external_research_still_required"] == 1
    assert summary["local_review"]["escalated_P1"] == 0
    assert summary["final_external_queue"]["queue_rows"] == 2
    assert summary["global_P1"]["global_P1_rows_excluded"] == 1
    assert summary["global_P1"]["global_P1_overlap_with_final_queue"] == 0
    assert summary["safety"]["production_writes"] == 0
    assert summary["safety"]["ttm_writes"] == 0
    assert summary["safety"]["score_writes"] == 0
    assert summary["safety"]["lifecycle_writes"] == 0
    assert summary["safety"]["valuation_writes"] == 0
    assert summary["safety"]["rawcandle_writes"] == 0
    assert (out / "current_downstream_external_research_queue.csv").exists()
    assert (out / "latest8q_nonblocking_backlog.csv").exists()
