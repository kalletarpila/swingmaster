from __future__ import annotations

import csv
import json
import sqlite3

from swingmaster.fundamentals.v3_phase8a2_queue_reduction import (
    CLASSIFICATION_USER_EVIDENCE,
    Phase8A2Paths,
    annotate_findings,
    build_evidence_units,
    offset_days,
    run_phase8a2,
    source_family,
    split_queues,
)


def write_csv(path, rows, fieldnames=None):
    fieldnames = fieldnames or list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def base_row(**overrides):
    row = {
        "issue_id": "P8-SEM-001",
        "issue_type": "SEMANTIC_FIELD_OUTLIER",
        "company_id": "1",
        "ticker": "TST",
        "company_name": "Test Inc",
        "market": "usa",
        "active": "1",
        "fiscal_year": "2025",
        "fiscal_quarter": "Q1",
        "period_end": "2025-03-31",
        "publish_date": "2025-05-01",
        "field_name": "revenue",
        "stored_value": "-1",
        "source_provenance": "",
        "age_bucket": "2025",
        "priority": "P1_CURRENT_MATERIAL",
        "latest_company_state_affected": "1",
        "ttm_ids": "1",
        "score_ids": "1",
        "lifecycle_ids": "1",
        "valuation_ids": "1",
        "ttm_count": "1",
        "score_count": "1",
        "lifecycle_count": "1",
        "valuation_count": "1",
    }
    row.update(overrides)
    return row


def make_minimal_db(path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE v3_quarter(quarter_id INTEGER,company_id INTEGER,fiscal_year INTEGER,fiscal_quarter TEXT,period_end_date TEXT,publish_date TEXT);
        CREATE TABLE v3_quarter_fundamentals(quarter_id INTEGER,revenue REAL,shares_outstanding REAL);
        CREATE TABLE v3_ttm(ttm_id INTEGER);
        CREATE TABLE v3_valuation(valuation_id INTEGER);
        CREATE TABLE v3_score(score_id INTEGER);
        CREATE TABLE v3_lifecycle(lifecycle_id INTEGER);
        INSERT INTO v3_quarter VALUES (1,1,2025,'Q1','2025-03-31','2025-02-01');
        INSERT INTO v3_quarter VALUES (2,1,2024,'Q4','2024-12-31','2025-02-01');
        INSERT INTO v3_quarter_fundamentals VALUES (1,-1,0);
        INSERT INTO v3_quarter_fundamentals VALUES (2,5,10);
        """
    )
    conn.commit()
    conn.close()


def test_offset_and_source_clustering_are_deterministic() -> None:
    provenance = json.dumps(
        {
            "source": "V2",
            "audit_type": "PHASE4B_FIELD_RECOVERY",
            "evidence_json": json.dumps({"recovery_mode": "DIRECT_SAME_Q_NULL_FILL"}),
        }
    )
    assert offset_days("2025-02-01", "2025-03-31") == -58
    assert source_family(provenance) == "V2:PHASE4B_FIELD_RECOVERY:DIRECT_SAME_Q_NULL_FILL"
    assert source_family("not json") == "UNPARSEABLE"


def test_all_issue_ids_accounted_and_queues_mutually_exclusive() -> None:
    findings = [
        base_row(issue_id="P8-SEM-001"),
        base_row(issue_id="P8-SEM-002", field_name="shares_outstanding", stored_value="0"),
        base_row(issue_id="P8-PUB-001", issue_type="PUBLISH_DATE_ANOMALY", field_name="publish_date", stored_value="2025-02-01", publish_date="2025-02-01"),
        base_row(issue_id="P8-OLD-001", fiscal_year="2020", period_end="2020-03-31", age_bucket="<=2020", priority="P4_LOW_CURRENT_MATERIALITY", latest_company_state_affected="0", ttm_ids="", score_ids="", lifecycle_ids="", valuation_ids="", ttm_count="0", score_count="0", lifecycle_count="0", valuation_count="0"),
        base_row(issue_id="P8-NEW-001", fiscal_year="2026", period_end="2026-03-31", age_bucket="2026", priority="P3_RECENT_UNCERTAIN", latest_company_state_affected="0"),
    ]
    annotated = annotate_findings(findings)
    units, dedup = build_evidence_units(annotated)
    queue_a, queue_b, queue_c = split_queues(units)
    assert {row["old_issue_id"] for row in dedup} == {row["issue_id"] for row in findings}
    assert len(dedup) == len(findings)
    assert not ({u["evidence_unit_id"] for u in queue_a} & {u["evidence_unit_id"] for u in queue_b})
    assert not ({u["evidence_unit_id"] for u in queue_b} & {u["evidence_unit_id"] for u in queue_c})
    dispositions = {row["issue_id"]: row["disposition"] for row in annotated}
    assert dispositions["P8-OLD-001"] == "LOW_MATERIALITY_ACCEPT"
    assert dispositions["P8-NEW-001"] == "WAIT_FOR_REFRESH"
    assert dispositions["P8-SEM-002"] == "MANUAL_A"


def test_same_quarter_multifield_combines_but_different_quarters_do_not() -> None:
    findings = [
        base_row(issue_id="A", field_name="revenue"),
        base_row(issue_id="B", field_name="cash"),
        base_row(issue_id="C", fiscal_quarter="Q2", period_end="2025-06-30", field_name="revenue"),
    ]
    units, _dedup = build_evidence_units(annotate_findings(findings))
    assert len(units) == 2
    combined = [u for u in units if u["period_end"] == "2025-03-31"][0]
    assert combined["affected_fields"] == "cash|revenue"


def test_corporate_action_candidate_does_not_auto_repair() -> None:
    annotated = annotate_findings([base_row(issue_id="S", field_name="shares_outstanding", stored_value="0")])
    assert annotated[0]["disposition"] == "MANUAL_A"
    assert annotated[0]["queue"] == "A"


def test_run_phase8a2_preserves_counts_and_performs_zero_writes(tmp_path) -> None:
    phase8 = tmp_path / "phase8"
    out = tmp_path / "out"
    phase8.mkdir()
    db = tmp_path / "v3.db"
    raw = tmp_path / "raw.db"
    make_minimal_db(db)
    sqlite3.connect(raw).execute("CREATE TABLE osakedata(osake TEXT,pvm TEXT,close REAL,market TEXT)").connection.close()

    master = []
    manual = []
    for i in range(111):
        latest = "1" if i == 0 else "0"
        age = "2025" if i < 83 else "2026"
        prio = "P1_CURRENT_MATERIAL" if age == "2025" else "P3_RECENT_UNCERTAIN"
        master.append(base_row(issue_id=f"P8-PUB-{i+1:03d}", issue_type="PUBLISH_DATE_ANOMALY", field_name="publish_date", stored_value="2025-02-01", publish_date="2025-02-01", age_bucket=age, priority=prio, latest_company_state_affected=latest))
        if i < 104:
            manual.append({"request_id": f"M{i}", "priority": prio})
    for i in range(237):
        age = "<=2020" if i < 7 else "2025"
        prio = "P4_LOW_CURRENT_MATERIALITY" if i < 7 else "P1_CURRENT_MATERIAL"
        master.append(base_row(issue_id=f"P8-SEM-{i+1:03d}", age_bucket=age, priority=prio, latest_company_state_affected="0"))
        if i < 223:
            manual.append({"request_id": f"SM{i}", "priority": prio})
    write_csv(phase8 / "phase8_master_anomaly_table.csv", master)
    write_csv(phase8 / "manual_evidence_requests.csv", manual, ["request_id", "priority"])
    (phase8 / "phase8_frozen_repair_set.csv").write_text("", encoding="utf-8")
    (phase8 / "phase8_summary.json").write_text(json.dumps({"classification": "FUNDAMENTALS_V3_PHASE8_MANUAL_EVIDENCE_REQUIRED"}), encoding="utf-8")

    summary = run_phase8a2(Phase8A2Paths(phase8, out, db, raw))
    assert summary["classification"] == CLASSIFICATION_USER_EVIDENCE
    assert summary["total_findings"] == 348
    assert summary["publish_findings"] == 111
    assert summary["semantic_findings"] == 237
    assert summary["raw_manual_requests"] == 327
    assert summary["production_counts_before"] == summary["production_counts_after"]
    assert summary["production_writes"] == 0
    assert (out / "manual_request_dedup_map.csv").exists()

