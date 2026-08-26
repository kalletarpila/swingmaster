from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

from swingmaster.fundamentals import v3_phase8a10b_p2p3_reprioritization as p2p3


def write_csv(path: Path, data: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in data for key in row})
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


def make_quarter(company_id: int, ticker: str, qid: int, fy: int, fq: str, period: str, publish: str) -> dict[str, object]:
    return {
        "company_id": company_id,
        "ticker": ticker,
        "company_name": ticker,
        "market": "usa",
        "profile": "ORDINARY",
        "active": 1,
        "quarter_id": qid,
        "fiscal_year": fy,
        "fiscal_quarter": fq,
        "period_end_date": period,
        "publish_date": publish,
        "market_availability_date": "",
    }


def make_db(path: Path) -> Path:
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
            period_end_date TEXT, publish_date TEXT, market_availability_date TEXT
        );
        CREATE TABLE v3_quarter_fundamentals(
            quarter_id INTEGER PRIMARY KEY, revenue REAL, ebit REAL, ebitda REAL, free_cashflow REAL,
            cash REAL, total_debt REAL, shares_outstanding REAL
        );
        CREATE TABLE v3_ttm(
            ttm_id INTEGER PRIMARY KEY, company_id INTEGER, endpoint_quarter_id INTEGER,
            q1_quarter_id INTEGER, q2_quarter_id INTEGER, q3_quarter_id INTEGER, q4_quarter_id INTEGER
        );
        CREATE TABLE v3_score(score_id INTEGER PRIMARY KEY, company_id INTEGER, as_of_quarter_id INTEGER, score_model_version TEXT, score_fingerprint TEXT);
        CREATE TABLE v3_lifecycle(lifecycle_id INTEGER PRIMARY KEY, company_id INTEGER, endpoint_quarter_id INTEGER, lifecycle_model_version TEXT, lifecycle_fingerprint TEXT);
        CREATE TABLE v3_valuation(valuation_id INTEGER PRIMARY KEY, company_id INTEGER, endpoint_quarter_id INTEGER);
        """
    )
    conn.executemany(
        "INSERT INTO v3_company VALUES (?,?,?,?,?,?,?,?,?,?)",
        [
            (1, "usa", "AAA", "AAA", "ORDINARY", 1, "test", "", "now", "now"),
            (2, "usa", "NEW", "NEW", "ORDINARY", 1, "test", "", "now", "now"),
            (3, "usa", "OLD", "OLD", "ORDINARY", 1, "test", "", "now", "now"),
        ],
    )
    rows = [
        (101, 1, 2024, "Q1", "2024-03-31", "2024-05-01", None),
        (102, 1, 2024, "Q2", "2024-06-30", "2024-08-01", None),
        (103, 1, 2024, "Q3", "2024-09-30", "2024-11-01", None),
        (104, 1, 2024, "Q4", "2024-12-31", "2025-02-01", None),
        (105, 1, 2025, "Q1", "2025-03-31", "2025-05-01", None),
        (106, 1, 2025, "Q2", "2025-06-30", "2025-08-01", None),
        (107, 1, 2025, "Q3", "2025-09-30", "2025-11-01", None),
        (108, 1, 2025, "Q4", "2025-12-31", "2026-02-01", None),
        (109, 1, 2026, "Q1", "2026-03-31", "2026-05-01", None),
        (201, 2, 2026, "Q1", "2026-03-31", "2026-05-01", None),
        (202, 2, 2026, "Q2", "2026-06-30", "2026-08-01", None),
        (301, 3, 2019, "Q1", "2019-03-31", "2019-05-01", None),
        (302, 3, 2019, "Q2", "2019-06-30", "2019-08-01", None),
        (303, 3, 2019, "Q3", "2019-09-30", "2019-11-01", None),
        (304, 3, 2019, "Q4", "2019-12-31", "2020-02-01", None),
    ]
    conn.executemany("INSERT INTO v3_quarter VALUES (?,?,?,?,?,?,?)", rows)
    conn.executemany(
        "INSERT INTO v3_quarter_fundamentals VALUES (?,?,?,?,?,?,?,?)",
        [(row[0], 1, 1, 1, 1, 1, 1, 1) for row in rows],
    )
    conn.execute("INSERT INTO v3_ttm VALUES (1,1,109,106,107,108,109)")
    conn.execute("INSERT INTO v3_ttm VALUES (2,2,202,201,201,202,202)")
    conn.execute("INSERT INTO v3_score VALUES (1,1,109,'m','sf')")
    conn.execute("INSERT INTO v3_lifecycle VALUES (1,1,109,'m','lf')")
    conn.execute("INSERT INTO v3_valuation VALUES (1,1,109)")
    conn.commit()
    conn.close()
    return path


def make_source_root(root: Path) -> Path:
    p1 = [{"ticker": "AAA", "company_id": 1, "quarter_id": 108, "fiscal_year": 2025, "fiscal_quarter": "Q4", "period_end_date": "2025-12-31", "publish_date": "2025-01-01", "pattern": "P1_CASE", "signals": "NEGATIVE_REPORTING_LAG"}]
    p2 = [
        {"ticker": "AAA", "company_id": 1, "quarter_id": 109, "fiscal_year": 2026, "fiscal_quarter": "Q1", "period_end_date": "2026-03-31", "publish_date": "2026-05-01", "pattern": "POSSIBLE_ONE_YEAR_FISCAL_SHIFT", "signals": "ONE_YEAR_GAP"},
        {"ticker": "AAA", "company_id": 1, "quarter_id": 105, "fiscal_year": 2025, "fiscal_quarter": "Q1", "period_end_date": "2025-03-31", "publish_date": "2025-05-01", "pattern": "LONG", "signals": ""},
        {"ticker": "OLD", "company_id": 3, "quarter_id": 301, "fiscal_year": 2019, "fiscal_quarter": "Q1", "period_end_date": "2019-03-31", "publish_date": "2019-05-01", "pattern": "SEVERE_LONG", "signals": ""},
        {"ticker": "AAA", "company_id": 1, "quarter_id": 108, "fiscal_year": 2025, "fiscal_quarter": "Q4", "period_end_date": "2025-12-31", "publish_date": "2025-01-01", "pattern": "P1_DUP", "signals": ""},
    ]
    p3 = [
        {"ticker": "NEW", "company_id": 2, "quarter_id": 201, "fiscal_year": 2026, "fiscal_quarter": "Q1", "period_end_date": "2026-03-31", "publish_date": "2026-05-01", "pattern": "MISSING_HISTORY", "sequence_status": "MISSING_HISTORY"},
        {"ticker": "AAA", "company_id": 1, "quarter_id": 106, "fiscal_year": 2025, "fiscal_quarter": "Q2", "period_end_date": "2025-06-30", "publish_date": "2025-08-01", "pattern": "MISSING_HISTORY", "sequence_status": "MISSING_HISTORY"},
        {"ticker": "OLD", "company_id": 3, "quarter_id": 302, "fiscal_year": 2019, "fiscal_quarter": "Q2", "period_end_date": "2019-06-30", "publish_date": "2019-08-01", "pattern": "MISSING_HISTORY", "sequence_status": "MISSING_HISTORY"},
    ]
    fiscal = [
        {"ticker": "AAA", "company_id": 1, "quarter_id": 106, "fiscal_year": 2025, "fiscal_quarter": "Q2", "period_end_date": "2025-06-30", "sequence_status": "MISSING_HISTORY"},
        {"ticker": "NEW", "company_id": 2, "quarter_id": 201, "fiscal_year": 2026, "fiscal_quarter": "Q1", "period_end_date": "2026-03-31", "sequence_status": "MISSING_HISTORY"},
    ]
    period = [
        {"ticker": "AAA", "company_id": 1, "quarter_id": 109, "fiscal_year": 2026, "fiscal_quarter": "Q1", "period_end_date": "2026-03-31", "period_gap_class": "ONE_YEAR_GAP", "duplicate_period_end": 0},
        {"ticker": "AAA", "company_id": 1, "quarter_id": 106, "fiscal_year": 2025, "fiscal_quarter": "Q2", "period_end_date": "2025-06-30", "period_gap_class": "REVERSE_OR_DUPLICATE", "duplicate_period_end": 1},
        {"ticker": "CRUS", "company_id": 9, "quarter_id": 901, "fiscal_year": 2025, "fiscal_quarter": "Q2", "period_end_date": "2025-06-28", "period_gap_class": "VALID_52_53_WEEK", "duplicate_period_end": 0},
    ]
    publish = [{"ticker": "AAA", "company_id": 1, "quarter_id": 106, "fiscal_year": 2025, "fiscal_quarter": "Q2", "period_end_date": "2025-06-30", "publish_date": "2025-08-01", "publish_gap_class": "REVERSE", "duplicate_publish_date": 0, "valid_same_day_multi_quarter": 0}]
    lag = [{"ticker": "AAA", "company_id": 1, "quarter_id": 109, "fiscal_year": 2026, "fiscal_quarter": "Q1", "period_end_date": "2026-03-31", "publish_date": "2026-05-01", "reporting_lag_class": "NORMAL"}]
    cross = [{"ticker": "AAA", "company_id": 1, "quarter_id": 109, "fiscal_year": 2026, "fiscal_quarter": "Q1", "period_end_date": "2026-03-31", "publish_date": "2026-05-01", "pattern": "POSSIBLE_ONE_YEAR_FISCAL_SHIFT", "signals": "ONE_YEAR_GAP"}]
    for name, data in {
        "global_P1.csv": p1,
        "global_P2.csv": p2,
        "global_P3.csv": p3,
        "fiscal_sequence_audit.csv": fiscal,
        "period_end_gap_audit.csv": period,
        "publish_sequence_audit.csv": publish,
        "reporting_lag_audit.csv": lag,
        "structural_cross_signal_candidates.csv": cross,
    }.items():
        write_csv(root / name, data)
    return root


def test_latest_quarter_rank_and_membership() -> None:
    quarters = [make_quarter(1, "AAA", 100 + idx, 2024 + idx // 4, f"Q{idx % 4 + 1}", f"202{idx // 4 + 4}-03-31", "2025-01-01") for idx in range(9)]
    membership = p2p3.latest_quarter_membership(quarters)
    assert membership[108]["latest_quarter_rank"] == 1
    assert membership[108]["is_latest_quarter"] == 1
    assert membership[100]["in_latest_8q"] == 0
    assert membership[101]["in_latest_8q"] == 1


def test_fewer_than_8_history_all_in_latest8() -> None:
    membership = p2p3.latest_quarter_membership([make_quarter(2, "NEW", 1, 2026, "Q1", "2026-03-31", "2026-05-01")])
    assert membership[1]["in_latest_8q"] == 1


def test_latest_4_membership() -> None:
    quarters = [make_quarter(1, "AAA", 100 + idx, 2024 + idx // 4, f"Q{idx % 4 + 1}", f"202{idx // 4 + 4}-03-31", "2025-01-01") for idx in range(8)]
    membership = p2p3.latest_quarter_membership(quarters)
    assert sum(row["in_latest_4q"] for row in membership.values()) == 4


def test_2024_plus_flag() -> None:
    assert p2p3.is_2024_plus({"period_end_date": "2023-12-31", "publish_date": "2024-02-01"}) == 1
    assert p2p3.is_2024_plus({"period_end_date": "2023-12-31", "publish_date": "2023-12-31"}) == 0


def test_pre2024_latest8_still_recent_priority() -> None:
    row = {"is_2024_plus": 0, "in_latest_8q": 1, "excluded_p1": 0, "signal_count": 1}
    assert p2p3.reclassify_p2(row) == "P2B_RECENT_NONBLOCKING"


def test_p1_excluded() -> None:
    assert p2p3.reclassify_p2({"excluded_p1": 1}) == "P1_EXCLUDED"


def test_current_ttm_impact_detection(tmp_path: Path) -> None:
    db = make_db(tmp_path / "v3.db")
    with p2p3.connect_ro(db) as conn:
        downstream = p2p3.current_downstream_sets(conn)
    assert 106 in downstream["current_ttm_inputs"]
    assert 105 not in downstream["current_ttm_inputs"]


def test_current_score_impact_detection(tmp_path: Path) -> None:
    db = make_db(tmp_path / "v3.db")
    with p2p3.connect_ro(db) as conn:
        downstream = p2p3.current_downstream_sets(conn)
    assert downstream["current_score"] == {109}


def test_current_lifecycle_impact_detection(tmp_path: Path) -> None:
    db = make_db(tmp_path / "v3.db")
    with p2p3.connect_ro(db) as conn:
        downstream = p2p3.current_downstream_sets(conn)
    assert downstream["current_lifecycle"] == {109}


def test_current_valuation_impact_detection(tmp_path: Path) -> None:
    db = make_db(tmp_path / "v3.db")
    with p2p3.connect_ro(db) as conn:
        downstream = p2p3.current_downstream_sets(conn)
    assert downstream["current_valuation"] == {109}


def test_multi_signal_count() -> None:
    signals = p2p3.signal_index(
        [{"quarter_id": 1, "sequence_status": "MISSING_HISTORY"}],
        [{"quarter_id": 1, "period_gap_class": "REVERSE_OR_DUPLICATE", "duplicate_period_end": 1}],
        [{"quarter_id": 1, "publish_gap_class": "REVERSE", "duplicate_publish_date": 0}],
        [{"quarter_id": 1, "reporting_lag_class": "NEGATIVE"}],
        [{"quarter_id": 1, "signals": "ONE_YEAR_GAP", "pattern": "POSSIBLE_ONE_YEAR_FISCAL_SHIFT"}],
    )
    assert len(signals[1]) >= 5


def test_p2a_classification() -> None:
    assert p2p3.reclassify_p2({"excluded_p1": 0, "is_recent_priority": 1, "affects_current_4q_ttm": 1, "signal_count": 1}) == "P2A_CURRENT_CRITICAL_REVIEW"


def test_p2b_classification() -> None:
    assert p2p3.reclassify_p2({"excluded_p1": 0, "is_recent_priority": 1, "signal_count": 1}) == "P2B_RECENT_NONBLOCKING"


def test_p2c_classification() -> None:
    assert p2p3.reclassify_p2({"excluded_p1": 0, "is_recent_priority": 0, "signal_count": 1}) == "P2C_HISTORICAL_DEFERRED"


def test_recent_harmless_p3_remains_informational() -> None:
    assert p2p3.reclassify_p3({"excluded_p1": 0, "is_recent_priority": 1, "signal_count": 1}) == "P3A_RECENT_INFORMATIONAL"


def test_multi_signal_p3_escalation() -> None:
    assert (
        p2p3.reclassify_p3(
            {"excluded_p1": 0, "is_recent_priority": 1, "signal_count": 2, "affects_current_4q_ttm": 1, "signals": "PERIOD_END:SEVERE_SHORT|PUBLISH_SEQUENCE:REVERSE"}
        )
        == "P3_ESCALATED"
    )


def test_sparse_history_not_current_critical_by_itself() -> None:
    assert p2p3.reclassify_p3({"excluded_p1": 0, "is_recent_priority": 1, "signal_count": 1, "affects_current_4q_ttm": 0}) == "P3A_RECENT_INFORMATIONAL"


def test_valid_52_53_week_not_escalated() -> None:
    row = {"ticker": "CRUS", "issue_type": "VALID_52_53_WEEK", "signals": "VALID_52_53_WEEK"}
    assert p2p3.classify_recommended_action(row, "P2B_RECENT_NONBLOCKING") == "VALID_52_53_WEEK"


def test_historical_deferred_rule() -> None:
    assert p2p3.reclassify_p3({"excluded_p1": 0, "is_recent_priority": 0, "signal_count": 1}) == "P3B_HISTORICAL_INFORMATIONAL"


def test_systemic_recent_pattern_does_not_escalate_old_nonimpact_by_itself() -> None:
    assert p2p3.reclassify_p2({"excluded_p1": 0, "is_recent_priority": 0, "systemic_recent_pattern": 1, "signal_count": 1}) == "P2C_HISTORICAL_DEFERRED"


def test_priority_score_prefers_latest_multi_signal() -> None:
    latest = {"ticker": "AAA", "quarter_id": 1, "is_latest_quarter": 1, "signal_count": 2, "signals": "PERIOD_END:SEVERE_SHORT"}
    old = {"ticker": "AAA", "quarter_id": 2, "is_latest_quarter": 0, "signal_count": 1, "signals": "REPORTING_LAG:LONG"}
    assert p2p3.priority_score(latest) < p2p3.priority_score(old)


def test_queue_columns_are_stable() -> None:
    row = p2p3.queue_row({"ticker": "AAA", "company_id": 1, "fiscal_year": 2026, "fiscal_quarter": "Q1"}, 1)
    assert list(row) == p2p3.QUEUE_COLUMNS


def test_run_outputs_and_safety(tmp_path: Path) -> None:
    db = make_db(tmp_path / "v3.db")
    raw = tmp_path / "osakedata.db"
    raw.write_text("raw", encoding="utf-8")
    source = make_source_root(tmp_path / "source")
    out = tmp_path / "out"
    summary = p2p3.run_phase8a10b_p2p3_reprioritization(
        p2p3.Phase8A10BP2P3Paths(artifact_root=out, source_audit_root=source, v3_db=db, rawcandle_db=raw)
    )
    assert summary["safety"]["production_writes"] == 0
    assert summary["safety"]["ttm_writes"] == 0
    assert summary["safety"]["score_writes"] == 0
    assert summary["safety"]["lifecycle_writes"] == 0
    assert summary["safety"]["valuation_writes"] == 0
    assert summary["safety"]["rawcandle_writes"] == 0
    assert summary["p2_reprioritization"]["P2A_rows"] == 1
    assert summary["p2_reprioritization"]["P2B_rows"] == 2
    assert summary["p2_reprioritization"]["P2C_rows"] == 0
    assert summary["p3_reprioritization"]["P3_ESCALATED_rows"] == 1
    assert summary["historical_deferred"]["deferred_rows_affecting_current_downstream"] == 0
    assert (out / "current_critical_2024plus_last8q_queue.csv").exists()
    assert (out / "historical_deferred_pre2024_noncritical.csv").exists()
