from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

from swingmaster.fundamentals import v3_phase8a10b_full_sequence_audit as audit


def write_csv(path: Path, data: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(data[0]))
        writer.writeheader()
        writer.writerows(data)


def quarter(ticker: str, fy: int, fq: str, period: str, publish: str, qid: int, company_id: int = 1) -> dict:
    return {
        "company_id": company_id,
        "ticker": ticker,
        "company_name": ticker,
        "active": 1,
        "market": "usa",
        "quarter_id": qid,
        "fiscal_year": fy,
        "fiscal_quarter": fq,
        "period_end_date": period,
        "publish_date": publish,
        "market_availability_date": "",
        "revenue": 10.0,
        "ebit": 1.0,
        "ebitda": 2.0,
        "free_cashflow": 3.0,
        "cash": 4.0,
        "total_debt": 5.0,
        "shares_outstanding": 6.0,
        "core_ready": 1,
        "latest_ttm_endpoint": 0,
        "score_presence": 0,
        "lifecycle_presence": 0,
        "valuation_presence": 0,
    }


def make_publish_root(root: Path) -> Path:
    root.mkdir()
    base = {
        "ticker": "BCTX",
        "fiscal_year": "2026",
        "fiscal_quarter": "Q1",
        "new_publish_date": "2025-12-11",
    }
    write_csv(root / "phase8a10a_publish_verified_frozen_apply_set.csv", [base])
    write_csv(
        root / "post_publish_original_17_retained_flags.csv",
        [
            {
                "ticker": "BCTX",
                "fiscal_year": "2026",
                "fiscal_quarter": "Q1",
                "retained_publish_tier": "R2_MARKET_AVAILABILITY_STALE_AFTER_PUBLISH_REPAIR",
            }
        ],
    )
    write_csv(
        root / "post_publish_apply_residuals.csv",
        [
            {
                "ticker": "BCTX",
                "fiscal_year": "2026",
                "fiscal_quarter": "Q1",
                "quarter_id": "10",
                "period_end_date": "2025-10-31",
                "publish_date": "2025-12-11",
                "market_availability_date": "2025-10-11",
            }
        ],
    )
    return root


def make_db(path: Path) -> Path:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY, market TEXT, ticker TEXT, company_name TEXT, active INTEGER);
        CREATE TABLE v3_quarter(
            quarter_id INTEGER PRIMARY KEY, company_id INTEGER, fiscal_year INTEGER, fiscal_quarter TEXT,
            period_end_date TEXT, publish_date TEXT, market_availability_date TEXT
        );
        CREATE TABLE v3_quarter_fundamentals(
            quarter_id INTEGER PRIMARY KEY, revenue REAL, ebit REAL, ebitda REAL, free_cashflow REAL,
            cash REAL, total_debt REAL, shares_outstanding REAL
        );
        CREATE TABLE v3_ttm(ttm_id INTEGER PRIMARY KEY, company_id INTEGER, endpoint_quarter_id INTEGER);
        CREATE TABLE v3_score(score_id INTEGER PRIMARY KEY, company_id INTEGER, as_of_quarter_id INTEGER, score_model_version TEXT, score_fingerprint TEXT);
        CREATE TABLE v3_lifecycle(lifecycle_id INTEGER PRIMARY KEY, company_id INTEGER, endpoint_quarter_id INTEGER, lifecycle_model_version TEXT, lifecycle_fingerprint TEXT);
        CREATE TABLE v3_valuation(valuation_id INTEGER PRIMARY KEY, company_id INTEGER, endpoint_quarter_id INTEGER);
        INSERT INTO v3_company VALUES (1,'usa','BCTX','BCTX',1),(2,'usa','OKCO','OKCO',1);
        INSERT INTO v3_quarter VALUES
          (10,1,2026,'Q1','2025-10-31','2025-12-11','2025-10-11'),
          (20,2,2025,'Q1','2025-03-31','2025-05-01',NULL),
          (21,2,2025,'Q2','2025-06-30','2025-08-01',NULL);
        INSERT INTO v3_quarter_fundamentals VALUES
          (10,1,1,1,1,1,1,1),(20,1,1,1,1,1,1,1),(21,1,1,1,1,1,1,1);
        INSERT INTO v3_ttm VALUES (1,2,21);
        INSERT INTO v3_score VALUES (1,2,21,'m','sf');
        INSERT INTO v3_lifecycle VALUES (1,2,21,'m','lf');
        INSERT INTO v3_valuation VALUES (1,2,21);
        """
    )
    conn.commit()
    conn.close()
    return path


def test_q1_to_q2_valid() -> None:
    data = [quarter("T", 2025, "Q1", "2025-03-31", "2025-05-01", 1), quarter("T", 2025, "Q2", "2025-06-30", "2025-08-01", 2)]
    fiscal, _summary = audit.fiscal_sequence_audit(data)
    assert fiscal[1]["sequence_status"] == "VALID"


def test_q2_to_q3_valid() -> None:
    data = [quarter("T", 2025, "Q2", "2025-06-30", "2025-08-01", 1), quarter("T", 2025, "Q3", "2025-09-30", "2025-11-01", 2)]
    assert audit.fiscal_sequence_audit(data)[0][1]["sequence_status"] == "VALID"


def test_q3_to_q4_valid() -> None:
    data = [quarter("T", 2025, "Q3", "2025-09-30", "2025-11-01", 1), quarter("T", 2025, "Q4", "2025-12-31", "2026-02-01", 2)]
    assert audit.fiscal_sequence_audit(data)[0][1]["sequence_status"] == "VALID"


def test_q4_to_next_fy_q1_valid() -> None:
    data = [quarter("T", 2025, "Q4", "2025-12-31", "2026-02-01", 1), quarter("T", 2026, "Q1", "2026-03-31", "2026-05-01", 2)]
    assert audit.fiscal_sequence_audit(data)[0][1]["sequence_status"] == "VALID"


def test_duplicate_fyq_detected_in_baseline(tmp_path: Path) -> None:
    db = make_db(tmp_path / "v3.db")
    with sqlite3.connect(db) as conn:
        conn.execute("INSERT INTO v3_quarter VALUES (22,2,2025,'Q2','2025-07-01','2025-08-02',NULL)")
        conn.row_factory = sqlite3.Row
        assert audit.baseline(conn, db)["duplicate_fy_fq"] == 1


def test_missing_quarter_classified_separately() -> None:
    data = [quarter("T", 2025, "Q1", "2025-03-31", "2025-05-01", 1), quarter("T", 2025, "Q3", "2025-09-30", "2025-11-01", 2)]
    fiscal, summary = audit.fiscal_sequence_audit(data)
    assert fiscal[1]["sequence_status"] == "MISSING_HISTORY"
    assert summary[0]["missing_quarter_observations"] == 1


def test_reversed_period_end_detected() -> None:
    data = [quarter("T", 2025, "Q1", "2025-06-30", "2025-08-01", 1), quarter("T", 2025, "Q2", "2025-03-31", "2025-05-01", 2)]
    assert audit.period_end_audit(data)[1]["period_gap_class"] == "REVERSE_OR_DUPLICATE"


def test_duplicate_period_end_detected() -> None:
    data = [quarter("T", 2025, "Q1", "2025-03-31", "2025-05-01", 1), quarter("T", 2025, "Q2", "2025-03-31", "2025-05-02", 2)]
    assert audit.period_end_audit(data)[0]["duplicate_period_end"] == 1


def test_52_53_week_valid_case_accepted() -> None:
    assert audit.period_gap_class(74, ticker="CRUS") == "VALID_52_53_WEEK"


def test_sparse_history_non_blocking() -> None:
    fiscal, _summary = audit.fiscal_sequence_audit([quarter("NEW", 2026, "Q3", "2026-09-30", "2026-11-01", 1)])
    assert fiscal[0]["sequence_status"] == "FIRST"


def test_one_year_shift_detected() -> None:
    assert audit.period_gap_class(365) == "ONE_YEAR_GAP"


def test_multi_quarter_shift_detected() -> None:
    data = [quarter("T", 2025, "Q1", "2025-03-31", "2025-05-01", 1), quarter("T", 2025, "Q4", "2025-12-31", "2026-02-01", 2)]
    assert audit.fiscal_sequence_audit(data)[1][0]["skipped_transitions"] == 1


def test_publish_before_period_end_detected() -> None:
    assert audit.reporting_lag_class(-10) == "NEGATIVE"


def test_same_day_brtx_multi_quarter_allowed() -> None:
    data = [quarter("BRTX", 2020, "Q2", "2020-06-30", "2021-04-12", 1), quarter("BRTX", 2020, "Q3", "2020-09-30", "2021-04-12", 2)]
    assert audit.publish_sequence_audit(data)[1]["valid_same_day_multi_quarter"] == 1


def test_ticker_name_change_does_not_create_identity_conflict() -> None:
    row = audit.publish_sequence_audit([quarter("KLRS", 2025, "Q1", "2025-03-31", "2025-05-01", 1)])[0]
    assert row["name_ticker_history_context"] == 1


def test_reporting_lag_computed_signed() -> None:
    assert audit.days_between("2025-03-31", "2025-03-01") == -30


def test_negative_lag_detected() -> None:
    assert audit.reporting_lag_class(-1) == "NEGATIVE"


def test_extreme_lag_not_automatically_p1() -> None:
    assert audit.reporting_lag_class(286) == "EXTREME"


def test_cross_signal_p1_logic() -> None:
    data = [quarter("T", 2025, "Q1", "2025-03-31", "2025-02-01", 1)]
    fiscal, _summary = audit.fiscal_sequence_audit(data)
    candidates = audit.cross_signal_candidates(fiscal, audit.period_end_audit(data), audit.publish_sequence_audit(data), audit.reporting_lag_audit(data), data)
    assert candidates[0]["severity"] == "P1"


def test_known_valid_special_qid_not_reintroduced_as_p1() -> None:
    data = [quarter("FNGR", 2024, "Q2", "2023-08-31", "2023-10-13", 37082)]
    fiscal, _summary = audit.fiscal_sequence_audit(data)
    candidates = audit.cross_signal_candidates(fiscal, audit.period_end_audit(data), audit.publish_sequence_audit(data), audit.reporting_lag_audit(data), data)
    assert candidates == []


def test_publish_residual_reconciliation_deterministic(tmp_path: Path) -> None:
    db = make_db(tmp_path / "v3.db")
    pubroot = make_publish_root(tmp_path / "pub")
    with audit.connect_ro(db) as conn:
        rows, summary = audit.publish_residual_reconciliation(conn, pubroot)
    assert summary["current_true_publish_R1"] == 0
    assert summary["current_true_publish_R2"] == 1
    assert rows[0]["actual_publish_date_still_correct"] == "YES"


def test_no_production_writes(tmp_path: Path) -> None:
    db = make_db(tmp_path / "v3.db")
    raw = tmp_path / "raw.db"
    raw.write_bytes(b"raw")
    summary = audit.run_phase8a10b_full_sequence_audit(audit.Phase8A10BPaths(tmp_path / "art", db, raw, make_publish_root(tmp_path / "pub")))
    assert summary["safety"]["production_writes"] == 0


def test_no_derived_writes(tmp_path: Path) -> None:
    db = make_db(tmp_path / "v3.db")
    raw = tmp_path / "raw.db"
    raw.write_bytes(b"raw")
    safety = audit.run_phase8a10b_full_sequence_audit(audit.Phase8A10BPaths(tmp_path / "art", db, raw, make_publish_root(tmp_path / "pub")))["safety"]
    assert safety["ttm_writes"] == safety["score_writes"] == safety["lifecycle_writes"] == safety["valuation_writes"] == 0


def test_no_rawcandle_writes(tmp_path: Path) -> None:
    db = make_db(tmp_path / "v3.db")
    raw = tmp_path / "raw.db"
    raw.write_bytes(b"raw")
    summary = audit.run_phase8a10b_full_sequence_audit(audit.Phase8A10BPaths(tmp_path / "art", db, raw, make_publish_root(tmp_path / "pub")))
    assert summary["safety"]["rawcandle_writes"] == 0
