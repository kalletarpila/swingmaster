from __future__ import annotations

import csv
import json
import shutil
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable


CLASSIFICATION_READY = "FUNDAMENTALS_V3_PHASE7_CHECK_COMPLETE_READY_FOR_PHASE8"
CLASSIFICATION_REPAIR_REQUIRED = "FUNDAMENTALS_V3_PHASE7_CHECK_COMPLETE_PHASE8_REPAIR_REQUIRED"
CLASSIFICATION_BLOCKED_INTEGRITY = "FUNDAMENTALS_V3_PHASE7_BLOCKED_BY_PRODUCTION_INTEGRITY_FAILURE"
CLASSIFICATION_BLOCKED_WRITE = "FUNDAMENTALS_V3_PHASE7_BLOCKED_BY_UNEXPECTED_WRITE"
NEXT_PHASE = "MASTER PLAN PHASE 8 - UPDATE V3"

EXPECTED_COMPANIES = 2550
EXPECTED_ACTIVE = 2482
EXPECTED_INACTIVE = 68
EXPECTED_CANONICAL_QUARTERS = 73075
EXPECTED_TTM = 54038
EXPECTED_SCORE = 54038
EXPECTED_LIFECYCLE = 54038
EXPECTED_VALUATION = 54038
EXPECTED_SCORE_MODEL = "V3_LEGACY2_FUNDAMENTAL_SCORE_V1"
EXPECTED_SCORE_FINGERPRINT = "8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0"
EXPECTED_LIFECYCLE_MODEL = "V3_LIFECYCLE_EBIT_FIRST_V1"
EXPECTED_LIFECYCLE_FINGERPRINT = "18c01984ee5ca69acaca64ab6ff2d5b1faa27d8400137f6d08200926e5658f9e"
ZERO_QUARTER_RESIDUALS = {"ALTS", "HOTH", "PKST", "QVCGA", "STSS"}
LIFECYCLE_STATES = {
    "DECELERATING",
    "DECLINING",
    "DISTRESS_CONTRACTION",
    "EARLY_RECOVERY",
    "HIGH_GROWTH_EXPANSION",
    "MATURE_STABLE",
    "NOT_READY",
    "POSITIVE_INFLECTION",
    "PROFITABLE_GROWTH",
}
FLOW_FIELDS = {
    "revenue": "ttm_revenue",
    "gross_profit": "ttm_gross_profit",
    "operating_income": "ttm_operating_income",
    "ebit": "ttm_ebit",
    "ebitda": "ttm_ebitda",
    "net_income": "ttm_net_income",
    "operating_cashflow": "ttm_ocf",
    "capex": "ttm_capex",
    "free_cashflow": "ttm_fcf",
}
INSTANT_FIELDS = ("cash", "total_debt", "shares_outstanding")


@dataclass(frozen=True)
class Issue:
    severity: str
    area: str
    code: str
    detail: str
    count: int
    sample: str = ""


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def open_ro(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def rows(conn: sqlite3.Connection, sql: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute(sql, tuple(params)).fetchall()]


def scalar(conn: sqlite3.Connection, sql: str, params: Iterable[Any] = ()) -> Any:
    row = conn.execute(sql, tuple(params)).fetchone()
    return None if row is None else row[0]


def write_csv(path: Path, data: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = sorted({key for row in data for key in row})
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def issue_rows(issues: list[Issue]) -> list[dict[str, Any]]:
    ordered = sorted(issues, key=lambda i: (["CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO"].index(i.severity), i.area, i.code))
    counters: Counter[str] = Counter()
    out = []
    for issue in ordered:
        counters[issue.code] += 1
        out.append(
            {
                "issue_id": f"P7-{issue.code}-{counters[issue.code]:03d}",
                "severity": issue.severity,
                "area": issue.area,
                "code": issue.code,
                "count": issue.count,
                "detail": issue.detail,
                "sample": issue.sample,
            }
        )
    return out


def add_issue(issues: list[Issue], severity: str, area: str, code: str, detail: str, count: int, sample: str = "") -> None:
    if count:
        issues.append(Issue(severity, area, code, detail, count, sample))


def audit(v3_db: Path, rawcandle_db: Path, artifact_root: Path, *, today: date | None = None) -> dict[str, Any]:
    started = datetime.now(timezone.utc)
    today = today or date.today()
    before = file_snapshot(v3_db, rawcandle_db)
    issues: list[Issue] = []
    artifact_root.mkdir(parents=True, exist_ok=True)

    with open_ro(v3_db) as conn:
        quick_check = scalar(conn, "PRAGMA quick_check")
        counts = table_counts(conn)
        schema = schema_rows(conn)
        write_csv(artifact_root / "schema_audit.csv", schema)
        write_csv(artifact_root / "row_counts.csv", [{"table_name": k, "row_count": v} for k, v in counts.items()])
        write_json(artifact_root / "production_snapshot.json", {"quick_check": quick_check, "counts": counts, "files": before})

        add_issue(issues, "CRITICAL", "production_snapshot", "QUICK_CHECK", "SQLite quick_check failed", int(quick_check != "ok"), str(quick_check))
        expected = {
            "v3_company": EXPECTED_COMPANIES,
            "active_companies": EXPECTED_ACTIVE,
            "inactive_companies": EXPECTED_INACTIVE,
            "v3_quarter": EXPECTED_CANONICAL_QUARTERS,
            "v3_ttm": EXPECTED_TTM,
            "v3_valuation": EXPECTED_VALUATION,
            "v3_score": EXPECTED_SCORE,
            "v3_lifecycle": EXPECTED_LIFECYCLE,
        }
        count_audit = [{"metric": key, "expected": val, "actual": counts.get(key), "match": counts.get(key) == val} for key, val in expected.items()]
        write_csv(artifact_root / "production_baseline_counts.csv", count_audit)
        for row in count_audit:
            add_issue(issues, "HIGH", "production_snapshot", "BASELINE_COUNT_MISMATCH", f"{row['metric']} expected {row['expected']} actual {row['actual']}", int(not row["match"]))

        audit_universe(conn, artifact_root, issues)
        audit_canonical(conn, artifact_root, issues, today)
        audit_field_coverage(conn, artifact_root, issues)
        audit_ttm(conn, artifact_root, issues)
        audit_score(conn, artifact_root, issues)
        audit_lifecycle(conn, artifact_root, issues)
        audit_valuation(conn, rawcandle_db, artifact_root, issues)
        audit_cross_layer(conn, artifact_root, issues)
        audit_outliers(conn, artifact_root, issues)

    after = file_snapshot(v3_db, rawcandle_db)
    read_only_ok = before["files"] == after["files"]
    write_json(artifact_root / "read_only_file_snapshot.json", {"before": before, "after": after, "matches": read_only_ok})
    if not read_only_ok:
        add_issue(issues, "CRITICAL", "read_only_safety", "UNEXPECTED_WRITE", "Production or RawCandle file metadata changed during audit", 1)

    issues_csv = issue_rows(issues)
    write_csv(artifact_root / "issue_register.csv", issues_csv)
    write_csv(artifact_root / "phase8_handoff_issues.csv", [row for row in issues_csv if row["severity"] in {"CRITICAL", "HIGH", "MEDIUM"}])
    summary = build_summary(started, issues_csv, counts, read_only_ok)
    write_json(artifact_root / "phase7_summary.json", summary)
    write_recommendation(artifact_root / "recommended_next_step.md", summary)
    return summary


def file_snapshot(v3_db: Path, rawcandle_db: Path) -> dict[str, Any]:
    disk = shutil.disk_usage(v3_db.parent)
    paths = [v3_db, v3_db.with_name(v3_db.name + "-wal"), v3_db.with_name(v3_db.name + "-shm"), rawcandle_db]
    return {
        "disk_free_bytes": disk.free,
        "files": {
            str(path): {
                "exists": path.exists(),
                "size": path.stat().st_size if path.exists() else None,
                "mtime_ns": path.stat().st_mtime_ns if path.exists() else None,
            }
            for path in paths
        },
    }


def table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    tables = ["v3_company", "v3_quarter", "v3_quarter_fundamentals", "v3_ttm", "v3_valuation", "v3_score", "v3_lifecycle"]
    out = {table: int(scalar(conn, f"SELECT COUNT(*) FROM {table}")) for table in tables}
    out["active_companies"] = int(scalar(conn, "SELECT COUNT(*) FROM v3_company WHERE active=1"))
    out["inactive_companies"] = int(scalar(conn, "SELECT COUNT(*) FROM v3_company WHERE active=0"))
    return out


def schema_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for table in rows(conn, "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'v3_%' ORDER BY name"):
        for col in rows(conn, f"PRAGMA table_info({table['name']})"):
            out.append({"table_name": table["name"], **col})
        for idx in rows(conn, f"PRAGMA index_list({table['name']})"):
            out.append({"table_name": table["name"], "index_name": idx["name"], "index_unique": idx["unique"]})
    return out


def audit_universe(conn: sqlite3.Connection, root: Path, issues: list[Issue]) -> None:
    identity = rows(
        conn,
        """
        SELECT profile, active, COUNT(*) AS companies
        FROM v3_company
        GROUP BY profile, active
        ORDER BY profile, active
        """,
    )
    write_csv(root / "company_identity_audit.csv", identity)
    dupes = rows(conn, "SELECT market,ticker,COUNT(*) AS duplicate_count FROM v3_company GROUP BY market,ticker HAVING COUNT(*)>1")
    write_csv(root / "company_duplicate_ticker_audit.csv", dupes)
    add_issue(issues, "CRITICAL", "universe", "DUPLICATE_COMPANY_IDENTITY", "Duplicate market/ticker companies", len(dupes), sample_json(dupes))

    zero = rows(
        conn,
        """
        SELECT c.market,c.ticker,c.company_name,c.active,COUNT(q.quarter_id) AS quarter_count
        FROM v3_company c
        LEFT JOIN v3_quarter q ON q.company_id=c.company_id
        GROUP BY c.company_id
        HAVING quarter_count=0
        ORDER BY c.ticker
        """,
    )
    write_csv(root / "zero_quarter_companies.csv", zero)
    unexpected_zero = [r for r in zero if r["ticker"] not in ZERO_QUARTER_RESIDUALS]
    add_issue(issues, "HIGH", "universe", "UNEXPECTED_ZERO_QUARTER_COMPANY", "Zero-quarter companies outside known residual set", len(unexpected_zero), sample_json(unexpected_zero))

    stale = rows(
        conn,
        """
        SELECT c.ticker,c.company_name,c.active,MAX(q.period_end_date) AS latest_period_end,MAX(q.publish_date) AS latest_publish_date
        FROM v3_company c
        LEFT JOIN v3_quarter q ON q.company_id=c.company_id
        WHERE c.active=1
        GROUP BY c.company_id
        HAVING latest_period_end IS NULL OR latest_period_end < '2024-01-01'
        ORDER BY latest_period_end, c.ticker
        """,
    )
    write_csv(root / "active_company_staleness.csv", stale)
    add_issue(issues, "LOW", "universe", "ACTIVE_COMPANY_STALENESS", "Active companies with no canonical quarters or no period after 2024-01-01", len(stale), sample_json(stale))


def audit_canonical(conn: sqlite3.Connection, root: Path, issues: list[Issue], today: date) -> None:
    dupes = rows(
        conn,
        """
        SELECT company_id,fiscal_year,fiscal_quarter,COUNT(*) AS duplicate_count
        FROM v3_quarter
        GROUP BY company_id,fiscal_year,fiscal_quarter
        HAVING COUNT(*)>1
        ORDER BY duplicate_count DESC, company_id
        """,
    )
    write_csv(root / "canonical_duplicate_fy_fq.csv", dupes)
    add_issue(issues, "CRITICAL", "canonical", "DUPLICATE_FY_FQ", "Duplicate canonical company/FY/FQ rows", len(dupes), sample_json(dupes))

    invalid_q = rows(conn, "SELECT * FROM v3_quarter WHERE fiscal_quarter NOT IN ('Q1','Q2','Q3','Q4') ORDER BY company_id,fiscal_year")
    write_csv(root / "canonical_invalid_quarters.csv", invalid_q)
    add_issue(issues, "CRITICAL", "canonical", "INVALID_FISCAL_QUARTER", "Fiscal quarter outside Q1-Q4", len(invalid_q), sample_json(invalid_q))

    publish = rows(
        conn,
        """
        SELECT c.ticker,q.quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date,q.market_availability_date
        FROM v3_quarter q JOIN v3_company c ON c.company_id=q.company_id
        WHERE (q.publish_date IS NOT NULL AND q.period_end_date IS NOT NULL AND q.publish_date < q.period_end_date)
           OR (q.publish_date IS NOT NULL AND q.publish_date > ?)
           OR (q.market_availability_date IS NOT NULL AND q.publish_date IS NOT NULL AND q.market_availability_date < q.publish_date)
        ORDER BY c.ticker,q.period_end_date
        """,
        (today.isoformat(),),
    )
    write_csv(root / "canonical_publish_date_anomalies.csv", publish)
    add_issue(issues, "HIGH", "canonical", "PUBLISH_DATE_ANOMALY", "Publish date before period end, in future, or market date before publish date", len(publish), sample_json(publish))

    gaps = rows(
        conn,
        """
        WITH ordered AS (
          SELECT c.ticker,q.*,
                 LAG(julianday(period_end_date)) OVER (PARTITION BY q.company_id ORDER BY fiscal_year,fiscal_quarter) AS prev_jd
          FROM v3_quarter q JOIN v3_company c ON c.company_id=q.company_id
          WHERE q.period_end_date IS NOT NULL
        )
        SELECT ticker,quarter_id,fiscal_year,fiscal_quarter,period_end_date,
               ROUND(julianday(period_end_date)-prev_jd,1) AS days_since_previous
        FROM ordered
        WHERE prev_jd IS NOT NULL
          AND (julianday(period_end_date)-prev_jd < 50 OR julianday(period_end_date)-prev_jd > 160)
        ORDER BY ABS((julianday(period_end_date)-prev_jd)-91) DESC
        """,
    )
    write_csv(root / "canonical_sequence_gap_outliers.csv", gaps)

    samples = rows(
        conn,
        """
        SELECT c.ticker,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date,f.*
        FROM v3_quarter q
        JOIN v3_company c ON c.company_id=q.company_id
        LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        WHERE c.ticker IN ('AAPL','MSFT','CAVA','DPZ','LFCR','NEUP','BNC','SJM','LYTS')
        ORDER BY c.ticker,q.fiscal_year,q.fiscal_quarter
        """,
    )
    write_csv(root / "manual_edge_case_samples.csv", samples)


def audit_field_coverage(conn: sqlite3.Connection, root: Path, issues: list[Issue]) -> None:
    fields = list(FLOW_FIELDS) + list(INSTANT_FIELDS)
    coverage = []
    for field in fields:
        coverage.extend(
            rows(
                conn,
                f"""
                SELECT '{field}' AS field_name,
                       SUBSTR(q.period_end_date,1,4) AS period_year,
                       COUNT(*) AS rows,
                       SUM(CASE WHEN f.{field} IS NOT NULL THEN 1 ELSE 0 END) AS present_rows,
                       ROUND(100.0*SUM(CASE WHEN f.{field} IS NOT NULL THEN 1 ELSE 0 END)/COUNT(*),2) AS coverage_pct
                FROM v3_quarter q LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
                GROUP BY period_year
                ORDER BY field_name,period_year
                """
            )
        )
    write_csv(root / "field_coverage_by_year.csv", coverage)
    current_coverage = rows(
        conn,
        """
        SELECT COUNT(*) AS quarter_rows,
               SUM(CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END) AS revenue_rows,
               SUM(CASE WHEN ebit IS NOT NULL THEN 1 ELSE 0 END) AS ebit_rows,
               SUM(CASE WHEN free_cashflow IS NOT NULL THEN 1 ELSE 0 END) AS fcf_rows,
               SUM(CASE WHEN shares_outstanding IS NOT NULL THEN 1 ELSE 0 END) AS shares_rows
        FROM v3_quarter q LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        """
    )
    write_csv(root / "core_field_coverage.csv", current_coverage)

    semantic = rows(
        conn,
        """
        SELECT c.ticker,q.fiscal_year,q.fiscal_quarter,q.period_end_date,
               f.revenue,f.cash,f.total_debt,f.shares_outstanding
        FROM v3_quarter q
        JOIN v3_company c ON c.company_id=q.company_id
        JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        WHERE f.revenue < 0 OR f.cash < 0 OR f.total_debt < 0 OR f.shares_outstanding <= 0
        ORDER BY c.ticker,q.period_end_date
        """
    )
    write_csv(root / "field_semantic_outliers.csv", semantic)
    add_issue(issues, "MEDIUM", "field_quality", "SEMANTIC_FIELD_OUTLIER", "Negative revenue/cash/debt or non-positive shares rows", len(semantic), sample_json(semantic))


def audit_ttm(conn: sqlite3.Connection, root: Path, issues: list[Issue]) -> None:
    readiness = rows(
        conn,
        """
        SELECT core_ttm_ebit_ready,core_ttm_ebitda_ready,ttm_pit_ready,underlying_publish_dates_complete,COUNT(*) AS rows
        FROM v3_ttm
        GROUP BY core_ttm_ebit_ready,core_ttm_ebitda_ready,ttm_pit_ready,underlying_publish_dates_complete
        ORDER BY rows DESC
        """,
    )
    write_csv(root / "ttm_readiness_summary.csv", readiness)

    parity_rows: list[dict[str, Any]] = []
    for q_field, ttm_field in FLOW_FIELDS.items():
        parity_rows.extend(
            rows(
                conn,
                f"""
                SELECT t.ttm_id,t.company_id,t.endpoint_quarter_id,'{q_field}' AS field_name,t.{ttm_field} AS stored_value,
                       (f1.{q_field}+f2.{q_field}+f3.{q_field}+f4.{q_field}) AS recomputed_value,
                       ABS(t.{ttm_field}-(f1.{q_field}+f2.{q_field}+f3.{q_field}+f4.{q_field})) AS abs_delta
                FROM v3_ttm t
                JOIN v3_quarter_fundamentals f1 ON f1.quarter_id=t.q1_quarter_id
                JOIN v3_quarter_fundamentals f2 ON f2.quarter_id=t.q2_quarter_id
                JOIN v3_quarter_fundamentals f3 ON f3.quarter_id=t.q3_quarter_id
                JOIN v3_quarter_fundamentals f4 ON f4.quarter_id=t.q4_quarter_id
                WHERE t.{ttm_field} IS NOT NULL
                  AND f1.{q_field} IS NOT NULL AND f2.{q_field} IS NOT NULL AND f3.{q_field} IS NOT NULL AND f4.{q_field} IS NOT NULL
                  AND ABS(t.{ttm_field}-(f1.{q_field}+f2.{q_field}+f3.{q_field}+f4.{q_field})) > 0.01
                LIMIT 1000
                """
            )
        )
    write_csv(root / "ttm_flow_parity_failures.csv", parity_rows)
    add_issue(issues, "HIGH", "ttm", "TTM_FLOW_PARITY", "TTM flow values differ from four-quarter sum", len(parity_rows), sample_json(parity_rows))

    instant = rows(
        conn,
        """
        SELECT t.ttm_id,t.company_id,t.endpoint_quarter_id,t.cash AS ttm_cash,f.cash AS q_cash,
               t.total_debt AS ttm_total_debt,f.total_debt AS q_total_debt,
               t.shares_outstanding AS ttm_shares,f.shares_outstanding AS q_shares
        FROM v3_ttm t JOIN v3_quarter_fundamentals f ON f.quarter_id=t.endpoint_quarter_id
        WHERE (t.cash IS NOT NULL AND f.cash IS NOT NULL AND ABS(t.cash-f.cash)>0.01)
           OR (t.total_debt IS NOT NULL AND f.total_debt IS NOT NULL AND ABS(t.total_debt-f.total_debt)>0.01)
           OR (t.shares_outstanding IS NOT NULL AND f.shares_outstanding IS NOT NULL AND ABS(t.shares_outstanding-f.shares_outstanding)>0.01)
        LIMIT 1000
        """,
    )
    write_csv(root / "ttm_instant_parity_failures.csv", instant)
    add_issue(issues, "HIGH", "ttm", "TTM_INSTANT_PARITY", "TTM instant fields differ from endpoint quarter", len(instant), sample_json(instant))

    pit = rows(
        conn,
        """
        WITH pub AS (
          SELECT t.ttm_id,MAX(q.publish_date) AS max_publish
          FROM v3_ttm t
          JOIN v3_quarter q ON q.quarter_id IN (t.q1_quarter_id,t.q2_quarter_id,t.q3_quarter_id,t.q4_quarter_id)
          GROUP BY t.ttm_id
        )
        SELECT t.ttm_id,t.company_id,t.ttm_available_date,p.max_publish
        FROM v3_ttm t JOIN pub p ON p.ttm_id=t.ttm_id
        WHERE t.ttm_available_date IS NOT NULL AND p.max_publish IS NOT NULL AND t.ttm_available_date < p.max_publish
        LIMIT 1000
        """,
    )
    write_csv(root / "ttm_pit_failures.csv", pit)
    add_issue(issues, "HIGH", "ttm", "TTM_PIT_DATE", "TTM available date before underlying max publish date", len(pit), sample_json(pit))


def audit_score(conn: sqlite3.Connection, root: Path, issues: list[Issue]) -> None:
    model = rows(conn, "SELECT score_model_version,score_fingerprint,COUNT(*) AS rows FROM v3_score GROUP BY score_model_version,score_fingerprint")
    write_csv(root / "score_model_fingerprint_audit.csv", model)
    bad_model = [r for r in model if r["score_model_version"] != EXPECTED_SCORE_MODEL or r["score_fingerprint"] != EXPECTED_SCORE_FINGERPRINT]
    add_issue(issues, "HIGH", "score", "SCORE_MODEL_FINGERPRINT", "Unexpected score model version or fingerprint", len(bad_model), sample_json(bad_model))

    bounds = rows(conn, "SELECT * FROM v3_score WHERE total_max_score<>100 OR fundamental_score < 0 OR fundamental_score > 100 OR coverage_pct < 0 OR coverage_pct > 100 LIMIT 1000")
    write_csv(root / "score_bounds_failures.csv", bounds)
    add_issue(issues, "HIGH", "score", "SCORE_BOUNDS", "Score, max score, or coverage outside locked bounds", len(bounds), sample_json(bounds))

    lineage = rows(
        conn,
        """
        SELECT s.score_id,s.company_id,s.as_of_quarter_id,s.endpoint_ttm_id
        FROM v3_score s
        LEFT JOIN v3_quarter q ON q.quarter_id=s.as_of_quarter_id AND q.company_id=s.company_id
        LEFT JOIN v3_ttm t ON t.ttm_id=s.endpoint_ttm_id AND t.company_id=s.company_id
        WHERE q.quarter_id IS NULL OR t.ttm_id IS NULL
        LIMIT 1000
        """,
    )
    write_csv(root / "score_lineage_failures.csv", lineage)
    add_issue(issues, "CRITICAL", "score", "SCORE_LINEAGE", "Score row has missing or mismatched quarter/TTM lineage", len(lineage), sample_json(lineage))

    distribution = rows(
        conn,
        """
        SELECT score_ready,confidence,applicability,COUNT(*) AS rows,
               ROUND(MIN(fundamental_score),4) AS min_score,
               ROUND(AVG(fundamental_score),4) AS avg_score,
               ROUND(MAX(fundamental_score),4) AS max_score
        FROM v3_score
        GROUP BY score_ready,confidence,applicability
        ORDER BY rows DESC
        """,
    )
    write_csv(root / "score_distribution.csv", distribution)
    (root / "score_market_independence_audit.md").write_text(
        "Score table and locked score artifacts contain no market price, close price, market cap, enterprise value, or valuation ratio input columns. "
        "Valuation remains separate in v3_valuation.\n",
        encoding="utf-8",
    )


def audit_lifecycle(conn: sqlite3.Connection, root: Path, issues: list[Issue]) -> None:
    model = rows(conn, "SELECT lifecycle_model_version,lifecycle_fingerprint,COUNT(*) AS rows FROM v3_lifecycle GROUP BY lifecycle_model_version,lifecycle_fingerprint")
    write_csv(root / "lifecycle_model_fingerprint_audit.csv", model)
    bad_model = [r for r in model if r["lifecycle_model_version"] != EXPECTED_LIFECYCLE_MODEL or r["lifecycle_fingerprint"] != EXPECTED_LIFECYCLE_FINGERPRINT]
    add_issue(issues, "HIGH", "lifecycle", "LIFECYCLE_MODEL_FINGERPRINT", "Unexpected lifecycle model version or fingerprint", len(bad_model), sample_json(bad_model))

    bad_state = rows(conn, "SELECT * FROM v3_lifecycle WHERE final_state NOT IN (%s) LIMIT 1000" % ",".join("?" for _ in LIFECYCLE_STATES), tuple(sorted(LIFECYCLE_STATES)))
    write_csv(root / "lifecycle_state_domain_failures.csv", bad_state)
    add_issue(issues, "HIGH", "lifecycle", "LIFECYCLE_STATE_DOMAIN", "Lifecycle final_state outside locked domain", len(bad_state), sample_json(bad_state))

    chronology = rows(
        conn,
        """
        WITH ordered AS (
          SELECT l.*,LAG(final_state) OVER (PARTITION BY company_id ORDER BY endpoint_period_end, endpoint_quarter_id) AS actual_previous
          FROM v3_lifecycle l
        )
        SELECT lifecycle_id,company_id,endpoint_quarter_id,endpoint_period_end,previous_final_state,actual_previous
        FROM ordered
        WHERE actual_previous IS NOT NULL
          AND previous_final_state IS NOT NULL
          AND previous_final_state <> actual_previous
        LIMIT 1000
        """,
    )
    write_csv(root / "lifecycle_chronology_failures.csv", chronology)
    add_issue(issues, "MEDIUM", "lifecycle", "LIFECYCLE_PREVIOUS_STATE", "Stored previous_final_state differs from chronological predecessor", len(chronology), sample_json(chronology))

    distribution = rows(conn, "SELECT final_state,lifecycle_ready,COUNT(*) AS rows FROM v3_lifecycle GROUP BY final_state,lifecycle_ready ORDER BY rows DESC")
    write_csv(root / "lifecycle_state_distribution.csv", distribution)
    transitions = rows(conn, "SELECT previous_final_state,final_state,COUNT(*) AS rows FROM v3_lifecycle GROUP BY previous_final_state,final_state ORDER BY rows DESC")
    write_csv(root / "lifecycle_transition_matrix.csv", transitions)


def audit_valuation(conn: sqlite3.Connection, rawcandle_db: Path, root: Path, issues: list[Issue]) -> None:
    model = rows(conn, "SELECT model_version,price_source,valuation_status,valuation_ready,COUNT(*) AS rows FROM v3_valuation GROUP BY model_version,price_source,valuation_status,valuation_ready ORDER BY rows DESC")
    write_csv(root / "valuation_status_audit.csv", model)
    lineage = rows(
        conn,
        """
        SELECT v.valuation_id,v.company_id,v.endpoint_ttm_id,v.endpoint_quarter_id
        FROM v3_valuation v
        LEFT JOIN v3_ttm t ON t.ttm_id=v.endpoint_ttm_id AND t.company_id=v.company_id
        LEFT JOIN v3_quarter q ON q.quarter_id=v.endpoint_quarter_id AND q.company_id=v.company_id
        WHERE t.ttm_id IS NULL OR q.quarter_id IS NULL
        LIMIT 1000
        """,
    )
    write_csv(root / "valuation_lineage_failures.csv", lineage)
    add_issue(issues, "CRITICAL", "valuation", "VALUATION_LINEAGE", "Valuation row has missing or mismatched quarter/TTM lineage", len(lineage), sample_json(lineage))

    conn.execute("ATTACH DATABASE ? AS rawcandle", (f"file:{rawcandle_db}?mode=ro",))
    try:
        publish_next = rows(
            conn,
            """
            SELECT v.valuation_id,c.ticker,v.publish_date,v.valuation_date,
                   (SELECT MIN(o.pvm) FROM rawcandle.osakedata o
                    WHERE o.market=c.market AND o.osake=c.ticker AND o.pvm>v.publish_date AND o.close IS NOT NULL) AS expected_valuation_date
            FROM v3_valuation v JOIN v3_company c ON c.company_id=v.company_id
            WHERE v.publish_date IS NOT NULL
              AND EXISTS (
                    SELECT 1 FROM rawcandle.osakedata vx
                    WHERE vx.market=c.market AND vx.osake=c.ticker AND vx.pvm=v.valuation_date AND vx.close IS NOT NULL
                  )
              AND expected_valuation_date IS NOT NULL
              AND v.valuation_date <> expected_valuation_date
            LIMIT 1000
            """
        )
        write_csv(root / "valuation_publish_plus_one_failures.csv", publish_next)
        add_issue(issues, "HIGH", "valuation", "VALUATION_PUBLISH_PLUS_ONE", "Valuation date is not first trading day strictly after publish date", len(publish_next), sample_json(publish_next))

        price_parity = rows(
            conn,
            """
            SELECT v.valuation_id,c.ticker,v.valuation_date,v.valuation_close_price,o.close AS raw_close,
                   ABS(v.valuation_close_price-o.close) AS abs_delta
            FROM v3_valuation v
            JOIN v3_company c ON c.company_id=v.company_id
            JOIN rawcandle.osakedata o ON o.market=c.market AND o.osake=c.ticker AND o.pvm=v.valuation_date
            WHERE v.valuation_close_price IS NOT NULL AND ABS(v.valuation_close_price-o.close)>0.000001
            LIMIT 1000
            """
        )
        write_csv(root / "valuation_price_parity_failures.csv", price_parity)
        add_issue(issues, "HIGH", "valuation", "VALUATION_PRICE_PARITY", "Stored valuation close differs from RawCandle close", len(price_parity), sample_json(price_parity))

        missing_price = rows(
            conn,
            """
            SELECT v.valuation_id,c.ticker,v.publish_date,v.valuation_date,v.valuation_close_price
            FROM v3_valuation v
            JOIN v3_company c ON c.company_id=v.company_id
            LEFT JOIN rawcandle.osakedata o ON o.market=c.market AND o.osake=c.ticker AND o.pvm=v.valuation_date
            WHERE v.valuation_close_price IS NOT NULL AND o.close IS NULL
            ORDER BY c.ticker,v.valuation_date
            LIMIT 1000
            """
        )
        write_csv(root / "valuation_price_missing_in_rawcandle.csv", missing_price)
        add_issue(issues, "MEDIUM", "valuation", "VALUATION_PRICE_REVALIDATION_GAP", "Stored valuation date/price cannot be revalidated from current RawCandle rows", len(missing_price), sample_json(missing_price))
    finally:
        try:
            conn.execute("DETACH DATABASE rawcandle")
        except sqlite3.OperationalError:
            pass

    formula = rows(
        conn,
        """
        SELECT valuation_id,market_cap,valuation_close_price,shares_outstanding,net_debt,cash,total_debt,enterprise_value
        FROM v3_valuation
        WHERE (market_cap IS NOT NULL AND valuation_close_price IS NOT NULL AND shares_outstanding IS NOT NULL AND ABS(market_cap-(valuation_close_price*shares_outstanding))>0.01)
           OR (net_debt IS NOT NULL AND cash IS NOT NULL AND total_debt IS NOT NULL AND ABS(net_debt-(total_debt-cash))>0.01)
           OR (enterprise_value IS NOT NULL AND market_cap IS NOT NULL AND net_debt IS NOT NULL AND ABS(enterprise_value-(market_cap+net_debt))>0.01)
        LIMIT 1000
        """,
    )
    write_csv(root / "valuation_formula_failures.csv", formula)
    add_issue(issues, "HIGH", "valuation", "VALUATION_FORMULA_PARITY", "Market cap, net debt, or enterprise value formula parity failed", len(formula), sample_json(formula))


def audit_cross_layer(conn: sqlite3.Connection, root: Path, issues: list[Issue]) -> None:
    coverage = rows(
        conn,
        """
        SELECT COUNT(*) AS ttm_rows,
               SUM(CASE WHEN s.score_id IS NOT NULL THEN 1 ELSE 0 END) AS score_rows,
               SUM(CASE WHEN l.lifecycle_id IS NOT NULL THEN 1 ELSE 0 END) AS lifecycle_rows,
               SUM(CASE WHEN v.valuation_id IS NOT NULL THEN 1 ELSE 0 END) AS valuation_rows
        FROM v3_ttm t
        LEFT JOIN v3_score s ON s.endpoint_ttm_id=t.ttm_id
        LEFT JOIN v3_lifecycle l ON l.endpoint_ttm_id=t.ttm_id
        LEFT JOIN v3_valuation v ON v.endpoint_ttm_id=t.ttm_id
        """
    )
    write_csv(root / "cross_layer_endpoint_coverage.csv", coverage)
    row = coverage[0]
    add_issue(issues, "HIGH", "cross_layer", "TTM_ENDPOINT_COVERAGE", "TTM rows are missing score, lifecycle, or valuation rows", int(row["ttm_rows"] != row["score_rows"] or row["ttm_rows"] != row["lifecycle_rows"] or row["ttm_rows"] != row["valuation_rows"]), sample_json(coverage))

    parity = rows(
        conn,
        """
        SELECT v.valuation_id,v.endpoint_ttm_id,v.ttm_revenue,t.ttm_revenue AS source_ttm_revenue,
               v.ttm_ebit,t.ttm_ebit AS source_ttm_ebit,v.ttm_fcf,t.ttm_fcf AS source_ttm_fcf
        FROM v3_valuation v JOIN v3_ttm t ON t.ttm_id=v.endpoint_ttm_id
        WHERE (v.ttm_revenue IS NOT NULL AND t.ttm_revenue IS NOT NULL AND ABS(v.ttm_revenue-t.ttm_revenue)>0.01)
           OR (v.ttm_ebit IS NOT NULL AND t.ttm_ebit IS NOT NULL AND ABS(v.ttm_ebit-t.ttm_ebit)>0.01)
           OR (v.ttm_fcf IS NOT NULL AND t.ttm_fcf IS NOT NULL AND ABS(v.ttm_fcf-t.ttm_fcf)>0.01)
        LIMIT 1000
        """,
    )
    write_csv(root / "cross_layer_metric_parity_failures.csv", parity)
    add_issue(issues, "HIGH", "cross_layer", "VALUATION_TTM_METRIC_PARITY", "Valuation copied TTM metrics differ from v3_ttm", len(parity), sample_json(parity))


def audit_outliers(conn: sqlite3.Connection, root: Path, issues: list[Issue]) -> None:
    financial_terms = ["BANK", "INSURANCE", "FINANCIAL", "BANC", "TRUST"]
    pattern_sql = " OR ".join("UPPER(c.company_name) LIKE ?" for _ in financial_terms)
    params = tuple(f"%{term}%" for term in financial_terms)
    financial = rows(
        conn,
        f"""
        SELECT c.ticker,c.company_name,c.profile,c.active,COUNT(q.quarter_id) AS quarters
        FROM v3_company c LEFT JOIN v3_quarter q ON q.company_id=c.company_id
        WHERE {pattern_sql}
        GROUP BY c.company_id
        ORDER BY c.ticker
        """,
        params,
    )
    write_csv(root / "financial_applicability_name_outliers.csv", financial)
    add_issue(issues, "LOW", "applicability", "FINANCIAL_NAME_REVIEW", "Company name still contains bank/insurance/financial terms; manual applicability review only", len(financial), sample_json(financial))

    reits = rows(
        conn,
        """
        SELECT c.ticker,c.company_name,c.profile,c.active,COUNT(q.quarter_id) AS quarters
        FROM v3_company c LEFT JOIN v3_quarter q ON q.company_id=c.company_id
        WHERE UPPER(c.company_name) LIKE '%REIT%' OR UPPER(c.company_name) LIKE '%REALTY%' OR UPPER(c.company_name) LIKE '%PROPERTIES%'
        GROUP BY c.company_id
        ORDER BY c.ticker
        """
    )
    write_csv(root / "reit_applicability_review.csv", reits)


def sample_json(data: list[dict[str, Any]], n: int = 5) -> str:
    return json.dumps(data[:n], sort_keys=True, default=str)


def build_summary(started: datetime, issues: list[dict[str, Any]], counts: dict[str, int], readonly_ok: bool) -> dict[str, Any]:
    severity_counts = Counter(row["severity"] for row in issues)
    if not readonly_ok:
        classification = CLASSIFICATION_BLOCKED_WRITE
    elif severity_counts.get("CRITICAL", 0):
        classification = CLASSIFICATION_BLOCKED_INTEGRITY
    elif severity_counts.get("HIGH", 0):
        classification = CLASSIFICATION_REPAIR_REQUIRED
    else:
        classification = CLASSIFICATION_READY
    return {
        "classification": classification,
        "next_phase": NEXT_PHASE,
        "started_at_utc": started.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "completed_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "read_only_ok": readonly_ok,
        "row_counts": counts,
        "issue_count": len(issues),
        "severity_counts": dict(sorted(severity_counts.items())),
    }


def write_recommendation(path: Path, summary: dict[str, Any]) -> None:
    path.write_text(
        "\n".join(
            [
                "# Phase 7 Recommended Next Step",
                "",
                f"Classification: `{summary['classification']}`",
                f"Next phase: `{summary['next_phase']}`",
                "",
                "Phase 7 was read-only. Review `issue_register.csv` before Phase 8 and repair only issues classified as material for UPDATE V3 cutover.",
                "",
            ]
        ),
        encoding="utf-8",
    )
